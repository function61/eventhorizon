// Factory for building client instances
package ehclientfactory

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"net/url"
	"sync"

	"github.com/function61/eventhorizon/pkg/eh"
	"github.com/function61/eventhorizon/pkg/ehclient"
	"github.com/function61/eventhorizon/pkg/keyserver"
	"github.com/function61/eventhorizon/pkg/system/ehsettings"
	"github.com/function61/eventhorizon/pkg/system/ehsettingsdomain"
	"github.com/function61/eventhorizon/pkg/system/ehstreammeta"
	"github.com/function61/gokit/crypto/cryptoutil"
	"github.com/function61/gokit/crypto/envelopeenc"
	"github.com/function61/gokit/log/logex"
	"github.com/function61/gokit/os/osutil"
	"github.com/function61/gokit/sync/syncutil"
)

// TODO: implement ClientFrom

func SystemClientFrom(
	getter ehclient.ConfigStringGetter,
	logger *log.Logger,
) (*ehclient.SystemClient, error) {
	sysConn := &sysConnection{
		keyServers: map[string]keyserver.Unsealer{},
		logger:     logger,
	}

	sysClient, err := ehclient.SystemClientFrom(getter, logger, sysConn)
	if err != nil {
		return nil, err
	}

	sysConn.sysClient = sysClient

	return sysClient, err
}

type sysConnection struct {
	sysClient    *ehclient.SystemClient
	settings     *ehsettings.App
	settingsMu   sync.Mutex
	keyServers   map[string]keyserver.Unsealer
	keyServersMu sync.Mutex
	logger       *log.Logger
}

func (d *sysConnection) ResolveDEK(ctx context.Context, stream eh.StreamName) ([]byte, error) {
	dekEnvelope, err := d.resolveDEKEnvelope(ctx, stream)
	if err != nil {
		return nil, err
	}

	keyServer, err := d.resolveKeyServer(ctx, *dekEnvelope, stream)
	if err != nil {
		return nil, err
	}

	return keyServer.UnsealEnvelope(ctx, *dekEnvelope)
}

// we're creating a new stream and it needs an encryption key (DEK).
// generate DEK and put it in an envelope.
// we'll need to use a KeyGroup to known which KEKs should be the envelope recipients
// (KEKs control access to the stream's data via controlling access to the DEK).
func (d *sysConnection) DEKv0EnvelopeForNewStream(
	ctx context.Context,
	stream eh.StreamName,
) (*envelopeenc.Envelope, error) {
	parent := stream.Parent()
	if parent == nil {
		return nil, errors.New("DekEnvelopeForStream: not supported for root stream")
	}

	parentEnvelope, err := d.resolveDekEnvelope(ctx, *parent)
	if err != nil {
		return nil, err
	}

	settings, err := d.getSettings(ctx)
	if err != nil {
		return nil, err
	}

	slotEncrypters := []envelopeenc.SlotEncrypter{}

	for _, slot := range parentEnvelope.KeySlots {
		if slot.Kind != envelopeenc.SlotKindRsaOaepSha256 {
			return nil, fmt.Errorf("DekEnvelopeForStream: parent has unsupported slot kind: %s", slot.Kind)
		}

		kek := settings.Kek(slot.KekId)
		if kek == nil {
			return nil, fmt.Errorf("DekEnvelopeForStream: KEK '%s' not found", slot.KekId)
		}

		pubKey, err := cryptoutil.ParsePemPkcs1EncodedRsaPublicKey([]byte(kek.PublicKey))
		if err != nil {
			return nil, fmt.Errorf("DekEnvelopeForStream: public key parsing: %w", err)
		}

		slotEncrypters = append(slotEncrypters, envelopeenc.RsaOaepSha256Encrypter(pubKey))
	}

	dek, err := keyserver.NewDEK()
	if err != nil {
		return nil, fmt.Errorf("DekEnvelopeForStream: %w", err)
	}

	// internally asserts for len(recipients) > 0
	return envelopeenc.EncryptDEK(stream.DEKResourceName(0).String(), dek, recipients...)
}

func (d *sysConnection) resolveDEKEnvelope(
	ctx context.Context,
	stream eh.StreamName,
) (*envelopeenc.Envelope, error) {
	// this is cached per-stream
	streamMeta, err := ehstreammeta.LoadUntilRealtime(
		ctx,
		stream,
		d.sysClient, // shouldn't recurse b/c ehstreammeta only reads unencrypted data
		ehstreammeta.GlobalCache)
	if err != nil {
		return nil, err
	}

	dekEnvelope := streamMeta.State.Data().DEK
	if dekEnvelope == nil {
		return nil, fmt.Errorf("no DEK envelope for %s", stream.String())
	}

	return dekEnvelope, nil
}

func (d *sysConnection) resolveKeyServer(
	ctx context.Context,
	dekEnvelope envelopeenc.Envelope,
	stream eh.StreamName,
) (keyserver.Unsealer, error) {
	// has special key for bootstrap purposes which we can decrypt without actual key server
	if stream.Equal(eh.SysSettings) {
		cwk, err := osutil.GetenvRequired("EVENTHORIZON_BOOTSTRAP_KEY")
		if err != nil {
			return nil, err
		}

		return newClusterWideKeyServer(cwk)
	}

	settings, err := d.getSettings(ctx)
	if err != nil {
		return nil, err
	}

	keyServer, err := func() (*ehsettings.KeyServer, error) {
		for _, slot := range dekEnvelope.KeySlots {
			keyServer := settings.KeyServerWithKekAttached(slot.KekId)
			if keyServer != nil {
				return keyServer, nil
			}
		}

		return nil, fmt.Errorf("no KeyServer found for %s", stream.String())
	}()
	if err != nil {
		return nil, err
	}

	return d.unsealerForKeyServer(*keyServer)
}

// cache wrapper for the actual factory
func (d *sysConnection) unsealerForKeyServer(keyServer ehsettings.KeyServer) (keyserver.Unsealer, error) {
	defer syncutil.LockAndUnlock(&d.keyServersMu)()

	unsealer, found := d.keyServers[keyServer.Id]
	if found {
		return unsealer, nil
	}

	unsealer, err := d.unsealerForKeyServerUncached(keyServer)
	if err == nil { // only cache successes
		d.keyServers[keyServer.Id] = unsealer
	}

	return unsealer, err
}

func (d *sysConnection) unsealerForKeyServerUncached(keyServer ehsettings.KeyServer) (keyserver.Unsealer, error) {
	if keyServer.Id != "internal" {
		return nil, fmt.Errorf("keyServer not internal: %s", keyServer.Id)
	}

	serverUrlRaw := d.sysClient.GetServerUrl()

	if serverUrlRaw != "" { // client perspective
		serverUrl, authToken, err := splitAuthTokenFromUrl(serverUrlRaw)
		if err != nil {
			return nil, err
		}

		return keyserver.NewClient(serverUrl, authToken, logex.Prefix("network", d.logger)), nil
	} else { // server perspective
		return keyserver.NewServer("default.key", logex.Prefix("keyserver-internaluse", d.logger))
	}
}

// load cluster settings (to see which KEKs are attached to which key servers and where they reside),
// but do it only once by caching
func (d *sysConnection) getSettings(ctx context.Context) (*ehsettings.Store, error) {
	defer syncutil.LockAndUnlock(&d.settingsMu)()

	if d.settings == nil {
		settings, err := ehsettings.LoadUntilRealtime(ctx, d.sysClient)
		if err != nil {
			return nil, fmt.Errorf("ehsettings: %w", err)
		}

		d.settings = settings
	}

	return d.settings.State, nil
}

func to32(in []byte) [32]byte {
	if len(in) != 32 {
		panic("in not 32 bytes")
	}
	var out [32]byte
	copy(out[:], in)
	return out
}

func newClusterWideKeyServer(clusterWideKeyBase64 string) (keyserver.Unsealer, error) {
	clusterWideKey, err := base64.RawURLEncoding.DecodeString(clusterWideKeyBase64)
	if err != nil {
		return nil, err
	}

	clusterWideKeyDecrypter := envelopeenc.NaclSecretBoxEncrypter(
		to32(clusterWideKey),
		ehsettingsdomain.ClusterWideKeyId)

	return &clusterWideKeyServer{clusterWideKeyDecrypter}, nil
}

// acts as in-memory key server with access to the cluster wide key (only used for
// decrypting the bootstrap settings that doesn't have very sensitive data)
type clusterWideKeyServer struct {
	clusterWideKey envelopeenc.SlotDecrypter
}

var _ keyserver.Unsealer = (*clusterWideKeyServer)(nil)

func (s *clusterWideKeyServer) UnsealEnvelope(ctx context.Context, envelope envelopeenc.Envelope) ([]byte, error) {
	return envelope.Decrypt(s.clusterWideKey)
}

// TODO: copied from ehserverclient
func splitAuthTokenFromUrl(rawUrl string) (string, string, error) {
	urlParsed, err := url.Parse(rawUrl)
	if err != nil {
		return "", "", err
	}

	if urlParsed.User == nil {
		return "", "", errors.New("auth token not set")
	}

	if urlParsed.User.Username() != "" {
		return "", "", fmt.Errorf("specifying username is not supported: %s", rawUrl)
	}

	authToken, _ := urlParsed.User.Password()
	if authToken == "" {
		return "", "", errors.New("auth token not set")
	}

	// remove user info from URL
	urlParsed.User = nil

	return urlParsed.String(), authToken, nil
}
