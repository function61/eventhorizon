package ehclient

// Convenience API over Writer to transparently encrypt events (eh.LogDataKindEncryptedData)

import (
	"context"
	"fmt"
	"log"

	"github.com/function61/eventhorizon/pkg/eh"
	"github.com/function61/eventhorizon/pkg/ehevent"
	"github.com/function61/eventhorizon/pkg/eheventencryption"
	"github.com/function61/gokit/log/logex"
	"github.com/function61/gokit/sync/syncutil"
)

func (e *SystemClient) Append(ctx context.Context, stream eh.StreamName, events ...ehevent.Event) error {
	return e.AppendStrings(ctx, stream, ehevent.Serialize(events...))
}

func (e *SystemClient) AppendStrings(ctx context.Context, stream eh.StreamName, eventsSerialized []string) error {
	dek, err := e.LoadDEK(ctx, stream)
	if err != nil {
		return err
	}

	eventsEncrypted, err := eheventencryption.Encrypt(ehevent.SerializeLines(eventsSerialized), dek)
	if err != nil {
		return err
	}

	_, err = e.EventLog.Append(ctx, stream, eh.LogData{
		Kind: eh.LogDataKindEncryptedData,
		Raw:  eventsEncrypted,
	})
	return err
}

func (e *SystemClient) AppendAfter(ctx context.Context, after eh.Cursor, events ...ehevent.Event) error {
	dek, err := e.LoadDEK(ctx, after.Stream())
	if err != nil {
		return err
	}

	eventsSerialized := ehevent.Serialize(events...)
	eventsEncrypted, err := eheventencryption.Encrypt(ehevent.SerializeLines(eventsSerialized), dek)
	if err != nil {
		return err
	}

	_, err = e.EventLog.AppendAfter(ctx, after, eh.LogData{
		Kind: eh.LogDataKindEncryptedData,
		Raw:  eventsEncrypted,
	})
	return err
}

// TODO: maybe make *eh.LogData be returned from a cb, because for encrypted LogData it
//       depends on the generated DEK
func (e *SystemClient) CreateStream(
	ctx context.Context,
	stream eh.StreamName,
	data *eh.LogData,
) (*eh.AppendResult, error) {
	// each stream needs a DEK (whether it will be used or not). we can't let the DB server
	// generate it b/c then the server could theoretically have access to the data. and we
	// prefer the crypto service generate the whole envelope, so not even application
	// servers have theoretically default un-audited access to the data.
	dekEnvelope, err := e.sysConn.DEKEnvelopeForNewStream(ctx, stream)
	if err != nil {
		return nil, fmt.Errorf("DEKEnvelopeForNewStream: %w", err)
	}

	return e.EventLog.CreateStream(ctx, stream, *dekEnvelope, data)
}

// loads DEK (Data Encryption Key) for a given stream (by loading DEK envelope and decrypting it)
func (e *SystemClient) LoadDEK(ctx context.Context, stream eh.StreamName) ([]byte, error) {
	// now that we're holding stream-specific mutex, we can without races read from DEK cache
	// to determine if we have it cached or not, and fetch it to cache if needed (all inside a lock)
	key := stream.String()
	defer e.deksCacheStreamMu.Lock(key)()

	dek := func() []byte {
		// we only have stream-wide lock, so we still need cache-wide lock for short whiles
		// where we do reads and writes
		defer syncutil.LockAndUnlock(&e.deksCacheMu)()

		return e.deksCache[key]
	}()

	if dek == nil {
		var err error
		dek, err = e.loadAndDecryptDEKEnvelope(ctx, stream)
		if err != nil {
			return nil, err
		}

		defer syncutil.LockAndUnlock(&e.deksCacheMu)()

		e.deksCache[key] = dek
	}

	return dek, nil
}

func (s *SystemClient) Logger(prefix string) *log.Logger {
	return logex.Prefix(prefix, s.logger)
}

// result of this will be cached, and this won't be called for same stream concurrently
func (e *SystemClient) loadAndDecryptDEKEnvelope(ctx context.Context, stream eh.StreamName) ([]byte, error) {
	// e.logl.Debug.Printf("resolving DEK for %s", stream.String())

	return e.sysConn.ResolveDEK(ctx, stream)
}
