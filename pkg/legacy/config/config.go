package config

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"fmt"
	ctypes "github.com/function61/eventhorizon/pkg/legacy/config/types"
	"github.com/function61/eventhorizon/pkg/legacy/util/sslca"
	"net/url"
)

const (
	WalManagerDataDir = "/eventhorizon-data/store-live"

	SeekableStorePath = "/eventhorizon-data/store-seekable"

	CompressedEncryptedStorePath = "/eventhorizon-data/store-compressed_and_encrypted"

	// using AES-256
	AesKeyLenBytes = 32

	BoltDbDir = "/eventhorizon-data"

	WriterHttpPort = 9092

	WalSizeThreshold = uint64(4 * 1024 * 1024)

	ChunkRotateThreshold = 8 * 1024 * 1024

	pubSubPort = 9091
)

// configuration context is used to pass configuration to different components
type Context struct {
	discovery        *ctypes.DiscoveryFile
	scalableStoreUrl *url.URL

	// on-the-fly signed server cert for this server instance
	serverKeyPair *tls.Certificate
}

func NewContext(discovery *ctypes.DiscoveryFile, scalableStoreUrl *url.URL) *Context {
	return &Context{discovery, scalableStoreUrl, nil}
}

func (c *Context) AuthToken() string {
	return c.discovery.AuthToken
}

func (c *Context) GetWriterIp() string {
	return c.discovery.WriterIp
}

func (c *Context) GetWriterPort() int {
	return WriterHttpPort
}

func (c *Context) GetWriterServerAddr() string {
	return fmt.Sprintf("%s:%d", c.GetWriterIp(), WriterHttpPort)
}

func (c *Context) GetPubSubServerBindAddr() string {
	// FIXME: currently expecting pub/sub server to be located on the same box
	//        as the writer
	return fmt.Sprintf("0.0.0.0:%d", pubSubPort)
}

func (c *Context) GetPubSubServerAddr() string {
	// FIXME: currently expecting pub/sub server to be located on the same box
	//        as the writer
	return fmt.Sprintf("%s:%d", c.GetWriterIp(), pubSubPort)
}

func (c *Context) ScalableStoreUrl() *url.URL {
	return c.scalableStoreUrl
}

// since we control both the servers and clients, it's easy being our own CA when
// we control both the servers and clients. Just configure our sole CA cert as the trust anchor.
func (c *Context) GetCaCertificates() *x509.CertPool {
	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM([]byte(c.discovery.CaCertificate)) {
		panic("failed to append CA certificate")
	}
	return caPool
}

// returns a valid server certificate for this Writer's IP address
func (c *Context) GetSignedServerCertificate() tls.Certificate {
	// cache it, many server components might ask for this
	if c.serverKeyPair == nil {
		// sign server cert on-the-fly for this IP
		cert, privKey := sslca.SignServerCert(
			c.GetWriterIp(),
			c.discovery.CaCertificate,
			c.discovery.CaPrivateKey)

		keyPair, err := tls.X509KeyPair(cert, privKey)
		if err != nil {
			panic(err)
		}

		c.serverKeyPair = &keyPair
	}

	return *c.serverKeyPair
}

// TODO: have separate key per stream, provisioned in ChildStreamCreated? the
// only drawback is that then you cannot directly access streams by knowing
// their name, but rather walk to the stream hierarchy from the root stream to
// the leaf.
// TODO: do not store this (at least in plaintext) in scalablestore
func (c *Context) GetStreamEncryptionKey() []byte {
	encryptionMasterKeyBytes := make([]byte, hex.DecodedLen(len(c.discovery.EncryptionMasterKey)))

	if _, err := hex.Decode(encryptionMasterKeyBytes, []byte(c.discovery.EncryptionMasterKey)); err != nil {
		panic(err)
	}

	if len(encryptionMasterKeyBytes) != AesKeyLenBytes {
		panic("invalid key len")
	}

	return encryptionMasterKeyBytes
}
