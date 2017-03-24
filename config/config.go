package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	ctypes "github.com/function61/pyramid/config/types"
	"github.com/function61/pyramid/util/sslca"
	"net/url"
)

const (
	WalManagerDataDir = "/pyramid-data/store-live"

	SeekableStorePath = "/pyramid-data/store-seekable"

	CompressedEncryptedStorePath = "/pyramid-data/store-compressed_and_encrypted"

	BoltDbDir = "/pyramid-data"

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

// since we control both the servers and clients, it's easy being our own CA.
// Just configure our sole CA cert as the sole trust anchor.
func (c *Context) GetCaCertificates() *x509.CertPool {
	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM([]byte(c.discovery.CaCertificate)) {
		panic("failed to append CA certificate")
	}
	return caPool
}

// signs a server cert on-the-fly for our IP address
func (c *Context) GetSignedServerCertificate() tls.Certificate {
	// cache it, so if many server components ask for this it is done only once
	if c.serverKeyPair == nil {
		// sign server cert on-the-fly for this IP. it's easy being a CA when we
		// control the configuration of both the servers and clients
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
