package config

import (
	"fmt"
	ctypes "github.com/function61/pyramid/config/types"
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
}

func NewContext(discovery *ctypes.DiscoveryFile, scalableStoreUrl *url.URL) *Context {
	return &Context{discovery, scalableStoreUrl}
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
