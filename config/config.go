package config

import (
	"fmt"
)

const (
	AUTH_TOKEN = "foo"

	WALMANAGER_DATADIR = "/pyramid-data/store-live"

	SEEKABLE_STORE_PATH = "/pyramid-data/store-seekable"

	COMPRESSED_ENCRYPTED_STORE_PATH = "/pyramid-data/store-compressed_and_encrypted"

	BOLTDB_DIR = "/pyramid-data"

	S3_BUCKET = "eventhorizon.fn61.net"

	S3_BUCKET_REGION = "us-east-1"

	PUBSUB_PORT = 9091

	WRITER_HTTP_PORT = 9092

	WAL_SIZE_THRESHOLD = uint64(4 * 1024 * 1024)

	CHUNK_ROTATE_THRESHOLD = 8 * 1024 * 1024
)

// configuration context is used to pass configuration to different components
type Context struct {
}

func NewContext() *Context {
	return &Context{}
}

func (c *Context) GetWriterIp() string {
	return "127.0.0.1"
}

func (c *Context) GetWriterPort() int {
	return WRITER_HTTP_PORT
}

func (c *Context) GetWriterServerAddr() string {
	return fmt.Sprintf("%s:%d", c.GetWriterIp(), WRITER_HTTP_PORT)
}

func (c *Context) GetPubSubServerBindAddr() string {
	// FIXME: currently expecting pub/sub server to be located on the same box
	//        as the writer
	return fmt.Sprintf("0.0.0.0:%d", PUBSUB_PORT)
}

func (c *Context) GetPubSubServerAddr() string {
	// FIXME: currently expecting pub/sub server to be located on the same box
	//        as the writer
	return fmt.Sprintf("%s:%d", c.GetWriterIp(), PUBSUB_PORT)
}
