package config

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
