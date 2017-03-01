package config

const (
	WALMANAGER_DATADIR = "/eventhorizon/wal"

	LONGTERMSHIPPER_PATH = "/eventhorizon/store/longterm"

	SEEKABLE_STORE_PATH = "/eventhorizon/store/seekable"

	COMPRESSED_ENCRYPTED_STORE_PATH = "/eventhorizon/store/compressed_and_encrypted"

	BOLTDB_DIR = "/eventhorizon"

	S3_BUCKET = "eventhorizon.fn61.net"

	PUBSUB_PORT = 9091

	WAL_SIZE_THRESHOLD = uint64(4 * 1024 * 1024)

	CHUNK_ROTATE_THRESHOLD = 8 * 1024 * 1024
)
