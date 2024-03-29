package ehclient

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/function61/eventhorizon/pkg/eh"
	"github.com/function61/eventhorizon/pkg/ehserver/ehdynamodb"
	"github.com/function61/eventhorizon/pkg/ehserver/ehserverclient"
	"github.com/function61/gokit/crypto/envelopeenc"
	"github.com/function61/gokit/log/logex"
	"github.com/function61/gokit/os/osutil"
	"github.com/function61/gokit/sync/syncutil"
)

// things we need from outside of client package to make the system as a whole work. this
// is basically to circumvent circular dependencies (using client required system stores
// which depend on Reader interface). this is also the reason for "ehclientfactory" package
// TODO: extract the interfaces the stores depend on, so we don't need this?
type SystemConnector interface {
	DEKv0EnvelopeForNewStream(context.Context, eh.StreamName) (*envelopeenc.EnvelopeBundle, error)
	ResolveDEK(context.Context, eh.StreamName) ([]byte, error)
}

type Tenant struct {
	stream eh.StreamName
}

func TenantId(id string) Tenant {
	if id == "" {
		panic("empty tenant")
	}

	return Tenant{eh.RootName.Child("t-" + id)}
}

// return tenant's stream's (e.g. "/t-314") child-stream
// ChildStream("foobar") => "/t-314/foobar"
func (t Tenant) ChildStream(name string) eh.StreamName {
	return t.stream.Child(name)
}

// combines together:
// - event log
// - snapshot store
// - tenant
type Client struct {
	Tenant
	*SystemClient
}

// same as Client, but *WITHOUT* tenant awareness, usually this is not used directly,
// as most data access cases are expected to be tenant-aware
type SystemClient struct {
	EventLog      eh.ReaderWriter
	SnapshotStore eh.SnapshotStore

	conf              Config
	logger            *log.Logger
	sysConn           SystemConnector
	deksCache         map[string][]byte
	deksCacheMu       sync.Mutex
	deksCacheStreamMu *syncutil.MutexMap
}

func ClientFrom(
	getter ConfigStringGetter,
	logger *log.Logger,
	sysConn SystemConnector,
) (*Client, error) {
	systemClient, err := SystemClientFrom(getter, logger, sysConn)
	if err != nil {
		return nil, err
	}

	return &Client{
		Tenant:       TenantId(systemClient.conf.tenantId),
		SystemClient: systemClient,
	}, nil
}

func SystemClientFrom(
	getter ConfigStringGetter,
	logger *log.Logger,
	sysConn SystemConnector,
) (*SystemClient, error) {
	conf, err := getConfig(getter)
	if err != nil {
		return nil, err
	}

	if conf.url != "" {
		// implements EventLog, SnapshotStore
		serverClient, err := ehserverclient.New(conf.url, logex.Prefix("network", logger))
		if err != nil {
			return nil, err
		}

		return &SystemClient{
			EventLog:          serverClient,
			SnapshotStore:     serverClient,
			conf:              *conf,
			logger:            logger,
			sysConn:           sysConn,
			deksCache:         map[string][]byte{},
			deksCacheStreamMu: syncutil.NewMutexMap(),
		}, nil
	} else {
		snapshots, err := NewDynamoDbSnapshotStore(
			conf.SnapshotsDynamoDbOptions())
		if err != nil {
			return nil, err
		}

		eventLog := ehdynamodb.New(conf.ClientDynamoDbOptions())

		return &SystemClient{
			EventLog:          eventLog,
			SnapshotStore:     snapshots,
			conf:              *conf,
			logger:            logger,
			sysConn:           sysConn,
			deksCache:         map[string][]byte{},
			deksCacheStreamMu: syncutil.NewMutexMap(),
		}, nil
	}
}

// returns empty if running at server side
func (s *SystemClient) GetServerUrl() string {
	return s.conf.url
}

type ConfigStringGetter func() (string, error)

// reads the config string from an environment variable
func ConfigFromENV() (string, error) {
	return osutil.GetenvRequired("EVENTHORIZON")
}

type Config struct {
	tenantId        string
	accessKeyId     string // AWS_ACCESS_KEY_ID
	accessKeySecret string // AWS_SECRET_ACCESS_KEY
	accessKeyToken  string // AWS_SESSION_TOKEN (only needed in Lambda)
	regionId        string
	env             environment
	url             string
}

func (c *Config) ClientDynamoDbOptions() ehdynamodb.DynamoDbOptions {
	return c.dynamoDbOptions(c.env.eventsTableName)
}

func (c *Config) SnapshotsDynamoDbOptions() ehdynamodb.DynamoDbOptions {
	return c.dynamoDbOptions(c.env.snapshotsTableName)
}

func (c *Config) dynamoDbOptions(tableName string) ehdynamodb.DynamoDbOptions {
	return ehdynamodb.DynamoDbOptions{
		AccessKeyId:     c.accessKeyId,
		AccessKeySecret: c.accessKeySecret,
		AccessKeyToken:  c.accessKeyToken,
		RegionId:        c.regionId,
		TableName:       tableName,
	}
}

func getConfig(getter ConfigStringGetter) (*Config, error) {
	confString, err := getter()
	if err != nil {
		return nil, err
	}

	// works for "http://" | "https://"
	if strings.HasPrefix(confString, "http") {
		return &Config{
			url: confString,
		}, nil
	}

	// format:
	//   <"prod" | "dev">:<tenant>:<accessKeyId>:<accessKeySecret>:<regionId>
	parts := strings.Split(confString, ":")
	if len(parts) != 5 {
		return nil, fmt.Errorf(
			"confString in incorrect format, should be 'env:tenant:accessKeyId:accessKeySecret:regionId', got %d part(s)",
			len(parts))
	}

	namespace := parts[0]
	accessKeyId := parts[2]
	accessKeySecret := parts[3]
	accessKeyToken := ""

	// if creds empty, fetch from another ENV variable.
	// https://docs.aws.amazon.com/lambda/latest/dg/configuration-envvars.html#configuration-envvars-runtime
	if accessKeyId == "" || accessKeySecret == "" {
		if accessKeyId != "" || accessKeySecret != "" {
			return nil, errors.New(
				"both accessKeyId and accessKeySecret must be empty or both must be set")
		}

		accessKeyId, err = osutil.GetenvRequired("AWS_ACCESS_KEY_ID")
		if err != nil {
			return nil, err
		}

		accessKeySecret, err = osutil.GetenvRequired("AWS_SECRET_ACCESS_KEY")
		if err != nil {
			return nil, err
		}

		accessKeyToken = os.Getenv("AWS_SESSION_TOKEN")
	}

	environment, err := func() (*environment, error) {
		switch namespace {
		case "prod":
			return &environment{"prod_eh_events", "prod_eh_snapshots"}, nil
		case "dev":
			return &environment{"dev_eh_events", "dev_eh_snapshots"}, nil
		default:
			return nil, fmt.Errorf("unknown environment: %s", namespace)
		}
	}()
	if err != nil {
		return nil, err
	}

	return &Config{
		tenantId:        parts[1],
		accessKeyId:     accessKeyId,
		accessKeySecret: accessKeySecret,
		accessKeyToken:  accessKeyToken,
		regionId:        parts[4],
		env:             *environment,
	}, nil
}

type environment struct {
	eventsTableName    string
	snapshotsTableName string
}
