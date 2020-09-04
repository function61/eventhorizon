package ehreader

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/function61/eventhorizon/pkg/cryptosvc"
	"github.com/function61/eventhorizon/pkg/eh"
	"github.com/function61/eventhorizon/pkg/ehserver/ehdynamodb"
	"github.com/function61/eventhorizon/pkg/ehserver/ehserverclient"
	"github.com/function61/eventhorizon/pkg/envelopeenc"
	"github.com/function61/gokit/envvar"
	"github.com/function61/gokit/syncutil"
)

type DekEnvelopeResolver func(context.Context, eh.StreamName) (*envelopeenc.Envelope, error)

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
	SystemClient
}

// same as Client, but *WITHOUT* tenant awareness, usually this is not used directly,
// as most data access cases are expected to be tenant-aware
type SystemClient struct {
	EventLog      eh.ReaderWriter
	SnapshotStore eh.SnapshotStore

	resolveDekEnvelope DekEnvelopeResolver
	cryptoSvc          *cryptosvc.Service
	deksCache          map[string][]byte
	deksCacheMu        *syncutil.MutexMap
}

func ClientFrom(getter ConfigStringGetter, resolveDekEnvelope DekEnvelopeResolver) (*Client, error) {
	systemClient, conf, err := makeSystemClientFrom(getter, resolveDekEnvelope)
	if err != nil {
		return nil, err
	}

	return &Client{
		Tenant:       TenantId(conf.tenantId),
		SystemClient: *systemClient,
	}, nil
}

func SystemClientFrom(getter ConfigStringGetter, resolveDekEnvelope DekEnvelopeResolver) (*SystemClient, error) {
	systemClient, _, err := makeSystemClientFrom(getter, resolveDekEnvelope)
	return systemClient, err
}

func makeSystemClientFrom(getter ConfigStringGetter, resolveDekEnvelope DekEnvelopeResolver) (*SystemClient, *Config, error) {
	conf, err := getConfig(getter)
	if err != nil {
		return nil, nil, err
	}

	if conf.url != "" {
		serverClient, err := ehserverclient.New(conf.url)
		if err != nil {
			return nil, nil, err
		}

		return &SystemClient{
			EventLog:           serverClient,
			SnapshotStore:      serverClient,
			resolveDekEnvelope: resolveDekEnvelope,
			cryptoSvc:          cryptosvc.New(nil),
			deksCache:          map[string][]byte{},
			deksCacheMu:        syncutil.NewMutexMap(),
		}, conf, nil
	}

	snapshots, err := NewDynamoDbSnapshotStore(
		conf.SnapshotsDynamoDbOptions())
	if err != nil {
		return nil, nil, err
	}

	eventLog := ehdynamodb.New(conf.ClientDynamoDbOptions())

	return &SystemClient{
		EventLog:           eventLog,
		SnapshotStore:      snapshots,
		resolveDekEnvelope: resolveDekEnvelope,
		cryptoSvc:          cryptosvc.New(nil),
		deksCache:          map[string][]byte{},
		deksCacheMu:        syncutil.NewMutexMap(),
	}, conf, nil
}

type ConfigStringGetter func() (string, error)

func ConfigFromEnv() (string, error) {
	return envvar.Required("EVENTHORIZON")
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

		accessKeyId, err = envvar.Required("AWS_ACCESS_KEY_ID")
		if err != nil {
			return nil, err
		}

		accessKeySecret, err = envvar.Required("AWS_SECRET_ACCESS_KEY")
		if err != nil {
			return nil, err
		}

		accessKeyToken = os.Getenv("AWS_SESSION_TOKEN")
	}

	environment, err := func() (*environment, error) {
		switch parts[0] {
		case "prod":
			return &environment{"prod_eh_events", "prod_eh_snapshots"}, nil
		case "dev":
			return &environment{"dev_eh_events", "dev_eh_snapshots"}, nil
		default:
			return nil, fmt.Errorf("unknown environment: %s", parts[0])
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
