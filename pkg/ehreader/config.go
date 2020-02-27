package ehreader

import (
	"errors"
	"fmt"
	"github.com/function61/eventhorizon/pkg/ehclient"
	"github.com/function61/gokit/envvar"
	"strings"
)

type Tenant struct {
	tenantId string
}

func TenantId(id string) Tenant {
	if id == "" {
		panic("empty tenant")
	}

	return Tenant{id}
}

// ("/users")
//   => "/t-314/users"
func (t Tenant) Stream(stream string) string {
	return "/t-" + t.tenantId + stream
}

type TenantCtx struct {
	Tenant
	Client ehclient.ReaderWriter
}

func NewTenantCtx(tenant Tenant, client ehclient.ReaderWriter) *TenantCtx {
	return &TenantCtx{tenant, client}
}

func TenantCtxFrom(getter configGetter) (*TenantCtx, error) {
	tenantCtx, _, err := makeTenantCtxFromEnv(getter)
	if err != nil {
		return nil, err
	}

	return tenantCtx, err
}

type TenantCtxWithSnapshots struct {
	Tenant
	Client        ehclient.ReaderWriter
	SnapshotStore SnapshotStore
}

func NewTenantCtxWithSnapshots(
	tenant Tenant,
	client ehclient.ReaderWriter,
	snapshotStore SnapshotStore,
) *TenantCtxWithSnapshots {
	return &TenantCtxWithSnapshots{tenant, client, snapshotStore}
}

func makeTenantCtxFromEnv(getter configGetter) (*TenantCtx, *Config, error) {
	conf, err := GetConfig(getter)
	if err != nil {
		return nil, nil, err
	}

	return NewTenantCtx(TenantId(conf.tenantId), ClientFromConfig(conf)), conf, nil
}

func TenantCtxWithSnapshotsFrom(
	getter configGetter,
	appContextId string,
) (*TenantCtxWithSnapshots, error) {
	tenantCtx, conf, err := makeTenantCtxFromEnv(getter)
	if err != nil {
		return nil, err
	}

	snapshots, err := NewDynamoDbSnapshotStore(
		conf.SnapshotsDynamoDbOptions(),
		appContextId)
	if err != nil {
		return nil, err
	}

	return NewTenantCtxWithSnapshots(tenantCtx.Tenant, tenantCtx.Client, snapshots), nil
}

func ClientFromConfig(conf *Config) *ehclient.Client {
	return ehclient.New(conf.ClientDynamoDbOptions())
}

type configGetter func() (string, error)

func ConfigFromEnv() (string, error) {
	return envvar.Required("EVENTHORIZON")
}

type Config struct {
	tenantId        string
	accessKeyId     string
	accessKeySecret string
	regionId        string
	env             environment
}

func (c *Config) ClientDynamoDbOptions() ehclient.DynamoDbOptions {
	return c.dynamoDbOptions(c.env.eventsTableName)
}

func (c *Config) SnapshotsDynamoDbOptions() ehclient.DynamoDbOptions {
	return c.dynamoDbOptions(c.env.snapshotsTableName)
}

func (c *Config) dynamoDbOptions(tableName string) ehclient.DynamoDbOptions {
	return ehclient.DynamoDbOptions{
		AccessKeyId:     c.accessKeyId,
		AccessKeySecret: c.accessKeySecret,
		RegionId:        c.regionId,
		TableName:       tableName,
	}
}

func GetConfig(getter configGetter) (*Config, error) {
	conf, err := getter()
	if err != nil {
		return nil, err
	}

	// format:
	//   <"prod" | "dev">:<tenant>:<accessKeyId>:<accessKeySecret>:<regionId>
	parts := strings.Split(conf, ":")
	if len(parts) != 5 {
		return nil, fmt.Errorf(
			"conf in incorrect format, should be 'env:tenant:accessKeyId:accessKeySecret:regionId', got %d part(s)",
			len(parts))
	}

	accessKeyId := parts[2]
	accessKeySecret := parts[3]

	// if creds empty, fetch from another ENV variable
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
		regionId:        parts[4],
		env:             *environment,
	}, nil
}

type environment struct {
	eventsTableName    string
	snapshotsTableName string
}
