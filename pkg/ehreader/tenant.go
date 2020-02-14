package ehreader

import (
	"github.com/function61/eventhorizon/pkg/ehclient"
)

// TODO: move these away
const (
	TenantIdFunction61 = "1"
	TenantIdJoonasHome = "2"
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

// creates TenantClient context
func (t Tenant) Client(client ehclient.ReaderWriter) TenantClient {
	return TenantClient{t, client}
}

type TenantClient struct {
	Tenant
	Client ehclient.ReaderWriter
}
