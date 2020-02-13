package ehreader

import (
	"fmt"
	"github.com/function61/eventhorizon/pkg/ehclient"
	"github.com/function61/gokit/envvar"
	"strings"
)

func TenantConfigFromEnv() (TenantClient, error) {
	conf, err := envvar.Required("EVENTHORIZON_TENANT")
	if err != nil {
		return TenantClient{}, err
	}

	parts := strings.Split(conf, ":")
	if len(parts) != 2 {
		return TenantClient{}, fmt.Errorf(
			"conf in incorrect format, should be 'env:tenant', got %d part(s)",
			len(parts))
	}

	// TODO: move this to ehclient?
	environment, err := func() (ehclient.Environment, error) {
		switch parts[0] {
		case "prod":
			return ehclient.Production(), nil
		case "dev":
			return ehclient.Development(), nil
		default:
			return ehclient.Environment{}, fmt.Errorf("unknown environment: %s", parts[0])
		}
	}()
	if err != nil {
		return TenantClient{}, err
	}

	return TenantId(parts[1]).Client(environment), nil
}
