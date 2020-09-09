package ehreader

import (
	"testing"

	"github.com/function61/gokit/testing/assert"
)

func TestGetConfig(t *testing.T) {
	conf, err := getConfig(inline("prod:123:akid:ase:rid"))
	assert.Ok(t, err)

	assert.EqualString(t, conf.tenantId, "123")
	assert.EqualString(t, conf.accessKeyId, "akid")
	assert.EqualString(t, conf.accessKeySecret, "ase")
	assert.EqualString(t, conf.regionId, "rid")
	assert.EqualString(t, conf.env.eventsTableName, "prod_eh_events")
	assert.EqualString(t, conf.env.snapshotsTableName, "prod_eh_snapshots")

	_, err = getConfig(inline("wrong:123:akid:ase:rid"))
	assert.EqualString(t, err.Error(), "unknown environment: wrong")
}

func TestGetConfigHttp(t *testing.T) {
	conf, err := getConfig(inline("https://:bearertoken@example.com/eventhorizon"))
	assert.Ok(t, err)

	assert.EqualString(t, conf.url, "https://:bearertoken@example.com/eventhorizon")
}

// makes inline ConfigStringGetter
func inline(val string) ConfigStringGetter {
	return func() (string, error) {
		return val, nil
	}
}
