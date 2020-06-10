package ehreader

import (
	"testing"

	"github.com/function61/gokit/assert"
)

func TestGetConfig(t *testing.T) {
	// makes inline configGetter
	inline := func(val string) func() (string, error) {
		return func() (string, error) {
			return val, nil
		}
	}

	conf, err := GetConfig(inline("prod:123:akid:ase:rid"))
	assert.Ok(t, err)

	assert.EqualString(t, conf.tenantId, "123")
	assert.EqualString(t, conf.accessKeyId, "akid")
	assert.EqualString(t, conf.accessKeySecret, "ase")
	assert.EqualString(t, conf.regionId, "rid")
	assert.EqualString(t, conf.env.eventsTableName, "prod_eh_events")
	assert.EqualString(t, conf.env.snapshotsTableName, "prod_eh_snapshots")

	_, err = GetConfig(inline("wrong:123:akid:ase:rid"))
	assert.EqualString(t, err.Error(), "unknown environment: wrong")
}
