package policy

import (
	"testing"

	"github.com/function61/gokit/assert"
)

func TestSerializeDeserialize(t *testing.T) {
	policySerialized := Serialize(testingPolicy())

	assert.EqualString(t, string(policySerialized), `{
    "statements": [
        {
            "effect": "Allow",
            "actions": [
                "eventhorizon:Read"
            ],
            "resources": [
                "f61rn:eventhorizon:/_system*"
            ]
        }
    ]
}
`)

	policy, err := Deserialize(policySerialized)
	assert.Ok(t, err)

	assert.EqualString(t, policy.Statements[0].Resources[0], "f61rn:eventhorizon:/_system*")
}

func testingPolicy() Policy {
	return NewPolicy(NewAllowStatement([]string{
		"eventhorizon:Read",
	}, "f61rn:eventhorizon:/_system*"))
}
