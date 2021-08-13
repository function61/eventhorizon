package policy

import (
	"testing"

	"github.com/function61/gokit/testing/assert"
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
                "f61:eventhorizon:/_system*"
            ]
        }
    ]
}
`)

	policy, err := Deserialize(policySerialized)
	assert.Ok(t, err)

	assert.EqualString(t, policy.Statements[0].Resources[0], "f61:eventhorizon:/_system*")
}

func testingPolicy() Policy {
	return NewPolicy(NewAllowStatement([]Action{
		NewAction("eventhorizon:Read"),
	}, eventHorizonRn.Child("/_system*")))
}
