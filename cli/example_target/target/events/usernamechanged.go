package events

import (
	"encoding/json"
	"github.com/function61/pyramid/cli/example_target/target/schema"
	"github.com/function61/pyramid/cli/example_target/target/transaction"
)

// UserNameChanged {"user_id": "66cad10b", "ts": "2016-06-06 06:06:06", "new_name": "Phyllis Vance", "reason": ".."}
type UserNameChanged struct {
	UserId  string `json:"user_id"`
	Ts      string `json:"ts"`
	NewName string `json:"new_name"`
	Reason  string `json:"reason"`
}

func applyUserNameChanged(tx *transaction.Tx, payload string) error {
	e := UserNameChanged{}

	if err := json.Unmarshal([]byte(payload), &e); err != nil {
		return err
	}

	return tx.Db.WithTransaction(tx.Tx).Update(&schema.User{
		ID:   e.UserId,
		Name: e.NewName,
	})
}
