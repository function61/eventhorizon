package events

import (
	"encoding/json"
	"github.com/function61/pyramid/cli/example_target/target/schema"
	"github.com/function61/pyramid/cli/example_target/target/transaction"
)

// UserCreated {"id": "66cad10b", "ts": "2001-01-27 00:00:00", "name": "Darryl Philbin", "company": "c3d2ff02"}
type UserCreated struct {
	Id      string `json:"id"`
	Ts      string `json:"ts"`
	Name    string `json:"name"`
	Company string `json:"company"`
}

func applyUserCreated(tx *transaction.Tx, payload string) error {
	e := UserCreated{}

	if err := json.Unmarshal([]byte(payload), &e); err != nil {
		return err
	}

	user := &schema.User{
		ID:      e.Id,
		Name:    e.Name,
		Company: e.Company,
	}

	return tx.Db.WithTransaction(tx.Tx).Save(user)
}
