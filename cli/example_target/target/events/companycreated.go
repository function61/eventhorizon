package events

import (
	"encoding/json"
	"github.com/function61/pyramid/cli/example_target/target/schema"
	"github.com/function61/pyramid/cli/example_target/target/transaction"
)

// CompanyCreated {"id": "c3d2ff02", "ts": "2015-03-14 00:00:00", "name": "Dunder Mifflin Paper"}
type CompanyCreated struct {
	Id   string `json:"id"`
	Ts   string `json:"ts"`
	Name string `json:"name"`
}

func applyCompanyCreated(tx *transaction.Tx, payload string) error {
	e := CompanyCreated{}

	if err := json.Unmarshal([]byte(payload), &e); err != nil {
		return err
	}

	company := &schema.Company{
		ID:   e.Id,
		Name: e.Name,
	}

	return tx.Db.WithTransaction(tx.Tx).Save(company)
}
