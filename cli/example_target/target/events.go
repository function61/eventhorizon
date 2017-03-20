package target

import (
	"encoding/json"
)

// these are events that are used to communicate changes to the domain model

// CompanyCreated {"id": "c3d2ff02", "ts": "2015-03-14 00:00:00", "name": "Dunder Mifflin Paper"}
type CompanyCreated struct {
	Id   string `json:"id"`
	Ts   string `json:"ts"`
	Name string `json:"name"`
}

func applyCompanyCreated(pa *Target, payload string) error {
	e := CompanyCreated{}

	if err := json.Unmarshal([]byte(payload), &e); err != nil {
		return err
	}

	company := &Company{
		ID:   e.Id,
		Name: e.Name,
	}

	return pa.db.WithTransaction(pa.tx).Save(company)
}

// UserCreated {"id": "66cad10b", "ts": "2001-01-27 00:00:00", "name": "Darryl Philbin", "company": "c3d2ff02"}
type UserCreated struct {
	Id      string `json:"id"`
	Ts      string `json:"ts"`
	Name    string `json:"name"`
	Company string `json:"company"`
}

func applyUserCreated(pa *Target, payload string) error {
	e := UserCreated{}

	if err := json.Unmarshal([]byte(payload), &e); err != nil {
		return err
	}

	user := &User{
		ID:      e.Id,
		Name:    e.Name,
		Company: e.Company,
	}

	return pa.db.WithTransaction(pa.tx).Save(user)
}

// UserNameChanged {"user_id": "66cad10b", "ts": "2016-06-06 06:06:06", "new_name": "Phyllis Vance", "reason": ".."}
type UserNameChanged struct {
	UserId  string `json:"user_id"`
	Ts      string `json:"ts"`
	NewName string `json:"new_name"`
	Reason  string `json:"reason"`
}

func applyUserNameChanged(pa *Target, payload string) error {
	e := UserNameChanged{}

	if err := json.Unmarshal([]byte(payload), &e); err != nil {
		return err
	}

	return pa.db.WithTransaction(pa.tx).Update(&User{
		ID:   e.UserId,
		Name: e.NewName,
	})
}

var eventNameToApplyFn = map[string]func(*Target, string) error{
	"CompanyCreated":  applyCompanyCreated,
	"UserCreated":     applyUserCreated,
	"UserNameChanged": applyUserNameChanged,
}
