package target

import (
	"encoding/json"
)

// these are events that are used to communicate changes to the domain model

// CompanyCreated {"id": 1, "ts": "2015-03-14 00:00:00", "name": "Dunder Mifflin Paper"}
type CompanyCreated struct {
	Id   int    `json:"id"`
	Ts   string `json:"ts"`
	Name string `json:"name"`
}

func parseCompanyCreated(payload string) interface{} {
	obj := CompanyCreated{}

	if err := json.Unmarshal([]byte(payload), &obj); err != nil {
		panic(err)
	}

	return &obj
}

// UserCreated {"id": 1, "ts": "2001-01-27 00:00:00", "name": "Darryl Philbin", "company": 1}
type UserCreated struct {
	Id      int    `json:"id"`
	Ts      string `json:"ts"`
	Name    string `json:"name"`
	Company int    `json:"company"`
}

func parseUserCreated(payload string) interface{} {
	obj := UserCreated{}

	if err := json.Unmarshal([]byte(payload), &obj); err != nil {
		panic(err)
	}

	return &obj
}

// UserNameChanged {"user_id": 16, "ts": "2016-06-06 06:06:06", "new_name": "Phyllis Vance", "reason": ".."}
type UserNameChanged struct {
	UserId  int    `json:"user_id"`
	Ts      string `json:"ts"`
	NewName string `json:"new_name"`
	Reason  string `json:"reason"`
}

func parseUserNameChanged(payload string) interface{} {
	obj := UserNameChanged{}

	if err := json.Unmarshal([]byte(payload), &obj); err != nil {
		panic(err)
	}

	return &obj
}

var eventNameToDecoderFn = map[string]func(string) interface{}{
	"CompanyCreated":  parseCompanyCreated,
	"UserCreated":     parseUserCreated,
	"UserNameChanged": parseUserNameChanged,
}
