package target

import (
	"encoding/json"
	"errors"
	"regexp"
)

type User struct {
	ID      int
	Name    string
	Company int
}

func parse(line string) interface{} {
	parseRe := regexp.MustCompile("^([a-zA-Z]+) (\\{.+)$")

	parsed := parseRe.FindStringSubmatch(line)

	if parsed == nil {
		panic(errors.New("Unable to parse line: " + line))
	}

	typ := parsed[1]
	payload := parsed[2]

	// yes, unfortunately we must repeat even the JSON unmarshaling to every case,
	// because if we'd declare "obj" *here* as interface{}, Unmarshal() would not
	// know the concrete type

	mappings := map[string]func(string) interface{}{
		"CompanyCreated":  parseCompanyCreated,
		"UserCreated":     parseUserCreated,
		"UserNameChanged": parseUserNameChanged,
	}

	if fn, ok := mappings[typ]; ok {
		return fn(payload)
	} else {
		return nil
	}
}

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

/*
CompanyCreated {"id": 1, "ts": "2015-03-14 00:00:00", "name": "Dunder Mifflin Paper"}
CompanyCreated {"id": 2, "ts": "2016-10-10 00:00:00", "name": "Michael Scott Paper Company"}
CompanyCreated {"id": 3, "ts": "2017-08-08 00:00:00", "name": "Vance Refrigeration"}
UserCreated {"id": 1, "ts": "2001-01-27 00:00:00", "name": "Darryl Philbin", "company": 1}
UserCreated {"id": 2, "ts": "2002-01-03 00:00:00", "name": "Dwight Schrute", "company": 2}
UserCreated {"id": 3, "ts": "2003-01-09 00:00:00", "name": "Angela Martin", "company": 1}
UserCreated {"id": 4, "ts": "2004-01-16 00:00:00", "name": "Creed Bratton", "company": 1}
UserCreated {"id": 5, "ts": "2005-02-20 00:00:00", "name": "Michael Scott", "company": 2}
UserCreated {"id": 6, "ts": "2006-02-20 00:00:00", "name": "Oscar Martinez", "company": 1}
UserCreated {"id": 7, "ts": "2007-02-21 00:00:00", "name": "Kelly Kapoor", "company": 1}
UserCreated {"id": 8, "ts": "2008-02-24 00:00:00", "name": "Pam Beesly", "company": 1}
UserCreated {"id": 9, "ts": "2009-02-28 00:00:00", "name": "Jim Halpert", "company": 1}
UserCreated {"id": 10, "ts": "2010-03-01 00:00:00", "name": "Andy Bernard", "company": 1}
UserCreated {"id": 11, "ts": "2011-03-02 00:00:00", "name": "Kevin Malone", "company": 1}
UserCreated {"id": 12, "ts": "2012-03-03 00:00:00", "name": "Erin Hannon", "company": 1}
UserCreated {"id": 13, "ts": "2013-03-24 00:00:00", "name": "Stanley Hudson", "company": 1}
UserCreated {"id": 14, "ts": "2014-09-25 00:00:00", "name": "Meredith Palmer", "company": 1}
UserCreated {"id": 15, "ts": "2015-09-26 00:00:00", "name": "Toby Flenderson", "company": 1}
UserCreated {"id": 16, "ts": "2016-09-27 00:00:00", "name": "Phyllis Vance", "company": 3}
UserCreated {"id": 17, "ts": "2017-09-28 00:00:00", "name": "Ryan Howard", "company": 1}
*/
