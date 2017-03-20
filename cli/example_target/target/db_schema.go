package target

// these are the structs used by Storm to read / write into the Bolt database

type User struct {
	ID      string
	Name    string
	Company string
}

type Company struct {
	ID   string
	Name string
}
