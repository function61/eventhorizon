package target

// these are the structs used by Storm to read / write into the Bolt database

type User struct {
	ID      int
	Name    string
	Company int
}

type Company struct {
	ID      int
	Name    string
}
