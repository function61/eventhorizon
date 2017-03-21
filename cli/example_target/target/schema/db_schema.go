package schema

// these are the structs used by Storm ORM to read / write into the Bolt database.
// normally, I wouldn't recommend using an ORM but for this example application
// it suits well to illustrate only how Pyramid works.

type User struct {
	ID      string
	Name    string
	Company string
}

type Company struct {
	ID   string
	Name string
}
