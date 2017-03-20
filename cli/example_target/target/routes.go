package target

import (
	"net/http"
	"encoding/json"
)

func (pa *Target) setupRoutes() {	
	http.Handle("/users", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var users []User
		err := pa.db.From("users").All(&users)
		if err != nil {
			panic(err)
		}

		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "    ")
		if err := encoder.Encode(users); err != nil {
			panic(err)
		}
	}))
}
