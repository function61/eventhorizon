package authmiddleware

import (
	"net/http"
	"strings"

	"github.com/function61/eventhorizon/pkg/legacy/config"
)

// Implements bearer token authentication, looks like:
//   Authorization: Bearer TOKEN_HERE

func Protect(next http.Handler, confCtx *config.Context) http.Handler {
	tokenShouldBe := confCtx.AuthToken()

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		foundToken := ""

		if strings.HasPrefix(auth, "Bearer ") {
			foundToken = auth[len("Bearer "):]
		}

		if foundToken == "" {
			http.Error(w, "Authorization: Bearer <token> expected", http.StatusUnauthorized)
			return
		}

		if foundToken != tokenShouldBe {
			http.Error(w, "Incorrect token", http.StatusUnauthorized)
			return
		}

		// found token matched => pass control to next handler in chain

		next.ServeHTTP(w, r)
	})
}
