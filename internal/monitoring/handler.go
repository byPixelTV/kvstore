package monitoring

import (
	"crypto/subtle"
	"net/http"
)

// Protect gives scrapers access independently of cluster and routing admin.
func Protect(token string, handler http.Handler) http.HandlerFunc {
	authorization := []byte("Bearer " + token)
	return func(w http.ResponseWriter, r *http.Request) {
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), authorization) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		handler.ServeHTTP(w, r)
	}
}
