package mcpkit

import (
	"net/http"
	"slices"
	"strings"
)

var (
	corsAllowMethods  = "GET, POST, DELETE"
	corsAllowHeaders  = "Authorization, Content-Type, Accept, Mcp-Session-Id, Mcp-Protocol-Version, Last-Event-ID"
	corsExposeHeaders = "Mcp-Session-Id, Mcp-Protocol-Version, WWW-Authenticate"
)

// cors answers CORS for an exact-match origin allow-list. An empty list
// disables CORS entirely (browsers then block cross-origin calls). A
// preflight from an origin that is not allowed gets 403.
func cors(origins []string, next http.Handler) http.Handler {
	if len(origins) == 0 {
		return next
	}
	allowed := make([]string, len(origins))
	for i, o := range origins {
		allowed[i] = strings.TrimSuffix(o, "/")
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin == "" {
			next.ServeHTTP(w, r)
			return
		}
		ok := slices.Contains(allowed, origin)
		preflight := r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != ""
		h := w.Header()
		h.Add("Vary", "Origin")
		if !ok {
			if preflight {
				w.WriteHeader(http.StatusForbidden)
				return
			}
			next.ServeHTTP(w, r)
			return
		}
		h.Set("Access-Control-Allow-Origin", origin)
		h.Set("Access-Control-Expose-Headers", corsExposeHeaders)
		if preflight {
			h.Add("Vary", "Access-Control-Request-Method")
			h.Add("Vary", "Access-Control-Request-Headers")
			h.Set("Access-Control-Allow-Methods", corsAllowMethods)
			h.Set("Access-Control-Allow-Headers", corsAllowHeaders)
			h.Set("Access-Control-Max-Age", "600")
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
