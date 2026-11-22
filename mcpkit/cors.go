package mcpkit

import (
	"net/http"

	"github.com/rs/cors"
)

// cors answers CORS for an exact-match origin allow-list with the headers the
// MCP Streamable HTTP transport needs, using the same library as kitchen's
// server package. An empty list disables CORS entirely (browsers then block
// cross-origin calls).
func corsHandler(origins []string, next http.Handler) http.Handler {
	if len(origins) == 0 {
		return next
	}
	return cors.New(cors.Options{
		AllowedOrigins: origins,
		AllowedMethods: []string{http.MethodGet, http.MethodPost, http.MethodDelete},
		AllowedHeaders: []string{
			"Authorization", "Content-Type", "Accept",
			"Mcp-Session-Id", "Mcp-Protocol-Version", "Last-Event-ID",
		},
		ExposedHeaders: []string{"Mcp-Session-Id", "Mcp-Protocol-Version", "WWW-Authenticate"},
		MaxAge:         600,
	}).Handler(next)
}
