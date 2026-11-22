package context

import (
	"github.com/dungnh3/kitchen/log"
)

// Log attribute keys of the request id and the authenticated user id.
// NewContext and WithUserID store them with log.WithAttrs, so every kitchen
// logger writes them on records logged with the request context
// (InfoContext, ErrorContext, ...), whatever logger the code uses.
//
// Other Info fields (client IP, device id) are personal data and are left
// out; log them explicitly where needed.
const (
	LogKeyRequestID = "request_id"
	LogKeyUserID    = "user_id"
)

func logRequestID(id string) log.Attr { return log.String(LogKeyRequestID, id) }
func logUserID(id string) log.Attr    { return log.String(LogKeyUserID, id) }
