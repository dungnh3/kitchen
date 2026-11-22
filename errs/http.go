package errs

import (
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fallbackBody is written if marshaling ever fails.
const fallbackBody = `{"error":{"code":500,"message":"internal error","status":"INTERNAL"}}`

// MarshalHTTP renders st as the AIP-193 HTTP error envelope and returns the
// body together with the HTTP status from HTTPStatusFromStatus:
//
//	{"error": {"code": 404, "message": "...", "status": "NOT_FOUND",
//	  "details": [{"@type": "type.googleapis.com/google.rpc.ErrorInfo", ...}]}}
//
// A nil st renders as INTERNAL. Details of unknown types are skipped.
func MarshalHTTP(st *status.Status) (body []byte, httpStatus int) {
	if st == nil {
		st = ErrInternal.GRPCStatus()
	}
	return marshalHTTP(st, HTTPStatusFromStatus(st))
}

func marshalHTTP(st *status.Status, httpStatus int) ([]byte, int) {
	return renderStatus(st).marshal(httpStatus)
}

// wwwAuthenticate returns the RFC 6750 challenge for st: "Bearer" for a 401,
// with error="invalid_token" when a token was sent but rejected, and
// error="insufficient_scope" (plus the missing scope) for INSUFFICIENT_SCOPE.
func wwwAuthenticate(code codes.Code, info *errdetails.ErrorInfo) string {
	reason := info.GetReason()
	switch {
	case code == codes.Unauthenticated:
		if reason == ErrTokenInvalid.reason || reason == ErrTokenExpired.reason {
			return `Bearer error="invalid_token"`
		}
		return "Bearer"
	case code == codes.PermissionDenied && reason == ErrInsufficientScope.reason:
		v := `Bearer error="insufficient_scope"`
		if s := info.GetMetadata()["missing_scope"]; scopeToken(s) {
			v += `, scope="` + s + `"`
		}
		return v
	}
	return ""
}

// scopeToken reports whether s is a non-empty RFC 6750 scope-token
// (%x21 / %x23-5B / %x5D-7E), safe inside a quoted string.
func scopeToken(s string) bool {
	if s == "" {
		return false
	}
	for i := range len(s) {
		c := s[i]
		if c < 0x21 || c > 0x7e || c == '"' || c == '\\' {
			return false
		}
	}
	return true
}
