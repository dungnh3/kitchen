package context

import (
	"net/textproto"
	"strings"
)

// HTTP header names. The gRPC metadata keys are the same names in lower
// case.
const (
	HeaderRequestID      = "X-Request-Id"
	HeaderUserID         = "User-Id"
	HeaderForwardedFor   = "X-Forwarded-For"
	HeaderUserAgent      = "User-Agent"
	HeaderAcceptLanguage = "Accept-Language"
	HeaderDeviceID       = "X-Device-Id"
	HeaderPlatform       = "X-Platform"
	HeaderAppVersion     = "X-App-Version"
	HeaderTimezone       = "X-Timezone"
	HeaderLocale         = "X-Locale"
	// HeaderClientIP carries the client IP resolved by a trusted hop (the
	// in-process REST gateway or a trusted upstream service). It is dropped
	// when sent by an untrusted caller.
	HeaderClientIP = "X-Kitchen-Client-Ip"
)

// Metadata keys grpc-gateway uses for the HTTP client's headers that gRPC
// reserves.
const (
	mdGatewayUserAgent      = "grpcgateway-user-agent"
	mdGatewayAcceptLanguage = "grpcgateway-accept-language"
)

// IdentityHeaders returns the headers that assert an identity. A server
// strips them from untrusted callers, and they are never captured in
// Info.Headers.
func IdentityHeaders() []string {
	return []string{HeaderUserID, HeaderClientIP}
}

// DefaultForwardHeaders returns the HTTP headers the REST gateway forwards to
// gRPC metadata and that Info captures, before any the application adds.
// User-Agent and Accept-Language are forwarded by grpc-gateway itself.
func DefaultForwardHeaders() []string {
	return []string{
		HeaderRequestID, HeaderUserID, HeaderDeviceID, HeaderPlatform,
		HeaderAppVersion, HeaderTimezone, HeaderLocale,
	}
}

// isSecret reports whether a header carries a credential; such headers are
// never captured nor returned by Header.
func isSecret(canonical string) bool {
	switch canonical {
	case "Authorization", "Proxy-Authorization", "Cookie", "Set-Cookie":
		return true
	}
	return false
}

// canonicalClientIP is HeaderClientIP in canonical form ("X-Kitchen-Client-Ip").
var canonicalClientIP = textproto.CanonicalMIMEHeaderKey(HeaderClientIP)

func isIdentity(canonical string) bool {
	return canonical == HeaderUserID || canonical == canonicalClientIP
}

// headerKey is a header name in canonical (HTTP) and lower-case (gRPC
// metadata) form, computed once.
type headerKey struct{ canon, lower string }

func keyOf(name string) headerKey {
	c := textproto.CanonicalMIMEHeaderKey(name)
	return headerKey{canon: c, lower: strings.ToLower(c)}
}

// captured is DefaultForwardHeaders without identity headers and the
// request id (Info.RequestID holds the validated one), precomputed.
var captured = func() []headerKey {
	var ks []headerKey
	for _, h := range DefaultForwardHeaders() {
		k := keyOf(h)
		if !isIdentity(k.canon) && !isSecret(k.canon) && k.canon != HeaderRequestID {
			ks = append(ks, k)
		}
	}
	return ks
}()

var (
	keyRequestID      = keyOf(HeaderRequestID)
	keyAppVersion     = keyOf(HeaderAppVersion)
	keyDeviceID       = keyOf(HeaderDeviceID)
	keyTimezone       = keyOf(HeaderTimezone)
	keyPlatform       = keyOf(HeaderPlatform)
	keyUserAgent      = keyOf(HeaderUserAgent)
	keyAcceptLanguage = keyOf(HeaderAcceptLanguage)
	keyGatewayUA      = headerKey{canon: "Grpcgateway-User-Agent", lower: mdGatewayUserAgent}
	keyGatewayAL      = headerKey{canon: "Grpcgateway-Accept-Language", lower: mdGatewayAcceptLanguage}
)

func lower(s string) string { return strings.ToLower(s) }
