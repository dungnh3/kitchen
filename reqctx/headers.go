package reqctx

import "strings"

// HTTP header names. gRPC metadata keys are the same names in lower case.
const (
	HeaderRequestID       = "X-Request-Id"
	HeaderUserID          = "User-Id"
	HeaderXUserID         = "X-User-Id"
	HeaderAnonymousUserID = "Anonymous-User-Id"
	HeaderDeviceID        = "X-Device-Id"
	HeaderDeviceType      = "X-Device-Type"
	HeaderPlatform        = "X-Platform"
	HeaderOSVersion       = "X-OS-Version"
	HeaderAppVersion      = "X-App-Version"
	HeaderApplicationID   = "X-Application-Id"
	HeaderLocalization    = "X-Localization"
	HeaderLocale          = "X-Locale"
	HeaderTimezone        = "X-Timezone"
	HeaderLatitude        = "X-Latitude"
	HeaderLongitude       = "X-Longitude"
	HeaderCountry         = "X-Country"
	HeaderCFCountry       = "CF-IPCountry"
	HeaderCFConnectingIP  = "CF-Connecting-IP"
	HeaderForwardedFor    = "X-Forwarded-For"
	HeaderUserAgent       = "User-Agent"
	// HeaderGatewayUserAgent is the metadata key grpc-gateway uses for the
	// HTTP client's User-Agent.
	HeaderGatewayUserAgent = "Grpcgateway-User-Agent"
	// HeaderClientIP carries the client IP resolved by a trusted hop (the
	// in-process REST gateway or a trusted upstream service). It is dropped
	// when sent by an untrusted caller.
	HeaderClientIP = "X-Kitchen-Client-Ip"

	HeaderAuthorization    = "Authorization"
	HeaderAPIKey           = "X-Api-Key"
	HeaderVendorKey        = "X-Vendor-Key"
	HeaderRefreshToken     = "Refresh-Token"
	HeaderFirebaseAppCheck = "X-Firebase-AppCheck"
	HeaderCSRFToken        = "X-CSRF-Token"
	HeaderNonce            = "X-Nonce"
)

// IdentityHeaders returns the headers that assert an authenticated principal.
// A server strips them from untrusted callers.
func IdentityHeaders() []string {
	return []string{HeaderUserID, HeaderXUserID, HeaderClientIP}
}

// ForwardHeaders returns the HTTP headers the REST gateway forwards to gRPC
// metadata by default (under their lower-case names). Authorization is
// always forwarded by grpc-gateway itself.
func ForwardHeaders() []string {
	return []string{
		HeaderRequestID, HeaderUserID, HeaderXUserID, HeaderAnonymousUserID,
		HeaderDeviceID, HeaderDeviceType, HeaderPlatform, HeaderOSVersion,
		HeaderAppVersion, HeaderApplicationID, HeaderLocalization, HeaderLocale,
		HeaderTimezone, HeaderLatitude, HeaderLongitude, HeaderCountry,
		HeaderCFCountry, HeaderCFConnectingIP, HeaderAPIKey, HeaderVendorKey,
		HeaderRefreshToken, HeaderFirebaseAppCheck, HeaderCSRFToken, HeaderNonce,
	}
}

// propagated lists the metadata keys AppendToOutgoing sends, as functions of
// the stored Info.
var propagated = []struct {
	key string
	get func(*Info) string
}{
	{key: lower(HeaderRequestID), get: func(i *Info) string { return i.RequestID }},
	{key: lower(HeaderAnonymousUserID), get: func(i *Info) string { return i.AnonymousUserID }},
	{key: lower(HeaderDeviceID), get: func(i *Info) string { return i.DeviceID }},
	{key: lower(HeaderDeviceType), get: func(i *Info) string { return i.DeviceType.headerValue() }},
	{key: lower(HeaderPlatform), get: func(i *Info) string { return string(i.Platform) }},
	{key: lower(HeaderOSVersion), get: func(i *Info) string { return i.OSVersion }},
	{key: lower(HeaderAppVersion), get: func(i *Info) string { return i.AppVersion }},
	{key: lower(HeaderApplicationID), get: func(i *Info) string { return i.ApplicationID }},
	{key: lower(HeaderLocalization), get: func(i *Info) string { return i.Localization }},
	{key: lower(HeaderLocale), get: func(i *Info) string { return i.Locale }},
	{key: lower(HeaderTimezone), get: func(i *Info) string { return i.Timezone }},
	{key: lower(HeaderCountry), get: func(i *Info) string { return i.Country }},
	{key: lower(HeaderClientIP), get: func(i *Info) string {
		if i.ClientIP.IsValid() {
			return i.ClientIP.String()
		}
		return ""
	}},
}

func lower(s string) string { return strings.ToLower(s) }
