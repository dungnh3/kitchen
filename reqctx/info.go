package reqctx

import (
	"context"
	"crypto/rand"
	"net/http"
	"net/netip"
	"strconv"
	"strings"

	"google.golang.org/grpc/metadata"
)

// DeviceType is the client device class sent in X-Device-Type.
type DeviceType int

// Device types.
const (
	DeviceTypeUnknown DeviceType = iota
	DeviceTypeDesktop
	DeviceTypeMobile
	DeviceTypeInvalid
)

func (d DeviceType) headerValue() string {
	if d == DeviceTypeUnknown {
		return ""
	}
	return strconv.Itoa(int(d))
}

// Platform is a client platform.
type Platform string

// Known platforms.
const (
	PlatformUnknown Platform = ""
	PlatformIOS     Platform = "ios"
	PlatformAndroid Platform = "android"
	PlatformHuawei  Platform = "huawei"
	PlatformWeb     Platform = "web"
)

// Info is the client information of one request. It is built once per
// request and is read-only afterwards.
type Info struct {
	RequestID       string
	AnonymousUserID string
	DeviceID        string
	DeviceType      DeviceType
	// Platform is X-Platform when it names a known platform, otherwise the
	// platform detected from the User-Agent.
	Platform      Platform
	OSVersion     string
	AppVersion    string
	ApplicationID string
	Localization  string
	Locale        string
	Timezone      string
	// Country is CF-IPCountry, falling back to X-Country.
	Country string
	// ClientIP is resolved with a trusted-proxy list; it is never taken from
	// a header a direct client controls.
	ClientIP  netip.Addr
	UserAgent string
}

type infoKey struct{}

type userIDKey struct{}

// NewContext returns a copy of ctx that carries info.
func NewContext(ctx context.Context, info Info) context.Context {
	return context.WithValue(ctx, infoKey{}, &info)
}

// FromContext returns the Info stored in ctx. ok is false when no server
// stored one (for example in a background job).
func FromContext(ctx context.Context) (info Info, ok bool) {
	p, ok := ctx.Value(infoKey{}).(*Info)
	if !ok {
		return Info{}, false
	}
	return *p, true
}

// WithUserID returns a copy of ctx that carries the authenticated user id.
// Only an authentication interceptor should call it, after verifying a
// credential.
func WithUserID(ctx context.Context, userID string) context.Context {
	return context.WithValue(ctx, userIDKey{}, userID)
}

// UserID returns the authenticated user id, or "" when the request is not
// authenticated.
func UserID(ctx context.Context) string {
	id, _ := ctx.Value(userIDKey{}).(string)
	return id
}

// Get returns the first value of the incoming gRPC metadata key (case
// insensitive), or "". Use it for values that are not part of Info, such as
// X-Api-Key or X-Firebase-AppCheck. Repeated values are never joined.
func Get(ctx context.Context, key string) string {
	md, _ := metadata.FromIncomingContext(ctx)
	return first(md, key)
}

// BearerToken returns the token of an "Authorization: Bearer <token>"
// incoming metadata value. ok is false when there is none.
func BearerToken(ctx context.Context) (token string, ok bool) {
	v := Get(ctx, HeaderAuthorization)
	const prefix = "bearer "
	if len(v) <= len(prefix) || !strings.EqualFold(v[:len(prefix)], prefix) {
		return "", false
	}
	token = strings.TrimSpace(v[len(prefix):])
	return token, token != ""
}

// FromMetadata builds an Info from gRPC metadata. ClientIP is left unset:
// the caller resolves it from the peer address with [TrustedProxies].
func FromMetadata(md metadata.MD) Info {
	ua := first(md, HeaderGatewayUserAgent)
	if ua == "" {
		ua = first(md, HeaderUserAgent)
	}
	return build(func(k string) string { return first(md, k) }, ua)
}

// FromHTTP builds an Info from an HTTP request. ClientIP is resolved with
// proxies; a nil proxies trusts no proxy and uses the connection address.
func FromHTTP(r *http.Request, proxies *TrustedProxies) Info {
	info := build(r.Header.Get, r.UserAgent())
	info.ClientIP = proxies.ClientIPFromRequest(r)
	return info
}

func build(get func(string) string, ua string) Info {
	info := Info{
		RequestID:       get(HeaderRequestID),
		AnonymousUserID: get(HeaderAnonymousUserID),
		DeviceID:        get(HeaderDeviceID),
		OSVersion:       get(HeaderOSVersion),
		AppVersion:      get(HeaderAppVersion),
		ApplicationID:   get(HeaderApplicationID),
		Localization:    get(HeaderLocalization),
		Locale:          get(HeaderLocale),
		Timezone:        get(HeaderTimezone),
		Country:         get(HeaderCFCountry),
		UserAgent:       ua,
	}
	if info.Country == "" {
		info.Country = get(HeaderCountry)
	}
	if n, err := strconv.Atoi(get(HeaderDeviceType)); err == nil && n > 0 && n <= int(DeviceTypeInvalid) {
		info.DeviceType = DeviceType(n)
	}
	info.Platform = parsePlatform(get(HeaderPlatform))
	if info.Platform == PlatformUnknown {
		info.Platform = PlatformFromUserAgent(ua)
	}
	if !ValidRequestID(info.RequestID) {
		info.RequestID = ""
	}
	return info
}

func parsePlatform(s string) Platform {
	switch p := Platform(strings.ToLower(strings.TrimSpace(s))); p {
	case PlatformIOS, PlatformAndroid, PlatformHuawei, PlatformWeb:
		return p
	}
	return PlatformUnknown
}

// PlatformFromUserAgent detects the client platform from a User-Agent. It
// does not mistake "axios" for iOS.
func PlatformFromUserAgent(ua string) Platform {
	ua = strings.ToLower(ua)
	containsAny := func(subs ...string) bool {
		for _, s := range subs {
			if strings.Contains(ua, s) {
				return true
			}
		}
		return false
	}
	switch {
	case ua == "":
		return PlatformUnknown
	case strings.Contains(ua, "android"):
		return PlatformAndroid
	case containsAny("iphone", "ipad", "ipod", "cfnetwork", "darwin"),
		strings.Contains(strings.ReplaceAll(ua, "axios", ""), "ios"):
		return PlatformIOS
	case strings.Contains(ua, "huawei"):
		return PlatformHuawei
	case containsAny("mozilla", "chrome", "safari", "firefox", "edge", "opera", "axios"):
		return PlatformWeb
	}
	return PlatformUnknown
}

const maxRequestIDLen = 128

// ValidRequestID reports whether id is a usable request id: 1 to 128
// printable ASCII characters without spaces.
func ValidRequestID(id string) bool {
	if id == "" || len(id) > maxRequestIDLen {
		return false
	}
	for i := range len(id) {
		if id[i] < 0x21 || id[i] > 0x7e {
			return false
		}
	}
	return true
}

// NewRequestID returns a random request id.
func NewRequestID() string { return rand.Text() }

func first(md metadata.MD, key string) string {
	vs := md.Get(key)
	if len(vs) == 0 {
		return ""
	}
	return strings.TrimSpace(vs[0])
}
