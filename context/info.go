package context

import (
	"context"
	"crypto/rand"
	"net/http"
	"net/netip"
	"net/textproto"
	"strings"

	"github.com/dungnh3/kitchen/log"
	"google.golang.org/grpc/metadata"
)

// Platform is a client platform.
type Platform string

// Known platforms.
const (
	PlatformUnknown Platform = ""
	PlatformIOS     Platform = "ios"
	PlatformAndroid Platform = "android"
	PlatformWeb     Platform = "web"
)

// Info is the client information of one request. It is built once per
// request and is read-only afterwards.
type Info struct {
	// RequestID is the client's X-Request-Id when valid (see ValidRequestID);
	// the server generates one otherwise.
	RequestID string
	// ClientIP is resolved with a trusted-proxy list; it is never taken from
	// a header a direct client controls.
	ClientIP netip.Addr
	// UserAgent is the HTTP client's User-Agent (for the REST gateway, the
	// browser's, not grpc-gateway's).
	UserAgent string
	// AcceptLanguage is the raw Accept-Language value.
	AcceptLanguage string
	// Platform is X-Platform when it names a known platform, otherwise the
	// platform detected from the User-Agent.
	Platform   Platform
	AppVersion string
	DeviceID   string
	// Timezone is the client's IANA time zone name (X-Timezone), unvalidated.
	Timezone string
	// Headers holds the first value of every captured header, keyed by
	// canonical name: DefaultForwardHeaders plus Options.Headers, without
	// identity headers, credentials and X-Request-Id (see RequestID). It is
	// shared and must not be modified; read it with Header.
	Headers map[string]string
}

// Header returns the captured value of the named header, or "".
// X-Request-Id is the validated RequestID.
func (i Info) Header(name string) string {
	c := textproto.CanonicalMIMEHeaderKey(name)
	if c == HeaderRequestID {
		return i.RequestID
	}
	return i.Headers[c]
}

// Options configures FromHTTP and FromMetadata.
type Options struct {
	// TrustedProxies resolves ClientIP behind proxies (FromHTTP only). Nil
	// trusts no proxy: the connection address is the client.
	TrustedProxies *TrustedProxies
	// Headers are captured in Info.Headers in addition to
	// DefaultForwardHeaders.
	Headers []string
}

// maxValueLen bounds every captured value; longer values are dropped.
const maxValueLen = 4096

type infoKey struct{}

type userIDKey struct{}

// NewContext returns a copy of ctx that carries info. The request id is
// also added to the log attributes of ctx (see LogKeyRequestID).
func NewContext(ctx context.Context, info Info) context.Context {
	if info.RequestID != "" {
		ctx = log.WithAttrs(ctx, logRequestID(info.RequestID))
	}
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

// Header returns the value of the named request header: the one captured in
// the Info of ctx, else the first value of the incoming gRPC metadata (for a
// gRPC server that stores no Info). Credentials and identity headers
// (IdentityHeaders) are never returned: they are client-controlled unless a
// server stripped them. X-Request-Id is the validated Info.RequestID.
func Header(ctx context.Context, name string) string {
	canonical := textproto.CanonicalMIMEHeaderKey(name)
	if isSecret(canonical) || isIdentity(canonical) {
		return ""
	}
	if p, ok := ctx.Value(infoKey{}).(*Info); ok {
		return p.Header(canonical)
	}
	vs := metadata.ValueFromIncomingContext(ctx, strings.ToLower(canonical))
	if len(vs) == 0 {
		return ""
	}
	v := strings.TrimSpace(vs[0])
	if canonical == HeaderRequestID && !ValidRequestID(v) {
		return ""
	}
	return v
}

// WithUserID returns a copy of ctx that carries the authenticated user id,
// also as a log attribute (LogKeyUserID). Only an authentication step or a
// trusted-peer check should call it.
func WithUserID(ctx context.Context, userID string) context.Context {
	if userID != "" {
		ctx = log.WithAttrs(ctx, logUserID(userID))
	}
	return context.WithValue(ctx, userIDKey{}, userID)
}

// UserID returns the authenticated user id, or "" when the request is not
// authenticated.
func UserID(ctx context.Context) string {
	id, _ := ctx.Value(userIDKey{}).(string)
	return id
}

// FromMetadata builds an Info from incoming gRPC metadata. ClientIP is left
// unset: the caller resolves it from the peer address with TrustedProxies.
func FromMetadata(md metadata.MD, opts Options) Info {
	get := func(k headerKey) string { return first(md, k.lower) }
	info := build(get, opts)
	info.UserAgent = bounded(firstOf(get, keyGatewayUA, keyUserAgent))
	info.AcceptLanguage = bounded(firstOf(get, keyAcceptLanguage, keyGatewayAL))
	info.Platform = platform(get(keyPlatform), info.UserAgent)
	return info
}

// FromHTTP builds an Info from an HTTP request, resolving ClientIP with
// opts.TrustedProxies.
func FromHTTP(r *http.Request, opts Options) Info {
	get := func(k headerKey) string {
		if vs := r.Header[k.canon]; len(vs) > 0 {
			return strings.TrimSpace(vs[0])
		}
		return ""
	}
	info := build(get, opts)
	info.UserAgent = bounded(get(keyUserAgent))
	info.AcceptLanguage = bounded(get(keyAcceptLanguage))
	info.Platform = platform(get(keyPlatform), info.UserAgent)
	info.ClientIP = opts.TrustedProxies.clientIPFromRequest(r)
	return info
}

func build(get func(headerKey) string, opts Options) Info {
	info := Info{
		RequestID:  get(keyRequestID),
		AppVersion: bounded(get(keyAppVersion)),
		DeviceID:   bounded(get(keyDeviceID)),
		Timezone:   bounded(get(keyTimezone)),
	}
	if !ValidRequestID(info.RequestID) {
		info.RequestID = ""
	}
	capture := func(k headerKey) {
		if v := bounded(get(k)); v != "" {
			if info.Headers == nil {
				info.Headers = make(map[string]string, len(captured)+len(opts.Headers))
			}
			info.Headers[k.canon] = v
		}
	}
	for _, k := range captured {
		capture(k)
	}
	for _, h := range opts.Headers {
		k := keyOf(h)
		if !isSecret(k.canon) && !isIdentity(k.canon) && k.canon != HeaderRequestID {
			capture(k)
		}
	}
	return info
}

func firstOf(get func(headerKey) string, keys ...headerKey) string {
	for _, k := range keys {
		if v := get(k); v != "" {
			return v
		}
	}
	return ""
}

// bounded drops values longer than maxValueLen.
func bounded(v string) string {
	if len(v) > maxValueLen {
		return ""
	}
	return v
}

func platform(header, ua string) Platform {
	switch p := Platform(strings.ToLower(strings.TrimSpace(header))); p {
	case PlatformIOS, PlatformAndroid, PlatformWeb:
		return p
	}
	return PlatformFromUserAgent(ua)
}

// PlatformFromUserAgent detects the client platform from a User-Agent. It
// does not mistake "axios" for iOS. Native apps that only send the CFNetwork
// token are reported as iOS (macOS apps send it too).
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
	case containsAny("iphone", "ipad", "ipod"),
		strings.Contains(strings.ReplaceAll(ua, "axios", ""), "ios"),
		strings.Contains(ua, "cfnetwork") && !strings.Contains(ua, "macintosh"):
		return PlatformIOS
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

// first returns the first value of md for a lower-case key.
func first(md metadata.MD, lowerKey string) string {
	vs := md[lowerKey]
	if len(vs) == 0 {
		return ""
	}
	return strings.TrimSpace(vs[0])
}
