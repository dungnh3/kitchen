package context

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"reflect"
	"strings"
	"testing"

	"github.com/dungnh3/kitchen/log"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestFromMetadata(t *testing.T) {
	md := metadata.Pairs(
		"x-request-id", "r1", "x-device-id", "d1", "x-platform", "Android",
		"x-app-version", "1.0", "x-timezone", "Asia/Ho_Chi_Minh", "x-locale", "vi",
		"user-agent", "grpc-go", "grpcgateway-user-agent", "Mozilla/5.0",
		"grpcgateway-accept-language", "vi-VN", "x-tenant", "t1",
		"user-id", "a", "authorization", "Bearer secret", "x-kitchen-client-ip", "1.1.1.1",
	)
	got := FromMetadata(md, Options{Headers: []string{"X-Tenant", "Authorization"}})
	want := Info{
		RequestID: "r1", DeviceID: "d1", Platform: PlatformAndroid, AppVersion: "1.0",
		Timezone: "Asia/Ho_Chi_Minh", UserAgent: "Mozilla/5.0", AcceptLanguage: "vi-VN",
		Headers: map[string]string{
			"X-Device-Id": "d1", "X-Platform": "Android", "X-App-Version": "1.0",
			"X-Timezone": "Asia/Ho_Chi_Minh", "X-Locale": "vi", "X-Tenant": "t1",
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got  %+v\nwant %+v", got, want)
	}
	if got.Header("x-tenant") != "t1" || got.Header("authorization") != "" || got.Header("user-id") != "" {
		t.Error("Header must be case-insensitive and never expose identity or credentials")
	}
	if FromMetadata(metadata.Pairs("x-request-id", "has space"), Options{}).RequestID != "" {
		t.Error("invalid request id accepted")
	}
	if FromMetadata(metadata.Pairs("x-device-id", strings.Repeat("a", maxValueLen+1)), Options{}).DeviceID != "" {
		t.Error("oversized value kept")
	}
	if FromMetadata(metadata.Pairs("user-agent", "okhttp/4 Android 14"), Options{}).UserAgent != "okhttp/4 Android 14" {
		t.Error("native gRPC user-agent")
	}
}

func TestFromHTTP(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.RemoteAddr = "10.0.0.5:1234"
	r.Header.Set("X-Forwarded-For", "5.5.5.5")
	r.Header.Set("CF-Connecting-IP", "6.6.6.6")
	r.Header.Set("User-Agent", "axios/1.0")
	r.Header.Set("Accept-Language", "vi")
	r.Header.Set("X-Tenant", "t1")
	tp, _ := NewTrustedProxies("10.0.0.0/8")
	info := FromHTTP(r, Options{TrustedProxies: tp, Headers: []string{"x-tenant"}})
	if info.ClientIP.String() != "5.5.5.5" || info.Platform != PlatformWeb || info.AcceptLanguage != "vi" || info.Header("X-Tenant") != "t1" {
		t.Fatalf("%+v", info)
	}
	if FromHTTP(r, Options{}).ClientIP.String() != "10.0.0.5" {
		t.Fatal("nil proxies must use RemoteAddr")
	}
	for in, want := range map[string]string{"[::1]:80": "::1", "1.2.3.4": "1.2.3.4", "bad": "invalid IP"} {
		if got := AddrFromHostPort(in).String(); got != want {
			t.Errorf("AddrFromHostPort(%q) = %q", in, got)
		}
	}
}

func TestPlatform(t *testing.T) {
	for ua, want := range map[string]Platform{
		"":                                 PlatformUnknown,
		"axios/1.6.0":                      PlatformWeb,
		"MyApp/1 CFNetwork/1410 Darwin/22": PlatformIOS,
		"Mozilla/5.0 (iPhone; CPU iPhone OS 17_0)":             PlatformIOS,
		"okhttp/4.12 Android 14":                               PlatformAndroid,
		"Mozilla/5.0 (Linux; Android 13; HUAWEI P30)":          PlatformAndroid,
		"Mozilla/5.0 (Windows NT 10.0) Chrome/120":             PlatformWeb,
		"Mozilla/5.0 (Macintosh; Intel Mac OS X) Safari/605.1": PlatformWeb,
		"curl/8":       PlatformUnknown,
		"MyApp iOS/17": PlatformIOS,
	} {
		if got := PlatformFromUserAgent(ua); got != want {
			t.Errorf("%q = %q, want %q", ua, got, want)
		}
	}
	if platform("IOS", "okhttp Android") != PlatformIOS || platform("huawei", "okhttp Android") != PlatformAndroid {
		t.Error("known X-Platform wins; unknown falls back to the User-Agent")
	}
}

func TestContextValues(t *testing.T) {
	ctx := context.Background()
	if _, ok := FromContext(ctx); ok || UserID(ctx) != "" {
		t.Fatal("empty context has values")
	}
	ctx = WithUserID(NewContext(ctx, Info{RequestID: "r", Headers: map[string]string{"X-Tenant": "t1"}}), "u")
	if info, ok := FromContext(ctx); !ok || info.RequestID != "r" || UserID(ctx) != "u" {
		t.Fatalf("got %+v %q", info, UserID(ctx))
	}
	if Header(ctx, "x-tenant") != "t1" {
		t.Error("Header from Info")
	}
	md := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-api-key", "k1", "x-api-key", "k2", "authorization", "Bearer t"))
	if Header(md, "X-Api-Key") != "k1" {
		t.Error("Header must return the first metadata value when there is no Info")
	}
	if Header(md, "Authorization") != "" {
		t.Error("credentials returned")
	}
}

func TestTrustedProxies(t *testing.T) {
	tp, err := NewTrustedProxies("10.0.0.0/8", "192.168.1.1", " ")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewTrustedProxies("nope"); err == nil {
		t.Error("bad cidr accepted")
	}
	a := netip.MustParseAddr
	tests := []struct {
		name   string
		remote string
		xff    []string
		want   string
	}{
		{"untrusted remote ignores headers", "1.1.1.1", []string{"9.9.9.9"}, "1.1.1.1"},
		{"rightmost untrusted", "10.0.0.1", []string{"6.6.6.6, 9.9.9.9", "10.1.1.1"}, "9.9.9.9"},
		{"all trusted -> remote", "10.0.0.1", []string{"10.2.2.2, 192.168.1.1"}, "10.0.0.1"},
		{"malformed rightmost untrusted -> remote", "10.0.0.1", []string{"6.6.6.6, junk, 10.3.3.3"}, "10.0.0.1"},
		{"spoofed left entries ignored", "10.0.0.1", []string{"1.2.3.4", "6.6.6.6, 10.1.1.1"}, "6.6.6.6"},
		{"ipv6 with zone", "10.0.0.1", []string{"[fe80::1%eth0]:1234, 10.1.1.1"}, "fe80::1"},
		{"no xff", "192.168.1.1", nil, "192.168.1.1"},
		{"mapped v4", "::ffff:10.0.0.1", []string{"9.9.9.9"}, "9.9.9.9"},
	}
	for _, tt := range tests {
		if got := tp.ClientIP(a(tt.remote), tt.xff); got.String() != tt.want {
			t.Errorf("%s: got %v want %s", tt.name, got, tt.want)
		}
	}
	fwd, err := NewTrustedProxiesHeader("Forwarded", "10.0.0.0/8")
	if err != nil {
		t.Fatal(err)
	}
	if got := fwd.ClientIP(a("10.0.0.1"), []string{`for=1.2.3.4, for="[2001:db8::1]:80";proto=https, for=10.9.9.9`}); got.String() != "2001:db8::1" || fwd.Header() != "Forwarded" {
		t.Errorf("Forwarded: %v", got)
	}
	if _, err := NewTrustedProxiesHeader("X-Real-Ip", "10.0.0.0/8"); err == nil {
		t.Error("only X-Forwarded-For and Forwarded are list headers")
	}
	var none *TrustedProxies
	if got := none.ClientIP(a("1.1.1.1"), []string{"9.9.9.9"}); got.String() != "1.1.1.1" {
		t.Errorf("nil proxies trusted XFF: %v", got)
	}
}

func TestAppendToOutgoing(t *testing.T) {
	ctx := NewContext(context.Background(), Info{
		RequestID: "r1", DeviceID: "d", Platform: PlatformIOS, AcceptLanguage: "vi",
		ClientIP: netip.MustParseAddr("1.2.3.4"),
	})
	ctx = WithUserID(ctx, "u1")
	ctx = metadata.NewIncomingContext(ctx, metadata.Pairs("authorization", "secret"))
	ctx = metadata.AppendToOutgoingContext(ctx, "x-request-id", "explicit")

	var got metadata.MD
	inv := func(ctx context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		got, _ = metadata.FromOutgoingContext(ctx)
		return nil
	}
	if err := UnaryClientInterceptor()(ctx, "/m", nil, nil, nil, inv); err != nil {
		t.Fatal(err)
	}
	checks := map[string]string{
		"user-id": "u1", "x-device-id": "d", "accept-language": "vi", "x-platform": "ios",
		"x-kitchen-client-ip": "1.2.3.4", "x-request-id": "explicit",
	}
	for k, v := range checks {
		if vs := got.Get(k); len(vs) != 1 || vs[0] != v {
			t.Errorf("%s = %v, want [%s]", k, vs, v)
		}
	}
	if len(got.Get("authorization")) != 0 {
		t.Error("incoming authorization leaked to outgoing")
	}
	if AppendToOutgoing(context.Background()) != context.Background() {
		t.Error("empty context modified")
	}
	var streamMD metadata.MD
	_, _ = StreamClientInterceptor()(ctx, &grpc.StreamDesc{}, nil, "/m",
		func(ctx context.Context, _ *grpc.StreamDesc, _ *grpc.ClientConn, _ string, _ ...grpc.CallOption) (grpc.ClientStream, error) {
			streamMD, _ = metadata.FromOutgoingContext(ctx)
			return nil, nil
		})
	if len(streamMD.Get("user-id")) != 1 {
		t.Error("stream interceptor did not propagate")
	}
}

func TestRequestID(t *testing.T) {
	id := NewRequestID()
	if !ValidRequestID(id) || id == NewRequestID() {
		t.Fatalf("bad id %q", id)
	}
	long := make([]byte, 129)
	for i := range long {
		long[i] = 'a'
	}
	if ValidRequestID(string(long)) || ValidRequestID("") || ValidRequestID("a\nb") {
		t.Error("invalid ids accepted")
	}
}

func TestLogAttrs(t *testing.T) {
	if len(log.AttrsFromContext(context.Background())) != 0 {
		t.Fatal("empty context")
	}
	ctx := NewContext(context.Background(), Info{RequestID: "r1"})
	ctx = WithUserID(ctx, "u1")
	ctx = WithUserID(ctx, "u2") // replaces, never duplicates
	got := map[string]string{}
	for _, a := range log.AttrsFromContext(ctx) {
		if _, dup := got[a.Key]; dup {
			t.Fatalf("duplicate %s", a.Key)
		}
		got[a.Key] = a.Value.String()
	}
	if got[LogKeyRequestID] != "r1" || got[LogKeyUserID] != "u2" || len(got) != 2 {
		t.Fatalf("attrs = %v", got)
	}
}

// Review 2026-11-21: identity headers are never returned by Header, even on
// the metadata fallback path, and X-Request-Id is always the validated id.
func TestHeaderIdentityAndRequestID(t *testing.T) {
	md := metadata.Pairs("user-id", "admin", "x-kitchen-client-ip", "1.2.3.4", "x-request-id", "bad id", "x-tenant", "t1")
	ctx := metadata.NewIncomingContext(context.Background(), md)
	for _, h := range []string{"User-Id", "X-Kitchen-Client-Ip", "X-Request-Id"} {
		if v := Header(ctx, h); v != "" {
			t.Errorf("metadata fallback %s = %q", h, v)
		}
	}
	if Header(ctx, "x-tenant") != "t1" {
		t.Error("other headers still readable")
	}
	info := FromMetadata(md, Options{})
	if info.RequestID != "" || info.Header("X-Request-Id") != "" {
		t.Errorf("invalid request id captured: %q %q", info.RequestID, info.Header("X-Request-Id"))
	}
	info.RequestID = "generated"
	if Header(NewContext(context.Background(), info), "X-Request-Id") != "generated" {
		t.Error("Header must return the final request id")
	}
}
