package reqctx

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestFromMetadata(t *testing.T) {
	md := metadata.Pairs(
		"x-request-id", "r1", "x-device-id", "d1", "x-device-type", "2",
		"x-platform", "Android", "x-app-version", "1.0", "x-timezone", "Asia/Ho_Chi_Minh",
		"x-country", "VN", "user-agent", "grpc-go", "grpcgateway-user-agent", "Mozilla/5.0",
		"user-id", "a", "user-id", "b",
	)
	got := FromMetadata(md)
	want := Info{
		RequestID: "r1", DeviceID: "d1", DeviceType: DeviceTypeMobile, Platform: PlatformAndroid,
		AppVersion: "1.0", Timezone: "Asia/Ho_Chi_Minh", Country: "VN", UserAgent: "Mozilla/5.0",
	}
	if got != want {
		t.Fatalf("got %+v\nwant %+v", got, want)
	}
	if FromMetadata(metadata.Pairs("x-device-type", "99", "x-request-id", "has space")).DeviceType != DeviceTypeUnknown {
		t.Error("invalid device type accepted")
	}
	if FromMetadata(metadata.Pairs("x-request-id", "has space")).RequestID != "" {
		t.Error("invalid request id accepted")
	}
	if FromMetadata(metadata.Pairs("cf-ipcountry", "SG", "x-country", "VN")).Country != "SG" {
		t.Error("CF-IPCountry must win")
	}
}

func TestPlatformFromUserAgent(t *testing.T) {
	for ua, want := range map[string]Platform{
		"":                                 PlatformUnknown,
		"axios/1.6.0":                      PlatformWeb,
		"MyApp/1 CFNetwork/1410 Darwin/22": PlatformIOS,
		"Mozilla/5.0 (iPhone; CPU iPhone OS 17_0)": PlatformIOS,
		"okhttp/4.12 Android 14":                   PlatformAndroid,
		"Mozilla/5.0 (Windows NT 10.0) Chrome/120": PlatformWeb,
		"HUAWEI-P30":   PlatformHuawei,
		"curl/8":       PlatformUnknown,
		"MyApp iOS/17": PlatformIOS,
	} {
		if got := PlatformFromUserAgent(ua); got != want {
			t.Errorf("%q = %q, want %q", ua, got, want)
		}
	}
}

func TestContextValues(t *testing.T) {
	ctx := context.Background()
	if _, ok := FromContext(ctx); ok || UserID(ctx) != "" {
		t.Fatal("empty context has values")
	}
	ctx = WithUserID(NewContext(ctx, Info{RequestID: "r"}), "u")
	if info, ok := FromContext(ctx); !ok || info.RequestID != "r" || UserID(ctx) != "u" {
		t.Fatalf("got %+v %q", info, UserID(ctx))
	}
}

func TestGetAndBearer(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"x-api-key", "k1", "x-api-key", "k2", "authorization", "BEARER  tok "))
	if Get(ctx, "X-Api-Key") != "k1" {
		t.Error("Get must return the first value, not a concatenation")
	}
	if tok, ok := BearerToken(ctx); !ok || tok != "tok" {
		t.Errorf("bearer = %q %v", tok, ok)
	}
	ctx = metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Basic x"))
	if _, ok := BearerToken(ctx); ok {
		t.Error("basic accepted as bearer")
	}
	if _, ok := BearerToken(context.Background()); ok {
		t.Error("no metadata accepted")
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
		cf     string
		want   string
	}{
		{"untrusted remote ignores headers", "1.1.1.1", []string{"9.9.9.9"}, "8.8.8.8", "1.1.1.1"},
		{"trusted remote uses CF", "10.0.0.1", []string{"9.9.9.9"}, "8.8.8.8", "8.8.8.8"},
		{"rightmost untrusted", "10.0.0.1", []string{"6.6.6.6, 9.9.9.9", "10.1.1.1"}, "", "9.9.9.9"},
		{"all trusted -> leftmost", "10.0.0.1", []string{"10.2.2.2, 192.168.1.1"}, "", "10.2.2.2"},
		{"malformed breaks chain", "10.0.0.1", []string{"6.6.6.6, junk, 10.3.3.3"}, "", "10.3.3.3"},
		{"no xff", "192.168.1.1", nil, "", "192.168.1.1"},
		{"mapped v4", "::ffff:10.0.0.1", []string{"9.9.9.9"}, "", "9.9.9.9"},
	}
	for _, tt := range tests {
		if got := tp.ClientIP(a(tt.remote), tt.xff, tt.cf); got.String() != tt.want {
			t.Errorf("%s: got %v want %s", tt.name, got, tt.want)
		}
	}
	var none *TrustedProxies
	if got := none.ClientIP(a("1.1.1.1"), []string{"9.9.9.9"}, ""); got.String() != "1.1.1.1" {
		t.Errorf("nil proxies trusted XFF: %v", got)
	}
}

func TestFromHTTP(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.RemoteAddr = "10.0.0.5:1234"
	r.Header.Set("X-Forwarded-For", "5.5.5.5")
	r.Header.Set("User-Agent", "axios/1.0")
	tp, _ := NewTrustedProxies("10.0.0.0/8")
	info := FromHTTP(r, tp)
	if info.ClientIP.String() != "5.5.5.5" || info.Platform != PlatformWeb {
		t.Fatalf("%+v", info)
	}
	if FromHTTP(r, nil).ClientIP.String() != "10.0.0.5" {
		t.Fatal("nil proxies must use RemoteAddr")
	}
	for in, want := range map[string]string{"[::1]:80": "::1", "1.2.3.4": "1.2.3.4", "bad": "invalid IP"} {
		if got := AddrFromString(in).String(); got != want {
			t.Errorf("AddrFromString(%q) = %q", in, got)
		}
	}
}

func TestAppendToOutgoing(t *testing.T) {
	ctx := NewContext(context.Background(), Info{
		RequestID: "r1", DeviceID: "d", DeviceType: DeviceTypeMobile, Platform: PlatformIOS,
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
		"user-id": "u1", "x-device-id": "d", "x-device-type": "2", "x-platform": "ios",
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
