package webmeta_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/netip"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/httpx"
	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/webmeta"
)

// Needs network access, so it has no Output and only compiles in tests.
func Example() {
	f, err := webmeta.New(webmeta.Config{}) // SSRF guard, 10s timeout, 2 MiB cap
	if err != nil {
		panic(err)
	}
	defer f.Close() // build one Fetcher and share it

	m, err := f.Fetch(context.Background(), "https://go.dev/")
	if err != nil {
		fmt.Println("fetch:", err)
		return
	}
	fmt.Println(m.Title, m.Description, m.Image, m.Icon)
}

// Needs network access, so it has no Output and only compiles in tests.
func ExampleFetcher_FetchAll() {
	f, err := webmeta.New(webmeta.Config{})
	if err != nil {
		panic(err)
	}
	defer f.Close()

	urls := []string{"https://go.dev/", "http://127.0.0.1/admin", "https://pkg.go.dev/"}
	for _, r := range f.FetchAll(context.Background(), urls, 4) {
		if r.Err != nil {
			fmt.Println(r.URL, "skipped:", r.Err) // one failure does not stop the others
			continue
		}
		fmt.Println(r.URL, r.Metadata.Title)
	}
}

func Example_errors() {
	f, err := webmeta.New(webmeta.Config{})
	if err != nil {
		panic(err)
	}
	defer f.Close()

	ctx := context.Background()
	for _, u := range []string{"ftp://example.com/file", "http://10.0.0.1/admin"} {
		_, err := f.Fetch(ctx, u)
		info, _ := errs.Info(err)
		fmt.Println(info.GetReason(), errs.Code(err), errs.HTTPStatus(err))
	}
	// Output:
	// INVALID_URL InvalidArgument 400
	// DESTINATION_NOT_ALLOWED InvalidArgument 400
}

func Example_handler() {
	f, err := webmeta.New(webmeta.Config{
		ClientOptions: []httpx.Option{httpx.WithLogger(log.Default())},
	})
	if err != nil {
		panic(err)
	}
	defer f.Close()

	// preview is the body of a service handler. It passes the request ctx
	// (deadline, cancellation, trace) and returns the errs error as is: the
	// server sends its code and safe message, the cause stays in the logs.
	preview := func(ctx context.Context, rawURL string) (*webmeta.Metadata, error) {
		m, err := f.Fetch(ctx, rawURL)
		if err != nil {
			return nil, err
		}
		return m, nil
	}

	_, err = preview(context.Background(), "http://169.254.169.254/latest/meta-data/")
	fmt.Println(errors.Is(err, httpx.ErrDestinationNotAllowed), errs.Code(err))
	// Output: true InvalidArgument
}

func ExampleConfig_clientOptions() {
	f, err := webmeta.New(webmeta.Config{
		ClientOptions: []httpx.Option{
			httpx.WithLogger(log.Discard()),
			httpx.WithRetry(httpx.RetryPolicy{}),
		},
	})
	fmt.Println(err == nil)
	f.Close()

	// A custom transport would bypass the SSRF guard, so New refuses it.
	_, err = webmeta.New(webmeta.Config{
		ClientOptions: []httpx.Option{httpx.WithTransport(http.DefaultTransport)},
	})
	fmt.Println(err != nil)
	// Output:
	// true
	// true
}

func ExampleConfig_allow() {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = fmt.Fprint(w, `<title>Local page</title>`)
	}))
	defer srv.Close()

	// TESTS ONLY: open exactly the test server's address and port.
	ap := netip.MustParseAddrPort(srv.Listener.Addr().String())
	f, err := webmeta.New(webmeta.Config{
		Allow: []netip.Prefix{netip.PrefixFrom(ap.Addr(), ap.Addr().BitLen())},
		Ports: []int{int(ap.Port())},
	})
	if err != nil {
		panic(err)
	}
	defer f.Close()

	m, err := f.Fetch(context.Background(), srv.URL)
	fmt.Println(m.Title, err)
	// Output: Local page <nil>
}
