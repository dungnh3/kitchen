package server

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"
)

func TestAdminStandalone(t *testing.T) {
	l := listen(t)
	adm := NewAdmin("", WithAdminServeListener(l), WithAdminLogger(quietLogger()), WithPprof(),
		WithAdminVersion("v1.2.3"),
		WithAdminHandler("GET /metrics", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte("m 1\n"))
		})),
		WithAdminChecks(Check{Name: "q", Func: func(context.Context) error { return nil }}))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- adm.Run(ctx) }()
	base := "http://" + l.Addr().String()
	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, err := noKeepAlive.Get(base + "/livez")
		if err == nil {
			_ = resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	for path, want := range map[string]int{"/readyz": 200, "/api/status": 404, "/metrics": 200, "/debug/pprof/": 200, "/nope": 404} {
		if resp, body := get(t, base+path, nil); resp.StatusCode != want {
			t.Errorf("%s = %d %s", path, resp.StatusCode, body)
		}
	}
	if _, body := get(t, base+"/version", nil); body != "{\"version\":\"v1.2.3\"}\n" {
		t.Errorf("version body %q", body)
	}
	if err := adm.Run(ctx); !errors.Is(err, ErrAlreadyStarted) {
		t.Errorf("second Run = %v", err)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
