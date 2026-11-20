package log_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"go.opentelemetry.io/otel/trace"
	"go.uber.org/goleak"

	"github.com/dungnh3/kitchen/log"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

var (
	testTraceID = trace.TraceID{0x4b, 0xf9, 0x2f, 0x35, 0x77, 0xb3, 0x4d, 0xa6, 0xa3, 0xce, 0x92, 0x9d, 0x0e, 0x0e, 0x47, 0x36}
	testSpanID  = trace.SpanID{0x00, 0xf0, 0x67, 0xaa, 0x0b, 0xa9, 0x02, 0xb7}
)

func spanCtx(sampled bool) context.Context {
	cfg := trace.SpanContextConfig{TraceID: testTraceID, SpanID: testSpanID}
	if sampled {
		cfg.TraceFlags = trace.FlagsSampled
	}
	return trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(cfg))
}

func decode(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for line := range strings.Lines(buf.String()) {
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("invalid JSON line %q: %v", line, err)
		}
		out = append(out, m)
	}
	return out
}

func TestJSONSchema(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, log.Options{GCPProjectID: "proj"})
	logger.WarnContext(spanCtx(true), "quota low", slog.String("user_id", "u1"), slog.Float64("ratio", 0.25))

	m := decode(t, &buf)[0]
	want := map[string]any{
		"severity":                             "WARNING",
		"message":                              "quota low",
		"user_id":                              "u1",
		"ratio":                                0.25,
		"logging.googleapis.com/trace":         "projects/proj/traces/4bf92f3577b34da6a3ce929d0e0e4736",
		"logging.googleapis.com/spanId":        "00f067aa0ba902b7",
		"logging.googleapis.com/trace_sampled": true,
	}
	for k, v := range want {
		if m[k] != v {
			t.Errorf("%s = %v, want %v", k, m[k], v)
		}
	}
	for _, k := range []string{"msg", "level", "source", "trace_id", "span_id"} {
		if _, ok := m[k]; ok {
			t.Errorf("unexpected key %q", k)
		}
	}
	if _, ok := m["time"].(string); !ok {
		t.Errorf("time missing: %v", m)
	}
	src, ok := m["logging.googleapis.com/sourceLocation"].(map[string]any)
	if !ok || !strings.HasSuffix(src["file"].(string), "log_test.go") || src["line"] == "" ||
		!strings.Contains(src["function"].(string), "TestJSONSchema") {
		t.Errorf("sourceLocation = %v", m["logging.googleapis.com/sourceLocation"])
	}
}

func TestTraceKeys(t *testing.T) {
	tests := []struct {
		name    string
		ctx     context.Context
		project string
		trace   any
		sampled any
	}{
		{"no span", context.Background(), "", nil, nil},
		{"bare id", spanCtx(false), "", "4bf92f3577b34da6a3ce929d0e0e4736", false},
		{"project", spanCtx(true), "p1", "projects/p1/traces/4bf92f3577b34da6a3ce929d0e0e4736", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			log.New(&buf, log.Options{GCPProjectID: tt.project, NoSource: true}).InfoContext(tt.ctx, "x")
			m := decode(t, &buf)[0]
			if m[log.KeyTrace] != tt.trace || m[log.KeyTraceSampled] != tt.sampled {
				t.Errorf("got trace=%v sampled=%v", m[log.KeyTrace], m[log.KeyTraceSampled])
			}
			if _, ok := m["logging.googleapis.com/sourceLocation"]; ok {
				t.Error("NoSource ignored")
			}
		})
	}
}

func TestSeverity(t *testing.T) {
	tests := []struct {
		level slog.Level
		want  string
	}{
		{slog.LevelDebug - 4, "DEBUG"}, {log.LevelDebug, "DEBUG"}, {log.LevelInfo, "INFO"},
		{log.LevelNotice, "NOTICE"}, {log.LevelWarn, "WARNING"}, {log.LevelError, "ERROR"},
		{log.LevelError + 1, "ERROR"}, {log.LevelCritical, "CRITICAL"}, {log.LevelAlert, "ALERT"},
		{log.LevelEmergency, "EMERGENCY"}, {log.LevelEmergency + 10, "EMERGENCY"},
	}
	for _, tt := range tests {
		var buf bytes.Buffer
		log.New(&buf, log.Options{Level: slog.Level(-100)}).Log(context.Background(), tt.level, "m")
		if got := decode(t, &buf)[0]["severity"]; got != tt.want {
			t.Errorf("level %v: severity %v, want %s", tt.level, got, tt.want)
		}
	}
}

func TestGroupsKeepContextAttrsTopLevel(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, log.Options{NoSource: true}).
		With("a", 1).
		WithGroup("g").
		With("b", 2).
		WithGroup("h")
	ctx := log.WithAttrs(spanCtx(true), slog.String("x-request-id", "r1"))
	logger.InfoContext(ctx, "msg", "c", 3)

	m := decode(t, &buf)[0]
	if m["a"] != 1.0 || m["x-request-id"] != "r1" || m[log.KeySpanID] != "00f067aa0ba902b7" {
		t.Fatalf("top-level attrs wrong: %v", m)
	}
	g := m["g"].(map[string]any)
	h := g["h"].(map[string]any)
	if g["b"] != 2.0 || h["c"] != 3.0 {
		t.Fatalf("groups wrong: %v", m)
	}

	// Empty groups are dropped, as slog does.
	buf.Reset()
	log.New(&buf, log.Options{NoSource: true}).WithGroup("empty").InfoContext(ctx, "m")
	if _, ok := decode(t, &buf)[0]["empty"]; ok {
		t.Error("empty group written")
	}
}

func TestWithAttrsAccumulatesWithoutAliasing(t *testing.T) {
	base := log.WithAttrs(context.Background(), slog.String("a", "1"))
	c1 := log.WithAttrs(base, slog.String("b", "1"))
	c2 := log.WithAttrs(base, slog.String("b", "2"))
	if got := log.AttrsFromContext(c1); len(got) != 2 || got[1].Value.String() != "1" {
		t.Fatalf("c1 = %v", got)
	}
	if got := log.AttrsFromContext(c2); got[1].Value.String() != "2" {
		t.Fatalf("c2 = %v", got)
	}
	if log.WithAttrs(base) != base {
		t.Error("WithAttrs with no attrs should return ctx unchanged")
	}
	if log.AttrsFromContext(context.Background()) != nil {
		t.Error("expected nil")
	}
}

func TestErr(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, log.Options{NoSource: true})
	logger.Error("failed", log.Err(errors.New("boom")))
	logger.Error("ok", log.Err(nil))
	lines := decode(t, &buf)
	if lines[0]["error"] != "boom" {
		t.Errorf("error = %v", lines[0]["error"])
	}
	if _, ok := lines[1]["error"]; ok {
		t.Error("nil error should be dropped")
	}
}

func TestLevelVarRuntimeChange(t *testing.T) {
	var buf bytes.Buffer
	lv := new(slog.LevelVar)
	logger := log.New(&buf, log.Options{Level: lv})
	logger.Debug("hidden")
	lv.Set(log.LevelDebug) // lowering works (the old SetLevel could only raise)
	logger.Debug("shown")
	lv.Set(log.LevelError)
	logger.Warn("hidden")
	if got := decode(t, &buf); len(got) != 1 || got[0]["message"] != "shown" {
		t.Fatalf("got %v", got)
	}
}

func TestDefaultLevelInfo(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, log.Options{})
	logger.Debug("hidden")
	logger.Info("shown")
	if got := decode(t, &buf); len(got) != 1 {
		t.Fatalf("got %d lines", len(got))
	}
}

func TestReplaceAttrOnlyUserAttrs(t *testing.T) {
	var buf bytes.Buffer
	redact := func(_ []string, a slog.Attr) slog.Attr {
		if a.Key == "password" || a.Key == slog.MessageKey {
			return slog.String(a.Key, "***")
		}
		return a
	}
	log.New(&buf, log.Options{NoSource: true, ReplaceAttr: redact}).Info("hi", "password", "secret")
	m := decode(t, &buf)[0]
	if m["password"] != "***" || m["message"] != "hi" {
		t.Fatalf("got %v", m)
	}
}

func TestTextFormat(t *testing.T) {
	var buf bytes.Buffer
	log.New(&buf, log.Options{Format: log.FormatText}).WarnContext(spanCtx(true), "hello", "k", "v")
	out := buf.String()
	for _, want := range []string{"level=WARNING", `msg=hello`, "k=v", "source=", "log_test.go:", "logging.googleapis.com/spanId=00f067aa0ba902b7"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in %q", want, out)
		}
	}
}

func TestParseLevel(t *testing.T) {
	tests := []struct {
		in      string
		want    slog.Level
		wantErr bool
	}{
		{"debug", log.LevelDebug, false}, {"INFO", log.LevelInfo, false}, {"Notice", log.LevelNotice, false},
		{"warn", log.LevelWarn, false}, {"WARNING", log.LevelWarn, false}, {"error", log.LevelError, false},
		{"critical", log.LevelCritical, false}, {"alert", log.LevelAlert, false},
		{"emergency", log.LevelEmergency, false}, {"INFO+2", slog.Level(2), false},
		{"verbose", 0, true}, {"", 0, true},
	}
	for _, tt := range tests {
		got, err := log.ParseLevel(tt.in)
		if (err != nil) != tt.wantErr || (err == nil && got != tt.want) {
			t.Errorf("ParseLevel(%q) = %v, %v", tt.in, got, err)
		}
	}
}

func TestOptionsFromEnv(t *testing.T) {
	t.Setenv("LOG_LEVEL", "debug")
	t.Setenv("LOG_FORMAT", "TEXT")
	t.Setenv("GOOGLE_CLOUD_PROJECT", "p")
	opts, err := log.OptionsFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if opts.Level.Level() != log.LevelDebug || opts.Format != log.FormatText || opts.GCPProjectID != "p" {
		t.Fatalf("got %+v", opts)
	}
	if _, ok := opts.Level.(*slog.LevelVar); !ok {
		t.Error("Level should be a *slog.LevelVar")
	}

	t.Setenv("LOG_LEVEL", "loud")
	t.Setenv("LOG_FORMAT", "xml")
	opts, err = log.OptionsFromEnv()
	if err == nil || !strings.Contains(err.Error(), "LOG_LEVEL") || !strings.Contains(err.Error(), "LOG_FORMAT") {
		t.Fatalf("err = %v", err)
	}
	if opts.Level.Level() != log.LevelInfo || opts.Format != log.FormatJSON {
		t.Fatalf("defaults not kept: %+v", opts)
	}
}

func TestContextHandlerWrapsMultiHandler(t *testing.T) {
	var a, b bytes.Buffer
	h := log.NewContextHandler(slog.NewMultiHandler(
		slog.NewJSONHandler(&a, nil),
		slog.NewJSONHandler(&b, nil),
	), "")
	slog.New(h).InfoContext(spanCtx(true), "m")
	for _, buf := range []*bytes.Buffer{&a, &b} {
		if decode(t, buf)[0][log.KeySpanID] != "00f067aa0ba902b7" {
			t.Errorf("missing span id: %s", buf.String())
		}
	}
}

func TestConcurrentUse(t *testing.T) {
	var mu sync.Mutex
	var buf bytes.Buffer
	w := writerFunc(func(p []byte) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		return buf.Write(p)
	})
	logger := log.New(w, log.Options{}).WithGroup("g").With("k", 1)
	ctx := log.WithAttrs(spanCtx(true), slog.String("r", "1"))
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			for range 50 {
				logger.InfoContext(ctx, "m", "i", 1)
			}
		})
	}
	wg.Wait()
	if n := len(decode(t, &buf)); n != 800 {
		t.Fatalf("lines = %d", n)
	}
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

func BenchmarkInfoContext(b *testing.B) {
	logger := log.New(discard{}, log.Options{NoSource: true})
	ctx := log.WithAttrs(spanCtx(true), slog.String("x-request-id", "r1"))
	b.ReportAllocs()
	for b.Loop() {
		logger.InfoContext(ctx, "message", slog.String("k", "v"), slog.Int("n", 1))
	}
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }
