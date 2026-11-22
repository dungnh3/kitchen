package log_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/trace"
	"go.uber.org/goleak"
	"go.uber.org/zap"

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
	logger := log.New(&buf, log.Options{Schema: log.SchemaGCP})
	logger.WarnContext(spanCtx(true), "quota low", slog.String("user_id", "u1"), slog.Float64("ratio", 0.25))

	m := decode(t, &buf)[0]
	want := map[string]any{
		"severity":                             "WARNING",
		"message":                              "quota low",
		"user_id":                              "u1",
		"ratio":                                0.25,
		"logging.googleapis.com/trace":         "4bf92f3577b34da6a3ce929d0e0e4736",
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

func TestTraceKeysPerSchema(t *testing.T) {
	const tid, sid = "4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7"
	tests := []struct {
		schema log.Schema
		ctx    context.Context
		want   map[string]any
	}{
		{log.SchemaOTel, spanCtx(true), map[string]any{"trace_id": tid, "span_id": sid, "trace_flags": "01"}},
		{log.SchemaOTel, spanCtx(false), map[string]any{"trace_id": tid, "trace_flags": "00"}},
		{log.SchemaGCP, spanCtx(false), map[string]any{log.KeyGCPTrace: tid, log.KeyGCPSpanID: sid, log.KeyGCPTraceSampled: false}},
		{log.SchemaAWS, spanCtx(true), map[string]any{"trace_id": tid, "span_id": sid, "xray_trace_id": "1-4bf92f35-77b34da6a3ce929d0e0e4736"}},
	}
	for _, tt := range tests {
		var buf bytes.Buffer
		log.New(&buf, log.Options{Schema: tt.schema, NoSource: true}).InfoContext(tt.ctx, "x")
		m := decode(t, &buf)[0]
		for k, v := range tt.want {
			if m[k] != v {
				t.Errorf("%s: %s = %v, want %v", tt.schema.Name, k, m[k], v)
			}
		}
	}
	var buf bytes.Buffer
	log.New(&buf, log.Options{NoSource: true}).InfoContext(context.Background(), "x")
	if _, ok := decode(t, &buf)[0]["trace_id"]; ok {
		t.Error("trace keys without a span")
	}
}

func TestSchemaKeys(t *testing.T) {
	for _, tt := range []struct {
		schema                      log.Schema
		time, level, msg, src, warn string
	}{
		{log.SchemaOTel, "timestamp", "level", "message", "source", "WARN"},
		{log.SchemaGCP, "time", "severity", "message", "logging.googleapis.com/sourceLocation", "WARNING"},
		{log.SchemaAWS, "timestamp", "level", "message", "location", "WARN"},
	} {
		var buf bytes.Buffer
		log.New(&buf, log.Options{Schema: tt.schema}).Warn("hi")
		m := decode(t, &buf)[0]
		ts, _ := m[tt.time].(string)
		if !strings.HasSuffix(ts, "Z") || m[tt.level] != tt.warn || m[tt.msg] != "hi" || m[tt.src] == nil {
			t.Errorf("%s: %v", tt.schema.Name, m)
		}
		if _, err := time.Parse(time.RFC3339Nano, ts); err != nil {
			t.Errorf("%s: time %q: %v", tt.schema.Name, ts, err)
		}
	}
}

func TestReservedKeysRenamed(t *testing.T) {
	for _, schema := range []log.Schema{log.SchemaOTel, log.SchemaGCP, log.SchemaAWS} {
		var buf bytes.Buffer
		ctx := log.WithAttrs(spanCtx(true), slog.String(schema.LevelKey, "from-ctx"))
		logger := log.New(&buf, log.Options{
			Schema: schema, NoSource: true,
			ContextAttrs: func(context.Context) []slog.Attr { return []slog.Attr{slog.String(schema.MessageKey, "extracted")} },
		}).With(slog.String(schema.TimeKey, "with"))
		// The forbidden keys are the point of this test.
		logger.InfoContext(ctx, "real", slog.String(schema.MessageKey, "spoof"), slog.String("msg", "spoof2"), //nolint:sloglint // spoofing on purpose
			slog.Any("level", slog.LevelError), slog.String(schema.Reserved[0], "fake-trace")) //nolint:sloglint // spoofing on purpose
		raw := buf.String()
		m := decode(t, &buf)[0]
		if m[schema.MessageKey] != "real" || m[schema.LevelKey] != schema.Level(slog.LevelInfo) {
			t.Fatalf("%s: built-in keys overridden: %s", schema.Name, raw)
		}
		for _, k := range []string{schema.MessageKey, "msg", "level", schema.TimeKey, schema.Reserved[0]} {
			if _, ok := m["attr."+k]; !ok {
				t.Errorf("%s: attr.%s missing: %s", schema.Name, k, raw)
			}
		}
		if strings.Count(raw, `"`+schema.MessageKey+`":`) != 1 || strings.Count(raw, `"`+schema.Reserved[0]+`":`) != 1 {
			t.Errorf("%s: duplicate key: %s", schema.Name, raw)
		}
	}
}

func TestContextAttrs(t *testing.T) {
	type key struct{}
	var buf bytes.Buffer
	logger := log.New(&buf, log.Options{NoSource: true, ContextAttrs: func(ctx context.Context) []slog.Attr {
		if id, ok := ctx.Value(key{}).(string); ok {
			return []slog.Attr{slog.String("request_id", id)}
		}
		return nil
	}})
	logger.WithGroup("g").InfoContext(context.WithValue(context.Background(), key{}, "r1"), "m", "a", 1)
	logger.Info("no ctx")
	lines := decode(t, &buf)
	if lines[0]["request_id"] != "r1" || lines[0]["g"].(map[string]any)["a"] != 1.0 {
		t.Fatalf("got %v", lines[0])
	}
	if _, ok := lines[1]["request_id"]; ok {
		t.Fatal("request_id without ctx")
	}
}

func TestCustomSchema(t *testing.T) {
	var buf bytes.Buffer
	custom := log.Schema{Name: "ecs", TimeKey: "@timestamp", LevelKey: "log.level", MessageKey: "message"}
	log.New(&buf, log.Options{Schema: custom, NoSource: true}).InfoContext(spanCtx(true), "m")
	m := decode(t, &buf)[0]
	if m["@timestamp"] == nil || m["log.level"] != "INFO" || m["trace_id"] == nil {
		t.Fatalf("got %v", m)
	}
}

func TestDetectSchema(t *testing.T) {
	for _, k := range []string{
		"K_SERVICE", "FUNCTION_TARGET", "GAE_SERVICE", "GOOGLE_CLOUD_PROJECT",
		"AWS_LAMBDA_FUNCTION_NAME", "ECS_CONTAINER_METADATA_URI_V4", "AWS_EXECUTION_ENV", "LOG_SCHEMA",
	} {
		t.Setenv(k, "")
	}
	if log.DetectSchema().Name != "otel" {
		t.Error("default")
	}
	t.Setenv("ECS_CONTAINER_METADATA_URI_V4", "http://169.254.170.2/v4/x")
	if log.DetectSchema().Name != "aws" {
		t.Error("ecs")
	}
	t.Setenv("K_SERVICE", "svc")
	if log.DetectSchema().Name != "gcp" {
		t.Error("cloud run")
	}
}

func TestSeverity(t *testing.T) {
	tests := []struct {
		level slog.Level
		want  string
	}{
		{slog.LevelDebug - 4, "DEBUG"},
		{log.LevelDebug, "DEBUG"},
		{log.LevelInfo, "INFO"},
		{log.LevelNotice, "NOTICE"},
		{log.LevelWarn, "WARNING"},
		{log.LevelError, "ERROR"},
		{log.LevelError + 1, "ERROR"},
		{log.LevelCritical, "CRITICAL"},
		{log.LevelAlert, "ALERT"},
		{log.LevelEmergency, "EMERGENCY"},
		{log.LevelEmergency + 10, "EMERGENCY"},
	}
	for _, tt := range tests {
		var buf bytes.Buffer
		log.New(&buf, log.Options{Schema: log.SchemaGCP, Level: slog.Level(-100)}).Log(context.Background(), tt.level, "m")
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
	log.New(&buf, log.Options{Format: log.FormatText, Backend: log.BackendSlog}).WarnContext(spanCtx(true), "hello", "k", "v")
	out := buf.String()
	for _, want := range []string{"level=WARN", `msg=hello`, "k=v", "source=", "log_test.go:", "span_id=00f067aa0ba902b7"} {
		if !strings.Contains(out, want) {
			t.Errorf("slog: missing %q in %q", want, out)
		}
	}
	buf.Reset()
	log.New(&buf, log.Options{Format: log.FormatText}).WarnContext(spanCtx(true), "hello", "k", "v")
	out = buf.String()
	for _, want := range []string{"\tWARN\t", "\thello\t", `"k": "v"`, `"source": "`, "log_test.go:", `"span_id": "00f067aa0ba902b7"`} {
		if !strings.Contains(out, want) && !strings.Contains(out, strings.ReplaceAll(want, "\t", " ")) {
			t.Errorf("zap console: missing %q in %q", want, out)
		}
	}
}

func TestParseLevel(t *testing.T) {
	tests := []struct {
		in      string
		want    slog.Level
		wantErr bool
	}{
		{"debug", log.LevelDebug, false},
		{"INFO", log.LevelInfo, false},
		{"Notice", log.LevelNotice, false},
		{"warn", log.LevelWarn, false},
		{"WARNING", log.LevelWarn, false},
		{"error", log.LevelError, false},
		{"critical", log.LevelCritical, false},
		{"alert", log.LevelAlert, false},
		{"emergency", log.LevelEmergency, false},
		{"INFO+2", slog.Level(2), false},
		{"verbose", 0, true},
		{"", 0, true},
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
	t.Setenv("LOG_SCHEMA", "")
	opts, err := log.OptionsFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if opts.Level.Level() != log.LevelDebug || opts.Format != log.FormatText || opts.Schema.Name != "gcp" {
		t.Fatalf("got %+v", opts)
	}
	if _, ok := opts.Level.(*slog.LevelVar); !ok {
		t.Error("Level should be a *slog.LevelVar")
	}

	t.Setenv("LOG_SCHEMA", "AWS")
	if opts, _ := log.OptionsFromEnv(); opts.Schema.Name != "aws" {
		t.Error("LOG_SCHEMA must override detection")
	}

	t.Setenv("LOG_LEVEL", "loud")
	t.Setenv("LOG_FORMAT", "xml")
	t.Setenv("LOG_SCHEMA", "splunk")
	opts, err = log.OptionsFromEnv()
	if err == nil || !strings.Contains(err.Error(), "LOG_LEVEL") || !strings.Contains(err.Error(), "LOG_FORMAT") || !strings.Contains(err.Error(), "LOG_SCHEMA") {
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
	), log.Options{})
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

// TestBackendParity checks that zap and slog write the same JSON (key order
// aside) for every schema.
func TestBackendParity(t *testing.T) {
	type stringer struct{ s string }
	for _, schema := range []log.Schema{log.SchemaOTel, log.SchemaGCP, log.SchemaAWS} {
		out := map[log.Backend][]map[string]any{}
		for _, b := range []log.Backend{log.BackendZap, log.BackendSlog} {
			var buf bytes.Buffer
			logger := log.New(&buf, log.Options{
				Backend: b, Schema: schema, Level: slog.Level(-8),
				ContextAttrs: func(context.Context) []slog.Attr { return []slog.Attr{slog.String("request_id", "r1")} },
				ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
					if a.Key == "password" {
						return slog.String("password", "***")
					}
					return a
				},
			})
			ctx := spanCtx(true)
			logger.With("svc", "a").WithGroup("req").With("id", 7).InfoContext(ctx, "grouped",
				slog.Group("inner", slog.Bool("ok", true), slog.Group("empty")), slog.Duration("took", 1500*time.Millisecond))
			logger.Log(ctx, log.LevelCritical, "critical", log.Err(errors.New("boom")), slog.Any("m", map[string]int{"a": 1}),
				slog.Group("", slog.String("inlined", "x")), slog.String("password", "p"), slog.Uint64("u", 3), slog.Float64("f", 0.5))
			logger.Log(ctx, log.LevelNotice, "notice", slog.String(schema.MessageKey, "spoof"), slog.Any("nil", nil), slog.Attr{})
			logger.Debug("debug", "t", time.Date(2026, 1, 2, 3, 4, 5, 6, time.UTC), "s", stringer{"x"})
			for _, m := range decode(t, &buf) {
				delete(m, schema.TimeKey)
				if src, ok := m[schema.SourceKey]; ok {
					if s, ok := src.(string); ok { // zap and slog agree on file:line
						m[schema.SourceKey] = s[strings.LastIndex(s, "/")+1:]
					}
				}
				delete(m, "t") // zap writes user times in UTC, slog in local time
				out[b] = append(out[b], m)
			}
		}
		if !reflect.DeepEqual(out[log.BackendZap], out[log.BackendSlog]) {
			t.Errorf("%s:\nzap  %v\nslog %v", schema.Name, out[log.BackendZap], out[log.BackendSlog])
		}
	}
}

func logError(l *slog.Logger) { l.Error("boom", log.Err(errors.New("db down"))) }

func TestStackTrace(t *testing.T) {
	for _, be := range []log.Backend{log.BackendZap, log.BackendSlog} {
		for _, schema := range []log.Schema{log.SchemaOTel, log.SchemaGCP, log.SchemaAWS} {
			var buf bytes.Buffer
			l := log.New(&buf, log.Options{Backend: be, Schema: schema, NoSource: true})
			logError(l)
			l.Warn("warn")
			lines := decode(t, &buf)
			st, _ := lines[0][schema.StackKey].(string)
			first := strings.SplitN(st, "\n", 3)
			if len(first) < 3 || !strings.HasPrefix(first[0], "goroutine ") || !strings.HasSuffix(first[0], " [running]:") ||
				!strings.HasPrefix(first[1], "github.com/dungnh3/kitchen/log_test.logError(") || !strings.Contains(first[2], "log_test.go:") {
				t.Errorf("%s/%s: stack does not start at the caller in debug.Stack format:\n%s", be, schema.Name, st)
			}
			if _, ok := lines[1][schema.StackKey]; ok {
				t.Errorf("%s/%s: stack on a WARN record", be, schema.Name)
			}
		}
	}
	var buf bytes.Buffer
	l := log.New(&buf, log.Options{NoStackTrace: true})
	logError(l)
	l2 := log.New(&buf, log.Options{StackTraceLevel: log.LevelWarn})
	l2.Warn("warn")
	lines := decode(t, &buf)
	if _, ok := lines[0][log.KeyExceptionStacktrace]; ok {
		t.Error("NoStackTrace ignored")
	}
	if _, ok := lines[1][log.KeyExceptionStacktrace]; !ok {
		t.Error("StackTraceLevel ignored")
	}
}

func TestContextAttrsDeduped(t *testing.T) {
	var buf bytes.Buffer
	l := log.New(&buf, log.Options{NoSource: true, ContextAttrs: func(context.Context) []slog.Attr {
		return []slog.Attr{slog.String("request_id", "from-ctx"), slog.String("user_id", "u1")}
	}})
	l.InfoContext(context.Background(), "m", slog.String("request_id", "explicit"))
	raw := buf.String()
	m := decode(t, &buf)[0]
	if strings.Count(raw, `"request_id":`) != 1 || m["request_id"] != "explicit" || m["user_id"] != "u1" {
		t.Fatalf("got %s", raw)
	}
}

func TestNewZap(t *testing.T) {
	var buf bytes.Buffer
	z := log.NewZap(&buf, log.Options{Schema: log.SchemaGCP})
	z.Warn("quota low", zap.String("user_id", "u1"))
	z.Debug("hidden")
	lines := decode(t, &buf)
	if len(lines) != 1 {
		t.Fatalf("lines = %d", len(lines))
	}
	m := lines[0]
	ts, _ := m["time"].(string)
	if m["severity"] != "WARNING" || m["message"] != "quota low" || m["user_id"] != "u1" ||
		!strings.HasSuffix(ts, "Z") || !strings.Contains(fmt.Sprint(m[log.KeyGCPSourceLocation]), "log_test.go") {
		t.Fatalf("got %v", m)
	}
}

func TestOptionsBackendFromEnv(t *testing.T) {
	t.Setenv("LOG_BACKEND", "")
	if o, _ := log.OptionsFromEnv(); o.Backend != log.BackendZap {
		t.Error("default backend must be zap")
	}
	t.Setenv("LOG_BACKEND", "SLOG")
	if o, _ := log.OptionsFromEnv(); o.Backend != log.BackendSlog {
		t.Error("LOG_BACKEND=slog")
	}
	t.Setenv("LOG_BACKEND", "logrus")
	if _, err := log.OptionsFromEnv(); err == nil || !strings.Contains(err.Error(), "LOG_BACKEND") {
		t.Errorf("err = %v", err)
	}
}

func BenchmarkBackends(b *testing.B) {
	for _, be := range []log.Backend{log.BackendZap, log.BackendSlog} {
		for _, src := range []bool{false, true} {
			name := string(be)
			if src {
				name += "+source"
			}
			b.Run(name, func(b *testing.B) {
				logger := log.New(discard{}, log.Options{Backend: be, NoSource: !src})
				ctx := log.WithAttrs(spanCtx(true), slog.String("x-request-id", "r1"))
				b.ReportAllocs()
				for b.Loop() {
					logger.InfoContext(ctx, "message", slog.String("k", "v"), slog.Int("n", 1))
				}
			})
		}
	}
}

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
