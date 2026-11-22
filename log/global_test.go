package log

import (
	"bytes"
	"context"
	stdlog "log"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
)

// syncBuf is a goroutine-safe buffer.
type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// The process logger: one stable *Logger that follows Setup, also for
// loggers taken or derived before Setup; slog's default and the standard
// library's log package write the kitchen format too.
func TestDefaultSetupSwap(t *testing.T) {
	oldSlog := slog.Default()
	t.Cleanup(func() { slog.SetDefault(oldSlog); installed.Store(nil) })

	early := Default()
	derived := early.With(String("component", "early"))
	if Default() != early {
		t.Fatal("Default must be stable")
	}

	var buf syncBuf
	SetDefault(New(&buf, Options{NoSource: true, Schema: SchemaGCP}))
	early.Info("from early")
	derived.Info("from derived")
	slog.Info("from slog") //nolint:sloglint // the global slog logger is what this checks
	stdlog.Print("from stdlib")
	out := buf.String()
	for _, msg := range []string{"from early", "from derived", "from slog", "from stdlib"} {
		if !strings.Contains(out, `"message":"`+msg+`"`) {
			t.Errorf("%q not written in the kitchen format: %s", msg, out)
		}
	}
	if !strings.Contains(out, `"component":"early"`) || strings.Count(out, `"severity":`) != 4 {
		t.Errorf("derived attrs or schema lost: %s", out)
	}

	// A second Setup reaches the same loggers (cached derived handlers too).
	var buf2 syncBuf
	SetDefault(New(&buf2, Options{NoSource: true}))
	derived.Info("again")
	if !strings.Contains(buf2.String(), `"message":"again"`) || !strings.Contains(buf2.String(), `"component":"early"`) {
		t.Errorf("re-setup not followed: %s", buf2.String())
	}
	SetDefault(nil)             // ignored
	SetDefault(Default())       // ignored: no self loop
	derived.Info("still there") // must not recurse
	if !strings.Contains(buf2.String(), "still there") {
		t.Error("SetDefault(Default()) broke the logger")
	}
}

func TestSetupValidates(t *testing.T) {
	if _, err := Setup(Options{Backend: "nope"}); err == nil {
		t.Error("unknown backend accepted")
	}
	if _, err := Setup(Options{Format: "xml"}); err == nil {
		t.Error("unknown format accepted")
	}
}

// Concurrent logging while the handler is swapped is race free.
func TestDefaultConcurrent(t *testing.T) {
	oldSlog := slog.Default()
	t.Cleanup(func() { slog.SetDefault(oldSlog); installed.Store(nil) })
	var buf syncBuf
	l := Default().With(String("k", "v"))
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() {
			for range 50 {
				l.InfoContext(context.Background(), "x")
				if i == 0 {
					SetDefault(New(&buf, Options{}))
				}
			}
		})
	}
	wg.Wait()
}

func TestDiscardAndOrDefault(t *testing.T) {
	Discard().Error("nothing")
	if OrDefault(nil) != Default() {
		t.Error("OrDefault(nil)")
	}
	d := Discard()
	if OrDefault(d) != d {
		t.Error("OrDefault(l)")
	}
}

// Options.Handlers adds sinks after the kitchen handler: they get records
// at or above Level with the request attributes; the kitchen output keeps its
// schema.
func TestOptionsHandlers(t *testing.T) {
	var main, sink syncBuf
	extra := slog.NewJSONHandler(&sink, &slog.HandlerOptions{Level: slog.LevelDebug})
	l := New(&main, Options{Schema: SchemaGCP, Level: LevelInfo, Handlers: []Handler{extra, nil}})
	ctx := WithAttrs(context.Background(), String("request_id", "r1"))
	l.DebugContext(ctx, "hidden")
	l.With(String("svc", "a")).InfoContext(ctx, "both")
	if !strings.Contains(main.String(), `"severity":"INFO"`) || !strings.Contains(main.String(), `"message":"both"`) {
		t.Fatalf("kitchen output: %s", main.String())
	}
	if s := sink.String(); !strings.Contains(s, `"msg":"both"`) || !strings.Contains(s, `"request_id":"r1"`) ||
		!strings.Contains(s, `"svc":"a"`) || strings.Contains(s, "hidden") {
		t.Fatalf("sink output: %s", s)
	}
}

// Setup writes to Options.Output; LOG_OUTPUT=stderr selects stderr from the
// environment (stdio programs: MCP servers, CLIs).
func TestSetupOutput(t *testing.T) {
	oldSlog := slog.Default()
	t.Cleanup(func() { slog.SetDefault(oldSlog); installed.Store(nil) })
	var buf syncBuf
	l, err := Setup(Options{Output: &buf, NoSource: true})
	if err != nil {
		t.Fatal(err)
	}
	l.Info("to output")
	if !strings.Contains(buf.String(), `"message":"to output"`) {
		t.Fatalf("Output not used: %q", buf.String())
	}
	t.Setenv("LOG_OUTPUT", "stderr")
	if opts, err := OptionsFromEnv(); err != nil || opts.Output != os.Stderr {
		t.Fatalf("LOG_OUTPUT=stderr: %v %v", opts.Output, err)
	}
	t.Setenv("LOG_OUTPUT", "file")
	if _, err := OptionsFromEnv(); err == nil {
		t.Fatal("invalid LOG_OUTPUT accepted")
	}
}
