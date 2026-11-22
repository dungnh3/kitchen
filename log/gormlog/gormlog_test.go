package gormlog_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"go.uber.org/goleak"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/dungnh3/kitchen/log/gormlog"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

func newLogger(t *testing.T, cfg gormlog.Config) (*gormlog.Logger, func() []map[string]any) {
	t.Helper()
	var buf bytes.Buffer
	cfg.Logger = slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{AddSource: true, Level: slog.LevelDebug}))
	return gormlog.New(cfg), func() []map[string]any {
		var out []map[string]any
		for line := range strings.Lines(buf.String()) {
			var m map[string]any
			if err := json.Unmarshal([]byte(line), &m); err != nil {
				t.Fatal(err)
			}
			out = append(out, m)
		}
		buf.Reset()
		return out
	}
}

func sqlFn(called *bool) func() (string, int64) {
	return func() (string, int64) {
		if called != nil {
			*called = true
		}
		return "SELECT * FROM users WHERE id = $1", 1
	}
}

func TestTrace(t *testing.T) {
	ctx := context.Background()
	recent := time.Now()
	slow := time.Now().Add(-time.Second)
	tests := []struct {
		name      string
		cfg       gormlog.Config
		begin     time.Time
		err       error
		wantLevel string
		wantMsg   string
	}{
		{"error", gormlog.Config{}, recent, errors.New("boom"), "ERROR", "gorm query failed"},
		{"not found logged", gormlog.Config{}, recent, gorm.ErrRecordNotFound, "ERROR", "gorm query failed"},
		{"not found ignored", gormlog.Config{IgnoreRecordNotFound: true}, recent, gorm.ErrRecordNotFound, "", ""},
		{"slow", gormlog.Config{}, slow, nil, "WARN", "gorm slow query"},
		{"slow disabled", gormlog.Config{SlowThreshold: -1}, slow, nil, "", ""},
		{"fast default", gormlog.Config{}, recent, nil, "", ""},
		{"fast info", gormlog.Config{Level: logger.Info}, recent, nil, "INFO", "gorm query"},
		{"silent", gormlog.Config{Level: logger.Silent}, recent, errors.New("x"), "", ""},
		{"error level hides slow", gormlog.Config{Level: logger.Error}, slow, nil, "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l, read := newLogger(t, tt.cfg)
			var called bool
			l.Trace(ctx, tt.begin, sqlFn(&called), tt.err)
			got := read()
			if tt.wantLevel == "" {
				if len(got) != 0 || called {
					t.Fatalf("expected nothing, got %v (fc called=%v)", got, called)
				}
				return
			}
			if len(got) != 1 || got[0]["level"] != tt.wantLevel || got[0]["msg"] != tt.wantMsg {
				t.Fatalf("got %v", got)
			}
			if got[0][gormlog.KeySQL] != "SELECT * FROM users WHERE id = $1" || got[0][gormlog.KeyRows] != 1.0 {
				t.Fatalf("attrs %v", got[0])
			}
			src, _ := got[0]["source"].(map[string]any)
			if !strings.HasSuffix(src["file"].(string), "gormlog_test.go") {
				t.Fatalf("source = %v", got[0]["source"])
			}
		})
	}
}

func TestLevelMethodsAndLogMode(t *testing.T) {
	l, read := newLogger(t, gormlog.Config{})
	ctx := context.Background()
	l.Info(ctx, "hidden %d", 1)
	l.Warn(ctx, "warn %d", 2)
	l.Error(ctx, "err %d", 3)
	got := read()
	if len(got) != 2 || got[0]["msg"] != "warn 2" || got[1]["msg"] != "err 3" {
		t.Fatalf("got %v", got)
	}
	l.LogMode(logger.Info).Info(ctx, "info %s", "x")
	if got := read(); len(got) != 1 || got[0]["msg"] != "info x" {
		t.Fatalf("got %v", got)
	}
	l.Info(ctx, "still hidden") // LogMode returned a copy
	if got := read(); len(got) != 0 {
		t.Fatalf("LogMode mutated receiver: %v", got)
	}
}

func TestParamsFilter(t *testing.T) {
	l := gormlog.New(gormlog.Config{})
	if _, p := l.ParamsFilter(context.Background(), "q", 1, "secret"); p != nil {
		t.Fatalf("params leaked: %v", p)
	}
	l = gormlog.New(gormlog.Config{LogParams: true})
	if _, p := l.ParamsFilter(context.Background(), "q", 1); len(p) != 1 {
		t.Fatalf("params = %v", p)
	}
}

func TestDisabledSlogLevelSkipsSQL(t *testing.T) {
	var buf bytes.Buffer
	l := gormlog.New(gormlog.Config{
		Logger: slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelError})),
		Level:  logger.Info,
	})
	var called bool
	l.Trace(context.Background(), time.Now(), sqlFn(&called), nil)
	if called || buf.Len() != 0 {
		t.Fatal("fc called or output written for a disabled level")
	}
}
