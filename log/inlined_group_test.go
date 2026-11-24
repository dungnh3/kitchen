package log_test

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/dungnh3/kitchen/log"
)

// An inlined group (empty key) must not smuggle schema-owned keys to the
// top level.
func TestInlinedGroupCannotSpoof(t *testing.T) {
	for _, backend := range []log.Backend{log.BackendZap, log.BackendSlog} {
		var buf bytes.Buffer
		l := log.New(&buf, log.Options{Backend: backend, Schema: log.SchemaGCP, NoSource: true})
		spoof := slog.Group("", slog.String("severity", "EMERGENCY"), slog.String("message", "spoof"),
			slog.Group("", slog.String("logging.googleapis.com/trace", "evil")))
		l.InfoContext(context.Background(), "hi", spoof)
		l.With(spoof).InfoContext(context.Background(), "with")
		for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
			for _, k := range []string{`"severity":`, `"message":`} {
				if n := strings.Count(line, k); n != 1 {
					t.Errorf("%s: %s appears %d times: %s", backend, k, n, line)
				}
			}
			if strings.Contains(line, `"logging.googleapis.com/trace":"evil"`) || !strings.Contains(line, `"attr.severity":"EMERGENCY"`) {
				t.Errorf("%s: spoofed keys not renamed: %s", backend, line)
			}
		}
	}
}
