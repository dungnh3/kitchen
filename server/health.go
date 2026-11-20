package server

import (
	"context"
	"encoding/json"
	"net/http"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

// Check is a named readiness check. Func should return quickly; it runs with
// a 5s timeout on every readiness probe.
type Check struct {
	Name string
	Func func(ctx context.Context) error
}

const checkTimeout = 5 * time.Second

// health tracks liveness and readiness.
type probes struct {
	ready   atomic.Bool
	checks  []Check
	version string
}

func newProbes(checks []Check, version string) *probes {
	if version == "" {
		if bi, ok := debug.ReadBuildInfo(); ok {
			version = bi.Main.Version
		}
	}
	return &probes{checks: checks, version: version}
}

// failed runs every check concurrently and returns the failures by name.
func (h *probes) failed(ctx context.Context) map[string]string {
	if !h.ready.Load() {
		return map[string]string{"server": "not serving"}
	}
	if len(h.checks) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, checkTimeout)
	defer cancel()
	var (
		mu  sync.Mutex
		out map[string]string
		wg  sync.WaitGroup
	)
	for _, c := range h.checks {
		wg.Go(func() {
			if err := c.Func(ctx); err != nil {
				mu.Lock()
				defer mu.Unlock()
				if out == nil {
					out = map[string]string{}
				}
				out[c.Name] = err.Error()
			}
		})
	}
	wg.Wait()
	return out
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// livez is always OK while the process serves HTTP: a draining server must
// not be restarted.
func (h *probes) livez(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"message": "OK"})
}

// readyz reports failures with their messages; it is meant for the admin
// port only.
func (h *probes) readyz(w http.ResponseWriter, r *http.Request) {
	if f := h.failed(r.Context()); len(f) > 0 {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"message": "not ready", "failed": f})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"message": "OK"})
}

// publicReady is readyz without failure details, for the public port.
func (h *probes) publicReady(w http.ResponseWriter, r *http.Request) {
	if len(h.failed(r.Context())) > 0 {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"message": "not ready"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"message": "OK"})
}

func (h *probes) versionz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"version": h.version})
}

// legacyPaths are the probe paths of the old kitchen HealthService, served
// on the public HTTP port without logging or tracing.
func (h *probes) legacyPaths() map[string]http.HandlerFunc {
	return map[string]http.HandlerFunc{
		"/health":     h.livez,
		"/api/status": h.livez,
		"/ready":      h.publicReady,
		"/version":    h.versionz,
	}
}
