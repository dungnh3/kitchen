package server

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dungnh3/kitchen/errs"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// Check is a named readiness check. Func should return quickly; it runs with
// a 5s timeout. Results are cached for one second and concurrent probes
// share one run, so probe traffic cannot multiply the load on dependencies.
type Check struct {
	Name string
	Func func(ctx context.Context) error
}

const checkTTL = time.Second

// checkTimeout bounds one readiness run (a variable for tests).
var checkTimeout = 5 * time.Second

// probes tracks liveness and readiness.
type probes struct {
	timeout time.Duration // bounds one run of the checks
	ready   atomic.Bool
	checks  []Check
	version string
	now     func() time.Time

	mu       sync.Mutex
	cached   map[string]string // last check failures
	cachedAt time.Time
	running  chan struct{} // closed when the in-flight run finishes; nil when idle
}

func newProbes(checks []Check, version string) *probes {
	if version == "" {
		if bi, ok := debug.ReadBuildInfo(); ok {
			version = bi.Main.Version
		}
	}
	return &probes{checks: checks, version: version, now: time.Now, timeout: checkTimeout}
}

// failed returns the failures by name: the server state, then the checks
// (cached for checkTTL; concurrent callers share one run).
func (h *probes) failed(ctx context.Context) map[string]string {
	if !h.ready.Load() {
		return map[string]string{"server": "not serving"}
	}
	if len(h.checks) == 0 {
		return nil
	}
	for {
		h.mu.Lock()
		if !h.cachedAt.IsZero() && h.now().Sub(h.cachedAt) < checkTTL {
			out := h.cached
			h.mu.Unlock()
			return out
		}
		if wait := h.running; wait != nil {
			h.mu.Unlock()
			select {
			case <-wait:
				continue
			case <-ctx.Done():
				return map[string]string{"server": "readiness check timed out"}
			}
		}
		done := make(chan struct{})
		h.running = done
		h.mu.Unlock()

		// The run is shared: detach it from this caller's cancellation.
		out := h.runChecks(context.WithoutCancel(ctx))
		h.mu.Lock()
		h.cached, h.cachedAt, h.running = out, h.now(), nil
		h.mu.Unlock()
		close(done)
		return out
	}
}

// runChecks runs every check concurrently and returns the failures by name.
func (h *probes) runChecks(ctx context.Context) map[string]string {
	ctx, cancel := context.WithTimeout(ctx, h.timeout)
	defer cancel()
	var (
		mu  sync.Mutex
		out map[string]string
		wg  sync.WaitGroup
	)
	fail := func(name, msg string) {
		mu.Lock()
		defer mu.Unlock()
		if out == nil {
			out = map[string]string{}
		}
		if _, dup := out[name]; !dup {
			out[name] = msg
		}
	}
	pending := make(map[string]bool, len(h.checks))
	for _, c := range h.checks {
		pending[c.Name] = true
	}
	for _, c := range h.checks {
		wg.Go(func() {
			// A panicking check is a failure, not a crash: /readyz is public.
			defer func() {
				if p := recover(); p != nil {
					fail(c.Name, fmt.Sprintf("panic: %v", p))
				}
				mu.Lock()
				delete(pending, c.Name)
				mu.Unlock()
			}()
			if err := c.Func(ctx); err != nil {
				fail(c.Name, err.Error())
			}
		})
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		// A check that ignores its ctx must not hold readiness forever; it
		// is reported as timed out (its goroutine ends when it returns).
		mu.Lock()
		names := make([]string, 0, len(pending))
		for n := range pending {
			names = append(names, n)
		}
		mu.Unlock()
		for _, n := range names {
			fail(n, "timeout: "+ctx.Err().Error())
		}
	}
	mu.Lock()
	defer mu.Unlock()
	return maps.Clone(out)
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

// readyz reports failures with their messages; it is served on the admin
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

// publicPaths are the probes served on the public HTTP port, before any
// middleware (no logging, tracing or auth). The version is not public.
func (h *probes) publicPaths() map[string]http.HandlerFunc {
	return map[string]http.HandlerFunc{
		"/livez":  h.livez,
		"/readyz": h.publicReady,
	}
}

// maxHeaderListSize bounds the metadata a gRPC client may send (64 KiB).
const maxHeaderListSize = 64 << 10

// drainingHealth is the grpc.health.v1 server whose Watch streams end when
// the server starts draining (the stock Watch only sends NOT_SERVING and
// would hold GracefulStop until the shutdown deadline).
type drainingHealth struct {
	*health.Server
	drain <-chan struct{}
}

func (h drainingHealth) Watch(req *healthpb.HealthCheckRequest, stream grpc.ServerStreamingServer[healthpb.HealthCheckResponse]) error {
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	go func() {
		select {
		case <-h.drain:
			cancel()
		case <-ctx.Done():
		}
	}()
	err := h.Server.Watch(req, watchStream{ServerStreamingServer: stream, ctx: ctx})
	select {
	case <-h.drain:
		return errs.ErrServiceDraining
	default:
		return err
	}
}

type watchStream struct {
	grpc.ServerStreamingServer[healthpb.HealthCheckResponse]
	ctx context.Context
}

func (w watchStream) Context() context.Context { return w.ctx }
