package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"
)

// Registry maps job kinds to their workers. Build it at start-up with
// [Register]; a [Client] only claims the kinds in its registry, so a job
// enqueued by a newer deploy waits instead of failing on an older worker.
type Registry struct {
	mu      sync.RWMutex
	entries map[string]*entry
}

type entry struct {
	kind    string
	timeout time.Duration // 0: client default; <0: none
	work    func(ctx context.Context, row Row) error
}

// NewRegistry returns an empty Registry.
func NewRegistry() *Registry { return &Registry{entries: map[string]*entry{}} }

// Register adds w as the worker of T's kind. It fails when the kind is
// invalid or already registered.
func Register[T Args](r *Registry, w Worker[T]) error {
	if r == nil || w == nil {
		return errors.New("jobs: nil registry or worker")
	}
	var zero T
	kind := zero.Kind()
	if !kindRE.MatchString(kind) {
		return fmt.Errorf("jobs: invalid kind %q", kind)
	}
	e := &entry{kind: kind}
	if t, ok := any(w).(interface{ Timeout() time.Duration }); ok {
		e.timeout = t.Timeout()
	}
	e.work = func(ctx context.Context, row Row) error {
		j := &Job[T]{Row: row}
		if err := json.Unmarshal(row.RawArgs, &j.Args); err != nil {
			return Cancel(fmt.Errorf("decode args of kind %q: %w", kind, err))
		}
		return w.Work(ctx, j)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.entries[kind]; dup {
		return fmt.Errorf("jobs: kind %q already registered", kind)
	}
	r.entries[kind] = e
	return nil
}

func (r *Registry) get(kind string) *entry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.entries[kind]
}

// Kinds returns the registered kinds, sorted.
func (r *Registry) Kinds() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.entries))
	for k := range r.entries {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}
