package log

import (
	"context"
	"log/slog"
	"slices"

	"go.opentelemetry.io/otel/trace"
)

type attrsKey struct{}

// WithAttrs returns a copy of ctx that carries attrs in addition to any
// attributes already stored in it. Handlers built by this package add them to
// every record logged with that context (InfoContext, ErrorContext, ...).
//
// Use it for request-scoped values such as a request id that a transport
// interceptor copies from incoming metadata.
func WithAttrs(ctx context.Context, attrs ...slog.Attr) context.Context {
	if len(attrs) == 0 {
		return ctx
	}
	return context.WithValue(ctx, attrsKey{}, slices.Concat(AttrsFromContext(ctx), attrs))
}

// AttrsFromContext returns the attributes stored by [WithAttrs]. The result
// must not be modified.
func AttrsFromContext(ctx context.Context) []slog.Attr {
	if ctx == nil {
		return nil
	}
	as, _ := ctx.Value(attrsKey{}).([]slog.Attr)
	return slices.Clip(as)
}

// NewContextHandler wraps next so that each record logged with a context gets:
//
//   - the Cloud Logging trace keys when the context carries a valid span
//     (see [KeyTrace], [KeySpanID], [KeyTraceSampled]); gcpProjectID, when not
//     empty, selects the projects/<id>/traces/<trace-id> form;
//   - the attributes stored with [WithAttrs].
//
// These attributes are always written at the top level of the record, even
// when the logger was derived with WithGroup. Use it to wrap handlers that do
// not come from [NewHandler], for example a [slog.MultiHandler].
func NewContextHandler(next slog.Handler, gcpProjectID string) slog.Handler {
	return &contextHandler{next: next, projectID: gcpProjectID}
}

type groupOrAttrs struct {
	group string      // non-empty: open a group
	attrs []slog.Attr // otherwise: attributes
}

type contextHandler struct {
	next      slog.Handler
	projectID string
	// goas buffers WithGroup/WithAttrs calls made after the first group, so
	// that context attributes can still be emitted at the top level.
	goas []groupOrAttrs
}

func (h *contextHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return h.next.Enabled(ctx, l)
}

func (h *contextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h
	}
	if len(h.goas) == 0 {
		// No group open: let the inner handler pre-format the attributes.
		return &contextHandler{next: h.next.WithAttrs(attrs), projectID: h.projectID}
	}
	return h.with(groupOrAttrs{attrs: slices.Clone(attrs)})
}

func (h *contextHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	return h.with(groupOrAttrs{group: name})
}

func (h *contextHandler) with(g groupOrAttrs) *contextHandler {
	return &contextHandler{
		next:      h.next,
		projectID: h.projectID,
		goas:      append(slices.Clip(h.goas), g),
	}
}

func (h *contextHandler) Handle(ctx context.Context, r slog.Record) error {
	top := h.contextAttrs(ctx)
	if len(h.goas) == 0 {
		if len(top) > 0 {
			r = r.Clone()
			r.AddAttrs(top...)
		}
		return h.next.Handle(ctx, r)
	}

	nr := slog.NewRecord(r.Time, r.Level, r.Message, r.PC)
	nr.AddAttrs(top...)
	inner := make([]slog.Attr, 0, r.NumAttrs())
	r.Attrs(func(a slog.Attr) bool {
		inner = append(inner, a)
		return true
	})
	for i := len(h.goas) - 1; i >= 0; i-- {
		g := h.goas[i]
		if g.group != "" {
			inner = []slog.Attr{{Key: g.group, Value: slog.GroupValue(inner...)}}
		} else {
			inner = slices.Concat(g.attrs, inner)
		}
	}
	nr.AddAttrs(inner...)
	return h.next.Handle(ctx, nr)
}

func (h *contextHandler) contextAttrs(ctx context.Context) []slog.Attr {
	if ctx == nil {
		return nil
	}
	extra := AttrsFromContext(ctx)
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return extra
	}
	tid := sc.TraceID().String()
	if h.projectID != "" {
		tid = "projects/" + h.projectID + "/traces/" + tid
	}
	out := make([]slog.Attr, 0, 3+len(extra))
	out = append(out,
		slog.String(KeyTrace, tid),
		slog.String(KeySpanID, sc.SpanID().String()),
		slog.Bool(KeyTraceSampled, sc.IsSampled()),
	)
	return append(out, extra...)
}
