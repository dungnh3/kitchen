package log

import (
	"context"
	"log/slog"
	"slices"

	"go.opentelemetry.io/otel/trace"
)

type attrsKey struct{}

// WithAttrs returns a copy of ctx that carries attrs in addition to the
// attributes already stored in it; an attribute replaces a stored one with
// the same key. Handlers built by this package add them to every record
// logged with that context (InfoContext, ErrorContext, ...).
//
// Package kitchen/context stores the request id and the user id this way, so
// every kitchen logger writes them; use it as well for scoped values such as
// a job id in a worker.
func WithAttrs(ctx context.Context, attrs ...slog.Attr) context.Context {
	if len(attrs) == 0 {
		return ctx
	}
	prev := AttrsFromContext(ctx)
	out := make([]slog.Attr, 0, len(prev)+len(attrs))
	for _, p := range prev {
		if !slices.ContainsFunc(attrs, func(a slog.Attr) bool { return a.Key == p.Key }) {
			out = append(out, p)
		}
	}
	return context.WithValue(ctx, attrsKey{}, append(out, attrs...))
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

// NewContextHandler wraps next so that each record gets, at the top level
// even when the logger was derived with WithGroup:
//
//   - the schema's trace-correlation attributes when the context carries a
//     valid span;
//   - the attributes returned by opts.ContextAttrs, then those stored with
//     [WithAttrs]; one whose key the record's own attributes already have
//     at the top level is skipped, so the record's value wins (attributes
//     added with Logger.With are not checked);
//   - a stack trace under Schema.StackKey when the level is at least
//     opts.StackTraceLevel (default ERROR), unless opts.NoStackTrace.
//
// It uses opts.Schema, ContextAttrs, StackTraceLevel and NoStackTrace; other
// fields are ignored.
//
// A top-level user attribute whose key the schema owns (its time, level,
// message, source and trace keys, slog's built-in keys and
// Schema.Reserved) is renamed to "attr.<key>", so logged data can never
// override the time, level, message or trace of a record.
//
// next should encode the schema's built-in keys; use it to wrap handlers
// that do not come from [NewHandler], for example a [slog.MultiHandler].
func NewContextHandler(next slog.Handler, opts Options) slog.Handler {
	schema := opts.Schema.withDefaults()
	h := &contextHandler{next: next, reserved: schema.reserved(), trace: schema.Trace, extract: opts.ContextAttrs}
	if !opts.NoStackTrace && schema.StackKey != "" {
		h.stackKey = schema.StackKey
		h.stackLevel = opts.StackTraceLevel
		if h.stackLevel == nil {
			h.stackLevel = LevelError
		}
	}
	return h
}

// reservedPrefix is prepended to user attributes that collide with the schema.
const reservedPrefix = "attr."

type groupOrAttrs struct {
	group string      // non-empty: open a group
	attrs []slog.Attr // otherwise: attributes
}

type contextHandler struct {
	next     slog.Handler
	reserved map[string]bool // read-only
	trace    func(trace.SpanContext) []slog.Attr
	extract  func(context.Context) []slog.Attr
	// stackKey is "" when stack traces are off.
	stackKey   string
	stackLevel slog.Leveler
	// goas buffers WithGroup/WithAttrs calls made after the first group, so
	// that context attributes can still be emitted at the top level.
	goas []groupOrAttrs
}

// inlined reports whether a is a group with an empty key, whose attributes
// slog and zap write at the level of a itself.
func inlined(a slog.Attr) bool {
	return a.Key == "" && a.Value.Resolve().Kind() == slog.KindGroup
}

// unsafe reports whether a top-level attribute needs rewriting: a reserved
// key, or an inlined group (which could hide one).
func (h *contextHandler) unsafe(a slog.Attr) bool { return h.reserved[a.Key] || inlined(a) }

// appendSafe appends a to dst with reserved keys renamed and inlined groups
// flattened (recursively), so no user attribute reaches the top level under
// a key the schema owns.
func (h *contextHandler) appendSafe(dst []slog.Attr, a slog.Attr) []slog.Attr {
	if inlined(a) {
		for _, ga := range a.Value.Resolve().Group() {
			dst = h.appendSafe(dst, ga)
		}
		return dst
	}
	if h.reserved[a.Key] {
		a.Key = reservedPrefix + a.Key
	}
	return append(dst, a)
}

// safe renames reserved keys and flattens inlined groups; it returns attrs
// itself when nothing changes.
func (h *contextHandler) safe(attrs []slog.Attr) []slog.Attr {
	i := slices.IndexFunc(attrs, h.unsafe)
	if i < 0 {
		return attrs
	}
	out := slices.Clone(attrs[:i])
	for _, a := range attrs[i:] {
		out = h.appendSafe(out, a)
	}
	return out
}

func (h *contextHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return h.next.Enabled(ctx, l)
}

func (h *contextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h
	}
	c := *h
	if len(h.goas) == 0 {
		// No group open: let the inner handler pre-format the attributes.
		c.next = h.next.WithAttrs(h.safe(attrs))
		return &c
	}
	c.goas = append(slices.Clip(h.goas), groupOrAttrs{attrs: slices.Clone(attrs)})
	return &c
}

func (h *contextHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	c := *h
	c.goas = append(slices.Clip(h.goas), groupOrAttrs{group: name})
	return &c
}

func (h *contextHandler) Handle(ctx context.Context, r slog.Record) error {
	top := h.contextAttrs(ctx)
	if h.stackKey != "" && r.Level >= h.stackLevel.Level() {
		top = append(top, slog.String(h.stackKey, stackTrace()))
	}
	if len(h.goas) == 0 {
		clash := false
		r.Attrs(func(a slog.Attr) bool {
			clash = h.unsafe(a)
			return !clash
		})
		top = h.dedupe(top, r)
		if !clash {
			if len(top) > 0 {
				r = r.Clone()
				r.AddAttrs(top...)
			}
			return h.next.Handle(ctx, r)
		}
		nr := slog.NewRecord(r.Time, r.Level, r.Message, r.PC)
		safe := make([]slog.Attr, 0, r.NumAttrs())
		r.Attrs(func(a slog.Attr) bool {
			safe = h.appendSafe(safe, a)
			return true
		})
		nr.AddAttrs(safe...)
		nr.AddAttrs(top...)
		return h.next.Handle(ctx, nr)
	}

	// A group is open: every record attribute is nested, so only the
	// context attributes are top level.
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

// dedupe drops context attributes whose output key the record already has at
// the top level (the record's own value wins). Record keys are compared after
// the reserved-key renaming, so schema-owned attributes (trace, stack) are
// never dropped in favour of a spoofed record attribute.
func (h *contextHandler) dedupe(top []slog.Attr, r slog.Record) []slog.Attr {
	if len(top) == 0 || r.NumAttrs() == 0 {
		return top
	}
	// top holds a few attributes: scan the record once per attribute instead
	// of building a key set (no allocation when nothing clashes).
	has := func(key string) bool {
		found := false
		r.Attrs(func(a slog.Attr) bool {
			found = h.outKeyHas(a, key)
			return !found
		})
		return found
	}
	if !slices.ContainsFunc(top, func(a slog.Attr) bool { return has(a.Key) }) {
		return top
	}
	return slices.DeleteFunc(slices.Clone(top), func(a slog.Attr) bool { return has(a.Key) })
}

// outKeyHas reports whether attribute a is written at the top level under
// key (after renaming; inlined groups are searched).
func (h *contextHandler) outKeyHas(a slog.Attr, key string) bool {
	if inlined(a) {
		return slices.ContainsFunc(a.Value.Resolve().Group(), func(ga slog.Attr) bool { return h.outKeyHas(ga, key) })
	}
	if h.reserved[a.Key] {
		return reservedPrefix+a.Key == key
	}
	return a.Key == key
}

func (h *contextHandler) contextAttrs(ctx context.Context) []slog.Attr {
	if ctx == nil {
		return nil
	}
	var extracted []slog.Attr
	if h.extract != nil {
		extracted = h.extract(ctx)
	}
	stored := AttrsFromContext(ctx)
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() && len(extracted) == 0 {
		return h.safe(stored)
	}
	var out []slog.Attr
	if sc.IsValid() {
		out = h.trace(sc)
	}
	out = append(out, h.safe(extracted)...)
	return append(out, h.safe(stored)...)
}

// sinkHandler feeds an extra sink (Options.Handlers): records at or above
// level, with the context attributes added.
type sinkHandler struct {
	next    slog.Handler
	level   slog.Leveler
	extract func(context.Context) []slog.Attr
}

func (h *sinkHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return l >= h.level.Level() && h.next.Enabled(ctx, l)
}

func (h *sinkHandler) Handle(ctx context.Context, r slog.Record) error {
	var attrs []slog.Attr
	if ctx != nil {
		if h.extract != nil {
			attrs = h.extract(ctx)
		}
		attrs = append(attrs, AttrsFromContext(ctx)...)
	}
	if len(attrs) > 0 {
		r = r.Clone()
		r.AddAttrs(attrs...)
	}
	return h.next.Handle(ctx, r)
}

func (h *sinkHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	c := *h
	c.next = h.next.WithAttrs(attrs)
	return &c
}

func (h *sinkHandler) WithGroup(name string) slog.Handler {
	c := *h
	c.next = h.next.WithGroup(name)
	return &c
}
