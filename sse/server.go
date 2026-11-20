package sse

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/metric"
)

var (
	// ErrStreamingUnsupported means the ResponseWriter cannot flush.
	ErrStreamingUnsupported = errors.New("sse: response writer does not support flushing")
	// ErrIdle means no message arrived within ServeOptions.MaxIdle.
	ErrIdle = errors.New("sse: stream idle timeout")
)

// ServerConfig configures a Server. Zero values select the documented defaults.
type ServerConfig struct {
	// MaxConnections caps concurrent connections on this server; 0 = no cap.
	MaxConnections int `json:"max_connections" yaml:"max_connections"`
	// MaxConnectionsPerStream caps viewers per stream; 0 = no cap.
	MaxConnectionsPerStream int `json:"max_connections_per_stream" yaml:"max_connections_per_stream"`
	// Heartbeat is the idle time after which ": ping" is written. Default 15s.
	Heartbeat time.Duration `json:"heartbeat" yaml:"heartbeat"`
	// WriteTimeout bounds every write+flush; a timeout ends the connection
	// (the client resumes with Last-Event-ID). Default 5s.
	WriteTimeout time.Duration `json:"write_timeout" yaml:"write_timeout"`
	// RetryHint is sent as "retry:" right after the headers. Default 3s.
	RetryHint time.Duration `json:"retry_hint" yaml:"retry_hint"`
	// Logger receives connection errors; nil = slog.Default().
	Logger *slog.Logger `json:"-" yaml:"-"`
	// MeterProvider for metrics; nil = otel.GetMeterProvider().
	MeterProvider metric.MeterProvider `json:"-" yaml:"-"`
}

// ServeOptions configures one Serve call.
type ServeOptions struct {
	// Ticket from Admit. When nil, Serve admits the connection itself.
	Ticket *Ticket
	// LastEventID overrides the Last-Event-ID request header (e.g. when it is
	// carried in a query parameter). Empty means "use the header".
	LastEventID string
	// Encoder makes the per-connection encoder; nil = NewRawEncoder.
	Encoder EncoderFactory
	// Transforms run in order on every visible message.
	Transforms []TransformFactory
	// Visible filters message types; nil = DefaultVisible.
	Visible func(eventType string) bool
	// MaxIdle ends the connection with EndIdle when no message arrives for
	// this long; 0 = never.
	MaxIdle time.Duration
	// FlushInterval, when positive, delays flushes to batch small writes.
	// 0 flushes as soon as nothing more is ready.
	FlushInterval time.Duration
}

// Server streams Source messages to HTTP clients.
type Server struct {
	src     Source
	cfg     ServerConfig
	log     *slog.Logger
	adm     *admission
	met     serverMetrics
	wg      sync.WaitGroup
	closed  atomic.Bool
	stop    chan struct{}
	stopOne sync.Once
}

// NewServer returns a Server reading from src.
func NewServer(src Source, cfg ServerConfig) (*Server, error) {
	if src == nil {
		return nil, errors.New("sse: nil source")
	}
	if cfg.MaxConnections < 0 || cfg.MaxConnectionsPerStream < 0 {
		return nil, errors.New("sse: negative connection limit")
	}
	if cfg.Heartbeat <= 0 {
		cfg.Heartbeat = 15 * time.Second
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = 5 * time.Second
	}
	if cfg.RetryHint <= 0 {
		cfg.RetryHint = 3 * time.Second
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	return &Server{
		src:  src,
		cfg:  cfg,
		log:  log,
		adm:  newAdmission(cfg.MaxConnections, cfg.MaxConnectionsPerStream),
		met:  newServerMetrics(cfg.MeterProvider),
		stop: make(chan struct{}),
	}, nil
}

// Admit reserves a connection slot before the response is committed, so the
// handler can still answer 503 (ErrTooManyConnections), 429
// (ErrTooManyForStream) or 503 (ErrServerClosed).
func (s *Server) Admit(ctx context.Context, streamID string) (*Ticket, error) {
	if s.closed.Load() {
		return nil, ErrServerClosed
	}
	if err := s.adm.acquire(streamID); err != nil {
		s.met.rejected.Add(ctx, 1)
		return nil, err
	}
	return &Ticket{a: s.adm, streamID: streamID}, nil
}

type item struct {
	m   Message
	err error
}

// Serve streams streamID to the client until the stream ends, the client goes
// away, or the server shuts down. Errors returned before anything is written
// (admission, ErrInvalidCursor, ErrStreamingUnsupported) leave the response
// uncommitted so the caller can write an HTTP error. A client disconnect and a
// clean end return nil; shutdown returns ErrServerClosed; a resume gap returns
// ErrReplayGap after the terminal event is written.
func (s *Server) Serve(w http.ResponseWriter, r *http.Request, streamID string, opts ServeOptions) (err error) {
	t := opts.Ticket
	if t == nil {
		if t, err = s.Admit(r.Context(), streamID); err != nil {
			return err
		}
	} else if t.streamID != streamID {
		return fmt.Errorf("sse: ticket for stream %q used for %q", t.streamID, streamID)
	}
	defer t.Release()
	if s.closed.Load() {
		return ErrServerClosed
	}

	cursor := opts.LastEventID
	if cursor == "" {
		cursor = r.Header.Get("Last-Event-ID")
	}
	if cursor != "" && !s.src.ValidCursor(cursor) {
		return ErrInvalidCursor
	}
	newEnc := opts.Encoder
	if newEnc == nil {
		newEnc = NewRawEncoder
	}
	enc := newEnc()

	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Accel-Buffering", "no")
	enc.Header(h)
	rc := http.NewResponseController(w)
	if err := rc.Flush(); err != nil {
		return ErrStreamingUnsupported
	}

	s.wg.Add(1)
	defer s.wg.Done()
	ctx := r.Context()
	s.met.conns.Add(ctx, 1)
	defer s.met.conns.Add(context.WithoutCancel(ctx), -1)

	c := &conn{s: s, w: w, rc: rc, enc: enc, opts: opts, visible: opts.Visible}
	if c.visible == nil {
		c.visible = DefaultVisible
	}
	for _, f := range opts.Transforms {
		c.transforms = append(c.transforms, f(ctx))
	}
	if err := c.write(ctx, []Event{{Retry: s.cfg.RetryHint}}); err != nil {
		return c.writeFailed(ctx, err)
	}
	return c.run(ctx, streamID, cursor)
}

// Shutdown stops admitting, ends every active Serve call (they return
// ErrServerClosed without a terminal event, so clients reconnect elsewhere)
// and waits for them. It returns ctx.Err() if the wait is cut short.
func (s *Server) Shutdown(ctx context.Context) error {
	s.closed.Store(true)
	s.stopOne.Do(func() { close(s.stop) })
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("sse: shutdown: %w", ctx.Err())
	}
}

// coalesceMax bounds how many ready messages are merged into one write.
const coalesceMax = 64

type conn struct {
	s          *Server
	w          http.ResponseWriter
	rc         *http.ResponseController
	enc        Encoder
	opts       ServeOptions
	visible    func(string) bool
	transforms []Transform
	events     []Event
	buf        []byte
	scratch    Message // reused so transforms can take a pointer without an allocation
	pending    bool    // written but not flushed (FlushInterval > 0)
	noDeadline bool    // the writer does not support write deadlines
}

func (c *conn) run(reqCtx context.Context, streamID, cursor string) error {
	ctx, cancel := context.WithCancel(reqCtx)
	// A bounded hand-off: the pump blocks when it is full (no drops), and
	// the writer can coalesce whatever is queued into one write.
	ch := make(chan item, coalesceMax)
	var pump sync.WaitGroup
	pump.Go(func() {
		defer close(ch)
		for m, err := range c.s.src.Subscribe(ctx, streamID, cursor) {
			select {
			case ch <- item{m, err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	})
	defer pump.Wait()
	defer cancel()

	heartbeat := time.NewTimer(c.s.cfg.Heartbeat)
	defer heartbeat.Stop()
	var idleC, flushC <-chan time.Time
	var idle, flush *time.Timer
	if c.opts.MaxIdle > 0 {
		idle = time.NewTimer(c.opts.MaxIdle)
		defer idle.Stop()
		idleC = idle.C
	}
	if c.opts.FlushInterval > 0 {
		flush = time.NewTimer(c.opts.FlushInterval)
		flush.Stop()
		defer flush.Stop()
		flushC = flush.C
	}

	for {
		select {
		case it, ok := <-ch:
			if reqCtx.Err() != nil {
				return nil
			}
			if idle != nil {
				idle.Reset(c.opts.MaxIdle)
			}
			end, reason, err := c.handle(ctx, it, ok, ch)
			if end {
				c.events = c.enc.End(c.events, reason)
			}
			if werr := c.flushEvents(ctx, !end && err == nil && flush != nil); werr != nil {
				return c.writeFailed(ctx, werr)
			}
			if end || err != nil {
				return err
			}
			if c.pending && flush != nil {
				flush.Reset(c.opts.FlushInterval)
			}
			heartbeat.Reset(c.s.cfg.Heartbeat)
		case <-flushC:
			if c.pending {
				if err := c.doFlush(); err != nil {
					return c.writeFailed(ctx, err)
				}
			}
		case <-heartbeat.C:
			if err := c.write(ctx, []Event{{Comment: "ping"}}); err != nil {
				return c.writeFailed(ctx, err)
			}
			heartbeat.Reset(c.s.cfg.Heartbeat)
		case <-idleC:
			c.events = c.enc.End(c.events[:0], EndIdle)
			if err := c.flushEvents(ctx, false); err != nil {
				return c.writeFailed(ctx, err)
			}
			return ErrIdle
		case <-c.s.stop:
			c.events = c.enc.End(c.events[:0], EndShutdown)
			if err := c.flushEvents(ctx, false); err != nil {
				return c.writeFailed(ctx, err)
			}
			return ErrServerClosed
		case <-reqCtx.Done():
			return nil
		}
	}
}

// handle encodes the first item plus everything else already waiting into
// c.events. It reports whether the connection must end, why, and the error
// Serve returns.
func (c *conn) handle(ctx context.Context, it item, ok bool, ch <-chan item) (bool, EndReason, error) {
	c.events = c.events[:0]
	for n := 0; ; n++ {
		if !ok {
			return true, EndDone, nil
		}
		if it.err != nil {
			if errors.Is(it.err, ErrReplayGap) {
				return true, EndGap, ErrReplayGap
			}
			return false, 0, it.err
		}
		if stop, err := c.apply(ctx, it.m); err != nil || stop {
			return stop, EndStopped, err
		}
		if n+1 >= coalesceMax {
			return false, 0, nil
		}
		select {
		case it, ok = <-ch:
		default:
			return false, 0, nil
		}
	}
}

func (c *conn) apply(ctx context.Context, msg Message) (stop bool, err error) {
	if !c.visible(msg.Type) {
		return false, nil
	}
	if len(c.transforms) == 0 {
		c.events = c.enc.Encode(c.events, msg)
		return false, nil
	}
	c.scratch = msg
	m := &c.scratch
	for _, t := range c.transforms {
		act, err := t(ctx, m)
		if err != nil {
			return false, err
		}
		switch act {
		case ActionSkip:
			return false, nil
		case ActionStop:
			c.events = c.enc.Encode(c.events, *m)
			return true, nil
		case ActionPass:
		}
	}
	c.events = c.enc.Encode(c.events, *m)
	return false, nil
}

func (c *conn) flushEvents(ctx context.Context, deferFlush bool) error {
	if len(c.events) == 0 {
		return nil
	}
	if deferFlush {
		return c.writeNoFlush(ctx, c.events)
	}
	return c.write(ctx, c.events)
}

func (c *conn) encode(ctx context.Context, events []Event) {
	c.buf = c.buf[:0]
	for _, e := range events {
		b, err := AppendEvent(c.buf, e)
		if err != nil {
			c.s.log.WarnContext(ctx, "sse: dropping unencodable event", "type", e.Type, "err", err)
			continue
		}
		c.buf = b
	}
}

func (c *conn) writeNoFlush(ctx context.Context, events []Event) error {
	c.encode(ctx, events)
	if len(c.buf) == 0 {
		return nil
	}
	c.deadline(time.Now().Add(c.s.cfg.WriteTimeout))
	_, err := c.w.Write(c.buf)
	c.deadline(time.Time{})
	if err != nil {
		return err
	}
	c.pending = true
	c.s.met.sent.Add(ctx, int64(len(events)))
	return nil
}

func (c *conn) write(ctx context.Context, events []Event) error {
	if err := c.writeNoFlush(ctx, events); err != nil {
		return err
	}
	if !c.pending {
		return nil
	}
	return c.doFlush()
}

func (c *conn) doFlush() error {
	c.deadline(time.Now().Add(c.s.cfg.WriteTimeout))
	err := c.rc.Flush()
	c.deadline(time.Time{})
	c.pending = false
	return err
}

// deadline sets the write deadline, remembering when the writer cannot.
func (c *conn) deadline(t time.Time) {
	if c.noDeadline {
		return
	}
	if err := c.rc.SetWriteDeadline(t); err != nil {
		c.noDeadline = true
	}
}

func (c *conn) writeFailed(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return nil // the client went away
	}
	c.s.met.writeErr.Add(context.WithoutCancel(ctx), 1)
	return fmt.Errorf("sse: write: %w", err)
}
