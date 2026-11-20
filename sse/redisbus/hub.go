package redisbus

import (
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/maphash"
	"iter"
	"log/slog"
	"math/rand/v2"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	"github.com/dungnh3/kitchen/sse"
	"github.com/dungnh3/kitchen/sse/internal/redisx"
)

// ErrHubClosed is yielded to subscribers when the hub is closed.
var ErrHubClosed = errors.New("redisbus: hub closed")

// Hub defaults.
const (
	DefaultBlock          = 2 * time.Second
	DefaultCount          = 256
	DefaultMaxKeysPerRead = 512
	DefaultRingSize       = 512
	DefaultCatchupBatch   = 256
)

// HubConfig configures a Hub. Zero values select defaults.
type HubConfig struct {
	// Keyspace must match the producers'.
	Keyspace Keyspace `json:"keyspace" yaml:"keyspace"`
	// Block is the XREAD BLOCK timeout. It also bounds how long Close waits
	// for a blocked reader. Default 2s.
	Block time.Duration `json:"block" yaml:"block"`
	// Count is the XREAD COUNT (entries per stream per read). Default 256.
	Count int64 `json:"count" yaml:"count"`
	// MaxKeysPerRead caps the streams served by one reader. Default 512.
	MaxKeysPerRead int `json:"max_keys_per_read" yaml:"max_keys_per_read"`
	// RingSize is the number of recent entries kept per stream in memory.
	// Subscribers further behind read from Redis. Default 512.
	RingSize int `json:"ring_size" yaml:"ring_size"`
	// CatchupBatch is the XRANGE page size for catch-up reads. Default 256.
	CatchupBatch int64 `json:"catchup_batch" yaml:"catchup_batch"`
	// RetryBackoff and MaxRetryBackoff bound the jittered backoff after a
	// failed read. Defaults 100ms and 5s.
	RetryBackoff    time.Duration `json:"retry_backoff" yaml:"retry_backoff"`
	MaxRetryBackoff time.Duration `json:"max_retry_backoff" yaml:"max_retry_backoff"`
	// Logger; nil = slog.Default().
	Logger *slog.Logger `json:"-" yaml:"-"`
	// MeterProvider; nil = otel.GetMeterProvider().
	MeterProvider metric.MeterProvider `json:"-" yaml:"-"`
}

// Hub multiplexes stream reads for one process. Readers each serve up to
// MaxKeysPerRead streams with one XREAD BLOCK; every stream keeps a bounded
// ring of recent entries, and subscribers pull from the ring at their own
// pace, falling back to XRANGE when they are behind it. Nothing is dropped and
// no subscriber can stall a reader. Hub implements sse.Source.
type Hub struct {
	rdb     redis.UniversalClient
	cfg     HubConfig
	log     *slog.Logger
	cluster bool
	id      string
	seed    maphash.Seed
	shards  [hubShards]hubShard

	gmu    sync.Mutex
	groups map[int]*group
	nextID int

	ctx     context.Context
	cancel  context.CancelFunc
	readers sync.WaitGroup

	met hubMetrics
}

const hubShards = 64

type hubShard struct {
	mu sync.Mutex
	m  map[string]*stream
}

type group struct {
	bucket  int // hash tag used by the group's wake keys
	readers []*reader
}

type entry struct {
	id  redisx.ID
	msg sse.Message
	end bool
}

type stream struct {
	id, key string
	ready   chan struct{}
	regErr  error
	refs    int // guarded by the hub shard mutex
	rd      *reader

	mu     sync.Mutex
	ring   []entry
	start  int
	n      int
	base   redisx.ID // the ring covers (base, last]
	last   redisx.ID
	ended  bool
	notify chan struct{}
}

type reader struct {
	h       *Hub
	g       *group
	wake    string
	mu      sync.Mutex
	streams map[string]*stream
	stop    context.CancelFunc
}

// NewHub returns a Hub reading through rdb. Call Run (or Close) to stop it.
func NewHub(rdb redis.UniversalClient, cfg HubConfig) (*Hub, error) {
	if rdb == nil {
		return nil, errors.New("redisbus: nil redis client")
	}
	ks, err := cfg.Keyspace.withDefaults()
	if err != nil {
		return nil, err
	}
	cfg.Keyspace = ks
	setDefault(&cfg.Block, DefaultBlock)
	setDefault(&cfg.Count, DefaultCount)
	setDefault(&cfg.MaxKeysPerRead, DefaultMaxKeysPerRead)
	setDefault(&cfg.RingSize, DefaultRingSize)
	setDefault(&cfg.CatchupBatch, DefaultCatchupBatch)
	setDefault(&cfg.RetryBackoff, 100*time.Millisecond)
	setDefault(&cfg.MaxRetryBackoff, 5*time.Second)
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	var raw [8]byte
	_, _ = crand.Read(raw[:])
	ctx, cancel := context.WithCancel(context.Background())
	h := &Hub{
		rdb:     rdb,
		cfg:     cfg,
		log:     log,
		cluster: redisx.IsCluster(rdb),
		id:      hex.EncodeToString(raw[:]),
		seed:    maphash.MakeSeed(),
		groups:  make(map[int]*group),
		ctx:     ctx,
		cancel:  cancel,
		met:     newHubMetrics(cfg.MeterProvider),
	}
	for i := range h.shards {
		h.shards[i].m = make(map[string]*stream)
	}
	return h, nil
}

func setDefault[T int | int64 | time.Duration](v *T, d T) {
	if *v <= 0 {
		*v = d
	}
}

// Run blocks until ctx is done, then closes the hub and waits for its readers.
func (h *Hub) Run(ctx context.Context) error {
	select {
	case <-ctx.Done():
	case <-h.ctx.Done():
	}
	h.Close()
	return nil
}

// Close stops all readers, ends every subscription with ErrHubClosed and
// waits for the readers to exit (at most about Block).
func (h *Hub) Close() {
	h.cancel()
	// assign starts readers under gmu after checking h.ctx; taking gmu here
	// orders every such start before Wait.
	h.gmu.Lock()
	h.gmu.Unlock() //nolint:staticcheck // empty critical section is the barrier
	h.readers.Wait()
}

// ValidCursor implements sse.Source: "<ms>-<seq>", "<ms>" or "0".
func (h *Hub) ValidCursor(cursor string) bool {
	_, err := redisx.ParseID(cursor)
	return err == nil
}

// Subscribe implements sse.Source. Data slices are shared between
// subscribers of the same stream and must not be modified.
func (h *Hub) Subscribe(ctx context.Context, streamID, cursor string) iter.Seq2[sse.Message, error] {
	return func(yield func(sse.Message, error) bool) {
		cur := redisx.Zero
		if cursor != "" {
			var err error
			if cur, err = redisx.ParseID(cursor); err != nil {
				yield(sse.Message{}, sse.ErrInvalidCursor)
				return
			}
		}
		if err := validStreamID(streamID); err != nil {
			yield(sse.Message{}, err)
			return
		}
		st, err := h.acquire(ctx, streamID)
		if err != nil {
			yield(sse.Message{}, err)
			return
		}
		defer h.release(ctx, st)
		h.follow(ctx, st, cur, cursor != "", yield)
	}
}

// follow implements the cursor-pull loop described in the package design.
func (h *Hub) follow(ctx context.Context, st *stream, cur redisx.ID, resume bool, yield func(sse.Message, error) bool) {
	st.mu.Lock()
	ahead := cur.Compare(st.last) > 0
	st.mu.Unlock()
	if ahead {
		// The cursor is newer than what this pod has read so far. That is fine
		// if the entry exists (another pod served it and our reader lags);
		// otherwise it is fabricated or beyond the end of the stream.
		if err := h.checkExists(ctx, st, cur); err != nil {
			yield(sse.Message{}, err)
			return
		}
	}

	resumedSent := !resume
	batch := make([]entry, 0, h.cfg.CatchupBatch)
	for {
		batch = batch[:0]
		st.mu.Lock()
		inRing := cur.Compare(st.base) >= 0
		var wait chan struct{}
		if inRing {
			batch = st.after(cur, batch, int(h.cfg.CatchupBatch))
			if len(batch) == 0 {
				if st.ended {
					st.mu.Unlock()
					return
				}
				wait = st.notify
			}
		}
		st.mu.Unlock()

		if !inRing {
			var err error
			// Past the first message the page must start at cur itself, so a
			// trim during catch-up is detected instead of skipped.
			batch, err = h.catchup(ctx, st, cur, !cur.IsZero(), batch)
			if err != nil {
				yield(sse.Message{}, err)
				return
			}
		}
		for _, e := range batch {
			if e.end {
				return
			}
			if !yield(e.msg, nil) {
				return
			}
			cur = e.id
		}
		if wait == nil {
			continue
		}
		if !resumedSent {
			resumedSent = true
			if !yield(sse.Message{Type: sse.TypeResumed}, nil) {
				return
			}
		}
		select {
		case <-wait:
		case <-ctx.Done():
			return
		case <-h.ctx.Done():
			yield(sse.Message{}, ErrHubClosed)
			return
		}
	}
}

func (h *Hub) checkExists(ctx context.Context, st *stream, cur redisx.ID) error {
	id := cur.String()
	msgs, err := h.rdb.XRangeN(ctx, st.key, id, id, 1).Result()
	if err != nil {
		return fmt.Errorf("redisbus: check cursor %s: %w", st.id, err)
	}
	if len(msgs) == 0 {
		return sse.ErrInvalidCursor
	}
	return nil
}

// catchup reads the next page after cur from Redis. With verify, the page
// starts at cur inclusive and cur must still exist; otherwise the cursor was
// trimmed (or the stream expired) and the result is sse.ErrReplayGap.
func (h *Hub) catchup(ctx context.Context, st *stream, cur redisx.ID, verify bool, dst []entry) ([]entry, error) {
	h.met.catchup.Add(ctx, 1)
	start := "-"
	if verify {
		start = cur.String()
	}
	msgs, err := h.rdb.XRangeN(ctx, st.key, start, "+", h.cfg.CatchupBatch+1).Result()
	if err != nil {
		if ctx.Err() != nil {
			return dst, ctx.Err()
		}
		return dst, fmt.Errorf("redisbus: catch-up %s: %w", st.id, err)
	}
	if verify {
		if len(msgs) == 0 || msgs[0].ID != start {
			return dst, sse.ErrReplayGap
		}
		msgs = msgs[1:]
	}
	if len(msgs) == 0 {
		// Nothing after cur in Redis although the ring starts later: the
		// stream expired or was deleted under us.
		return dst, sse.ErrReplayGap
	}
	for _, m := range msgs {
		e, ok := toEntry(m)
		if !ok {
			continue
		}
		dst = append(dst, e)
	}
	return dst, nil
}

func toEntry(m redis.XMessage) (entry, bool) {
	id, err := redisx.ParseID(m.ID)
	if err != nil {
		return entry{}, false
	}
	typ, _ := redisx.Field(m, fieldType)
	data, _ := redisx.Field(m, fieldData)
	if typ == endType {
		return entry{id: id, end: true}, true
	}
	return entry{id: id, msg: sse.Message{ID: m.ID, Type: typ, Data: []byte(data)}}, true
}

// after appends ring entries newer than cur (at most max). Caller holds st.mu.
func (st *stream) after(cur redisx.ID, dst []entry, maxN int) []entry {
	size := len(st.ring)
	// The ring is sorted by ID: binary-search the first entry after cur.
	lo, hi := 0, st.n
	for lo < hi {
		mid := lo + (hi-lo)/2
		if st.ring[(st.start+mid)%size].id.Compare(cur) <= 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	for i := lo; i < st.n && len(dst) < maxN; i++ {
		dst = append(dst, st.ring[(st.start+i)%size])
	}
	return dst
}

// push appends entries read by the reader and wakes subscribers.
func (st *stream) push(es []entry) {
	st.mu.Lock()
	defer st.mu.Unlock()
	changed := false
	size := len(st.ring)
	for _, e := range es {
		if e.id.Compare(st.last) <= 0 || st.ended {
			continue
		}
		if st.n == size {
			st.base = st.ring[st.start].id
			st.ring[st.start] = e
			st.start = (st.start + 1) % size
		} else {
			st.ring[(st.start+st.n)%size] = e
			st.n++
		}
		st.last = e.id
		st.ended = e.end
		changed = true
	}
	if changed {
		close(st.notify)
		st.notify = make(chan struct{})
	}
}

func (h *Hub) shard(id string) *hubShard {
	return &h.shards[maphash.String(h.seed, id)%hubShards]
}

func (h *Hub) acquire(ctx context.Context, streamID string) (*stream, error) {
	if h.ctx.Err() != nil {
		return nil, ErrHubClosed
	}
	sh := h.shard(streamID)
	sh.mu.Lock()
	st := sh.m[streamID]
	if st == nil {
		st = &stream{
			id:     streamID,
			key:    h.cfg.Keyspace.Key(streamID),
			ready:  make(chan struct{}),
			refs:   1,
			ring:   make([]entry, h.cfg.RingSize),
			notify: make(chan struct{}),
		}
		sh.m[streamID] = st
		sh.mu.Unlock()
		st.regErr = h.register(st) //nolint:contextcheck // registration is shared by all waiters; it runs on the hub context
		close(st.ready)
	} else {
		st.refs++
		sh.mu.Unlock()
		select {
		case <-st.ready:
		case <-ctx.Done():
			h.release(ctx, st)
			return nil, ctx.Err()
		}
	}
	if st.regErr != nil {
		h.release(ctx, st)
		return nil, st.regErr
	}
	return st, nil
}

// register reads the stream's current tail and hands it to a reader. It
// runs on the hub context so one impatient subscriber cannot fail others.
func (h *Hub) register(st *stream) error {
	ctx, cancel := context.WithTimeout(h.ctx, 10*time.Second)
	defer cancel()
	msgs, err := h.rdb.XRevRangeN(ctx, st.key, "+", "-", 1).Result()
	if err != nil {
		return fmt.Errorf("redisbus: register %s: %w", st.id, err)
	}
	if len(msgs) == 1 {
		e, ok := toEntry(msgs[0])
		if ok {
			st.base, st.last, st.ended = e.id, e.id, e.end
		}
	}
	h.met.streams.Add(ctx, 1)
	if st.ended {
		return nil // nothing will be appended; subscribers read from Redis
	}
	h.assign(ctx, st)
	return nil
}

func (h *Hub) release(ctx context.Context, st *stream) {
	sh := h.shard(st.id)
	sh.mu.Lock()
	st.refs--
	if st.refs > 0 {
		sh.mu.Unlock()
		return
	}
	if sh.m[st.id] == st {
		delete(sh.m, st.id)
	}
	sh.mu.Unlock()
	if st.regErr == nil {
		h.met.streams.Add(context.WithoutCancel(ctx), -1)
	}
	h.unassign(ctx, st)
}

func (h *Hub) groupOf(st *stream) int {
	if h.cluster {
		return h.cfg.Keyspace.Bucket(st.id)
	}
	return 0
}

// assign adds st to a reader of its slot group with spare capacity, starting
// a reader when none has room, and wakes that reader.
func (h *Hub) assign(ctx context.Context, st *stream) { //nolint:contextcheck // readers live as long as the hub
	gid := h.groupOf(st)
	h.gmu.Lock()
	g := h.groups[gid]
	if g == nil {
		g = &group{bucket: gid}
		h.groups[gid] = g
	}
	var r *reader
	for _, c := range g.readers {
		c.mu.Lock()
		room := len(c.streams) < h.cfg.MaxKeysPerRead
		if room {
			c.streams[st.key] = st
			r = c
		}
		c.mu.Unlock()
		if room {
			break
		}
	}
	started := false
	if r == nil {
		if h.ctx.Err() != nil {
			h.gmu.Unlock()
			return
		}
		h.nextID++
		rctx, stop := context.WithCancel(h.ctx) //nolint:contextcheck // readers live as long as the hub, not the caller
		r = &reader{
			h:       h,
			g:       g,
			wake:    h.cfg.Keyspace.Prefix + redisx.HashTag(g.bucket) + ":__wake:" + h.id + ":" + strconv.Itoa(h.nextID),
			streams: map[string]*stream{st.key: st},
			stop:    stop,
		}
		g.readers = append(g.readers, r)
		h.readers.Go(func() { r.run(rctx) })
		h.met.readers.Add(ctx, 1)
		started = true
	}
	st.rd = r
	h.gmu.Unlock()
	if !started {
		r.poke(ctx)
	}
}

func (h *Hub) unassign(ctx context.Context, st *stream) {
	h.gmu.Lock()
	defer h.gmu.Unlock()
	r := st.rd
	if r == nil {
		return
	}
	st.rd = nil
	r.mu.Lock()
	if r.streams[st.key] == st {
		delete(r.streams, st.key)
	}
	empty := len(r.streams) == 0
	r.mu.Unlock()
	if empty {
		h.retire(ctx, r)
	}
}

// retire stops an empty reader. Caller holds h.gmu.
func (h *Hub) retire(ctx context.Context, r *reader) {
	g := r.g
	for i, c := range g.readers {
		if c == r {
			g.readers = append(g.readers[:i], g.readers[i+1:]...)
			break
		}
	}
	if len(g.readers) == 0 {
		delete(h.groups, g.bucket)
	}
	r.stop()
	h.met.readers.Add(context.WithoutCancel(ctx), -1)
}

// poke interrupts the reader's blocking XREAD so it picks up a new stream.
func (r *reader) poke(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	pipe := r.h.rdb.Pipeline()
	pipe.XAdd(ctx, &redis.XAddArgs{Stream: r.wake, MaxLen: 1, Values: []any{"w", "1"}})
	pipe.PExpire(ctx, r.wake, time.Minute)
	if _, err := pipe.Exec(ctx); err != nil && ctx.Err() == nil {
		// Not fatal: the reader still picks the stream up within Block.
		r.h.log.Warn("redisbus: wake reader", "err", err)
	}
}

func (r *reader) run(ctx context.Context) {
	defer func() {
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
		defer cancel()
		_ = r.h.rdb.Del(cctx, r.wake).Err()
	}()
	wakeCur := "0-0"
	backoff := r.h.cfg.RetryBackoff
	args := make([]string, 0, 2*(r.h.cfg.MaxKeysPerRead+1))
	for ctx.Err() == nil {
		args = args[:0]
		args = append(args, r.wake)
		r.mu.Lock()
		keys := make([]*stream, 0, len(r.streams))
		for k, st := range r.streams {
			args = append(args, k)
			keys = append(keys, st)
		}
		r.mu.Unlock()
		args = append(args, wakeCur)
		for _, st := range keys {
			st.mu.Lock()
			args = append(args, st.last.String())
			st.mu.Unlock()
		}
		res, err := r.h.rdb.XRead(ctx, &redis.XReadArgs{Streams: args, Count: r.h.cfg.Count, Block: r.h.cfg.Block}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				backoff = r.h.cfg.RetryBackoff
				continue
			}
			if ctx.Err() != nil {
				return
			}
			r.h.met.readErrors.Add(ctx, 1)
			r.h.log.Warn("redisbus: xread failed", "err", err, "streams", len(keys))
			if !sleepCtx(ctx, jitter(backoff)) {
				return
			}
			backoff = min(2*backoff, r.h.cfg.MaxRetryBackoff)
			continue
		}
		backoff = r.h.cfg.RetryBackoff
		for _, xs := range res {
			if xs.Stream == r.wake {
				if n := len(xs.Messages); n > 0 {
					wakeCur = xs.Messages[n-1].ID
				}
				continue
			}
			r.mu.Lock()
			st := r.streams[xs.Stream]
			r.mu.Unlock()
			if st == nil {
				continue
			}
			es := make([]entry, 0, len(xs.Messages))
			ended := false
			for _, m := range xs.Messages {
				if e, ok := toEntry(m); ok {
					es = append(es, e)
					ended = ended || e.end
				}
			}
			st.push(es)
			if ended {
				r.mu.Lock()
				if r.streams[xs.Stream] == st {
					delete(r.streams, xs.Stream)
				}
				r.mu.Unlock()
			}
		}
	}
}

func jitter(d time.Duration) time.Duration {
	return d/2 + rand.N(d/2+1) //nolint:gosec // jitter does not need crypto randomness
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

type hubMetrics struct {
	streams    metric.Int64UpDownCounter
	readers    metric.Int64UpDownCounter
	catchup    metric.Int64Counter
	readErrors metric.Int64Counter
}

func newHubMetrics(mp metric.MeterProvider) hubMetrics {
	if mp == nil {
		mp = otel.GetMeterProvider()
	}
	m := mp.Meter(sse.ScopeName)
	streams, _ := m.Int64UpDownCounter("sse.hub.streams", metric.WithDescription("Streams with at least one subscriber."))
	readers, _ := m.Int64UpDownCounter("sse.hub.readers", metric.WithDescription("Running multiplexed readers."))
	catchup, _ := m.Int64Counter("sse.hub.catchup.reads", metric.WithDescription("XRANGE catch-up reads by subscribers."))
	readErrors, _ := m.Int64Counter("sse.hub.read.errors", metric.WithDescription("Failed multiplexed XREAD calls."))
	return hubMetrics{streams: streams, readers: readers, catchup: catchup, readErrors: readErrors}
}
