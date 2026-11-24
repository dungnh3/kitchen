package redisbus

import (
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/maphash"
	"iter"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/redisx"
	"github.com/dungnh3/kitchen/sse"
	"github.com/dungnh3/kitchen/sse/internal/streamid"
	"github.com/dungnh3/kitchen/timex"
)

// ErrHubClosed is yielded to subscribers when the hub is closed.
var ErrHubClosed = errors.New("redisbus: hub closed")

// Hub defaults.
const (
	DefaultBlock          = 2 * time.Second
	DefaultCount          = 256
	DefaultMaxKeysPerRead = 512
	DefaultRingSize       = 512
	DefaultRingBytes      = 1 << 20
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
	// Subscribers further behind read from Redis. Default 512. The ring
	// grows with traffic, so an idle stream holds no entries.
	RingSize int `json:"ring_size" yaml:"ring_size"`
	// RingBytes caps the payload bytes a stream's ring holds; the oldest
	// entries leave the ring first (subscribers that still need them read
	// them from Redis). Default 1 MiB.
	RingBytes int `json:"ring_bytes" yaml:"ring_bytes"`
	// CatchupBatch is the XRANGE page size for catch-up reads. Default 256.
	CatchupBatch int64 `json:"catchup_batch" yaml:"catchup_batch"`
	// RetryBackoff and MaxRetryBackoff bound the jittered backoff after a
	// failed read. Defaults 100ms and 5s.
	RetryBackoff    time.Duration `json:"retry_backoff" yaml:"retry_backoff"`
	MaxRetryBackoff time.Duration `json:"max_retry_backoff" yaml:"max_retry_backoff"`
	// Logger; nil = log.Default().
	Logger *log.Logger `json:"-" yaml:"-"`
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
	log     *log.Logger
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
	id  streamid.ID
	msg sse.Message
	end bool
}

type stream struct {
	id, key string
	ready   chan struct{}
	regErr  error
	refs    int     // guarded by the hub shard mutex
	rd      *reader // guarded by the hub's gmu

	maxN, maxBytes int // ring limits (HubConfig.RingSize, RingBytes)

	mu     sync.Mutex
	ring   []entry // circular; grows up to maxN entries
	start  int
	n      int
	bytes  int         // payload bytes of the entries in the ring
	base   streamid.ID // the ring covers (base, last]
	last   streamid.ID
	ended  bool
	notify chan struct{}
}

type reader struct {
	h       *Hub
	g       *group
	wake    string
	retired bool // guarded by the hub's gmu
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
	setDefault(&cfg.RingBytes, DefaultRingBytes)
	setDefault(&cfg.CatchupBatch, DefaultCatchupBatch)
	setDefault(&cfg.RetryBackoff, 100*time.Millisecond)
	setDefault(&cfg.MaxRetryBackoff, 5*time.Second)
	logger := log.OrDefault(cfg.Logger)
	var raw [8]byte
	_, _ = crand.Read(raw[:])
	ctx, cancel := context.WithCancel(context.Background())
	h := &Hub{
		rdb:     rdb,
		cfg:     cfg,
		log:     logger,
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

// CheckCursor implements sse.Source. It rejects an invalid stream ID
// (ErrInvalidStreamID) for any cursor, including "" (no Last-Event-ID).
// A cursor is "<ms>-<seq>", "<ms>" or "0".
// A well-formed cursor newer than the stream's last entry was never issued
// and is ErrInvalidCursor. One older than the retained entries, or for an
// expired stream, passes: Subscribe reports it as sse.ErrReplayGap, so the
// client gets a stream.replay_gap event and re-fetches state.
func (h *Hub) CheckCursor(ctx context.Context, streamID, cursor string) error {
	if err := validStreamID(streamID); err != nil {
		return err
	}
	if cursor == "" {
		return nil
	}
	cur, err := streamid.ParseID(cursor)
	if err != nil {
		return sse.ErrInvalidCursor
	}
	if cur.IsZero() {
		return nil
	}
	last, err := h.rdb.XRevRangeN(ctx, h.cfg.Keyspace.Key(streamID), "+", "-", 1).Result()
	if err != nil {
		return fmt.Errorf("redisbus: check cursor %s: %w", streamID, err)
	}
	if len(last) == 0 {
		return nil // expired or never written: a replay gap, not a bad cursor
	}
	lastID, err := streamid.ParseID(last[0].ID)
	if err != nil {
		return fmt.Errorf("redisbus: check cursor %s: bad entry ID %q", streamID, last[0].ID)
	}
	if cur.Compare(lastID) > 0 {
		return sse.ErrInvalidCursor
	}
	return nil
}

// Subscribe implements sse.Source. Data slices are shared between
// subscribers of the same stream and must not be modified.
func (h *Hub) Subscribe(ctx context.Context, streamID, cursor string) iter.Seq2[sse.Message, error] {
	return func(yield func(sse.Message, error) bool) {
		cur := streamid.Zero
		if cursor != "" {
			var err error
			if cur, err = streamid.ParseID(cursor); err != nil {
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
func (h *Hub) follow(ctx context.Context, st *stream, cur streamid.ID, resume bool, yield func(sse.Message, error) bool) {
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
	// Grows on demand: a live subscriber reads one entry at a time and must
	// not pin a full catch-up page.
	var batch []entry
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
			if cur.IsZero() && !resume {
				// A viewer without Last-Event-ID replays the retained
				// stream; say so when its head was trimmed.
				truncated, err := h.headTrimmed(ctx, st)
				if err != nil {
					yield(sse.Message{}, err)
					return
				}
				if truncated && !yield(sse.Message{Type: sse.TypeTruncated}, nil) {
					return
				}
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

// checkExists checks a cursor newer than this pod's view of the stream. An
// existing entry is fine (our reader lags). A cursor beyond the stream's
// last entry was never issued: ErrInvalidCursor. Otherwise the entry is gone
// (the stream expired or was trimmed after the client read it): a replay
// gap, so the client gets a terminal stream.replay_gap instead of a
// response that ends without one and an endless reconnect loop.
func (h *Hub) checkExists(ctx context.Context, st *stream, cur streamid.ID) error {
	id := cur.String()
	msgs, err := h.rdb.XRangeN(ctx, st.key, id, id, 1).Result()
	if err != nil {
		return fmt.Errorf("redisbus: check cursor %s: %w", st.id, err)
	}
	if len(msgs) == 1 {
		return nil
	}
	last, err := h.rdb.XRevRangeN(ctx, st.key, "+", "-", 1).Result()
	if err != nil {
		return fmt.Errorf("redisbus: check cursor %s: %w", st.id, err)
	}
	if len(last) == 1 {
		if lastID, err := streamid.ParseID(last[0].ID); err == nil && cur.Compare(lastID) > 0 {
			return sse.ErrInvalidCursor
		}
	}
	return sse.ErrReplayGap
}

// headTrimmed reports whether entries were trimmed from the head of the
// stream (MAXLEN), so a full replay is missing its beginning. The producer
// never deletes single entries, so entries-added > length means a trim.
// Redis < 7.0 does not report entries-added; the check then passes.
func (h *Hub) headTrimmed(ctx context.Context, st *stream) (bool, error) {
	info, err := h.rdb.XInfoStream(ctx, st.key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) || strings.Contains(err.Error(), "no such key") {
			return false, nil
		}
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		return false, fmt.Errorf("redisbus: stream info %s: %w", st.id, err)
	}
	return info.EntriesAdded > info.Length, nil
}

// catchup reads the next page after cur from Redis. With verify, the page
// starts at cur inclusive and cur must still exist; otherwise the cursor was
// trimmed (or the stream expired) and the result is sse.ErrReplayGap.
func (h *Hub) catchup(ctx context.Context, st *stream, cur streamid.ID, verify bool, dst []entry) ([]entry, error) {
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
	id, err := streamid.ParseID(m.ID)
	if err != nil {
		return entry{}, false
	}
	typ, _ := streamid.Field(m, fieldType)
	data, _ := streamid.Field(m, fieldData)
	if typ == endType {
		return entry{id: id, end: true}, true
	}
	return entry{id: id, msg: sse.Message{ID: m.ID, Type: typ, Data: []byte(data)}}, true
}

// after appends ring entries newer than cur (at most max). Caller holds st.mu.
func (st *stream) after(cur streamid.ID, dst []entry, maxN int) []entry {
	if st.n == 0 {
		return dst
	}
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

// entrySize is what an entry counts against RingBytes.
func entrySize(e entry) int { return len(e.msg.Data) + len(e.msg.Type) + len(e.msg.ID) }

// evict drops the oldest ring entry. Caller holds st.mu.
func (st *stream) evict() {
	old := st.ring[st.start]
	st.base = old.id
	st.bytes -= entrySize(old)
	st.ring[st.start] = entry{} // release the payload
	st.start = (st.start + 1) % len(st.ring)
	st.n--
}

// grow enlarges a full ring that is below maxN. Caller holds st.mu.
func (st *stream) grow() {
	size := min(max(2*len(st.ring), 8), st.maxN)
	ring := make([]entry, size)
	for i := range st.n {
		ring[i] = st.ring[(st.start+i)%len(st.ring)]
	}
	st.ring, st.start = ring, 0
}

// push appends entries read by the reader and wakes subscribers. The ring
// keeps at most maxN entries and maxBytes payload bytes; an entry larger
// than maxBytes on its own is not kept (subscribers read it from Redis).
func (st *stream) push(es []entry) {
	st.mu.Lock()
	defer st.mu.Unlock()
	changed := false
	for _, e := range es {
		if e.id.Compare(st.last) <= 0 || st.ended {
			continue
		}
		sz := entrySize(e)
		for st.n > 0 && (st.n == st.maxN || st.bytes+sz > st.maxBytes) {
			st.evict()
		}
		if sz > st.maxBytes {
			st.base = e.id // the ring is empty and covers (e, e]
		} else {
			if st.n == len(st.ring) {
				st.grow()
			}
			st.ring[(st.start+st.n)%len(st.ring)] = e
			st.n++
			st.bytes += sz
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
			id:       streamID,
			key:      h.cfg.Keyspace.Key(streamID),
			ready:    make(chan struct{}),
			refs:     1,
			maxN:     h.cfg.RingSize,
			maxBytes: h.cfg.RingBytes,
			notify:   make(chan struct{}),
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
			wake:    h.cfg.Keyspace.Prefix + streamid.HashTag(g.bucket) + ":__wake:" + h.id + ":" + strconv.Itoa(h.nextID),
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

// retire stops an empty reader, once. Caller holds h.gmu. A stream that
// ended leaves its reader while its subscribers still point at it, so a
// reader can become empty (and be retired) more than once; a second retire
// must not delete the group that replaced this reader's group.
func (h *Hub) retire(ctx context.Context, r *reader) {
	if r.retired {
		return
	}
	r.retired = true
	g := r.g
	for i, c := range g.readers {
		if c == r {
			g.readers = append(g.readers[:i], g.readers[i+1:]...)
			break
		}
	}
	if len(g.readers) == 0 && h.groups[g.bucket] == g {
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
		r.h.log.WarnContext(ctx, "redisbus: wake reader", "err", err)
	}
}

func (r *reader) run(ctx context.Context) {
	defer func() {
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
		defer cancel()
		_ = r.h.rdb.Del(cctx, r.wake).Err()
	}()
	wakeCur := "0-0"
	attempt := 0
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
				attempt = 0
				continue
			}
			if ctx.Err() != nil {
				return
			}
			r.h.met.readErrors.Add(ctx, 1)
			r.h.log.WarnContext(ctx, "redisbus: xread failed", "err", err, "streams", len(keys))
			attempt++
			if !sleepCtx(ctx, timex.Backoff(r.h.cfg.RetryBackoff, r.h.cfg.MaxRetryBackoff, attempt)) {
				return
			}
			continue
		}
		attempt = 0
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
