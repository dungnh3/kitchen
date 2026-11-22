// Package relay drains the outbox shards written by redisbus.Producer
// (mirror or split mode) through Redis consumer groups and publishes every
// entry to a kitchen pubsub.Publisher topic (pubsub/gcp, kafka, sqs, ...).
//
// It is not a transactional outbox: the source of truth is Redis. Run Redis
// with maxmemory-policy noeviction and keep MaxAge below the outbox retention.
//
// Delivery:
//
//   - At least once. An entry is acknowledged only after it was published or
//     dead-lettered (DLQ XADD and XACK in one script). Consumers dedupe, e.g.
//     on (run_id, event_id).
//   - Ordered per ordering key within a replica. After a failure the key is
//     held: its entries stay pending, the publisher's ResumeKey is called
//     (if it implements pubsub.KeyResumer), and the pending entries are
//     retried in ID order after a timex.Backoff delay. Other keys keep flowing.
//   - Poison entries (Classify returns Poison; by default errors marked
//     pubsub.Permanent) are dead-lettered after
//     PoisonAttempts; unroutable entries at once; entries older than MaxAge
//     whose key keeps failing are dead-lettered with reason "expired".
//   - Entries of a dead consumer are taken over with XAUTOCLAIM once idle for
//     MinIdle. After a takeover, order across the takeover is best effort.
package relay

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/pubsub"
	"github.com/dungnh3/kitchen/redisx"
	"github.com/dungnh3/kitchen/sse"
	"github.com/dungnh3/kitchen/sse/internal/streamid"
	"github.com/dungnh3/kitchen/timex"
)

// Class classifies a publish error.
type Class int

const (
	// Transient errors are retried with backoff, forever (until MaxAge).
	Transient Class = iota
	// Poison errors dead-letter the entry after PoisonAttempts.
	Poison
)

// Entry is one outbox entry as read from Redis.
type Entry struct {
	ID       string // Redis entry ID
	Shard    string // outbox shard key
	Type     string
	Data     []byte
	StreamID string // the source sse stream
}

// Route maps an entry to a message. A non-nil error dead-letters the entry.
type Route func(e Entry) (*pubsub.Message, error)

// DefaultRoute orders by source stream (Key) and sets the attributes type,
// stream and outbox_id.
func DefaultRoute(e Entry) (*pubsub.Message, error) {
	return &pubsub.Message{
		Key:        e.StreamID,
		Data:       e.Data,
		Attributes: map[string]string{"type": e.Type, "stream": e.StreamID, AttrOutboxID: e.ID},
	}, nil
}

// Config configures a Relay. Zero values select defaults.
type Config struct {
	// Stream is the outbox base key (redisbus.OutboxConfig.Stream). Required.
	Stream string `json:"stream" yaml:"stream"`
	// Topic is the pubsub topic entries are published to. Required.
	Topic string `json:"topic" yaml:"topic"`
	// Buckets must equal the producers' Keyspace.Buckets. Default 64.
	Buckets int `json:"buckets" yaml:"buckets"`
	// Group and Consumer name the consumer group and this replica. Required.
	Group    string `json:"group" yaml:"group"`
	Consumer string `json:"consumer" yaml:"consumer"`
	// Batch is the read COUNT. Default 100.
	Batch int64 `json:"batch" yaml:"batch"`
	// Block is the XREADGROUP BLOCK timeout. Default 2s.
	Block time.Duration `json:"block" yaml:"block"`
	// MaxInFlight bounds ordering keys published concurrently. Default 64.
	MaxInFlight int `json:"max_in_flight" yaml:"max_in_flight"`
	// PublishTimeout bounds one Publish call. Default 20s.
	PublishTimeout time.Duration `json:"publish_timeout" yaml:"publish_timeout"`
	// RetryBackoff / MaxRetryBackoff bound the per-key backoff. Defaults 500ms / 30s.
	RetryBackoff    time.Duration `json:"retry_backoff" yaml:"retry_backoff"`
	MaxRetryBackoff time.Duration `json:"max_retry_backoff" yaml:"max_retry_backoff"`
	// PoisonAttempts before a Poison-classified entry is dead-lettered. Default 5.
	PoisonAttempts int `json:"poison_attempts" yaml:"poison_attempts"`
	// MaxAge dead-letters entries of a failing key older than this. Default 5h.
	MaxAge time.Duration `json:"max_age" yaml:"max_age"`
	// MinIdle is how long another consumer's entry must be idle before it is
	// taken over. Default 1m. ReclaimInterval is how often to look. Default 30s.
	MinIdle         time.Duration `json:"min_idle" yaml:"min_idle"`
	ReclaimInterval time.Duration `json:"reclaim_interval" yaml:"reclaim_interval"`
	// Route maps entries to messages; nil = DefaultRoute.
	Route Route `json:"-" yaml:"-"`
	// Classify classifies publish errors; nil = DefaultClassify.
	Classify func(error) Class `json:"-" yaml:"-"`
	// Logger; nil = log.Default().
	Logger *log.Logger `json:"-" yaml:"-"`
	// MeterProvider; nil = otel.GetMeterProvider().
	MeterProvider metric.MeterProvider `json:"-" yaml:"-"`
}

func (c *Config) defaults() error {
	if c.Stream == "" || strings.ContainsAny(c.Stream, "{}") {
		return errors.New("relay: Stream must be set and must not contain braces")
	}
	if c.Group == "" || c.Consumer == "" || c.Topic == "" {
		return errors.New("relay: Group, Consumer and Topic are required")
	}
	if c.Buckets == 0 {
		c.Buckets = 64
	}
	if c.Buckets < 0 || c.Buckets > 16384 {
		return fmt.Errorf("relay: buckets %d out of range", c.Buckets)
	}
	def(&c.Batch, 100)
	def(&c.Block, 2*time.Second)
	def(&c.MaxInFlight, 64)
	def(&c.PublishTimeout, 20*time.Second)
	def(&c.RetryBackoff, 500*time.Millisecond)
	def(&c.MaxRetryBackoff, 30*time.Second)
	def(&c.PoisonAttempts, 5)
	def(&c.MaxAge, 5*time.Hour)
	def(&c.MinIdle, time.Minute)
	def(&c.ReclaimInterval, 30*time.Second)
	if c.Route == nil {
		c.Route = DefaultRoute
	}
	if c.Classify == nil {
		c.Classify = DefaultClassify
	}
	c.Logger = log.OrDefault(c.Logger)
	return nil
}

// DefaultClassify treats errors marked with pubsub.Permanent (or wrapping
// pubsub.ErrPermanent) as Poison and everything else as Transient.
func DefaultClassify(err error) Class {
	if errors.Is(err, pubsub.ErrPermanent) {
		return Poison
	}
	return Transient
}

func def[T int | int64 | time.Duration](v *T, d T) {
	if *v <= 0 {
		*v = d
	}
}

//go:embed dlq.lua
var dlqLua string

var dlqScript = redis.NewScript(dlqLua)

// Relay drains outbox shards into a kitchen pubsub.Publisher.
type Relay struct {
	rdb redis.UniversalClient
	cfg Config
	pub pubsub.Publisher
	met relayMetrics
}

// New returns a Relay publishing to cfg.Topic with pub (for example
// pubsub/gcp with ordering enabled). It does not touch Redis until Run.
func New(rdb redis.UniversalClient, cfg Config, pub pubsub.Publisher) (*Relay, error) {
	if rdb == nil || pub == nil {
		return nil, errors.New("relay: nil redis client or publisher")
	}
	if err := cfg.defaults(); err != nil {
		return nil, err
	}
	return &Relay{rdb: rdb, cfg: cfg, pub: pub, met: newRelayMetrics(cfg.MeterProvider)}, nil
}

// Shards returns the outbox shard keys, e.g. "outbox:orch:{b0}".
func (r *Relay) Shards() []string {
	out := make([]string, r.cfg.Buckets)
	for b := range out {
		out[b] = shardKey(r.cfg.Stream, b)
	}
	return out
}

func shardKey(stream string, bucket int) string { return stream + ":" + streamid.HashTag(bucket) }

// DLQKey returns the dead-letter stream of a shard.
func DLQKey(shard string) string { return shard + ":dlq" }

// Run creates the consumer groups if needed and relays until ctx is done.
// In-flight publishes finish (bounded by PublishTimeout) before it returns.
func (r *Relay) Run(ctx context.Context) error {
	shards := r.Shards()
	for _, s := range shards {
		err := r.rdb.XGroupCreateMkStream(ctx, s, r.cfg.Group, "0").Err()
		if err != nil && !strings.HasPrefix(err.Error(), "BUSYGROUP") {
			return fmt.Errorf("relay: create group on %s: %w", s, err)
		}
	}
	// One loop per slot-compatible partition: every shard on Cluster, all
	// shards together otherwise.
	var parts [][]string
	if redisx.IsCluster(r.rdb) {
		for _, s := range shards {
			parts = append(parts, []string{s})
		}
	} else {
		parts = [][]string{shards}
	}
	var wg sync.WaitGroup
	for _, p := range parts {
		wg.Go(func() { newLoop(r, p).run(ctx) })
	}
	wg.Wait()
	return nil
}

type backoffState struct {
	until    time.Time
	attempts int
}

type loop struct {
	r        *Relay
	shards   []string
	backoff  map[string]backoffState // ordering key → hold
	attempts map[string]int          // entry ID → poison attempts
	pending  bool                    // our PEL may hold entries to retry
	reclaim  time.Time
	sem      chan struct{}
}

func newLoop(r *Relay, shards []string) *loop {
	return &loop{
		r: r, shards: shards, backoff: map[string]backoffState{}, attempts: map[string]int{},
		pending: true, sem: make(chan struct{}, r.cfg.MaxInFlight),
	}
}

func (l *loop) run(ctx context.Context) {
	cfg := l.r.cfg
	errAttempt := 0
	for ctx.Err() == nil {
		if now := time.Now(); now.After(l.reclaim) {
			l.reclaim = now.Add(cfg.ReclaimInterval)
			if l.claim(ctx) {
				l.pending = true
			}
		}
		drained := true
		if l.pending {
			var err error
			if drained, err = l.processPending(ctx); err != nil {
				if !l.readFailed(ctx, err, &errAttempt) {
					return
				}
				continue
			}
		}
		if !drained {
			continue // finish the pending list before reading new entries
		}
		entries, err := l.read(ctx, ">", cfg.Block)
		if err != nil {
			if !l.readFailed(ctx, err, &errAttempt) {
				return
			}
			continue
		}
		errAttempt = 0
		l.process(ctx, entries)
	}
}

func (l *loop) readFailed(ctx context.Context, err error, attempt *int) bool {
	if ctx.Err() != nil {
		return false
	}
	l.r.cfg.Logger.WarnContext(ctx, "relay: read failed", "err", err)
	*attempt++
	return sleep(ctx, timex.Backoff(l.r.cfg.RetryBackoff, l.r.cfg.MaxRetryBackoff, *attempt))
}

// read reads from every shard of the loop. id is ">" for new entries or an
// entry ID for this consumer's pending list.
func (l *loop) read(ctx context.Context, id string, block time.Duration) ([]Entry, error) {
	streams := make([]string, 0, 2*len(l.shards))
	streams = append(streams, l.shards...)
	for range l.shards {
		streams = append(streams, id)
	}
	args := &redis.XReadGroupArgs{Group: l.r.cfg.Group, Consumer: l.r.cfg.Consumer, Streams: streams, Count: l.r.cfg.Batch, Block: block}
	if block <= 0 {
		args.Block = -1
	}
	res, err := l.r.rdb.XReadGroup(ctx, args).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []Entry
	for _, s := range res {
		for _, m := range s.Messages {
			out = append(out, toEntry(s.Stream, m))
		}
	}
	return out, nil
}

func toEntry(shard string, m redis.XMessage) Entry {
	typ, _ := streamid.Field(m, "type")
	stream, _ := streamid.Field(m, "stream")
	e := Entry{ID: m.ID, Shard: shard, Type: typ, StreamID: stream}
	if data, ok := streamid.Field(m, "data"); ok {
		e.Data = []byte(data)
	}
	if _, ok := m.Values["data"]; !ok {
		e.Data = nil
	}
	return e
}

// maxPendingPages bounds one pass over the pending list.
const maxPendingPages = 100

// processPending re-reads this consumer's pending entries per shard (which
// also keeps them from looking idle to other consumers) and retries those
// whose key is not on hold. It reports whether the whole list was read.
func (l *loop) processPending(ctx context.Context) (bool, error) {
	l.pending = false
	for _, shard := range l.shards {
		cursor := "0"
		for page := 0; ; page++ {
			if page == maxPendingPages {
				return false, nil
			}
			res, err := l.r.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group: l.r.cfg.Group, Consumer: l.r.cfg.Consumer,
				Streams: []string{shard, cursor}, Count: l.r.cfg.Batch, Block: -1,
			}).Result()
			if err != nil && !errors.Is(err, redis.Nil) {
				return false, err
			}
			var entries []Entry
			for _, s := range res {
				for _, m := range s.Messages {
					entries = append(entries, toEntry(shard, m))
				}
			}
			if len(entries) == 0 {
				break
			}
			l.process(ctx, entries)
			cursor = entries[len(entries)-1].ID
			if int64(len(entries)) < l.r.cfg.Batch {
				break
			}
		}
	}
	if len(l.backoff) > 0 {
		l.pending = true
	}
	return true, nil
}

// process publishes a batch: per ordering key in order, keys in parallel.
func (l *loop) process(ctx context.Context, entries []Entry) {
	if len(entries) == 0 {
		return
	}
	type keyed struct {
		key     string
		entries []Entry
	}
	var order []*keyed
	byKey := map[string]*keyed{}
	var dead []deadLetter
	for _, e := range entries {
		if e.Data == nil {
			dead = append(dead, deadLetter{e, "malformed", "entry has no data field"})
			continue
		}
		msg, err := l.r.cfg.Route(e)
		if err != nil {
			dead = append(dead, deadLetter{e, "unroutable", err.Error()})
			continue
		}
		k := byKey[msg.Key]
		if k == nil {
			k = &keyed{key: msg.Key}
			byKey[msg.Key] = k
			order = append(order, k)
		}
		k.entries = append(k.entries, e)
	}
	l.deadLetter(ctx, dead)

	now := time.Now()
	var (
		mu     sync.Mutex
		acks   = map[string][]string{}
		dlq    []deadLetter
		failed = map[string]bool{}
		counts []map[string]int
		wg     sync.WaitGroup
	)
	for _, k := range order {
		if b, held := l.backoff[k.key]; held && now.Before(b.until) {
			l.pending = true
			continue // stays pending; retried in order after the backoff
		}
		attempts := make(map[string]int, len(k.entries))
		for _, e := range k.entries {
			attempts[e.ID] = l.attempts[e.ID]
		}
		l.sem <- struct{}{}
		wg.Go(func() {
			defer func() { <-l.sem }()
			okIDs, dl, fail := l.publishKey(ctx, k.key, k.entries, attempts)
			mu.Lock()
			defer mu.Unlock()
			for _, e := range okIDs {
				acks[e.Shard] = append(acks[e.Shard], e.ID)
			}
			dlq = append(dlq, dl...)
			if fail {
				failed[k.key] = true
			}
			counts = append(counts, attempts)
		})
	}
	wg.Wait()
	for _, attempts := range counts {
		for id, n := range attempts {
			l.attemptsSet(id, n)
		}
	}
	l.ack(ctx, acks)
	l.deadLetter(ctx, dlq)
	for _, k := range order {
		if failed[k.key] {
			n := l.backoff[k.key].attempts + 1
			d := timex.Backoff(l.r.cfg.RetryBackoff, l.r.cfg.MaxRetryBackoff, n)
			l.backoff[k.key] = backoffState{until: time.Now().Add(d), attempts: n}
			l.pending = true
		} else if _, held := l.backoff[k.key]; held && !now.Before(l.backoff[k.key].until) {
			delete(l.backoff, k.key)
		}
	}
}

// attemptsSet runs on the loop goroutine only.
func (l *loop) attemptsSet(id string, n int) {
	if n == 0 {
		delete(l.attempts, id)
		return
	}
	l.attempts[id] = n
}

type deadLetter struct {
	e      Entry
	reason string
	err    string
}

// publishKey publishes one key's entries in order, stopping at the first
// failure. attempts is updated in place (0 = forget).
func (l *loop) publishKey(ctx context.Context, key string, entries []Entry, attempts map[string]int) (ok []Entry, dead []deadLetter, failed bool) {
	cfg := l.r.cfg
	for _, e := range entries {
		msg, _ := cfg.Route(e) // routable: checked by process
		pctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.PublishTimeout)
		_, err := l.r.pub.Publish(pctx, cfg.Topic, msg)
		cancel()
		if err == nil {
			ok = append(ok, e)
			attempts[e.ID] = 0
			l.r.met.published.Add(ctx, 1)
			continue
		}
		l.r.met.errors.Add(ctx, 1)
		if cfg.Classify(err) == Poison {
			attempts[e.ID]++
			if attempts[e.ID] >= cfg.PoisonAttempts {
				attempts[e.ID] = 0
				dead = append(dead, deadLetter{e, "poison", err.Error()})
				continue
			}
		} else if age(e.ID) > cfg.MaxAge {
			attempts[e.ID] = 0
			dead = append(dead, deadLetter{e, "expired", err.Error()})
			continue
		}
		cfg.Logger.WarnContext(ctx, "relay: publish failed; holding key", "key", key, "entry", e.ID, "err", err)
		if res, ok := l.r.pub.(pubsub.KeyResumer); ok && key != "" {
			res.ResumeKey(cfg.Topic, key)
		}
		return ok, dead, true
	}
	return ok, dead, false
}

func age(id string) time.Duration {
	p, err := streamid.ParseID(id)
	if err != nil {
		return 0
	}
	return time.Since(time.UnixMilli(int64(p.Ms))) //nolint:gosec // entry IDs are wall-clock ms
}

func (l *loop) ack(ctx context.Context, acks map[string][]string) {
	actx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	for shard, ids := range acks {
		if err := l.r.rdb.XAck(actx, shard, l.r.cfg.Group, ids...).Err(); err != nil {
			// The entries stay pending and are published again: at least once.
			l.r.cfg.Logger.WarnContext(ctx, "relay: ack failed", "shard", shard, "n", len(ids), "err", err)
			l.pending = true
		}
	}
}

func (l *loop) deadLetter(ctx context.Context, dl []deadLetter) {
	if len(dl) == 0 {
		return
	}
	dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	for _, d := range dl {
		err := dlqScript.Run(dctx, l.r.rdb, []string{d.e.Shard, DLQKey(d.e.Shard)},
			l.r.cfg.Group, d.e.ID, d.e.Type, string(d.e.Data), d.e.StreamID, d.reason, d.err).Err()
		if err != nil {
			l.r.cfg.Logger.ErrorContext(ctx, "relay: dead-letter failed", "entry", d.e.ID, "err", err)
			l.pending = true
			continue
		}
		delete(l.attempts, d.e.ID)
		l.r.met.dead.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", d.reason)))
		l.r.cfg.Logger.WarnContext(ctx, "relay: dead-lettered", "entry", d.e.ID, "reason", d.reason, "err", d.err)
	}
}

// claim takes over entries idle for MinIdle (their consumer died).
func (l *loop) claim(ctx context.Context) bool {
	claimed := false
	for _, shard := range l.shards {
		start := "0-0"
		for range maxPendingPages {
			ids, next, err := l.r.rdb.XAutoClaimJustID(ctx, &redis.XAutoClaimArgs{
				Stream: shard, Group: l.r.cfg.Group, Consumer: l.r.cfg.Consumer,
				MinIdle: l.r.cfg.MinIdle, Start: start, Count: l.r.cfg.Batch,
			}).Result()
			if err != nil {
				if ctx.Err() == nil {
					l.r.cfg.Logger.WarnContext(ctx, "relay: autoclaim failed", "shard", shard, "err", err)
				}
				break
			}
			claimed = claimed || len(ids) > 0
			if next == "0-0" || next == "" {
				break
			}
			start = next
		}
	}
	return claimed
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

type relayMetrics struct {
	published metric.Int64Counter
	dead      metric.Int64Counter
	errors    metric.Int64Counter
}

func newRelayMetrics(mp metric.MeterProvider) relayMetrics {
	if mp == nil {
		mp = otel.GetMeterProvider()
	}
	m := mp.Meter(sse.ScopeName)
	published, _ := m.Int64Counter("sse.relay.published", metric.WithDescription("Outbox entries published."))
	dead, _ := m.Int64Counter("sse.relay.dead_lettered", metric.WithDescription("Outbox entries dead-lettered, by reason."))
	errs, _ := m.Int64Counter("sse.relay.publish.errors", metric.WithDescription("Failed publish attempts."))
	return relayMetrics{published: published, dead: dead, errors: errs}
}
