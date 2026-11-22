package redisbus

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc/codes"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/sse"
)

var (
	//go:embed append.lua
	appendLua    string
	appendScript = redis.NewScript(appendLua)
	//go:embed end.lua
	endLua    string
	endScript = redis.NewScript(endLua)
)

var (
	// ErrStreamEnded means append was called after End. FailedPrecondition.
	ErrStreamEnded = errs.New(codes.FailedPrecondition, "SSE_STREAM_ENDED", errs.Message("stream has ended"))
	// ErrOutboxDiverged means split mode wrote the stream but not the outbox. The
	// returned entry ID is valid; the projection misses this event.
	ErrOutboxDiverged = errs.New(codes.Internal, "SSE_OUTBOX_DIVERGED", errs.Message("redisbus: outbox write failed after stream write"))
	// ErrInvalidMessage means empty or reserved type, CR/LF/NUL in type, or payload too large.
	ErrInvalidMessage = errs.New(codes.InvalidArgument, "SSE_INVALID_MESSAGE", errs.Message("redisbus: invalid message"))
)

// Producer defaults.
const (
	DefaultMaxLen          = 10_000
	DefaultActiveTTL       = 24 * time.Hour
	DefaultDoneTTL         = 2 * time.Minute
	DefaultMaxPayloadBytes = 1 << 20
	DefaultMinIDRetention  = 6 * time.Hour
)

// OutboxConfig enables the outbox leg: every Append is also written to the
// outbox shard of the stream's bucket, which relay.Relay drains.
type OutboxConfig struct {
	// Stream is the outbox base key, e.g. "outbox:orch". Required.
	Stream string `json:"stream" yaml:"stream"`
	// MaxLen caps each shard (approximate); 0 = no count cap.
	MaxLen int64 `json:"max_len" yaml:"max_len"`
	// MinIDRetention trims shard entries older than this on each write.
	// Default 6h; negative disables time-based trimming.
	MinIDRetention time.Duration `json:"min_id_retention" yaml:"min_id_retention"`
}

// ProducerConfig configures a Producer. Zero values select defaults.
type ProducerConfig struct {
	Keyspace Keyspace `json:"keyspace" yaml:"keyspace"`
	// MaxLen trims each stream to about this many entries. Default 10000.
	MaxLen int64 `json:"max_len" yaml:"max_len"`
	// ActiveTTL is refreshed on every Append. Default 24h; negative = none.
	ActiveTTL time.Duration `json:"active_ttl" yaml:"active_ttl"`
	// DoneTTL is set by End. Default 2m; negative = none.
	DoneTTL time.Duration `json:"done_ttl" yaml:"done_ttl"`
	// MaxPayloadBytes rejects larger payloads. Default 1 MiB.
	MaxPayloadBytes int `json:"max_payload_bytes" yaml:"max_payload_bytes"`
	// Outbox enables the outbox leg (nil = none).
	Outbox *OutboxConfig `json:"outbox" yaml:"outbox"`
	// OutboxClient, when set, writes the outbox leg on another Redis after the
	// stream write (split mode, best effort). When nil the leg is written
	// atomically with the stream entry (mirror mode).
	OutboxClient redis.UniversalClient `json:"-" yaml:"-"`
	// MeterProvider for metrics; nil = otel.GetMeterProvider().
	MeterProvider metric.MeterProvider `json:"-" yaml:"-"`
}

// Producer appends messages to streams. It keeps no per-stream state, so any
// number of processes, retries and replays may write the same stream.
type Producer struct {
	rdb     redis.UniversalClient
	cfg     ProducerConfig
	appends metric.Int64Counter
}

// NewProducer returns a Producer writing through rdb.
func NewProducer(rdb redis.UniversalClient, cfg ProducerConfig) (*Producer, error) {
	if rdb == nil {
		return nil, errors.New("redisbus: nil redis client")
	}
	ks, err := cfg.Keyspace.withDefaults()
	if err != nil {
		return nil, err
	}
	cfg.Keyspace = ks
	if cfg.MaxLen == 0 {
		cfg.MaxLen = DefaultMaxLen
	}
	if cfg.ActiveTTL == 0 {
		cfg.ActiveTTL = DefaultActiveTTL
	}
	if cfg.DoneTTL == 0 {
		cfg.DoneTTL = DefaultDoneTTL
	}
	if cfg.MaxPayloadBytes <= 0 {
		cfg.MaxPayloadBytes = DefaultMaxPayloadBytes
	}
	if cfg.MaxLen < 0 {
		return nil, errors.New("redisbus: negative MaxLen")
	}
	if o := cfg.Outbox; o != nil {
		if o.Stream == "" || strings.ContainsAny(o.Stream, "{}") {
			return nil, errors.New("redisbus: outbox stream must be set and must not contain braces")
		}
		oc := *o
		if oc.MinIDRetention == 0 {
			oc.MinIDRetention = DefaultMinIDRetention
		}
		cfg.Outbox = &oc
	} else if cfg.OutboxClient != nil {
		return nil, errors.New("redisbus: OutboxClient set without Outbox")
	}
	mp := cfg.MeterProvider
	if mp == nil {
		mp = otel.GetMeterProvider()
	}
	appends, _ := mp.Meter(sse.ScopeName).Int64Counter("sse.producer.appends",
		metric.WithDescription("Producer appends by result."))
	return &Producer{rdb: rdb, cfg: cfg, appends: appends}, nil
}

// Keyspace returns the effective keyspace.
func (p *Producer) Keyspace() Keyspace { return p.cfg.Keyspace }

func ms(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return d.Milliseconds()
}

func (p *Producer) validate(streamID string, m sse.Message) error {
	if err := validStreamID(streamID); err != nil {
		return err
	}
	if m.Type == "" || m.Type == endType || strings.ContainsAny(m.Type, "\r\n\x00") {
		return fmt.Errorf("%w: type %q", ErrInvalidMessage, m.Type)
	}
	if len(m.Data) > p.cfg.MaxPayloadBytes {
		return fmt.Errorf("%w: payload %d bytes exceeds %d", ErrInvalidMessage, len(m.Data), p.cfg.MaxPayloadBytes)
	}
	return nil
}

// Append adds m to streamID and returns its entry ID. m.ID is ignored. It
// returns ErrStreamEnded after End, and in split mode the ID together with
// ErrOutboxDiverged when only the outbox leg failed.
func (p *Producer) Append(ctx context.Context, streamID string, m sse.Message) (string, error) {
	if err := p.validate(streamID, m); err != nil {
		p.count(ctx, "invalid")
		return "", err
	}
	key := p.cfg.Keyspace.Key(streamID)
	keys := []string{key}
	args := []any{m.Type, m.Data, p.cfg.MaxLen, ms(p.cfg.ActiveTTL), streamID, int64(0), ""}
	o := p.cfg.Outbox
	mirror := o != nil && p.cfg.OutboxClient == nil
	if mirror {
		keys = append(keys, p.cfg.Keyspace.OutboxKey(o.Stream, streamID))
		args[5] = o.MaxLen
		args[6] = minID(o.MinIDRetention)
	}
	id, err := appendScript.Run(ctx, p.rdb, keys, args...).Text()
	if err != nil {
		if strings.HasPrefix(err.Error(), "SSEENDED") {
			p.count(ctx, "ended")
			return "", fmt.Errorf("%w: %s", ErrStreamEnded, streamID)
		}
		p.count(ctx, "error")
		return "", fmt.Errorf("redisbus: append %s: %w", streamID, err)
	}
	if o != nil && !mirror {
		if err := p.splitOutbox(ctx, streamID, m); err != nil {
			p.count(ctx, "diverged")
			return id, errors.Join(ErrOutboxDiverged, err)
		}
	}
	p.count(ctx, "ok")
	return id, nil
}

func (p *Producer) splitOutbox(ctx context.Context, streamID string, m sse.Message) error {
	o := p.cfg.Outbox
	args := &redis.XAddArgs{
		Stream: p.cfg.Keyspace.OutboxKey(o.Stream, streamID),
		Values: []any{fieldType, m.Type, fieldData, m.Data, fieldStream, streamID},
		Approx: true,
	}
	if id := minID(o.MinIDRetention); id != "" {
		args.MinID = id
	} else if o.MaxLen > 0 {
		args.MaxLen = o.MaxLen
	}
	pipe := p.cfg.OutboxClient.Pipeline()
	pipe.XAdd(ctx, args)
	if args.MinID != "" && o.MaxLen > 0 {
		pipe.XTrimMaxLenApprox(ctx, args.Stream, o.MaxLen, 0)
	}
	_, err := pipe.Exec(ctx)
	return err
}

func minID(retention time.Duration) string {
	if retention <= 0 {
		return ""
	}
	return strconv.FormatInt(time.Now().Add(-retention).UnixMilli(), 10)
}

// End appends the end-of-stream sentinel (once; repeated calls are no-ops)
// and sets DoneTTL. Readers finish cleanly when they reach it, and later
// Appends fail with ErrStreamEnded. It returns the sentinel's entry ID.
func (p *Producer) End(ctx context.Context, streamID string) (string, error) {
	if err := validStreamID(streamID); err != nil {
		return "", err
	}
	id, err := endScript.Run(ctx, p.rdb, []string{p.cfg.Keyspace.Key(streamID)}, ms(p.cfg.DoneTTL), p.cfg.MaxLen).Text()
	if err != nil {
		return "", fmt.Errorf("redisbus: end %s: %w", streamID, err)
	}
	return id, nil
}

func (p *Producer) count(ctx context.Context, result string) {
	p.appends.Add(context.WithoutCancel(ctx), 1, metric.WithAttributes(attribute.String("result", result)))
}
