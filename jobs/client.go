package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/otelx"
)

// maxUniqueKeyBytes bounds InsertOpts.UniqueKey.
const maxUniqueKeyBytes = 255

// Client inserts jobs and, with [Client.Run], runs them. TTx is the
// transaction type of the driver. A client without a registry only inserts.
type Client[TTx any] struct {
	drv   Driver[TTx]
	o     options
	log   *log.Logger
	tel   *telemetry
	kinds []string

	mu      sync.Mutex
	running map[int64]runningJob
	wakers  []chan struct{}
	started bool
	cancel  context.CancelFunc
	done    chan struct{}
}

// NewClient returns a Client on drv. It does not touch the database.
func NewClient[TTx any](drv Driver[TTx], opts ...Option) (*Client[TTx], error) {
	if drv == nil {
		return nil, errors.New("jobs: nil driver")
	}
	o := defaultOptions()
	for _, f := range opts {
		f(&o)
	}
	if err := o.validate(); err != nil {
		return nil, err
	}
	c := &Client[TTx]{drv: drv, o: o, log: log.OrDefault(o.logger), tel: newTelemetry(&o), running: map[int64]runningJob{}}
	if o.registry != nil {
		c.kinds = o.registry.Kinds()
	}
	return c, nil
}

// Insert inserts one job in a transaction of its own. [Client.InsertTx]
// inserts in the caller's transaction.
func (c *Client[TTx]) Insert(ctx context.Context, args Args, opts *InsertOpts) (InsertResult, error) {
	res, err := c.InsertMany(ctx, []InsertParams{{Args: args, Opts: opts}})
	if err != nil {
		return InsertResult{}, err
	}
	return res[0], nil
}

// InsertTx inserts one job inside tx. The job is visible to workers when tx
// commits and never if it rolls back.
func (c *Client[TTx]) InsertTx(ctx context.Context, tx TTx, args Args, opts *InsertOpts) (InsertResult, error) {
	res, err := c.InsertManyTx(ctx, tx, []InsertParams{{Args: args, Opts: opts}})
	if err != nil {
		return InsertResult{}, err
	}
	return res[0], nil
}

// InsertMany inserts jobs in one transaction of its own.
func (c *Client[TTx]) InsertMany(ctx context.Context, params []InsertParams) ([]InsertResult, error) {
	return c.insertMany(ctx, params, c.drv.Insert)
}

// InsertManyTx inserts jobs inside tx.
func (c *Client[TTx]) InsertManyTx(ctx context.Context, tx TTx, params []InsertParams) ([]InsertResult, error) {
	return c.insertMany(ctx, params, func(ctx context.Context, jobs []NewJob) ([]InsertResult, error) {
		return c.drv.InsertTx(ctx, tx, jobs)
	})
}

func (c *Client[TTx]) insertMany(ctx context.Context, params []InsertParams, do func(context.Context, []NewJob) ([]InsertResult, error)) (_ []InsertResult, err error) {
	if len(params) == 0 {
		return nil, nil
	}
	ctx, span, meta := c.tel.startInsert(ctx, len(params))
	defer otelx.End(span, &err)
	metaJSON, err := json.Marshal(meta)
	if err != nil {
		return nil, err
	}
	rows := make([]NewJob, len(params))
	for i, p := range params {
		if rows[i], err = c.resolve(p); err != nil {
			return nil, fmt.Errorf("job %d: %w", i, err)
		}
		rows[i].Metadata = metaJSON
	}
	res, err := do(ctx, rows)
	if err != nil {
		return nil, err
	}
	if len(res) != len(rows) {
		return nil, fmt.Errorf("jobs: driver returned %d results for %d jobs", len(res), len(rows))
	}
	for i, r := range res {
		if !r.Duplicate {
			c.tel.inserted.Add(ctx, 1, metric.WithAttributes(jobAttrs(rows[i].Kind, rows[i].Queue)...))
		}
	}
	span.SetAttributes(attribute.Int("kitchen.jobs.count", len(rows)))
	c.signal()
	return res, nil
}

// resolve validates p and applies the defaults: first the client's, then the
// args', then the call's.
func (c *Client[TTx]) resolve(p InsertParams) (NewJob, error) {
	invalid := func(format string, a ...any) (NewJob, error) {
		return NewJob{}, errs.Wrap(fmt.Errorf("jobs: "+format, a...), errs.ErrInvalidArgument)
	}
	if p.Args == nil {
		return invalid("nil args")
	}
	kind := p.Args.Kind()
	if !kindRE.MatchString(kind) {
		return invalid("invalid kind %q", kind)
	}
	opts := InsertOpts{Queue: "default", Priority: 1, MaxAttempts: c.o.maxAttempts}
	merge := func(o InsertOpts) {
		if o.Queue != "" {
			opts.Queue = o.Queue
		}
		if o.Priority != 0 {
			opts.Priority = o.Priority
		}
		if o.MaxAttempts != 0 {
			opts.MaxAttempts = o.MaxAttempts
		}
		if !o.ScheduledAt.IsZero() {
			opts.ScheduledAt = o.ScheduledAt
		}
		if o.UniqueKey != "" {
			opts.UniqueKey = o.UniqueKey
		}
		opts.KeepUnique = opts.KeepUnique || o.KeepUnique
	}
	if d, ok := p.Args.(InsertDefaults); ok {
		merge(d.InsertOpts())
	}
	if p.Opts != nil {
		merge(*p.Opts)
	}
	switch {
	case !queueRE.MatchString(opts.Queue):
		return invalid("invalid queue name %q", opts.Queue)
	case opts.Priority < 1 || opts.Priority > 32767:
		return invalid("priority %d out of range 1..32767", opts.Priority)
	case opts.MaxAttempts < 1:
		return invalid("max attempts %d must be at least 1", opts.MaxAttempts)
	case len(opts.UniqueKey) > maxUniqueKeyBytes:
		return invalid("unique key is %d bytes, the limit is %d", len(opts.UniqueKey), maxUniqueKeyBytes)
	}
	args, err := json.Marshal(p.Args)
	if err != nil {
		return invalid("encode args of kind %q: %v", kind, err)
	}
	return NewJob{
		Kind: kind, Queue: opts.Queue, Args: args, Priority: opts.Priority, MaxAttempts: opts.MaxAttempts,
		Delay: delayUntil(opts.ScheduledAt), UniqueKey: opts.UniqueKey, KeepUnique: opts.KeepUnique,
	}, nil
}

// Get returns a job by ID, or [ErrNotFound].
func (c *Client[TTx]) Get(ctx context.Context, id int64) (*Row, error) {
	return c.drv.Get(ctx, id)
}
