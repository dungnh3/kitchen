package pubsub

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/ratelimit"
)

// nackLogRate caps "message nacked" warnings per subscriber, so an outage
// that fails every message does not flood the logs. The metrics still count
// every nack.
var nackLogRate = ratelimit.Rule{Rate: 10, Per: time.Second}

// sampledLog writes at most rule.Rate warnings per rule.Per and reports how
// many were suppressed since the last one written.
type sampledLog struct {
	log        *log.Logger
	store      *ratelimit.LocalStore
	rule       ratelimit.Rule
	suppressed atomic.Int64
}

func newSampledLog(l *log.Logger, rule ratelimit.Rule) *sampledLog {
	return &sampledLog{log: l, store: ratelimit.NewLocalStore(1), rule: rule}
}

// nacked logs a nacked message at Warn level, sampled.
func (s *sampledLog) nacked(ctx context.Context, attrs ...log.Attr) {
	if d, err := s.store.Allow(ctx, "warn", s.rule); err == nil && !d.Allowed {
		s.suppressed.Add(1)
		return
	}
	if n := s.suppressed.Swap(0); n > 0 {
		attrs = append(attrs, log.Int64("suppressed", n))
	}
	s.log.LogAttrs(ctx, log.LevelWarn, "pubsub: message nacked", attrs...)
}
