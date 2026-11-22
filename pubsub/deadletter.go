package pubsub

import (
	"context"
	"fmt"
	"strconv"
	"time"
	"unicode/utf8"
)

// Attributes set by [DeadLetterTo] on a dead-lettered copy.
const (
	AttrDeadLetterCause    = "dead-letter-cause"
	AttrDeadLetterSource   = "dead-letter-source"
	AttrDeadLetterID       = "dead-letter-message-id"
	AttrDeadLetterAttempts = "dead-letter-delivery-attempt"
	AttrDeadLetterKey      = "dead-letter-key"
)

// maxCauseLen bounds the dead-letter-cause attribute (brokers cap attribute
// sizes; Pub/Sub allows 1024 bytes per value).
const maxCauseLen = 512

// DeadLetterTo returns a [DeadLetterFunc] that publishes a copy of each
// permanently failed message to topic on pub, so it can be inspected and
// replayed instead of being dropped. It works with every backend, for
// example to give Kafka the dead-letter topic it lacks:
//
//	pubsub.WithDeadLetter(pubsub.DeadLetterTo(pub, "orders.dlq"))
//
// The copy keeps Data and Attributes and adds the cause, the source topic
// or subscription, the original message ID, its delivery attempt and its
// ordering key (the AttrDeadLetter* attributes). It is published without a
// Key, so one failed dead-letter publish cannot pause a key on the
// dead-letter topic. The cause is truncated to 512 bytes and is for
// operators, so keep secrets out of handler errors. When the
// publish fails the error is returned, so the subscriber nacks the message
// and it is not lost.
func DeadLetterTo(pub Publisher, topic string) DeadLetterFunc {
	return func(ctx context.Context, m *Message, cause error) error {
		c := m.Clone()
		// The copy is published unkeyed: a failed keyed publish would pause
		// the key on the dead-letter topic and block later dead letters.
		c.ID, c.Topic, c.Key, c.PublishTime, c.DeliveryAttempt = "", "", "", time.Time{}, 0
		if c.Attributes == nil {
			c.Attributes = make(map[string]string, 4)
		}
		c.Attributes[AttrDeadLetterCause] = truncate(fmt.Sprint(cause), maxCauseLen)
		c.Attributes[AttrDeadLetterSource] = m.Topic
		c.Attributes[AttrDeadLetterID] = m.ID
		c.Attributes[AttrDeadLetterAttempts] = strconv.Itoa(m.DeliveryAttempt)
		if m.Key != "" {
			c.Attributes[AttrDeadLetterKey] = m.Key
		}
		if _, err := pub.Publish(ctx, topic, c); err != nil {
			return fmt.Errorf("pubsub: dead-letter to %s: %w", topic, err)
		}
		return nil
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}
