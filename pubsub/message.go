// Package pubsub defines a small, backend-neutral messaging API and the
// consumer engine shared by every backend.
//
// A [Publisher] sends messages to a topic. A [Source] is the seam to a broker
// (Google Cloud Pub/Sub, Kafka, SQS, the in-memory broker in
// [github.com/dungnh3/kitchen/pubsub/memory], ...). A [Subscriber] pulls from a
// Source and runs a [Handler] or [BatchHandler] with bounded concurrency,
// per-key FIFO ordering, in-process retry with backoff, dead-lettering and a
// graceful drain.
//
// Settlement is driven only by the handler result:
//
//   - nil: the message is acked;
//   - an error: retried in process, then nacked so the broker redelivers it
//     (or dead-letters it by its own policy);
//   - an error wrapping [ErrPermanent] (see [Permanent]): not retried; the
//     DeadLetter hook runs (see [WithDeadLetter]) and the message is acked;
//   - a panic: recovered and treated as an ordinary error.
//
// Handlers must be idempotent: every backend delivers at least once.
//
// Tracing and metrics follow the OpenTelemetry messaging semantic conventions
// and are on by default, using the global providers and propagator.
package pubsub

import (
	"maps"
	"time"
)

// Message is a message sent or received through a [Publisher] or [Source].
type Message struct {
	// ID is the broker-assigned identifier. It is set on received messages and
	// ignored on publish.
	ID string
	// Topic is the topic, queue or subscription the message was received from.
	// It is set on received messages and ignored on publish.
	Topic string
	// Key orders messages: messages with the same non-empty Key are delivered
	// and handled in publish order. It maps to the Pub/Sub ordering key, the
	// Kafka record key and the SQS FIFO MessageGroupId.
	Key string
	// Data is the payload.
	Data []byte
	// Attributes carries string metadata, including the propagated trace
	// context.
	Attributes map[string]string
	// PublishTime is when the broker accepted the message (received messages
	// only).
	PublishTime time.Time
	// DeliveryAttempt is the 1-based delivery count reported by the broker, or
	// 0 when the broker does not report it.
	DeliveryAttempt int
}

// Clone returns a copy of m with its own Attributes map. Data is shared.
func (m *Message) Clone() *Message {
	c := *m
	c.Attributes = maps.Clone(m.Attributes)
	return &c
}
