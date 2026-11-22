package pubsub

import (
	"context"
	"time"
)

// Publisher sends messages to a topic.
//
// Implementations are safe for concurrent use. Publish copies what it needs
// before returning and never mutates msgs.
type Publisher interface {
	// Publish sends msgs to topic and waits until every message is accepted
	// by the broker or has failed. It returns the broker IDs in input order.
	// When some messages fail it returns the IDs of the others and a
	// *PublishError. Publishing a message whose ordering key is paused fails
	// with an error wrapping ErrKeyPaused.
	Publish(ctx context.Context, topic string, msgs ...*Message) ([]string, error)
	// Close flushes pending messages and releases resources. Later Publish
	// calls fail with ErrClosed. It does not close clients passed in by the
	// caller. Safe to call more than once.
	Close(ctx context.Context) error
}

// KeyResumer is implemented by publishers that pause an ordering key after a
// failed publish. Call ResumeKey after handling the failure (for example after
// re-publishing the failed message) to accept the key again.
type KeyResumer interface {
	ResumeKey(topic, key string)
}

// Delivery is a received message plus its settlement. Ack and Nack are called
// exactly once, by the [Subscriber].
type Delivery interface {
	Message() *Message
	// Ack confirms processing; the broker will not redeliver the message.
	Ack()
	// Nack asks the broker to redeliver the message.
	Nack()
}

// Source is the seam between the [Subscriber] engine and a broker.
type Source interface {
	// Receive calls fn for every received message until ctx is done or a
	// fatal error occurs. It returns nil when ctx is done.
	//
	// fn is called sequentially, in delivery order, for messages sharing a
	// non-empty Key, and may be called concurrently for different keys. A
	// nacked keyed message must be redelivered before later messages of the
	// same key. fn may block to apply backpressure; it returns promptly once
	// its ctx is done. Receive returns only after every fn call has returned.
	Receive(ctx context.Context, fn func(ctx context.Context, d Delivery)) error
	// System returns the OpenTelemetry messaging.system value, e.g.
	// "gcp_pubsub", "kafka", "aws_sqs".
	System() string
}

// Handler processes one message. See the package documentation for how the
// result settles the message.
type Handler func(ctx context.Context, m *Message) error

// BatchHandler processes a batch of messages. Return nil to ack all, a
// *BatchError to fail some, or any other error to fail the whole batch.
type BatchHandler func(ctx context.Context, ms []*Message) error

// DeadLetterFunc receives a message that failed permanently. Return nil to
// ack the message, or an error to nack it so it is not lost.
type DeadLetterFunc func(ctx context.Context, m *Message, cause error) error

// Redelivery describes how a broker redelivers a nacked message. A [Source]
// reports it through [RedeliveryAdvisor]; the [Subscriber] uses it to decide
// whether the key fence is useful (see [KeyFenceAuto]) and when a nack is
// the broker's last one.
type Redelivery struct {
	// Fence reports whether fencing a key after a final nack keeps that key
	// in order: true when the broker redelivers the nacked message to this
	// consumer before later messages of its key (Kafka partitions, an
	// in-process broker) or when messages already in transit can overtake
	// the redelivery (Pub/Sub ordered delivery). False when the broker
	// already holds back the rest of the key until the nacked message is
	// settled (SQS FIFO message groups), where a fence only causes
	// needless nacks.
	Fence bool
	// FenceTTL bounds a fence for this broker; 0 means the subscriber's
	// WithKeyFenceTTL value.
	FenceTTL time.Duration
	// MaxDeliveryAttempts is the delivery attempt at which the broker moves
	// a nacked message to its own dead-letter destination, or 0 when it has
	// none or it is unknown. The subscriber never fences a key on a nack
	// that is the broker's last one: that message will not come back.
	MaxDeliveryAttempts int
}

// RedeliveryAdvisor is optionally implemented by a [Source]. The subscriber
// calls Redelivery on every final nack of a keyed message, concurrently, so
// it must be cheap and safe for concurrent use. Its result may change once
// Receive has started (for example after the subscription settings are read).
type RedeliveryAdvisor interface {
	Redelivery() Redelivery
}

// Consumer identifies who consumes a [Source], for telemetry and logs.
type Consumer struct {
	// Group is the consumer group (Kafka), recorded as
	// messaging.consumer.group.name.
	Group string
	// Subscription is the subscription (Pub/Sub, the memory broker),
	// recorded as messaging.destination.subscription.name.
	Subscription string
}

// ConsumerIdentifier is optionally implemented by a [Source]; the
// [Subscriber] adds its Consumer to every consumer metric, span and log
// line, so dashboards can split by consumer.
type ConsumerIdentifier interface {
	Consumer() Consumer
}
