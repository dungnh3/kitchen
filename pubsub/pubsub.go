package pubsub

import "context"

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
