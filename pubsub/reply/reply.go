// Package reply provides request/reply on top of asynchronous messaging: a
// caller sends a request (for example with a [pubsub.Publisher]) carrying a
// correlation ID and waits for the worker's reply to that ID.
//
//	id := reply.NewID()
//	w, err := broker.Expect(ctx, id)      // register before sending
//	defer w.Close()
//	m.Attributes[reply.IDAttribute] = id
//	_, err = pub.Publish(ctx, "jobs", m)  // send the request
//	payload, err := w.Wait(ctx)           // block for the reply
//
//	// worker side
//	err = broker.Reply(ctx, m.Attributes[reply.IDAttribute], result)
//
// See [github.com/dungnh3/kitchen/pubsub/reply/redis] for a Redis broker.
package reply

import (
	"context"
	"crypto/rand"
	"encoding/base64"

	"google.golang.org/grpc/codes"

	"github.com/dungnh3/kitchen/errs"
)

// IDAttribute is the conventional message attribute carrying the reply ID.
const IDAttribute = "reply-id"

// ErrInvalidID is returned for IDs that are empty, too long or contain
// characters outside [A-Za-z0-9_-].
var ErrInvalidID = errs.New(codes.InvalidArgument, "INVALID_REPLY_ID", errs.Message("invalid reply id"))

// Broker delivers replies to waiters.
type Broker interface {
	// Expect registers interest in the reply to id. Call it before sending
	// the request so that a fast reply cannot be lost.
	Expect(ctx context.Context, id string) (Waiter, error)
	// Reply delivers payload to the waiter of id. A reply nobody waits for
	// expires after the broker's TTL.
	Reply(ctx context.Context, id string, payload []byte) error
}

// Waiter waits for one reply.
type Waiter interface {
	// Wait blocks until the reply arrives or ctx is done (returning ctx.Err()).
	Wait(ctx context.Context) ([]byte, error)
	// Close releases the registration. Safe to call more than once.
	Close() error
}

// NewID returns a random 128-bit, URL-safe ID. Reply IDs act as capabilities
// (whoever knows the ID can answer), so they must be unguessable.
func NewID() string {
	var b [16]byte
	_, _ = rand.Read(b[:]) // crypto/rand.Read never fails (Go 1.24+)
	return base64.RawURLEncoding.EncodeToString(b[:])
}

// ValidateID returns ErrInvalidID unless id is 1–128 characters of
// [A-Za-z0-9_-].
func ValidateID(id string) error {
	if len(id) == 0 || len(id) > 128 {
		return ErrInvalidID
	}
	for i := range len(id) {
		c := id[i]
		if !idChar(c) {
			return ErrInvalidID
		}
	}
	return nil
}

func idChar(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_'
}
