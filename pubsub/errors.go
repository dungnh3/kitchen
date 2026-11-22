package pubsub

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"google.golang.org/grpc/codes"

	"github.com/dungnh3/kitchen/errs"
)

var (
	// ErrPermanent marks a handler failure that retrying cannot fix (for
	// example a malformed payload). Wrap it with [Permanent] or fmt.Errorf
	// "%w"; the message is dead-lettered and acked instead of retried. It is a
	// signal from a handler to the engine, never returned to callers, so it
	// is a plain error rather than an errs one.
	ErrPermanent = errors.New("pubsub: permanent failure")

	// ErrKeyPaused is returned by Publish for a message whose ordering key is
	// paused because an earlier publish with that key failed. Publishing with
	// the key resumes after [KeyResumer.ResumeKey], so FIFO order is kept.
	ErrKeyPaused = errs.New(codes.FailedPrecondition, "PUBSUB_KEY_PAUSED",
		errs.Message("ordering key paused after a failed publish"))

	// ErrClosed is returned when using a Publisher or Subscriber after Close or
	// after Run has returned.
	ErrClosed = errs.New(codes.Unavailable, "PUBSUB_CLOSED", errs.Message("publisher or subscriber closed"))
)

// Permanent wraps err so that errors.Is(err, ErrPermanent) reports true. It
// returns nil for a nil err.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanentError{err}
}

type permanentError struct{ err error }

func (e permanentError) Error() string   { return "permanent: " + e.err.Error() }
func (e permanentError) Unwrap() []error { return []error{e.err, ErrPermanent} }

// IsPermanent reports whether err wraps [ErrPermanent].
func IsPermanent(err error) bool { return errors.Is(err, ErrPermanent) }

// PublishError reports a partially failed Publish. IDs has one entry per input
// message (empty for failed ones); Failed maps the input index to its error.
type PublishError struct {
	IDs    []string
	Failed map[int]error
}

func (e *PublishError) Error() string {
	idx := slices.Sorted(maps.Keys(e.Failed))
	var b strings.Builder
	fmt.Fprintf(&b, "pubsub: %d of %d messages failed to publish", len(e.Failed), len(e.IDs))
	for i, k := range idx {
		if i == 3 {
			fmt.Fprintf(&b, "; ...")
			break
		}
		fmt.Fprintf(&b, "; [%d] %v", k, e.Failed[k])
	}
	return b.String()
}

// Unwrap returns the per-message errors so errors.Is/As see through.
func (e *PublishError) Unwrap() []error {
	idx := slices.Sorted(maps.Keys(e.Failed))
	out := make([]error, 0, len(idx))
	for _, k := range idx {
		out = append(out, e.Failed[k])
	}
	return out
}

// BatchError is returned by a [BatchHandler] to report per-message failures.
// Failed maps an index of the batch slice to its error; messages not listed
// are acked. A Failed entry wrapping [ErrPermanent] is dead-lettered at once;
// the others are retried.
type BatchError struct {
	Failed map[int]error
}

func (e *BatchError) Error() string {
	return fmt.Sprintf("pubsub: %d messages of the batch failed", len(e.Failed))
}

// Unwrap returns the per-message errors.
func (e *BatchError) Unwrap() []error {
	idx := slices.Sorted(maps.Keys(e.Failed))
	out := make([]error, 0, len(idx))
	for _, k := range idx {
		out = append(out, e.Failed[k])
	}
	return out
}
