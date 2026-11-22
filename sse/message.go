package sse

import (
	"context"
	"iter"
	"strings"

	"google.golang.org/grpc/codes"

	"github.com/dungnh3/kitchen/errs"
)

// Lifecycle event types known to the library. Services declare their own
// types (any non-empty string without CR/LF/NUL; dot.notation by convention).
const (
	TypeStarted   = "stream.started"    // the writer started producing
	TypeToken     = "stream.token"      // an incremental output chunk
	TypeDone      = "stream.done"       // clean end of stream
	TypeError     = "stream.error"      // terminal error
	TypeCancelled = "stream.cancelled"  // cancelled before completion
	TypeResumed   = "stream.resumed"    // synthetic: resume catch-up reached live
	TypeReplayGap = "stream.replay_gap" // synthetic: the resume cursor was trimmed
)

// Message is one entry of a logical stream.
type Message struct {
	// ID is the stream position (a Redis entry ID for redisbus). It is the SSE
	// id field and the resume cursor. Synthetic messages have no ID.
	ID string
	// Type is the event type (the SSE event field).
	Type string
	// Data is the opaque payload, usually JSON.
	Data []byte
}

// Source delivers the messages of a stream. redisbus.Hub implements it.
type Source interface {
	// Subscribe yields the messages after cursor (all retained messages when
	// cursor is ""), then live messages, until the stream ends (the sequence
	// finishes with no error) or ctx is done. It yields ErrReplayGap when the
	// cursor is no longer retained and ErrInvalidCursor for a malformed or
	// future cursor; both are terminal. Serve calls CheckCursor first, so a
	// bad cursor is normally refused before the response is committed.
	Subscribe(ctx context.Context, streamID, cursor string) iter.Seq2[Message, error]
	// CheckCursor reports whether cursor may resume streamID: nil, or
	// ErrInvalidCursor for a malformed cursor or one newer than the stream.
	// A cursor that is merely too old passes; Subscribe reports the gap.
	// Serve calls it before writing headers.
	CheckCursor(ctx context.Context, streamID, cursor string) error
}

var (
	// ErrReplayGap means the resume cursor has been trimmed or expired; the
	// client must re-fetch state instead of resuming. OutOfRange.
	ErrReplayGap = errs.New(codes.OutOfRange, "SSE_REPLAY_GAP", errs.Message("resume position is no longer retained"))
	// ErrInvalidCursor means a Last-Event-ID that is malformed or ahead of
	// the stream. InvalidArgument (400).
	ErrInvalidCursor = errs.New(codes.InvalidArgument, "INVALID_LAST_EVENT_ID", errs.Message("invalid Last-Event-ID"))
)

// DefaultVisible reports whether eventType is one of the library's lifecycle
// types. It is the default ServeOptions.Visible: unknown types are hidden
// until the service declares them, so internal events never leak by accident.
func DefaultVisible(eventType string) bool {
	switch eventType {
	case TypeStarted, TypeToken, TypeDone, TypeError, TypeCancelled, TypeResumed, TypeReplayGap:
		return true
	}
	return false
}

// VisibleTypes returns a Visible func that shows the library's lifecycle types
// plus the listed types and prefixes (entries ending in "." match a prefix,
// e.g. "step.").
func VisibleTypes(types ...string) func(string) bool {
	exact := make(map[string]struct{}, len(types))
	var prefixes []string
	for _, t := range types {
		if strings.HasSuffix(t, ".") {
			prefixes = append(prefixes, t)
			continue
		}
		exact[t] = struct{}{}
	}
	return func(t string) bool {
		if DefaultVisible(t) {
			return true
		}
		if _, ok := exact[t]; ok {
			return true
		}
		for _, p := range prefixes {
			if strings.HasPrefix(t, p) {
				return true
			}
		}
		return false
	}
}
