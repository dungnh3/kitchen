package sse

import (
	"io"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"

	"github.com/dungnh3/kitchen/errs"
)

// ErrInvalidEvent is returned when an Event field cannot be encoded on the
// wire: ID or Type contains CR, LF or NUL, or Comment contains CR or LF.
var ErrInvalidEvent = errs.New(codes.InvalidArgument, "SSE_INVALID_EVENT", errs.Message("sse: invalid event field"))

// Event is one Server-Sent Event as defined by the WHATWG HTML standard.
// Zero-value fields are omitted from the wire.
type Event struct {
	// ID is sent as "id: <ID>". Browsers echo the last one in the
	// Last-Event-ID header when they reconnect.
	ID string
	// Type is sent as "event: <Type>". Empty means the "message" event.
	Type string
	// Data is sent as one "data:" line per line of Data. CRLF, CR and LF all
	// end a line, as in the spec's parser.
	Data []byte
	// Retry, when positive, is sent as "retry: <milliseconds>".
	Retry time.Duration
	// Comment, when non-empty, is sent as ": <Comment>" before other fields.
	Comment string
}

// IsZero reports whether e has no fields set.
func (e Event) IsZero() bool {
	return e.ID == "" && e.Type == "" && e.Data == nil && e.Retry <= 0 && e.Comment == ""
}

// dispatches reports whether the event carries fields the browser dispatches
// as an event (as opposed to a comment or retry-only frame).
func (e Event) dispatches() bool { return e.ID != "" || e.Type != "" || e.Data != nil }

// AppendEvent appends the wire encoding of e to dst. It is the allocation-free
// core used by Server; WriteTo wraps it.
func AppendEvent(dst []byte, e Event) ([]byte, error) {
	if strings.ContainsAny(e.ID, "\r\n\x00") || strings.ContainsAny(e.Type, "\r\n\x00") ||
		strings.ContainsAny(e.Comment, "\r\n") {
		return dst, ErrInvalidEvent
	}
	if e.Comment != "" {
		dst = append(dst, ": "...)
		dst = append(dst, e.Comment...)
		dst = append(dst, '\n')
	}
	if e.ID != "" {
		dst = append(dst, "id: "...)
		dst = append(dst, e.ID...)
		dst = append(dst, '\n')
	}
	if e.Type != "" {
		dst = append(dst, "event: "...)
		dst = append(dst, e.Type...)
		dst = append(dst, '\n')
	}
	if e.Retry > 0 {
		dst = append(dst, "retry: "...)
		dst = strconv.AppendInt(dst, e.Retry.Milliseconds(), 10)
		dst = append(dst, '\n')
	}
	if e.dispatches() {
		dst = appendData(dst, e.Data)
	}
	return append(dst, '\n'), nil
}

func appendData(dst, data []byte) []byte {
	for {
		i := indexEOL(data)
		dst = append(dst, "data: "...)
		if i < 0 {
			dst = append(dst, data...)
			return append(dst, '\n')
		}
		dst = append(dst, data[:i]...)
		dst = append(dst, '\n')
		if data[i] == '\r' && i+1 < len(data) && data[i+1] == '\n' {
			i++
		}
		data = data[i+1:]
	}
}

func indexEOL(b []byte) int {
	for i, c := range b {
		if c == '\n' || c == '\r' {
			return i
		}
	}
	return -1
}

// WriteTo encodes e and writes it to w in a single Write call.
func (e Event) WriteTo(w io.Writer) (int64, error) {
	b, err := AppendEvent(nil, e)
	if err != nil {
		return 0, err
	}
	n, err := w.Write(b)
	return int64(n), err
}
