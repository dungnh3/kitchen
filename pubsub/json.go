package pubsub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

// JSON adapts a typed handler to a [Handler]. The payload is decoded with
// encoding/json; a decode failure is permanent (the message is dead-lettered,
// not retried).
func JSON[T any](h func(ctx context.Context, v T, m *Message) error) Handler {
	return func(ctx context.Context, m *Message) error {
		var v T
		if err := json.Unmarshal(m.Data, &v); err != nil {
			return Permanent(fmt.Errorf("decode %T: %w", v, err))
		}
		return h(ctx, v, m)
	}
}

// NewJSON returns a message whose Data is the JSON encoding of v.
func NewJSON(v any) (*Message, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("pubsub: encode %T: %w", v, err)
	}
	return &Message{Data: b, Attributes: map[string]string{"content-type": "application/json"}}, nil
}

// JSONBatch adapts a typed batch handler to a [BatchHandler]. Each payload is
// decoded with encoding/json; a message that cannot be decoded is reported
// as permanently failed (dead-lettered, not retried) without blocking the
// rest of the batch. h receives the decoded values and their messages,
// aligned by index. A *BatchError returned by h uses indexes of those
// slices; JSONBatch maps them back to the original batch. Any other error
// from h fails every decoded message.
func JSONBatch[T any](h func(ctx context.Context, vs []T, ms []*Message) error) BatchHandler {
	return func(ctx context.Context, ms []*Message) error {
		failed := map[int]error{}
		vs := make([]T, 0, len(ms))
		ok := make([]*Message, 0, len(ms))
		orig := make([]int, 0, len(ms)) // index in ok → index in ms
		for i, m := range ms {
			var v T
			if err := json.Unmarshal(m.Data, &v); err != nil {
				failed[i] = Permanent(fmt.Errorf("decode %T: %w", v, err))
				continue
			}
			vs = append(vs, v)
			ok = append(ok, m)
			orig = append(orig, i)
		}
		if len(ok) > 0 {
			err := h(ctx, vs, ok)
			var be *BatchError
			switch {
			case err == nil:
			case errors.As(err, &be) && be != nil:
				for j, e := range be.Failed {
					if j >= 0 && j < len(orig) {
						failed[orig[j]] = e
					}
				}
			default:
				if len(failed) == 0 {
					return err // whole-batch failure: let the subscriber retry and isolate
				}
				for _, i := range orig {
					failed[i] = err
				}
			}
		}
		if len(failed) == 0 {
			return nil
		}
		return &BatchError{Failed: failed}
	}
}
