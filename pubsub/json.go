package pubsub

import (
	"context"
	"encoding/json"
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
