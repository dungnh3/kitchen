package cache

import "encoding/json"

// Codec encodes cache values for Redis. Implementations must be safe for
// concurrent use.
type Codec[V any] interface {
	Marshal(v V) ([]byte, error)
	Unmarshal(data []byte, v *V) error
}

// JSON is the default [Codec], using encoding/json.
type JSON[V any] struct{}

// Marshal implements [Codec].
func (JSON[V]) Marshal(v V) ([]byte, error) { return json.Marshal(v) }

// Unmarshal implements [Codec].
func (JSON[V]) Unmarshal(data []byte, v *V) error { return json.Unmarshal(data, v) }
