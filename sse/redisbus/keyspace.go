// Package redisbus implements the sse stream bus on Redis Streams: a stateless
// Producer for writers and a multiplexed per-pod Hub for readers.
//
// Keys carry a bucketed hash tag (sse:{b17}:<streamID>) so one multi-key XREAD
// serves many streams on Redis Cluster as well as on standalone or Sentinel.
// Writers and readers must use the same Keyspace. Run Redis with
// maxmemory-policy noeviction: streams are bounded by MAXLEN and TTLs, never by
// eviction.
package redisbus

import (
	"errors"
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/sse/internal/streamid"
)

// Default keyspace values.
const (
	DefaultPrefix  = "sse:"
	DefaultBuckets = 64
)

// endType marks the end-of-stream sentinel entry.
const endType = "__sse_end__"

// Entry field names.
const (
	fieldType   = "type"
	fieldData   = "data"
	fieldStream = "stream"
)

// ErrInvalidStreamID rejects empty stream IDs or IDs with braces (which would
// change the hash slot) or control characters. InvalidArgument (400).
var ErrInvalidStreamID = errs.New(codes.InvalidArgument, "SSE_INVALID_STREAM_ID", errs.Message("redisbus: invalid stream id"))

// Keyspace maps stream IDs to Redis keys. The zero value uses DefaultPrefix
// and DefaultBuckets. Changing Buckets changes every key: treat it as a
// breaking change for in-flight streams.
type Keyspace struct {
	// Prefix is prepended to every key. Default "sse:".
	Prefix string `json:"prefix" yaml:"prefix"`
	// Buckets is the number of hash-tag buckets. Default 64.
	Buckets int `json:"buckets" yaml:"buckets"`
}

func (k Keyspace) withDefaults() (Keyspace, error) {
	if k.Prefix == "" {
		k.Prefix = DefaultPrefix
	}
	if k.Buckets == 0 {
		k.Buckets = DefaultBuckets
	}
	if k.Buckets < 0 || k.Buckets > 16384 {
		return k, fmt.Errorf("redisbus: buckets %d out of range [1,16384]", k.Buckets)
	}
	if strings.ContainsAny(k.Prefix, "{}") {
		return k, errors.New("redisbus: prefix must not contain braces")
	}
	return k, nil
}

// Bucket returns the bucket of streamID.
func (k Keyspace) Bucket(streamID string) int { return streamid.Bucket(streamID, k.Buckets) }

// Key returns the Redis key of streamID, e.g. "sse:{b17}:run_1".
func (k Keyspace) Key(streamID string) string {
	return k.Prefix + streamid.HashTag(k.Bucket(streamID)) + ":" + streamID
}

// OutboxKey returns the outbox shard key for streamID's bucket, e.g.
// "outbox:orch:{b17}". It shares the stream key's hash slot.
func (k Keyspace) OutboxKey(outbox, streamID string) string {
	return OutboxShardKey(outbox, k.Bucket(streamID))
}

// OutboxShardKey returns the key of outbox shard bucket.
func OutboxShardKey(outbox string, bucket int) string {
	return outbox + ":" + streamid.HashTag(bucket)
}

func validStreamID(id string) error {
	if id == "" || len(id) > 512 || strings.ContainsAny(id, "{}\r\n\x00") {
		return fmt.Errorf("%w: %q", ErrInvalidStreamID, id)
	}
	return nil
}
