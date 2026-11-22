// Package streamid holds Redis Streams helpers shared by redisbus and relay.
package streamid

import (
	"errors"
	"hash/fnv"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
)

// MaxIDLen is the longest valid stream entry ID: two uint64 values and a dash.
const MaxIDLen = 20 + 1 + 20

// ErrBadID reports a string that is not a Redis stream entry ID.
var ErrBadID = errors.New("not a redis stream entry id")

// ID is a parsed Redis stream entry ID (<ms>-<seq>).
type ID struct{ Ms, Seq uint64 }

// Zero is the ID "0-0", which precedes every entry.
var Zero = ID{}

// ParseID parses "<ms>-<seq>" or "<ms>" (seq 0). It rejects anything else,
// including the special IDs "$", ">", "+" and "-".
func ParseID(s string) (ID, error) {
	if s == "" || len(s) > MaxIDLen {
		return ID{}, ErrBadID
	}
	msPart, seqPart, hasSeq := strings.Cut(s, "-")
	ms, err := parseUint(msPart)
	if err != nil {
		return ID{}, err
	}
	var seq uint64
	if hasSeq {
		if seq, err = parseUint(seqPart); err != nil {
			return ID{}, err
		}
	}
	return ID{Ms: ms, Seq: seq}, nil
}

func parseUint(s string) (uint64, error) {
	if s == "" {
		return 0, ErrBadID
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, ErrBadID
		}
	}
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, ErrBadID
	}
	return v, nil
}

// String formats the ID as "<ms>-<seq>".
func (id ID) String() string {
	b := make([]byte, 0, MaxIDLen)
	b = strconv.AppendUint(b, id.Ms, 10)
	b = append(b, '-')
	b = strconv.AppendUint(b, id.Seq, 10)
	return string(b)
}

// Compare returns -1, 0 or +1.
func (id ID) Compare(o ID) int {
	switch {
	case id.Ms < o.Ms:
		return -1
	case id.Ms > o.Ms:
		return 1
	case id.Seq < o.Seq:
		return -1
	case id.Seq > o.Seq:
		return 1
	}
	return 0
}

// IsZero reports whether id is "0-0".
func (id ID) IsZero() bool { return id == Zero }

// Bucket maps a stream ID to a bucket in [0, buckets).
func Bucket(streamID string, buckets int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(streamID))
	return int(h.Sum32() % uint32(buckets)) //nolint:gosec // buckets is validated > 0
}

// HashTag returns the hash-tag fragment for a bucket, e.g. "{b17}".
func HashTag(bucket int) string { return "{b" + strconv.Itoa(bucket) + "}" }

// Field returns the string value of a stream entry field, or "".
func Field(m redis.XMessage, name string) (string, bool) {
	v, ok := m.Values[name]
	if !ok {
		return "", false
	}
	switch t := v.(type) {
	case string:
		return t, true
	case []byte:
		return string(t), true
	}
	return "", false
}
