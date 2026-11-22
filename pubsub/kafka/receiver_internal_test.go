package kafka

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/pubsub"
)

// franz-go calls OnPartitionsRevoked with an empty map at the end of every
// cooperative session. That must not wait for the partitions that are kept.
func TestEmptyRevokeDoesNotWaitForKeptPartitions(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, err := NewSource("g", []string{"t"}, WithRevokeTimeout(time.Second), WithLogger(log.Discard()))
		if err != nil {
			t.Fatal(err)
		}
		r := newReceiver(context.Background(), s, nil)
		r.onAssigned(context.Background(), nil, map[string][]int32{"t": {0}})
		ps := r.parts[topicPart{"t", 0}]
		ps.out = 1 // a record held by a handler on a partition that is kept
		done := make(chan struct{})
		go func() {
			defer close(done)
			r.onRevoked(context.Background(), nil, map[string][]int32{})
		}()
		synctest.Wait()
		select {
		case <-done:
		default:
			t.Error("onRevoked(empty) blocked waiting for a partition that is not revoked")
			r.mu.Lock()
			ps.out = 0
			r.mu.Unlock()
			r.notifySettled()
			<-done
		}
	})
}

// A periodic commit whose snapshot was taken before a newer one must not
// reach the broker after it and move the committed offset backward.
func TestCommitsAreSerialized(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, err := NewSource("g", []string{"t"}, WithLogger(log.Discard()))
		if err != nil {
			t.Fatal(err)
		}
		tp := topicPart{"t", 0}
		r := newReceiver(context.Background(), s, nil)
		r.onAssigned(context.Background(), nil, map[string][]int32{"t": {0}})
		ps := r.parts[tp]
		ps.commitTo, ps.dirty = kgo.EpochOffset{Epoch: -1, Offset: 100}, true

		var applied []int64 // offsets in the order the broker applies them
		release := make(chan struct{})
		calls := 0
		r.commitSync = func(_ context.Context, offs map[string]map[int32]kgo.EpochOffset, done func(*kgo.Client, *kmsg.OffsetCommitRequest, *kmsg.OffsetCommitResponse, error)) {
			r.mu.Lock()
			calls++
			first := calls == 1
			r.mu.Unlock()
			if first {
				<-release // the first commit is slow on the wire
			}
			r.mu.Lock()
			applied = append(applied, offs[tp.topic][tp.part].Offset)
			r.mu.Unlock()
			done(nil, nil, nil, nil)
		}

		older := make(chan struct{})
		go func() { defer close(older); r.commit() }() // snapshots 100
		synctest.Wait()

		r.mu.Lock()
		ps.commitTo, ps.dirty = kgo.EpochOffset{Epoch: -1, Offset: 150}, true
		r.mu.Unlock()
		newer := make(chan struct{})
		go func() { defer close(newer); r.commitParts(in([]topicPart{tp})) }() // snapshots 150
		synctest.Wait()
		close(release)
		<-older
		<-newer

		if len(applied) == 0 || applied[len(applied)-1] != 150 {
			t.Fatalf("broker applied offsets %v; the last commit must be the newest (150)", applied)
		}
	})
}

// Records are delivered only for partitions this member owns under the
// assignment that was current when they were polled.
func TestRecordsOfUnownedPartitionsAreDropped(t *testing.T) {
	assign := func(r *receiver) { r.onAssigned(context.Background(), nil, map[string][]int32{"t": {0}}) }
	revoke := func(r *receiver) { r.onRevoked(context.Background(), nil, map[string][]int32{"t": {0}}) }
	lose := func(r *receiver) { r.onLost(context.Background(), nil, map[string][]int32{"t": {0}}) }
	for _, tc := range []struct {
		name        string
		before      []func(*receiver) // before the poll
		after       []func(*receiver) // between the poll and handling its records
		wantDeliver bool
	}{
		{"owned", []func(*receiver){assign}, nil, true},
		{"never assigned", nil, nil, false},
		{"revoked after poll", []func(*receiver){assign}, []func(*receiver){revoke}, false},
		{"lost after poll", []func(*receiver){assign}, []func(*receiver){lose}, false},
		{"reassigned after poll", []func(*receiver){assign}, []func(*receiver){revoke, assign}, false},
		{"reassigned before poll", []func(*receiver){assign, revoke, assign}, nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := NewSource("g", []string{"t"}, WithRevokeTimeout(time.Second), WithLogger(log.Discard()))
			if err != nil {
				t.Fatal(err)
			}
			var got []pubsub.Delivery
			r := newReceiver(context.Background(), s, func(_ context.Context, d pubsub.Delivery) { got = append(got, d) })
			for _, f := range tc.before {
				f(r)
			}
			gen := r.gen
			for _, f := range tc.after {
				f(r)
			}
			r.onRecord(&kgo.Record{Topic: "t", Partition: 0, Offset: 7, Key: []byte("k")}, gen)
			if (len(got) == 1) != tc.wantDeliver {
				t.Fatalf("delivered %d records, want delivered = %v", len(got), tc.wantDeliver)
			}
			if ps := r.parts[topicPart{"t", 0}]; !tc.wantDeliver && ps != nil && len(ps.pending) > 0 {
				t.Fatalf("dropped record still pending (would be committed): %d", len(ps.pending))
			}
		})
	}
}
