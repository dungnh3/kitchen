package kafka_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dungnh3/kitchen/pubsub"
	"github.com/dungnh3/kitchen/pubsub/kafka"
)

// produceParts writes perPart records with unique keys to each partition of
// "orders".
func produceParts(t *testing.T, seeds []string, parts, perPart int, tag string) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(seeds...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	var recs []*kgo.Record
	for p := range parts {
		for i := range perPart {
			v := fmt.Sprintf("%s-p%d-%d", tag, p, i)
			recs = append(recs, &kgo.Record{Topic: "orders", Partition: int32(p), Key: []byte(v), Value: []byte(v)})
		}
	}
	if err := cl.ProduceSync(context.Background(), recs...).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

func newSource(t *testing.T, seeds []string, revoke time.Duration) *kafka.Source {
	t.Helper()
	src, err := kafka.NewSource("billing", []string{"orders"},
		kafka.WithClientOptions(kgo.SeedBrokers(seeds...), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
			kgo.FetchMaxWait(50*time.Millisecond)),
		kafka.WithCommitInterval(20*time.Millisecond), kafka.WithRevokeTimeout(revoke), kafka.WithLogger(quiet))
	if err != nil {
		t.Fatal(err)
	}
	return src
}

func waitUntil(d time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(5 * time.Millisecond)
	}
	return true
}

// Records already returned by a poll for a partition that is revoked while
// the poll goroutine is blocked in fn (engine backpressure) must not be
// delivered by the old owner after the revoke: the new owner processes them.
func TestRevokedPartitionRecordsNotDeliveredByOldOwner(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(2, "orders"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	seeds := c.ListenAddrs()
	produceParts(t, seeds, 2, 20, "a")

	type ev struct {
		part int32
		at   time.Time
		val  string
	}
	partOf := func(id string) int32 {
		var p int32
		_, _ = fmt.Sscanf(id, "orders/%d/", &p)
		return p
	}
	var mu sync.Mutex
	var aGot, bGot []ev
	unblock := make(chan struct{})
	first := true
	stopA := receiveRaw(t, newSource(t, seeds, 500*time.Millisecond), func(_ context.Context, d pubsub.Delivery) {
		m := d.Message()
		mu.Lock()
		aGot = append(aGot, ev{partOf(m.ID), time.Now(), string(m.Data)})
		blk := first
		first = false
		mu.Unlock()
		if blk {
			// Engine backpressure: fn blocks while the rest of the polled
			// batch is still in hand.
			<-unblock
		}
		d.Ack()
	})
	if !waitUntil(5*time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return len(aGot) == 1 }) {
		t.Fatal("A got nothing")
	}
	stopB := receiveRaw(t, newSource(t, seeds, 500*time.Millisecond), func(_ context.Context, d pubsub.Delivery) {
		m := d.Message()
		mu.Lock()
		bGot = append(bGot, ev{partOf(m.ID), time.Now(), string(m.Data)})
		mu.Unlock()
		d.Ack()
	})
	if !waitUntil(15*time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return len(bGot) > 0 }) {
		t.Fatal("B got nothing")
	}
	revokedAt := time.Now()
	close(unblock)
	time.Sleep(time.Second) // let A drain its polled batch
	stopB()
	stopA()

	mu.Lock()
	defer mu.Unlock()
	bParts := map[int32]bool{}
	for _, e := range bGot {
		bParts[e.part] = true
	}
	var stale []string
	for _, e := range aGot {
		if bParts[e.part] && e.at.After(revokedAt) {
			stale = append(stale, e.val)
		}
	}
	if len(stale) > 0 {
		t.Fatalf("old owner delivered %d records of a revoked partition after the revoke: %v", len(stale), stale)
	}
}

// Shutdown waits at most about one RevokeTimeout for records the engine
// never settles, not once in Receive and again in the Close-time revoke.
func TestShutdownWaitsOneRevokeTimeout(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "orders"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	seeds := c.ListenAddrs()
	produceParts(t, seeds, 1, 1, "s")

	const revoke = time.Second
	var mu sync.Mutex
	var held []pubsub.Delivery
	stop := receiveRaw(t, newSource(t, seeds, revoke), func(_ context.Context, d pubsub.Delivery) {
		mu.Lock()
		defer mu.Unlock()
		held = append(held, d) // never settled before shutdown
	})
	if !waitUntil(10*time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return len(held) == 1 }) {
		t.Fatal("record not delivered")
	}
	start := time.Now()
	stop()
	took := time.Since(start)
	mu.Lock()
	held[0].Ack() // late settlement after Receive returned is ignored
	mu.Unlock()
	if took >= revoke*18/10 {
		t.Fatalf("shutdown took %v with RevokeTimeout %v; want about one RevokeTimeout", took, revoke)
	}
}

func TestSourceAdvisesFence(t *testing.T) {
	src, err := kafka.NewSource("billing", []string{"orders"}, kafka.WithLogger(quiet))
	if err != nil {
		t.Fatal(err)
	}
	adv, ok := any(src).(pubsub.RedeliveryAdvisor)
	if !ok {
		t.Fatal("kafka.Source does not implement pubsub.RedeliveryAdvisor")
	}
	if got := adv.Redelivery(); got != (pubsub.Redelivery{Fence: true}) {
		t.Fatalf("Redelivery() = %+v, want Fence only", got)
	}
}

func TestPublishAfterCloseFails(t *testing.T) {
	seeds := cluster(t)
	cl, err := kgo.NewClient(kgo.SeedBrokers(seeds...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	pub, err := kafka.NewPublisher(cl)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := pub.Publish(ctx, "orders", &pubsub.Message{Data: []byte("before")}); err != nil {
		t.Fatal(err)
	}
	for i := range 2 { // Close is idempotent
		if err := pub.Close(ctx); err != nil {
			t.Fatalf("close %d: %v", i, err)
		}
	}
	if _, err := pub.Publish(ctx, "orders", &pubsub.Message{Data: []byte("after")}); !errors.Is(err, pubsub.ErrClosed) {
		t.Fatalf("Publish after Close: err = %v, want ErrClosed", err)
	}
}

// A keyed message that fails to publish pauses its key: later messages of
// the key fail with ErrKeyPaused until ResumeKey, so a retry of the failed
// message cannot land after newer ones.
func TestPublishFailurePausesKey(t *testing.T) {
	seeds := cluster(t)
	// Records above the batch limit fail client-side (MessageTooLarge).
	cl, err := kgo.NewClient(kgo.SeedBrokers(seeds...), kgo.ProducerBatchMaxBytes(1024))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	pub, err := kafka.NewPublisher(cl)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	big := []byte(strings.Repeat("x", 4096))
	_, err = pub.Publish(ctx, "orders", &pubsub.Message{Key: "k", Data: big}, &pubsub.Message{Key: "other", Data: []byte("ok")})
	var pe *pubsub.PublishError
	if !errors.As(err, &pe) || len(pe.Failed) != 1 || pe.Failed[0] == nil {
		t.Fatalf("first publish: err = %v, want a PublishError for message 0", err)
	}

	steps := []struct {
		name   string
		resume bool
		key    string
		paused bool
	}{
		{"failed key is paused", false, "k", true},
		{"still paused", false, "k", true},
		{"other key flows", false, "other", false},
		{"unkeyed flows", false, "", false},
		{"resumed key flows", true, "k", false},
	}
	for _, st := range steps {
		t.Run(st.name, func(t *testing.T) {
			if st.resume {
				resumer, ok := any(pub).(pubsub.KeyResumer)
				if !ok {
					t.Fatal("kafka.Publisher does not implement pubsub.KeyResumer")
				}
				resumer.ResumeKey("orders", st.key)
			}
			_, err := pub.Publish(ctx, "orders", &pubsub.Message{Key: st.key, Data: []byte("small")})
			if got := errors.Is(err, pubsub.ErrKeyPaused); got != st.paused {
				t.Fatalf("err = %v, paused = %v, want %v", err, got, st.paused)
			}
			if !st.paused && err != nil {
				t.Fatalf("err = %v", err)
			}
		})
	}
}
