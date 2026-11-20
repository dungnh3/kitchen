package gcp_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	gpubsub "cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"cloud.google.com/go/pubsub/v2/pstest"
	"go.uber.org/goleak"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/dungnh3/kitchen/pubsub"
	"github.com/dungnh3/kitchen/pubsub/gcp"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		// gRPC and OpenCensus background goroutines of the Google client.
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
		goleak.IgnoreAnyFunction("google.golang.org/grpc/internal/grpcsync.(*CallbackSerializer).run"),
		goleak.IgnoreAnyFunction("google.golang.org/grpc/internal/transport.(*controlBuffer).get"),
		// The v2 client's stream keep-alive outlives Receive briefly.
		goleak.IgnoreTopFunction("cloud.google.com/go/pubsub/v2.(*messageIterator).streamKeepAliveHandler"),
	)
}

const project = "p"

func newClient(t *testing.T) *gpubsub.Client {
	t.Helper()
	srv := pstest.NewServer()
	conn, err := grpc.NewClient(srv.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	c, err := gpubsub.NewClient(context.Background(), project, option.WithGRPCConn(conn))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = c.Close()
		_ = conn.Close()
		_ = srv.Close()
	})
	return c
}

func setup(t *testing.T, c *gpubsub.Client, topic, sub string) {
	t.Helper()
	ctx := context.Background()
	tn := fmt.Sprintf("projects/%s/topics/%s", project, topic)
	if _, err := c.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: tn}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name: fmt.Sprintf("projects/%s/subscriptions/%s", project, sub), Topic: tn,
		EnableMessageOrdering: true, AckDeadlineSeconds: 10,
	}); err != nil {
		t.Fatal(err)
	}
}

func TestPublishAndSubscribeInKeyOrder(t *testing.T) {
	c := newClient(t)
	setup(t, c, "orders", "orders-sub")
	ctx := context.Background()

	pub, err := gcp.NewPublisher(c)
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close(ctx)
	var msgs []*pubsub.Message
	for i := range 20 {
		msgs = append(msgs, &pubsub.Message{Key: fmt.Sprint("k", i%3), Data: []byte(fmt.Sprint(i)), Attributes: map[string]string{"n": fmt.Sprint(i)}})
	}
	ids, err := pub.Publish(ctx, "orders", msgs...)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 20 || slices.Contains(ids, "") {
		t.Fatalf("ids = %v", ids)
	}

	src, err := gcp.NewSource(c, "orders-sub", gcp.WithMaxOutstanding(100, 1<<20))
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	got := map[string][]int{}
	total := 0
	done := make(chan struct{})
	failedOnce := false
	s, err := pubsub.NewSubscriber(src, func(ctx context.Context, m *pubsub.Message) error {
		mu.Lock()
		defer mu.Unlock()
		var n int
		fmt.Sscan(string(m.Data), &n)
		if n == 4 && !failedOnce {
			failedOnce = true
			return errors.New("transient") // retried in process
		}
		if m.Topic != "orders-sub" || m.Attributes["n"] != fmt.Sprint(n) || m.ID == "" {
			t.Errorf("bad message %+v", m)
		}
		got[m.Key] = append(got[m.Key], n)
		total++
		if total == 20 {
			close(done)
		}
		return nil
	}, pubsub.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))), pubsub.WithRetry(2, time.Millisecond, time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	rctx, cancel := context.WithCancel(ctx)
	runErr := make(chan error)
	go func() { runErr <- s.Run(rctx) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout")
	}
	cancel()
	if err := <-runErr; err != nil {
		t.Fatal(err)
	}
	for k, seq := range got {
		if !slices.IsSorted(seq) {
			t.Fatalf("key %s out of order: %v", k, seq)
		}
	}
}

func TestFailedKeyStaysPausedUntilResumed(t *testing.T) {
	c := newClient(t)
	ctx := context.Background()
	pub, _ := gcp.NewPublisher(c)
	defer pub.Close(ctx)

	// The topic does not exist: the first keyed publish fails and pauses k.
	if _, err := pub.Publish(ctx, "missing", &pubsub.Message{Key: "k", Data: []byte("1")}); err == nil || errors.Is(err, pubsub.ErrKeyPaused) {
		t.Fatalf("first publish err = %v", err)
	}
	_, err := pub.Publish(ctx, "missing", &pubsub.Message{Key: "k", Data: []byte("2")})
	if !errors.Is(err, pubsub.ErrKeyPaused) {
		t.Fatalf("second publish err = %v, want ErrKeyPaused", err)
	}
	pub.ResumeKey("missing", "k")
	if _, err := pub.Publish(ctx, "missing", &pubsub.Message{Key: "k", Data: []byte("3")}); errors.Is(err, pubsub.ErrKeyPaused) {
		t.Fatal("key still paused after ResumeKey")
	}
	_ = pub.Close(ctx)
	if _, err := pub.Publish(ctx, "missing", &pubsub.Message{}); !errors.Is(err, pubsub.ErrClosed) {
		t.Fatalf("after Close err = %v", err)
	}
}
