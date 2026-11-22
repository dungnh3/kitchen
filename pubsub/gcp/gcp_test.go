package gcp_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	gpubsub "cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"cloud.google.com/go/pubsub/v2/pstest"
	"go.uber.org/goleak"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/log"
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
	defer func() { _ = pub.Close(ctx) }()
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
	s, err := pubsub.NewSubscriber(src, func(_ context.Context, m *pubsub.Message) error {
		mu.Lock()
		defer mu.Unlock()
		var n int
		_, _ = fmt.Sscan(string(m.Data), &n)
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
	}, pubsub.WithLogger(log.Discard()), pubsub.WithRetry(2, time.Millisecond, time.Millisecond))
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
	defer func() { _ = pub.Close(ctx) }()

	// The topic does not exist: the first keyed publish fails and pauses k.
	// Its error reports both: the broker failure and the paused key.
	_, err := pub.Publish(ctx, "missing", &pubsub.Message{Key: "k", Data: []byte("1")})
	if !errors.Is(err, pubsub.ErrKeyPaused) {
		t.Fatalf("first publish err = %v, want ErrKeyPaused", err)
	}
	if !errors.Is(err, errs.ErrDependencyUnavailable) || errs.Code(err) != codes.Unavailable {
		t.Fatalf("first publish err = %v (code %v), want the broker failure as UNAVAILABLE", err, errs.Code(err))
	}
	if !strings.Contains(err.Error(), "NotFound") {
		t.Fatalf("first publish err = %v, want the cause kept", err)
	}
	// An unkeyed failure pauses nothing.
	if _, err := pub.Publish(ctx, "missing", &pubsub.Message{Data: []byte("x")}); errors.Is(err, pubsub.ErrKeyPaused) {
		t.Fatalf("unkeyed publish err = %v, want no ErrKeyPaused", err)
	}
	_, err = pub.Publish(ctx, "missing", &pubsub.Message{Key: "k", Data: []byte("2")})
	if !errors.Is(err, pubsub.ErrKeyPaused) {
		t.Fatalf("second publish err = %v, want ErrKeyPaused", err)
	}
	pub.ResumeKey("missing", "k")
	// Resumed: the publish reaches the broker again (and fails there).
	if _, err := pub.Publish(ctx, "missing", &pubsub.Message{Key: "k", Data: []byte("3")}); !errors.Is(err, errs.ErrDependencyUnavailable) {
		t.Fatalf("publish after ResumeKey err = %v: key still paused", err)
	}
	_ = pub.Close(ctx)
	if _, err := pub.Publish(ctx, "missing", &pubsub.Message{}); !errors.Is(err, pubsub.ErrClosed) {
		t.Fatalf("after Close err = %v", err)
	}
}

func TestExactlyOnceAckAndGoogleTraceContext(t *testing.T) {
	c := newClient(t)
	ctx := context.Background()
	tn := fmt.Sprintf("projects/%s/topics/eod", project)
	if _, err := c.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: tn}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name: fmt.Sprintf("projects/%s/subscriptions/eod-sub", project), Topic: tn,
		EnableExactlyOnceDelivery: true, AckDeadlineSeconds: 10,
	}); err != nil {
		t.Fatal(err)
	}
	pub, _ := gcp.NewPublisher(c)
	defer func() { _ = pub.Close(ctx) }()
	const tp = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
	// Published by a client using the Google library's built-in tracing.
	if _, err := pub.Publish(ctx, "eod", &pubsub.Message{Data: []byte("x"), Attributes: map[string]string{
		"googclient_traceparent": tp,
	}}); err != nil {
		t.Fatal(err)
	}

	src, err := gcp.NewSource(c, "eod-sub", gcp.WithMaxAckExtension(10*time.Second),
		gcp.WithSourceLogger(log.Discard()))
	if err != nil {
		t.Fatal(err)
	}
	got := make(chan *pubsub.Message, 1)
	rctx, cancel := context.WithCancel(ctx)
	done := make(chan error)
	go func() {
		done <- src.Receive(rctx, func(_ context.Context, d pubsub.Delivery) {
			d.Ack() // does not wait; the result is checked in the background
			select {
			case got <- d.Message():
			default:
			}
		})
	}()
	var m *pubsub.Message
	select {
	case m = <-got:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if m.Attributes["traceparent"] != tp {
		t.Fatalf("attributes = %v, want traceparent mapped from googclient_traceparent", m.Attributes)
	}
}
