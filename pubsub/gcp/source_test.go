package gcp_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	gpubsub "cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"cloud.google.com/go/pubsub/v2/pstest"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/pubsub"
	"github.com/dungnh3/kitchen/pubsub/gcp"
)

// syncBuffer is an io.Writer safe for concurrent use, for capturing logs.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func newClientWith(t *testing.T, opts ...pstest.ServerReactorOption) *gpubsub.Client {
	t.Helper()
	srv := pstest.NewServer(opts...)
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

type subSpec struct {
	ordered  bool
	eod      bool
	maxTries int32 // dead-letter policy max delivery attempts; 0 = no policy
}

func createSub(t *testing.T, c *gpubsub.Client, topic, sub string, spec subSpec) {
	t.Helper()
	ctx := context.Background()
	tn := fmt.Sprintf("projects/%s/topics/%s", project, topic)
	if _, err := c.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: tn}); err != nil {
		t.Fatal(err)
	}
	ps := &pubsubpb.Subscription{
		Name: fmt.Sprintf("projects/%s/subscriptions/%s", project, sub), Topic: tn,
		EnableMessageOrdering: spec.ordered, EnableExactlyOnceDelivery: spec.eod, AckDeadlineSeconds: 10,
	}
	if spec.maxTries > 0 {
		dl := fmt.Sprintf("projects/%s/topics/%s-dlq", project, topic)
		if _, err := c.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: dl}); err != nil {
			t.Fatal(err)
		}
		ps.DeadLetterPolicy = &pubsubpb.DeadLetterPolicy{DeadLetterTopic: dl, MaxDeliveryAttempts: spec.maxTries}
	}
	if _, err := c.SubscriptionAdminClient.CreateSubscription(ctx, ps); err != nil {
		t.Fatal(err)
	}
}

// receiveOne publishes msg to topic, runs src.Receive until the first
// delivery, acks it and returns it with src.Redelivery() as seen at that
// point. Receive must have returned when receiveOne returns.
func receiveOne(t *testing.T, c *gpubsub.Client, src *gcp.Source, topic string, msg *pubsub.Message) (*pubsub.Message, pubsub.Redelivery) {
	t.Helper()
	ctx := context.Background()
	pub, _ := gcp.NewPublisher(c)
	if _, err := pub.Publish(ctx, topic, msg); err != nil {
		t.Fatal(err)
	}
	_ = pub.Close(ctx)

	type got struct {
		m  *pubsub.Message
		rd pubsub.Redelivery
	}
	ch := make(chan got, 1)
	rctx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- src.Receive(rctx, func(_ context.Context, d pubsub.Delivery) {
			d.Ack()
			select {
			case ch <- got{d.Message(), src.Redelivery()}:
			default:
			}
		})
	}()
	var g got
	select {
	case g = <-ch:
	case err := <-done:
		t.Fatalf("Receive returned early: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("timeout")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	return g.m, g.rd
}

// The Source reads the subscription settings at Receive start: Key is the
// ordering key only on ordered subscriptions (elsewhere it is readable in
// OrderingKeyAttribute), and Redelivery reports whether the key fence helps
// and the dead-letter policy's max delivery attempts. When the settings
// cannot be read it keeps the safe defaults; WithSubscriptionConfig states
// them without reading.
func TestSourceUsesSubscriptionSettings(t *testing.T) {
	ordered := pubsub.Redelivery{Fence: true, FenceTTL: 5 * time.Second}
	tests := []struct {
		name     string
		spec     subSpec
		denyGet  bool // GetSubscription fails with PermissionDenied
		opts     []gcp.SourceOption
		wantKey  string
		wantAttr string // OrderingKeyAttribute value, "" = absent
		want     pubsub.Redelivery
		wantWarn bool
	}{
		{name: "ordered", spec: subSpec{ordered: true}, wantKey: "k", want: ordered},
		{
			name: "ordered with DLQ", spec: subSpec{ordered: true, maxTries: 5}, wantKey: "k",
			want: pubsub.Redelivery{Fence: true, FenceTTL: 5 * time.Second, MaxDeliveryAttempts: 5},
		},
		{name: "unordered", spec: subSpec{}, wantAttr: "k", want: pubsub.Redelivery{}},
		{
			name: "unordered with DLQ", spec: subSpec{maxTries: 7}, wantAttr: "k",
			want: pubsub.Redelivery{MaxDeliveryAttempts: 7},
		},
		{
			name: "settings unreadable: safe defaults", spec: subSpec{maxTries: 7}, denyGet: true,
			wantKey: "k", want: ordered, wantWarn: true,
		},
		{
			name: "stated unordered, no read needed", spec: subSpec{maxTries: 7}, denyGet: true,
			opts: []gcp.SourceOption{gcp.WithSubscriptionConfig(false, 7)}, wantAttr: "k",
			want: pubsub.Redelivery{MaxDeliveryAttempts: 7},
		},
		{
			name: "stated ordered, no read needed", spec: subSpec{ordered: true, maxTries: 5}, denyGet: true,
			opts: []gcp.SourceOption{gcp.WithSubscriptionConfig(true, 5)}, wantKey: "k",
			want: pubsub.Redelivery{Fence: true, FenceTTL: 5 * time.Second, MaxDeliveryAttempts: 5},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var ropts []pstest.ServerReactorOption
			if tt.denyGet {
				ropts = append(ropts, pstest.WithErrorInjection("GetSubscription", codes.PermissionDenied, "denied"))
			}
			c := newClientWith(t, ropts...)
			createSub(t, c, "t", "s", tt.spec)
			var logs syncBuffer
			opts := append([]gcp.SourceOption{gcp.WithSourceLogger(log.New(&logs, log.Options{Format: log.FormatJSON}))}, tt.opts...)
			src, err := gcp.NewSource(c, "s", opts...)
			if err != nil {
				t.Fatal(err)
			}
			if rd := src.Redelivery(); rd != ordered && len(tt.opts) == 0 {
				t.Errorf("Redelivery before Receive = %+v, want %+v", rd, ordered)
			}
			m, rd := receiveOne(t, c, src, "t", &pubsub.Message{Key: "k", Data: []byte("x"), Attributes: map[string]string{"a": "1"}})
			if m.Key != tt.wantKey {
				t.Errorf("Key = %q, want %q", m.Key, tt.wantKey)
			}
			if got := m.Attributes[gcp.OrderingKeyAttribute]; got != tt.wantAttr {
				t.Errorf("attribute %s = %q, want %q", gcp.OrderingKeyAttribute, got, tt.wantAttr)
			}
			if m.Attributes["a"] != "1" {
				t.Errorf("attributes = %v, want the published ones kept", m.Attributes)
			}
			if rd != tt.want {
				t.Errorf("Redelivery = %+v, want %+v", rd, tt.want)
			}
			if warned := strings.Contains(logs.String(), "subscription settings"); warned != tt.wantWarn {
				t.Errorf("warning logged = %t, want %t; logs: %s", warned, tt.wantWarn, logs.String())
			}
		})
	}
}

// blockingAck makes the fake server's Acknowledge RPC wait until release is
// closed and then fail with PermissionDenied.
type blockingAck struct{ release chan struct{} }

func (b blockingAck) React(any) (bool, any, error) {
	<-b.release
	return true, nil, status.Error(codes.PermissionDenied, "denied")
}

// On an exactly-once subscription Ack returns at once (it does not wait for
// the server's confirmation) and a failed acknowledgement is logged with the
// message ID and status before Receive returns.
func TestAckDoesNotBlockAndFailedAckIsLogged(t *testing.T) {
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	c := newClientWith(t, pstest.ServerReactorOption{FuncName: "Acknowledge", Reactor: blockingAck{release}})
	t.Cleanup(unblock) // runs before the server is closed
	createSub(t, c, "eod", "eod-sub", subSpec{eod: true})
	ctx := context.Background()
	pub, _ := gcp.NewPublisher(c)
	ids, err := pub.Publish(ctx, "eod", &pubsub.Message{Data: []byte("x")})
	if err != nil {
		t.Fatal(err)
	}
	_ = pub.Close(ctx)

	var logs syncBuffer
	src, err := gcp.NewSource(c, "eod-sub", gcp.WithSourceLogger(log.New(&logs, log.Options{Format: log.FormatJSON})),
		gcp.WithMaxAckExtension(10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	acked := make(chan struct{}, 1)
	rctx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- src.Receive(rctx, func(_ context.Context, d pubsub.Delivery) {
			d.Ack() // must not wait for the blocked Acknowledge RPC
			select {
			case acked <- struct{}{}:
			default:
			}
		})
	}()
	select {
	case <-acked:
	case <-time.After(10 * time.Second):
		t.Fatal("Ack blocked the handler while the server had not confirmed it")
	}
	unblock()
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	out := logs.String() // Receive has returned: the result must be logged by now
	for _, want := range []string{ids[0], "permission_denied", `"operation":"ack"`} {
		if !strings.Contains(out, want) {
			t.Fatalf("logs missing %q: %s", want, out)
		}
	}
}

// Against the Pub/Sub emulator (set PUBSUB_EMULATOR_HOST): the settings the
// Source reads from a real GetSubscription drive Key and Redelivery.
func TestEmulatorSubscriptionSettings(t *testing.T) {
	if os.Getenv("PUBSUB_EMULATOR_HOST") == "" {
		t.Skip("PUBSUB_EMULATOR_HOST not set")
	}
	tests := []struct {
		name    string
		spec    subSpec
		wantKey string
		want    pubsub.Redelivery
	}{
		{
			"ordered with DLQ",
			subSpec{ordered: true, maxTries: 5},
			"k",
			pubsub.Redelivery{Fence: true, FenceTTL: 5 * time.Second, MaxDeliveryAttempts: 5},
		},
		{"unordered", subSpec{}, "", pubsub.Redelivery{}},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			c, err := gpubsub.NewClient(ctx, project) // uses PUBSUB_EMULATOR_HOST
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = c.Close() }()
			sfx := fmt.Sprintf("%d-%d", time.Now().UnixNano(), i)
			createSub(t, c, "t"+sfx, "s"+sfx, tt.spec)
			src, err := gcp.NewSource(c, "s"+sfx, gcp.WithSourceLogger(log.Discard()))
			if err != nil {
				t.Fatal(err)
			}
			m, rd := receiveOne(t, c, src, "t"+sfx, &pubsub.Message{Key: "k", Data: []byte("x")})
			if m.Key != tt.wantKey || rd != tt.want {
				t.Fatalf("Key = %q, Redelivery = %+v; want %q, %+v", m.Key, rd, tt.wantKey, tt.want)
			}
		})
	}
}
