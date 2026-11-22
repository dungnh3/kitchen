package gcp_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	gpubsub "cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"

	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/pubsub"
	"github.com/dungnh3/kitchen/pubsub/gcp"
)

// End to end on the emulator: an ordered subscription with a dead-letter
// policy of 5 attempts, a poison message A, then C with the same key. Pub/Sub
// moves A to the dead-letter topic; C must be processed, not nacked by the
// key fence until Pub/Sub dead-letters it too (the review's C1: with the old
// 1m fence the dead-letter topic received [A C] and the handler never saw C).
func TestEmulatorFenceDoesNotDeadLetterLaterMessages(t *testing.T) {
	if os.Getenv("PUBSUB_EMULATOR_HOST") == "" {
		t.Skip("PUBSUB_EMULATOR_HOST not set")
	}
	for _, tc := range []struct {
		name string
		opts []gcp.SourceOption
		mode pubsub.KeyFenceMode
	}{
		{name: "auto, settings read from the subscription", mode: pubsub.KeyFenceAuto},
		{name: "forced on, settings stated", mode: pubsub.KeyFenceOn, opts: []gcp.SourceOption{gcp.WithSubscriptionConfig(true, 5)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			c, err := gpubsub.NewClient(ctx, project)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = c.Close() }()
			sfx := fmt.Sprint(time.Now().UnixNano())
			tn, dn := "projects/"+project+"/topics/t"+sfx, "projects/"+project+"/topics/d"+sfx
			for _, n := range []string{tn, dn} {
				if _, err := c.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: n}); err != nil {
					t.Fatal(err)
				}
			}
			dsub := "projects/" + project + "/subscriptions/ds" + sfx
			if _, err := c.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{Name: dsub, Topic: dn}); err != nil {
				t.Fatal(err)
			}
			sub := "s" + sfx
			if _, err := c.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{
				Name: "projects/" + project + "/subscriptions/" + sub, Topic: tn, EnableMessageOrdering: true, AckDeadlineSeconds: 10,
				DeadLetterPolicy: &pubsubpb.DeadLetterPolicy{DeadLetterTopic: dn, MaxDeliveryAttempts: 5},
			}); err != nil {
				t.Fatal(err)
			}
			pub, _ := gcp.NewPublisher(c)
			defer func() { _ = pub.Close(ctx) }()
			if _, err := pub.Publish(ctx, tn, &pubsub.Message{Key: "k", Data: []byte("A")}); err != nil {
				t.Fatal(err)
			}
			src, err := gcp.NewSource(c, sub, append([]gcp.SourceOption{gcp.WithSourceLogger(log.Discard())}, tc.opts...)...)
			if err != nil {
				t.Fatal(err)
			}
			var mu sync.Mutex
			var seen []string
			lastA, gotC := make(chan struct{}), make(chan struct{})
			var onceA, onceC sync.Once
			s, err := pubsub.NewSubscriber(src, func(_ context.Context, m *pubsub.Message) error {
				mu.Lock()
				seen = append(seen, fmt.Sprintf("%s#%d", m.Data, m.DeliveryAttempt))
				mu.Unlock()
				switch string(m.Data) {
				case "A":
					if m.DeliveryAttempt >= 5 {
						onceA.Do(func() { close(lastA) })
					}
					return errors.New("poison")
				case "C":
					onceC.Do(func() { close(gotC) })
				}
				return nil
			}, pubsub.WithLogger(log.Discard()), pubsub.WithRetry(0, 0, 0), pubsub.WithKeyFence(tc.mode))
			if err != nil {
				t.Fatal(err)
			}
			rctx, cancel := context.WithCancel(ctx)
			errc := make(chan error, 1)
			go func() { errc <- s.Run(rctx) }()
			select {
			case <-lastA:
			case <-time.After(60 * time.Second):
				t.Fatal("A never reached delivery attempt 5")
			}
			if _, err := pub.Publish(ctx, tn, &pubsub.Message{Key: "k", Data: []byte("C")}); err != nil {
				t.Fatal(err)
			}
			select {
			case <-gotC:
			case <-time.After(20 * time.Second):
				mu.Lock()
				t.Errorf("C was never handled; handler saw %v", seen)
				mu.Unlock()
			}
			defer func() {
				cancel()
				if err := <-errc; err != nil {
					t.Error(err)
				}
			}()
			// The subscriber keeps pulling: Pub/Sub forwards to the dead-letter topic asynchronously; read
			// until A arrives, then a little longer to catch a stray C.
			dctx, dcancel := context.WithTimeout(ctx, 30*time.Second)
			defer dcancel()
			var dmu sync.Mutex
			var dead []string
			_ = c.Subscriber(dsub).Receive(dctx, func(_ context.Context, m *gpubsub.Message) {
				dmu.Lock()
				dead = append(dead, string(m.Data))
				if string(m.Data) == "A" {
					time.AfterFunc(3*time.Second, dcancel)
				}
				dmu.Unlock()
				m.Ack()
			})
			dmu.Lock()
			defer dmu.Unlock()
			if slices.Contains(dead, "C") || !slices.Contains(dead, "A") {
				t.Fatalf("dead-letter topic got %v, want only A", dead)
			}
		})
	}
}
