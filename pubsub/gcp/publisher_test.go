package gcp_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	gpubsub "cloud.google.com/go/pubsub/v2"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/pubsub"
	"github.com/dungnh3/kitchen/pubsub/gcp"
)

// v2 refuses hedging together with message ordering, which the Publisher
// always enables: every Publish would fail, so NewPublisher must refuse it.
func TestNewPublisherRejectsHedging(t *testing.T) {
	c := newClient(t)
	tests := []struct {
		name    string
		opts    []gcp.PublisherOption
		wantErr bool
	}{
		{"defaults", nil, false},
		{"settings without hedging", []gcp.PublisherOption{gcp.WithPublishSettings(gpubsub.PublishSettings{CountThreshold: 10})}, false},
		{"hedging", []gcp.PublisherOption{gcp.WithPublishSettings(gpubsub.PublishSettings{HedgingSettings: &gpubsub.HedgingSettings{}})}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := gcp.NewPublisher(c, tt.opts...)
			if (err != nil) != tt.wantErr {
				t.Fatalf("NewPublisher err = %v, wantErr %t", err, tt.wantErr)
			}
			if err != nil {
				if !errors.Is(err, errs.ErrInvalidArgument) {
					t.Fatalf("err = %v, want InvalidArgument", err)
				}
				return
			}
			_ = p.Close(context.Background())
		})
	}
}

// A topic ID and its full name are one topic: they share the handle, so a
// key paused through one spelling is paused through the other, and ResumeKey
// works with either.
func TestTopicIDAndFullNameShareOneHandle(t *testing.T) {
	c := newClient(t)
	ctx := context.Background()
	pub, _ := gcp.NewPublisher(c)
	defer func() { _ = pub.Close(ctx) }()
	full := fmt.Sprintf("projects/%s/topics/missing", project)

	// The topic does not exist: the keyed publish fails and pauses k.
	if _, err := pub.Publish(ctx, "missing", &pubsub.Message{Key: "k", Data: []byte("1")}); err == nil {
		t.Fatal("publish to a missing topic succeeded")
	}
	if _, err := pub.Publish(ctx, full, &pubsub.Message{Key: "k", Data: []byte("2")}); !errors.Is(err, pubsub.ErrKeyPaused) || errors.Is(err, errs.ErrDependencyUnavailable) {
		t.Fatalf("publish by full name err = %v, want ErrKeyPaused (same topic, same paused key)", err)
	}
	pub.ResumeKey(full, "k")
	// Resumed: the publish reaches the broker again (and fails there).
	if _, err := pub.Publish(ctx, "missing", &pubsub.Message{Key: "k", Data: []byte("3")}); !errors.Is(err, errs.ErrDependencyUnavailable) {
		t.Fatalf("publish by ID after ResumeKey(full name) err = %v: key still paused", err)
	}
}

// A Publish that obtained the topic handle before Close and publishes after
// it must fail with pubsub.ErrClosed, not with v2's ErrPublisherStopped
// reported as a broker outage.
func TestPublishOnStoppedHandleReturnsErrClosed(t *testing.T) {
	c := newClient(t)
	setup(t, c, "closing", "closing-sub")
	ctx := context.Background()
	pub, _ := gcp.NewPublisher(c)
	defer func() { _ = pub.Close(ctx) }()
	if _, err := pub.Publish(ctx, "closing", &pubsub.Message{Data: []byte("1")}); err != nil {
		t.Fatal(err)
	}
	// Simulate Close stopping the cached handle while a Publish is past the
	// closed check: stop the very handle the Publisher uses.
	gcp.StopTopicForTest(pub, "closing")

	for _, key := range []string{"", "k"} {
		_, err := pub.Publish(ctx, "closing", &pubsub.Message{Key: key, Data: []byte("2")})
		if !errors.Is(err, pubsub.ErrClosed) {
			t.Fatalf("key %q: err = %v, want ErrClosed", key, err)
		}
	}
}

// A partially filled WithPublishSettings keeps the v2 defaults for the zero
// fields; in particular Timeout 0 would mean no publish deadline at all.
func TestPartialPublishSettingsKeepDefaults(t *testing.T) {
	c := newClient(t)
	d := gpubsub.DefaultPublishSettings
	tests := []struct {
		name string
		in   gpubsub.PublishSettings
		want gpubsub.PublishSettings
	}{
		{"zero", gpubsub.PublishSettings{}, d},
		{"count only", gpubsub.PublishSettings{CountThreshold: 10}, func() gpubsub.PublishSettings {
			w := d
			w.CountThreshold = 10
			return w
		}()},
		{"explicit values kept", gpubsub.PublishSettings{
			DelayThreshold: 5 * time.Millisecond, CountThreshold: 7, ByteThreshold: 1000,
			Timeout: 3 * time.Second, NumGoroutines: 2, EnableCompression: true, CompressionBytesThreshold: 10,
			FlowControlSettings: gpubsub.FlowControlSettings{MaxOutstandingMessages: 5, MaxOutstandingBytes: 100, LimitExceededBehavior: gpubsub.FlowControlBlock},
		}, gpubsub.PublishSettings{
			DelayThreshold: 5 * time.Millisecond, CountThreshold: 7, ByteThreshold: 1000,
			Timeout: 3 * time.Second, NumGoroutines: 2, EnableCompression: true, CompressionBytesThreshold: 10,
			FlowControlSettings: gpubsub.FlowControlSettings{MaxOutstandingMessages: 5, MaxOutstandingBytes: 100, LimitExceededBehavior: gpubsub.FlowControlBlock},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := gcp.NewPublisher(c, gcp.WithPublishSettings(tt.in))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = p.Close(context.Background()) }()
			if got := gcp.TopicSettingsForTest(p, "t"); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("settings = %+v\nwant       %+v", got, tt.want)
			}
		})
	}
}
