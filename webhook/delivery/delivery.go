// Package delivery sends webhooks durably: one [jobs] job per message and
// endpoint, retried on a fixed schedule over hours.
//
// The service enqueues a delivery with the jobs client, inside the
// transaction that changes the business data, so a webhook is sent if and
// only if the change committed:
//
//	args, _ := delivery.NewArgs(endpointID, "invoice.paid", payload)
//	_, err := client.InsertTx(ctx, tx, args, nil)
//
// The worker ([NewWorker], registered in the jobs registry) looks the
// endpoint up through [Endpoints] at every attempt, signs the payload with
// the endpoint's secrets ([webhook.Signer]) and POSTs it through httpx with
// the SSRF guard on, because endpoint URLs come from customers. A 2xx
// response completes the job. 410 Gone stops delivery to that endpoint. Any
// other response or failure is retried after the next delay of the schedule
// (default 5s, 5m, 30m, 2h, 5h, 10h: seven attempts over about 17.6 hours)
// and the job is discarded when it runs out of attempts.
//
// Storing endpoints and secrets, and showing delivery logs, is business
// code: implement [Endpoints] and, for logs or disabling failing endpoints,
// [Config].OnResult.
package delivery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/dungnh3/kitchen/jobs"
	"github.com/dungnh3/kitchen/webhook"
)

// Kind is the jobs kind of a delivery. It is stored in the jobs table.
const Kind = "kitchen.webhook.delivery"

// DefaultQueue is the jobs queue of deliveries. Work it in the worker
// process (jobs.WithQueues) next to the service's own queues.
const DefaultQueue = "webhooks"

// DefaultSchedule is the delay before each retry. The job has one attempt
// more than the schedule has entries.
var DefaultSchedule = []time.Duration{
	5 * time.Second, 5 * time.Minute, 30 * time.Minute, 2 * time.Hour, 5 * time.Hour, 10 * time.Hour,
}

// Args is one message to one endpoint: the payload of a delivery job.
type Args struct {
	// MessageID is the webhook-id, the same for every attempt and every
	// endpoint of a message; receivers deduplicate by it.
	MessageID string `json:"message_id"`
	// EndpointID identifies the endpoint for [Endpoints].
	EndpointID string `json:"endpoint_id"`
	// EventType names the event (for example "invoice.paid"), for logs and
	// [Result]. It is not sent; put it in the payload too.
	EventType string `json:"event_type,omitempty"`
	// Payload is the JSON body.
	Payload json.RawMessage `json:"payload"`
}

// Kind implements jobs.Args.
func (Args) Kind() string { return Kind }

// InsertOpts implements jobs.InsertDefaults: the "webhooks" queue, one
// attempt more than [DefaultSchedule] has entries, and a unique key on
// endpoint and message, kept until the job is deleted, so enqueueing a
// message twice sends it once.
func (a Args) InsertOpts() jobs.InsertOpts {
	return jobs.InsertOpts{
		Queue:       DefaultQueue,
		MaxAttempts: len(DefaultSchedule) + 1,
		UniqueKey:   "webhook:" + a.EndpointID + ":" + a.MessageID,
		KeepUnique:  true,
	}
}

// NewArgs returns the delivery of a new message (with a new
// [webhook.NewMessageID]) to endpointID. payload is encoded as JSON.
func NewArgs(endpointID, eventType string, payload any) (Args, error) {
	if endpointID == "" {
		return Args{}, errors.New("delivery: empty endpoint id")
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return Args{}, fmt.Errorf("delivery: encode payload: %w", err)
	}
	return Args{MessageID: webhook.NewMessageID(), EndpointID: endpointID, EventType: eventType, Payload: b}, nil
}

// To returns the same message for another endpoint, keeping its
// MessageID. Use it to fan one event out to every subscribed endpoint.
func (a Args) To(endpointID string) Args {
	a.EndpointID = endpointID
	return a
}

// ErrEndpointNotFound is returned by [Endpoints] for an endpoint that no
// longer exists; its pending deliveries are cancelled.
var ErrEndpointNotFound = errors.New("delivery: endpoint not found")

// Endpoint is where and how a message is sent.
type Endpoint struct {
	// URL receives the POST. It must be https unless Config.AllowHTTP.
	URL string
	// Secrets sign the message ("whsec_" or "whsk_"). Give two during a
	// rotation; the receiver accepts either.
	Secrets []string
	// Disabled cancels pending deliveries to the endpoint.
	Disabled bool
}

// Endpoints looks endpoints up. It is called at every attempt, so a
// changed URL, a rotated secret or a disabled endpoint takes effect on the
// next retry. Return [ErrEndpointNotFound] for a deleted endpoint; any
// other error is retried.
type Endpoints interface {
	Endpoint(ctx context.Context, id string) (Endpoint, error)
}

// EndpointsFunc adapts a function to [Endpoints].
type EndpointsFunc func(ctx context.Context, id string) (Endpoint, error)

// Endpoint calls f.
func (f EndpointsFunc) Endpoint(ctx context.Context, id string) (Endpoint, error) { return f(ctx, id) }
