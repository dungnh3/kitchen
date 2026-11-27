package jobs_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dungnh3/kitchen/jobs"
	"github.com/dungnh3/kitchen/jobs/jobstest"
	"github.com/dungnh3/kitchen/log"
)

// SendWelcome is the payload of a job. Kind names it in the database and
// must never change while jobs of it are queued.
type SendWelcome struct {
	UserID string `json:"user_id"`
}

func (SendWelcome) Kind() string { return "send_welcome" }

func Example() {
	ctx := context.Background()
	drv := jobstest.New() // jobspgx.New(pool) or jobsmysql.New(db) in a service

	// Workers are registered once, typed by their args.
	reg := jobs.NewRegistry()
	sent := make(chan string, 1)
	err := jobs.Register(reg, jobs.WorkFunc[SendWelcome](func(_ context.Context, j *jobs.Job[SendWelcome]) error {
		sent <- j.Args.UserID // must be idempotent: a job can run twice
		return nil
	}))
	if err != nil {
		panic(err)
	}

	client, err := jobs.NewClient(drv, jobs.WithRegistry(reg), jobs.WithLogger(log.Discard()))
	if err != nil {
		panic(err)
	}
	stopped := make(chan error, 1)
	go func() { stopped <- client.Run(ctx) }() // the worker process

	// The enqueuer calls InsertTx(ctx, tx, ...) inside its business
	// transaction so the job exists if and only if the transaction commits.
	res, err := client.Insert(ctx, SendWelcome{UserID: "u-42"}, nil)
	if err != nil {
		panic(err)
	}
	fmt.Println("sent to", <-sent)

	// Close stops fetching, waits for running jobs and records their results.
	if err := client.Close(ctx); err != nil {
		panic(err)
	}
	if err := <-stopped; err != nil {
		panic(err)
	}
	row, _ := client.Get(ctx, res.ID)
	fmt.Println(row.State, "after", row.Attempt, "attempt")
	// Output:
	// sent to u-42
	// completed after 1 attempt
}

func ExampleClient_InsertTx() {
	ctx := context.Background()
	client, err := jobs.NewClient(jobstest.New(), jobs.WithLogger(log.Discard()))
	if err != nil {
		panic(err)
	}

	// A unique key makes the insert idempotent while the job is pending:
	// retrying the request that enqueues it does not enqueue it twice.
	opts := &jobs.InsertOpts{Queue: "emails", UniqueKey: "welcome:u-42", ScheduledAt: time.Now().Add(time.Minute)}
	for range 2 {
		res, err := client.InsertTx(ctx, jobstest.Tx{}, SendWelcome{UserID: "u-42"}, opts)
		if err != nil {
			panic(err)
		}
		fmt.Println("job", res.ID, "duplicate:", res.Duplicate)
	}
	// Output:
	// job 1 duplicate: false
	// job 1 duplicate: true
}

// Cancel ends a job for good: it is not retried, and its error is kept.
func ExampleCancel() {
	ctx := context.Background()
	reg := jobs.NewRegistry()
	ran := make(chan struct{}, 1)
	_ = jobs.Register(reg, jobs.WorkFunc[SendWelcome](func(context.Context, *jobs.Job[SendWelcome]) error {
		ran <- struct{}{}
		return jobs.Cancel(errors.New("the user was deleted"))
	}))
	client, _ := jobs.NewClient(jobstest.New(), jobs.WithRegistry(reg), jobs.WithLogger(log.Discard()))
	stopped := make(chan error, 1)
	go func() { stopped <- client.Run(ctx) }()

	res, _ := client.Insert(ctx, SendWelcome{UserID: "gone"}, nil)
	<-ran
	_ = client.Close(ctx)
	if err := <-stopped; err != nil {
		panic(err)
	}
	row, _ := client.Get(ctx, res.ID)
	fmt.Println(row.State, "-", row.Errors[0].Error)
	// Output: cancelled - job cancelled: the user was deleted
}

// Report prints a report every night at 00:00 UTC, once across all replicas.
type Report struct{}

func (Report) Kind() string { return "report" }

func ExampleWithPeriodic() {
	reg := jobs.NewRegistry()
	_ = jobs.Register(reg, jobs.WorkFunc[Report](func(context.Context, *jobs.Job[Report]) error {
		return nil // a failed run is retried like any other job
	}))
	// Every replica of the service runs the same client; the schedule has
	// one winner per slot because each slot's job has a unique key.
	client, err := jobs.NewClient(jobstest.New(),
		jobs.WithRegistry(reg),
		jobs.WithPeriodic(jobs.Every(24*time.Hour, func() jobs.Args { return Report{} })),
		jobs.WithQueues(jobs.Queue{Name: "default", Workers: 4}),
	)
	if err != nil {
		panic(err)
	}
	_ = client // client.Run(ctx) in the worker
}
