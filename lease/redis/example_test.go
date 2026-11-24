package redis_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/lease"
	leaseredis "github.com/dungnh3/kitchen/lease/redis"
	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/redisx"
)

func Example() {
	ctx := context.Background()
	rdb, err := redisx.Open(ctx, redisx.Config{Addrs: []string{"localhost:6379"}})
	if err != nil {
		return
	}
	defer func() { _ = rdb.Close() }()
	locker, _ := leaseredis.New(rdb)

	elector, _ := lease.NewElector(locker, "billing-scheduler")
	_ = elector.Run(ctx, func(ctx context.Context, l lease.Lease) error {
		// Leader only. Pass l.Token as a fencing token to protected writes;
		// return when ctx is done (lease lost or shutdown).
		_ = l.Token
		<-ctx.Done()
		return nil
	})
}

func ExampleNew() {
	ctx := context.Background()
	mr, _ := miniredis.Run() // in a service: your Redis address
	defer mr.Close()
	rdb, err := redisx.Open(ctx, redisx.Config{Addrs: []string{mr.Addr()}})
	if err != nil {
		panic(err)
	}
	defer func() { _ = rdb.Close() }()

	locker, err := leaseredis.New(rdb)
	if err != nil {
		panic(err)
	}
	logger := log.Discard() // in a service: the *log.Logger from main

	err = lease.Do(ctx, locker, "invoice-batch", 30*time.Second, func(_ context.Context, l lease.Lease) error {
		// Only the lease holder runs this. Long work must watch ctx.
		fmt.Println("running with token", l.Token)
		return nil
	}, lease.DoLogger(logger))
	fmt.Println(err)
	// Output:
	// running with token 1
	// <nil>
}

func Example_notHeld() {
	ctx := context.Background()
	mr, _ := miniredis.Run()
	defer mr.Close()
	rdb, _ := redisx.Open(ctx, redisx.Config{Addrs: []string{mr.Addr()}})
	defer func() { _ = rdb.Close() }()
	locker, _ := leaseredis.New(rdb)

	// Another process holds the lease.
	_, _ = locker.Acquire(ctx, "invoice-batch", "pod-a", time.Minute)

	err := lease.Do(ctx, locker, "invoice-batch", time.Minute, func(context.Context, lease.Lease) error {
		return nil // not called
	}, lease.DoOwner("pod-b"))
	fmt.Println(errors.Is(err, lease.ErrNotHeld), errs.HTTPStatus(err))
	// Output: true 409
}

func Example_elector() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mr, _ := miniredis.Run()
	defer mr.Close()
	rdb, _ := redisx.Open(ctx, redisx.Config{Addrs: []string{mr.Addr()}})
	defer func() { _ = rdb.Close() }()
	locker, _ := leaseredis.New(rdb)

	elector, err := lease.NewElector(locker, "billing-scheduler",
		lease.WithTTL(15*time.Second),
		lease.WithLogger(log.Discard()), // in a service: the *log.Logger from main
	)
	if err != nil {
		panic(err)
	}
	err = elector.Run(ctx, func(ctx context.Context, l lease.Lease) error {
		fmt.Println("leader, token", l.Token)
		cancel() // here: shutdown. Normally lead runs until ctx is done.
		<-ctx.Done()
		return nil
	})
	fmt.Println(err)
	// Output:
	// leader, token 1
	// <nil>
}

// fencedStore stands for the resource a lease protects (a database row, a
// file). It rejects writes with a token older than the newest it has seen.
type fencedStore struct{ lastToken uint64 }

var errStaleToken = errors.New("stale fencing token")

func (s *fencedStore) Write(token uint64) error {
	if token < s.lastToken {
		return errStaleToken
	}
	s.lastToken = token
	return nil
}

func Example_fencing() {
	ctx := context.Background()
	mr, _ := miniredis.Run()
	defer mr.Close()
	rdb, _ := redisx.Open(ctx, redisx.Config{Addrs: []string{mr.Addr()}})
	defer func() { _ = rdb.Close() }()
	locker, _ := leaseredis.New(rdb)
	store := &fencedStore{}

	// Pod A takes the lease, then pauses (GC, network) past its TTL.
	a, _ := locker.Acquire(ctx, "report", "pod-a", time.Second)
	mr.FastForward(2 * time.Second)

	// Pod B takes over and writes with a newer token.
	b, _ := locker.Acquire(ctx, "report", "pod-b", time.Second)
	fmt.Println("B:", store.Write(b.Token))

	// Pod A wakes up and tries to write: the store refuses the old token.
	fmt.Println("A:", errors.Is(store.Write(a.Token), errStaleToken))
	// Output:
	// B: <nil>
	// A: true
}
