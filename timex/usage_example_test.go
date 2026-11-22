package timex_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dungnh3/kitchen/timex"
)

// A reconnect loop: wait timex.Backoff between attempts and stop when ctx
// ends. HTTP clients get the same policy from httpx.WithRetry instead.
func ExampleBackoff() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	connect := func(attempt int) error {
		if attempt < 3 {
			return errors.New("refused")
		}
		return nil
	}
	for attempt := 1; ; attempt++ {
		if err := connect(attempt); err == nil {
			fmt.Println("connected after", attempt, "attempts")
			return
		}
		t := time.NewTimer(timex.Backoff(time.Millisecond, 10*time.Millisecond, attempt))
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
	}
	// Output: connected after 3 attempts
}

// Spread TTLs written at the same moment so they do not all expire together.
func ExampleJitter() {
	ttl := timex.Jitter(10*time.Minute, 0.1) // 9m–11m
	fmt.Println(ttl >= 9*time.Minute && ttl <= 11*time.Minute)
	// Output: true
}

func ExampleDays() {
	from := time.Date(2025, 3, 30, 0, 0, 0, 0, time.UTC)
	for d := range timex.Days(from, from.AddDate(0, 0, 2)) {
		fmt.Println(d.Format(time.DateOnly))
	}
	// Output:
	// 2025-03-30
	// 2025-03-31
	// 2025-04-01
}

func ExampleParseInLocation() {
	ict := time.FixedZone("ICT", 7*3600)
	t, _ := timex.ParseInLocation("2025-03-04 08:30:00", ict)
	fmt.Println(t)
	_, err := timex.Parse("  ")
	fmt.Println(errors.Is(err, timex.ErrEmpty))
	// Output:
	// 2025-03-04 08:30:00 +0700 ICT
	// true
}
