package timex_test

import (
	"testing"
	"time"

	"github.com/dungnh3/kitchen/timex"
)

func TestBackoff(t *testing.T) {
	tests := []struct {
		base, maxDelay time.Duration
		attempt        int
		lo, hi         time.Duration
	}{
		{time.Second, 30 * time.Second, 1, 500 * time.Millisecond, time.Second},
		{time.Second, 30 * time.Second, 0, 500 * time.Millisecond, time.Second},
		{time.Second, 30 * time.Second, 3, 2 * time.Second, 4 * time.Second},
		{time.Second, 30 * time.Second, 50, 15 * time.Second, 30 * time.Second},
		{time.Second, 0, 5, 500 * time.Millisecond, time.Second}, // max raised to base
		{0, time.Second, 3, 0, 0},
		{time.Nanosecond, time.Nanosecond, 1, time.Nanosecond, time.Nanosecond},
	}
	for _, tt := range tests {
		for range 200 {
			if d := timex.Backoff(tt.base, tt.maxDelay, tt.attempt); d < tt.lo || d > tt.hi {
				t.Fatalf("Backoff(%v, %v, %d) = %v, want [%v, %v]", tt.base, tt.maxDelay, tt.attempt, d, tt.lo, tt.hi)
			}
		}
	}
}

func TestJitter(t *testing.T) {
	const d = time.Second
	for range 1000 {
		if j := timex.Jitter(d, 0.1); j < 900*time.Millisecond || j > 1100*time.Millisecond {
			t.Fatalf("Jitter(1s, 0.1) = %v", j)
		}
		if j := timex.Jitter(d, 5); j < 0 || j > 2*d {
			t.Fatalf("Jitter(1s, 5) = %v, frac must clamp to 1", j)
		}
	}
	if timex.Jitter(d, 0) != d || timex.Jitter(d, -1) != d || timex.Jitter(0, 0.5) != 0 {
		t.Fatal("Jitter must return d unchanged for frac ≤ 0 or d ≤ 0")
	}
}
