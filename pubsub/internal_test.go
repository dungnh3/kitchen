package pubsub

import (
	"errors"
	"testing"
	"time"
)

func TestBackoffDelay(t *testing.T) {
	tests := []struct {
		attempt  int
		lo, hi   time.Duration
		base, mx time.Duration
	}{
		{1, 500 * time.Millisecond, time.Second, time.Second, 30 * time.Second},
		{2, time.Second, 2 * time.Second, time.Second, 30 * time.Second},
		{10, 15 * time.Second, 30 * time.Second, time.Second, 30 * time.Second},
		{1000, 15 * time.Second, 30 * time.Second, time.Second, 30 * time.Second},
		{1, 0, 0, 0, 0},
	}
	for _, tt := range tests {
		for range 100 {
			d := backoffDelay(tt.base, tt.mx, tt.attempt)
			if d < tt.lo || d > tt.hi {
				t.Fatalf("attempt %d: %v not in [%v, %v]", tt.attempt, d, tt.lo, tt.hi)
			}
		}
	}
}

func TestErrors(t *testing.T) {
	base := errors.New("bad json")
	p := Permanent(base)
	if !errors.Is(p, ErrPermanent) || !errors.Is(p, base) {
		t.Fatal("Permanent must wrap both")
	}
	if Permanent(nil) != nil {
		t.Fatal("Permanent(nil) != nil")
	}
	pe := &PublishError{IDs: []string{"1", ""}, Failed: map[int]error{1: ErrKeyPaused}}
	if !errors.Is(pe, ErrKeyPaused) {
		t.Fatal("PublishError must unwrap")
	}
	if errorType(pe) != "partial" {
		t.Fatalf("errorType = %s", errorType(pe))
	}
}
