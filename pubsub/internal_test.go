package pubsub

import (
	"errors"
	"testing"
)

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
