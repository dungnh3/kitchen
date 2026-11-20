package reply_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/dungnh3/kitchen/pubsub/reply"
)

func TestNewIDIsValidAndUnique(t *testing.T) {
	seen := map[string]bool{}
	for range 1000 {
		id := reply.NewID()
		if err := reply.ValidateID(id); err != nil {
			t.Fatalf("NewID %q invalid: %v", id, err)
		}
		if seen[id] {
			t.Fatalf("duplicate id %q", id)
		}
		seen[id] = true
	}
}

func TestValidateID(t *testing.T) {
	for _, id := range []string{"", "a b", "a:b", "*", "{x}", strings.Repeat("a", 129)} {
		if err := reply.ValidateID(id); !errors.Is(err, reply.ErrInvalidID) {
			t.Errorf("ValidateID(%q) = %v", id, err)
		}
	}
}
