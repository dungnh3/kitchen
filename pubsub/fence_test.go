package pubsub

import (
	"strconv"
	"testing"
	"time"
)

func TestKeyFences(t *testing.T) {
	now := time.Unix(0, 0)
	f := newKeyFences()
	f.now = func() time.Time { return now }
	m := func(id, key string) *Message { return &Message{ID: id, Key: key} }

	f.block("k", "1", time.Minute)
	if f.admit(m("2", "k"), 1) || f.admit(m("3", "k"), 2) {
		t.Fatal("other messages of a fenced key admitted")
	}
	if !f.admit(m("x", ""), 0) || !f.admit(m("9", "other"), 0) {
		t.Fatal("unkeyed or other keys must pass")
	}
	// The redelivery of 1 advances the fence; 2 and 3, nacked meanwhile,
	// are awaited next, in order.
	if !f.admit(m("1", "k"), 3) || !f.admit(m("2", "k"), 4) || !f.admit(m("3", "k"), 5) {
		t.Fatal("the redeliveries must advance and lift the fence")
	}
	f.block("k", "4", time.Minute)
	now = now.Add(time.Minute)
	if !f.admit(m("5", "k"), 0) {
		t.Fatal("expired fence still blocks")
	}
	f.block("k", "6", 5*time.Second) // per-entry ttl
	now = now.Add(5 * time.Second)
	if !f.admit(m("7", "k"), 0) {
		t.Fatal("short per-entry ttl not honoured")
	}
	f.block("k", "8", 0)
	if !f.admit(m("9", "k"), 0) {
		t.Fatal("ttl 0 must not fence")
	}
}

// Many live fences (an outage that nacks every key) must not make each
// block scan the whole map: the sweep is amortised.
func TestKeyFencesSweepAmortised(t *testing.T) {
	f := newKeyFences()
	f.now = func() time.Time { return time.Unix(0, 0) }
	const live = 30000
	for i := range live {
		f.block("k"+strconv.Itoa(i), "id", time.Minute)
	}
	// Count sweeps through the map size trigger instead of timing.
	if f.sweepAt < live {
		t.Fatalf("sweepAt = %d after %d live fences, want >= %d (each block would rescan the map)", f.sweepAt, live, live)
	}
}

func BenchmarkKeyFenceBlockLive(b *testing.B) {
	for _, live := range []int{100, 2000, 30000} {
		b.Run(strconv.Itoa(live), func(b *testing.B) {
			f := newKeyFences()
			for i := range live {
				f.block("k"+strconv.Itoa(i), "id", time.Minute)
			}
			b.ResetTimer()
			for i := range b.N {
				f.block("x"+strconv.Itoa(i%live), "id", time.Minute)
			}
		})
	}
}

// Redeliveries are awaited in arrival order, whatever order the nacks
// happened in, and a re-failed head goes back to the front.
func TestKeyFencesAwaitInArrivalOrder(t *testing.T) {
	f := newKeyFences()
	m := func(id string) *Message { return &Message{ID: id, Key: "k"} }
	f.block("k", "17", time.Minute) // 17 ran and failed
	// 20 (arrival 4) is rejected at receive before 18, 19 (arrivals 2, 3)
	// are rejected at the head of their queue.
	if f.peek(m("20"), 4) || f.admit(m("18"), 2) || f.admit(m("19"), 3) {
		t.Fatal("messages of a fenced key admitted")
	}
	if !f.admit(m("17"), 5) {
		t.Fatal("awaited redelivery 17 rejected")
	}
	f.block("k", "17", time.Minute) // 17 failed again: back to the front
	if f.admit(m("18"), 6) {
		t.Fatal("18 admitted before 17's redelivery")
	}
	for _, id := range []string{"17", "18", "19", "20"} {
		if !f.admit(m(id), 7) {
			t.Fatalf("awaited redelivery %s rejected (fence %v)", id, f.m["k"])
		}
	}
	if len(f.m) != 0 {
		t.Fatalf("fence not lifted after every redelivery: %v", f.m)
	}
	if !f.admit(m("21"), 8) {
		t.Fatal("new message rejected after the fence lifted")
	}
}
