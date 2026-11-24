package redisbus

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/dungnh3/kitchen/sse"
)

// TestResumeProperty checks the delivery guarantee under random ring and
// page sizes, writes concurrent with reads, and random disconnects: a viewer
// that reconnects with the last ID it received, again and again, receives
// every entry of the stream exactly once and in order, unless a resume
// position was trimmed, which must surface as ErrReplayGap (never a silent
// skip).
func TestResumeProperty(t *testing.T) {
	for seed := range uint64(40) {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, 0x5eed)) //nolint:gosec // reproducible test seeds
			_, rdb := newRedis(t)
			maxLen := int64(0)
			trim := rng.IntN(4) == 0
			if trim {
				maxLen = int64(5 + rng.IntN(20))
			}
			p := newProducer(t, rdb, ProducerConfig{MaxLen: maxLen})
			h := newHub(t, rdb, HubConfig{
				RingSize:     []int{1, 2, 4, 8, 512}[rng.IntN(5)],
				RingBytes:    []int{16, 64, 1 << 20}[rng.IntN(3)],
				CatchupBatch: []int64{1, 2, 3, 256}[rng.IntN(4)],
			})
			ctx := t.Context()
			total := 1 + rng.IntN(60)
			before := rng.IntN(total + 1)
			appendN(t, p, "s", 0, before)

			// The writer appends the rest while viewers read, then Ends.
			wdone := make(chan struct{})
			go func() {
				defer close(wdone)
				for i := before; i < total; i++ {
					if _, err := p.Append(ctx, "s", sse.Message{Type: sse.TypeToken, Data: fmt.Appendf(nil, "t%d", i)}); err != nil {
						t.Error(err)
						return
					}
				}
				if _, err := p.End(ctx, "s"); err != nil {
					t.Error(err)
				}
			}()

			var got []string
			cursor := ""
			truncated := false
			for round := 0; ; round++ {
				if round > 200 {
					t.Fatal("too many reconnects")
				}
				stopAfter := rng.IntN(8) // disconnect after this many messages
				cctx, cancel := context.WithCancel(ctx)
				n, ended := 0, true
				var gotErr error
				for m, err := range h.Subscribe(cctx, "s", cursor) {
					if err != nil {
						gotErr = err
						break
					}
					switch m.Type {
					case sse.TypeResumed:
						if cursor == "" {
							t.Fatal("stream.resumed without a cursor")
						}
						continue
					case sse.TypeTruncated:
						if cursor != "" || len(got) != 0 {
							t.Fatal("stream.truncated after the first message")
						}
						truncated = true
						continue
					}
					got = append(got, string(m.Data))
					cursor = m.ID
					n++
					if n == stopAfter {
						ended = false
						break
					}
				}
				cancel()
				if gotErr != nil {
					if trim && errors.Is(gotErr, sse.ErrReplayGap) {
						<-wdone
						return // a trimmed cursor surfaced as a gap: allowed
					}
					t.Fatalf("round %d: %v", round, gotErr)
				}
				if ended {
					break
				}
			}
			<-wdone
			first := 0
			if trim && len(got) > 0 {
				// A trimmed head is allowed (and flagged on real Redis);
				// what remains must be contiguous.
				_, _ = fmt.Sscanf(got[0], "t%d", &first)
			} else if truncated {
				t.Fatal("stream.truncated on an untrimmed stream")
			}
			want := make([]string, 0, total)
			for i := first; i < total; i++ {
				want = append(want, fmt.Sprintf("t%d", i))
			}
			if !slices.Equal(got, want) {
				t.Fatalf("got %v\nwant %v", got, want)
			}
		})
	}
}
