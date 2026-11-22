package timex

import (
	"math/rand/v2"
	"time"
)

// Backoff returns the delay before retry number attempt (1-based) of an
// exponential backoff: base·2^(attempt-1), capped at maxDelay, with "equal
// jitter" (a uniform value between half and all of it), so retries of many
// clients spread out instead of arriving together.
//
// A base ≤ 0 returns 0; a maxDelay below base is raised to base; an attempt
// below 1 counts as 1. Use it for retry and reconnect loops instead of
// re-implementing the formula.
func Backoff(base, maxDelay time.Duration, attempt int) time.Duration {
	if base <= 0 {
		return 0
	}
	maxDelay = max(maxDelay, base)
	d := base
	for i := 1; i < attempt && d < maxDelay; i++ {
		d *= 2
	}
	d = min(d, maxDelay)
	half := d / 2
	if half <= 0 {
		return d
	}
	return half + rand.N(d-half+1) //nolint:gosec // G404: retry jitter spreads load; it is not a secret.
}

// Jitter returns d scaled by a uniform random factor in [1-frac, 1+frac],
// so TTLs or intervals set at the same moment do not expire together. frac
// is clamped to [0, 1]; frac 0 or d ≤ 0 returns d unchanged.
func Jitter(d time.Duration, frac float64) time.Duration {
	if d <= 0 || frac <= 0 {
		return d
	}
	frac = min(frac, 1)
	f := 1 + frac*(2*rand.Float64()-1) //nolint:gosec // G404: load spreading, not a secret.
	return time.Duration(float64(d) * f)
}
