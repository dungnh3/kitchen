// Package xslices holds the few generic slice helpers the standard library
// does not provide. Prefer [slices] and [maps] for everything else.
package xslices

// Map returns a new slice holding fn applied to every element of s, in order.
// It returns nil when s is empty.
func Map[S ~[]T, T, U any](s S, fn func(T) U) []U {
	if len(s) == 0 {
		return nil
	}
	out := make([]U, len(s))
	for i, v := range s {
		out[i] = fn(v)
	}
	return out
}

// KeyBy indexes s by key. When two elements share a key, the first one wins.
func KeyBy[S ~[]T, T any, K comparable](s S, key func(T) K) map[K]T {
	m := make(map[K]T, len(s))
	for _, v := range s {
		k := key(v)
		if _, ok := m[k]; !ok {
			m[k] = v
		}
	}
	return m
}

// Uniq returns the distinct elements of s in first-seen order.
// Unlike [slices.Compact], s does not need to be sorted.
func Uniq[S ~[]T, T comparable](s S) S {
	return UniqBy(s, func(v T) T { return v })
}

// UniqBy returns the elements of s whose key is seen for the first time,
// in order.
func UniqBy[S ~[]T, T any, K comparable](s S, key func(T) K) S {
	if len(s) == 0 {
		return nil
	}
	seen := make(map[K]struct{}, len(s))
	out := make(S, 0, len(s))
	for _, v := range s {
		k := key(v)
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, v)
	}
	return out
}
