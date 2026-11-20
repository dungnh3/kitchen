// Package xmaps holds generic map helpers missing from the standard [maps]
// package.
package xmaps

// Diff returns the entries of a that are missing from b or whose value in b
// differs (compared with ==). It never returns nil.
func Diff[M ~map[K]V, K, V comparable](a, b M) M {
	return DiffFunc(a, b, func(x, y V) bool { return x == y })
}

// DiffFunc is like [Diff] but compares values with eq. Use it with
// reflect.DeepEqual for maps of non-comparable values such as map[string]any.
func DiffFunc[M ~map[K]V, K comparable, V any](a, b M, eq func(V, V) bool) M {
	out := make(M)
	for k, va := range a {
		if vb, ok := b[k]; !ok || !eq(va, vb) {
			out[k] = va
		}
	}
	return out
}
