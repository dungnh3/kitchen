package xmaps_test

import (
	"fmt"
	"maps"
	"reflect"
	"testing"

	"github.com/dungnh3/kitchen/xmaps"
)

func TestDiff(t *testing.T) {
	a := map[string]int{"x": 1, "y": 2, "z": 3}
	b := map[string]int{"x": 1, "y": 20}
	got := xmaps.Diff(a, b)
	if want := map[string]int{"y": 2, "z": 3}; !maps.Equal(got, want) {
		t.Fatalf("Diff = %v, want %v", got, want)
	}
	if got := xmaps.Diff(map[string]int(nil), nil); got == nil || len(got) != 0 {
		t.Fatalf("Diff(nil, nil) = %#v, want empty non-nil", got)
	}
}

func TestDiffFuncDeepEqual(t *testing.T) {
	a := map[string]any{"tags": []string{"a"}, "n": 1.0, "same": map[string]any{"k": "v"}}
	b := map[string]any{"tags": []string{"b"}, "n": 1.0, "same": map[string]any{"k": "v"}}
	got := xmaps.DiffFunc(a, b, func(x, y any) bool { return reflect.DeepEqual(x, y) })
	if len(got) != 1 || got["tags"] == nil {
		t.Fatalf("DiffFunc = %v", got)
	}
}

func ExampleDiffFunc() {
	before := map[string]any{"name": "Ann", "roles": []string{"admin"}}
	after := map[string]any{"name": "Ann", "roles": []string{"viewer"}}
	changed := xmaps.DiffFunc(after, before, func(x, y any) bool { return reflect.DeepEqual(x, y) })
	fmt.Println(changed)
	// Output: map[roles:[viewer]]
}
