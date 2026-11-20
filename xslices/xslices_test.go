package xslices_test

import (
	"fmt"
	"slices"
	"strconv"
	"testing"

	"github.com/dungnh3/kitchen/xslices"
)

type user struct {
	ID   string
	Name string
}

func TestMap(t *testing.T) {
	if got := xslices.Map([]int(nil), strconv.Itoa); got != nil {
		t.Fatalf("Map(nil) = %v, want nil", got)
	}
	got := xslices.Map([]int{1, 2, 3}, strconv.Itoa)
	if !slices.Equal(got, []string{"1", "2", "3"}) {
		t.Fatalf("Map = %v", got)
	}
}

func TestKeyByFirstWins(t *testing.T) {
	m := xslices.KeyBy([]user{{"1", "a"}, {"2", "b"}, {"1", "c"}}, func(u user) string { return u.ID })
	if len(m) != 2 || m["1"].Name != "a" || m["2"].Name != "b" {
		t.Fatalf("KeyBy = %v", m)
	}
}

func TestUniq(t *testing.T) {
	tests := []struct {
		in, want []int
	}{
		{nil, nil},
		{[]int{1}, []int{1}},
		{[]int{3, 1, 3, 2, 1}, []int{3, 1, 2}},
	}
	for _, tt := range tests {
		if got := xslices.Uniq(tt.in); !slices.Equal(got, tt.want) {
			t.Errorf("Uniq(%v) = %v, want %v", tt.in, got, tt.want)
		}
	}
}

func TestUniqBy(t *testing.T) {
	got := xslices.UniqBy([]user{{"1", "a"}, {"2", "b"}, {"1", "c"}}, func(u user) string { return u.ID })
	if len(got) != 2 || got[0].Name != "a" || got[1].Name != "b" {
		t.Fatalf("UniqBy = %v", got)
	}
}

func ExampleMap() {
	users := []user{{"1", "Ann"}, {"2", "Bob"}}
	fmt.Println(xslices.Map(users, func(u user) string { return u.Name }))
	// Output: [Ann Bob]
}

func ExampleUniqBy() {
	users := []user{{"1", "Ann"}, {"2", "Bob"}, {"1", "Ann again"}}
	ids := xslices.Uniq(xslices.Map(users, func(u user) string { return u.ID }))
	fmt.Println(ids, len(xslices.UniqBy(users, func(u user) string { return u.ID })))
	// Output: [1 2] 2
}
