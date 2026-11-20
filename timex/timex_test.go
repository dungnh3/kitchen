package timex_test

import (
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/dungnh3/kitchen/timex"
)

func TestParse(t *testing.T) {
	tests := []struct {
		in   string
		want time.Time
	}{
		{"2025-03-04 10:11:12", time.Date(2025, 3, 4, 10, 11, 12, 0, time.UTC)},
		{"2025-03-04", time.Date(2025, 3, 4, 0, 0, 0, 0, time.UTC)},
		{" 2025/03/04 ", time.Date(2025, 3, 4, 0, 0, 0, 0, time.UTC)},
		{"10:11:12", time.Date(0, 1, 1, 10, 11, 12, 0, time.UTC)},
		{"2025-03", time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC)},
		{"2025/03", time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC)},
		{"2025-03-04T10:11:12+07:00", time.Date(2025, 3, 4, 3, 11, 12, 0, time.UTC)},
	}
	for _, tt := range tests {
		got, err := timex.Parse(tt.in)
		if err != nil {
			t.Errorf("Parse(%q): %v", tt.in, err)
			continue
		}
		if !got.Equal(tt.want) {
			t.Errorf("Parse(%q) = %v, want %v", tt.in, got, tt.want)
		}
	}
	if _, err := timex.Parse("  "); !errors.Is(err, timex.ErrEmpty) {
		t.Errorf("Parse(blank) err = %v", err)
	}
	if _, err := timex.Parse("04-03-2025"); err == nil {
		t.Error("Parse(bad) want error")
	}
}

func TestParseInLocation(t *testing.T) {
	loc := time.FixedZone("ICT", 7*3600)
	got, err := timex.ParseInLocation("2025-03-04", loc)
	if err != nil || got.Location() != loc || got.UTC().Hour() != 17 {
		t.Fatalf("ParseInLocation = %v, %v", got, err)
	}
}

func TestMustParsePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("want panic")
		}
	}()
	timex.MustParse("nope")
}

func TestBoundariesKeepLocation(t *testing.T) {
	loc := time.FixedZone("ICT", 7*3600)
	in := time.Date(2024, 2, 10, 1, 30, 0, 0, loc) // 2024-02-09 18:30 UTC
	tests := []struct {
		name string
		got  time.Time
		want time.Time
	}{
		{"StartOfDay", timex.StartOfDay(in), time.Date(2024, 2, 10, 0, 0, 0, 0, loc)},
		{"EndOfDay", timex.EndOfDay(in), time.Date(2024, 2, 10, 23, 59, 59, 999999999, loc)},
		{"StartOfMonth", timex.StartOfMonth(in), time.Date(2024, 2, 1, 0, 0, 0, 0, loc)},
		{"EndOfMonth", timex.EndOfMonth(in), time.Date(2024, 2, 29, 23, 59, 59, 999999999, loc)},
		{"StartOfYear", timex.StartOfYear(in), time.Date(2024, 1, 1, 0, 0, 0, 0, loc)},
		{"EndOfYear", timex.EndOfYear(in), time.Date(2024, 12, 31, 23, 59, 59, 999999999, loc)},
	}
	for _, tt := range tests {
		if !tt.got.Equal(tt.want) || tt.got.Location() != loc {
			t.Errorf("%s = %v, want %v", tt.name, tt.got, tt.want)
		}
	}
}

func TestDays(t *testing.T) {
	from := time.Date(2024, 2, 27, 15, 0, 0, 0, time.UTC)
	to := time.Date(2024, 3, 1, 1, 0, 0, 0, time.UTC)
	var got []int
	for d := range timex.Days(from, to) {
		got = append(got, d.Day())
	}
	if !slices.Equal(got, []int{27, 28, 29, 1}) {
		t.Fatalf("Days = %v", got)
	}
	n := 0
	for range timex.Days(to, from) {
		n++
	}
	if n != 0 {
		t.Fatalf("reversed Days yielded %d", n)
	}
	for range timex.Days(from, to) {
		break // early stop must not panic
	}
}

func ExampleParse() {
	t, err := timex.Parse("2025-03-04")
	fmt.Println(t, err)
	// Output: 2025-03-04 00:00:00 +0000 UTC <nil>
}

func ExampleStartOfDay() {
	ict := time.FixedZone("ICT", 7*3600)
	t := time.Date(2025, 3, 4, 1, 30, 0, 0, ict)
	fmt.Println(timex.StartOfDay(t))
	fmt.Println(timex.EndOfMonth(t).Format(time.RFC3339Nano))
	// Output:
	// 2025-03-04 00:00:00 +0700 ICT
	// 2025-03-31T23:59:59.999999999+07:00
}
