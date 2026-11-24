// Package timex provides time parsing and calendar-boundary helpers.
//
// All boundary helpers keep the location of their input: StartOfDay of a time
// in Asia/Ho_Chi_Minh is local midnight, not UTC midnight. The package has no
// default time zone; load one with [time.LoadLocation] in the application
// (import _ "time/tzdata" if the runtime image has no zoneinfo).
package timex

import (
	"fmt"
	"iter"
	"strings"
	"time"

	"google.golang.org/grpc/codes"

	"github.com/dungnh3/kitchen/errs"
)

// Layouts accepted by [Parse], tried in order.
var layouts = []string{
	time.RFC3339Nano,
	time.DateTime,
	time.DateOnly,
	"2006/01/02",
	time.TimeOnly,
	"2006-01",
	"2006/01",
}

// ErrEmpty is returned by [Parse] for an empty or blank string.
var ErrEmpty = errs.New(codes.InvalidArgument, "EMPTY_TIME", errs.Message("empty time string"))

// Parse parses s using the first matching layout among RFC 3339,
// "2006-01-02 15:04:05", "2006-01-02", "2006/01/02", "15:04:05", "2006-01"
// and "2006/01". Values without an offset are interpreted as UTC.
func Parse(s string) (time.Time, error) {
	return ParseInLocation(s, time.UTC)
}

// ParseInLocation is like [Parse] but interprets values without an offset in
// loc.
func ParseInLocation(s string, loc *time.Location) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, ErrEmpty
	}
	for _, layout := range layouts {
		if t, err := time.ParseInLocation(layout, s, loc); err == nil {
			return t, nil
		}
	}
	return time.Time{}, errs.Wrap(fmt.Errorf("timex: unrecognised time %q", s), errs.ErrInvalidArgument)
}

// MustParse is like [Parse] but panics on error. Use it for constants and
// tests only.
func MustParse(s string) time.Time {
	t, err := Parse(s)
	if err != nil {
		panic(err)
	}
	return t
}

// StartOfDay returns midnight at the start of t's day in t's location.
func StartOfDay(t time.Time) time.Time {
	y, m, d := t.Date()
	return time.Date(y, m, d, 0, 0, 0, 0, t.Location())
}

// EndOfDay returns the last nanosecond of t's day in t's location.
func EndOfDay(t time.Time) time.Time {
	y, m, d := t.Date()
	return time.Date(y, m, d+1, 0, 0, 0, 0, t.Location()).Add(-time.Nanosecond)
}

// StartOfMonth returns midnight on the first day of t's month.
func StartOfMonth(t time.Time) time.Time {
	y, m, _ := t.Date()
	return time.Date(y, m, 1, 0, 0, 0, 0, t.Location())
}

// EndOfMonth returns the last nanosecond of t's month.
func EndOfMonth(t time.Time) time.Time {
	y, m, _ := t.Date()
	return time.Date(y, m+1, 1, 0, 0, 0, 0, t.Location()).Add(-time.Nanosecond)
}

// StartOfYear returns midnight on January 1st of t's year.
func StartOfYear(t time.Time) time.Time {
	return time.Date(t.Year(), time.January, 1, 0, 0, 0, 0, t.Location())
}

// EndOfYear returns the last nanosecond of t's year.
func EndOfYear(t time.Time) time.Time {
	return time.Date(t.Year()+1, time.January, 1, 0, 0, 0, 0, t.Location()).Add(-time.Nanosecond)
}

// Days yields the start of every calendar day from from's day to to's day,
// inclusive, in from's location. It yields nothing when to's day is before
// from's day.
func Days(from, to time.Time) iter.Seq[time.Time] {
	return func(yield func(time.Time) bool) {
		end := StartOfDay(to.In(from.Location()))
		for d := StartOfDay(from); !d.After(end); {
			if !yield(d) {
				return
			}
			y, m, dd := d.Date()
			d = time.Date(y, m, dd+1, 0, 0, 0, 0, d.Location())
		}
	}
}
