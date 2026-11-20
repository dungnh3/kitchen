package redisx

import "testing"

func TestParseID(t *testing.T) {
	good := map[string]ID{"0": {}, "0-0": {}, "1732089600000-12": {1732089600000, 12}, "5": {5, 0}}
	for s, want := range good {
		got, err := ParseID(s)
		if err != nil || got != want {
			t.Errorf("ParseID(%q) = %v, %v", s, got, err)
		}
	}
	for _, s := range []string{"", "-", "$", ">", "+", "1-", "-1", "a-1", "1-2-3", "1 -2", "99999999999999999999999-0", "1-0\n"} {
		if _, err := ParseID(s); err == nil {
			t.Errorf("ParseID(%q) accepted", s)
		}
	}
	a, b := ID{1, 2}, ID{1, 3}
	if a.Compare(b) != -1 || b.Compare(a) != 1 || a.Compare(a) != 0 || (ID{2, 0}).Compare(b) != 1 {
		t.Error("Compare")
	}
	if a.String() != "1-2" || !Zero.IsZero() {
		t.Error("String/IsZero")
	}
}

func TestBucketStable(t *testing.T) {
	first := Bucket("run-1", 64)
	if Bucket("run-1", 64) != first || HashTag(7) != "{b7}" {
		t.Fatal("bucket not stable")
	}
	for i := range 1000 {
		if b := Bucket(string(rune(i)), 64); b < 0 || b >= 64 {
			t.Fatal(b)
		}
	}
}
