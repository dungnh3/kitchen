package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestRunArgErrors(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	tests := []struct {
		args []string
		want string
	}{
		{nil, "missing command"},
		{[]string{"up"}, "-database"},
		{[]string{"-bogus"}, "flag provided but not defined"},
	}
	for _, tt := range tests {
		var out, errb bytes.Buffer
		err := run(context.Background(), tt.args, &out, &errb)
		if err == nil || !strings.Contains(err.Error(), tt.want) {
			t.Errorf("run(%v) = %v, want %q", tt.args, err, tt.want)
		}
	}
}
