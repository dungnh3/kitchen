package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
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
		{[]string{"create"}, "create needs"},
		{[]string{"create", "../evil"}, "create needs"},
		{[]string{"-database", "x", "-dialect", "oracle", "up"}, "unknown dialect"},
	}
	for _, tt := range tests {
		var out, errb bytes.Buffer
		err := run(context.Background(), tt.args, &out, &errb)
		if err == nil || !strings.Contains(err.Error(), tt.want) {
			t.Errorf("run(%v) = %v, want %q", tt.args, err, tt.want)
		}
	}
}

func TestCreate(t *testing.T) {
	dir := t.TempDir()
	var out bytes.Buffer
	if err := run(context.Background(), []string{"-path", dir, "create", "add_users"}, &out, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	path := strings.TrimSpace(out.String())
	if filepath.Dir(path) != dir || !strings.HasSuffix(path, "_add_users.sql") {
		t.Fatalf("path = %q", path)
	}
	b, err := os.ReadFile(path) //nolint:gosec // test temp dir.
	if err != nil || !strings.Contains(string(b), "-- +goose Up") {
		t.Fatalf("content = %q, %v", b, err)
	}
}
