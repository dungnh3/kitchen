package webmeta

import (
	"errors"
	"net/netip"
	"testing"
)

func TestForbidden(t *testing.T) {
	tests := []struct {
		ip   string
		want bool
	}{
		{"127.0.0.1", true},
		{"10.1.2.3", true},
		{"172.16.0.1", true},
		{"192.168.1.1", true},
		{"169.254.169.254", true}, // cloud metadata
		{"100.64.0.1", true},
		{"0.0.0.0", true},
		{"255.255.255.255", true},
		{"224.0.0.1", true},
		{"::1", true},
		{"fe80::1", true},
		{"fc00::1", true},
		{"::ffff:127.0.0.1", true},
		{"::ffff:10.0.0.1", true},
		{"64:ff9b::a00:1", true},
		{"2002:a00:1::", true},
		{"8.8.8.8", false},
		{"1.1.1.1", false},
		{"2606:4700:4700::1111", false},
	}
	for _, tt := range tests {
		if got := forbidden(netip.MustParseAddr(tt.ip)); got != tt.want {
			t.Errorf("forbidden(%s) = %v, want %v", tt.ip, got, tt.want)
		}
	}
}

func TestDialControl(t *testing.T) {
	if err := dialControl("tcp", "127.0.0.1:80", nil); !errors.Is(err, ErrForbiddenAddress) {
		t.Fatalf("loopback err = %v", err)
	}
	if err := dialControl("tcp", "garbage", nil); !errors.Is(err, ErrForbiddenAddress) {
		t.Fatalf("garbage err = %v", err)
	}
	if err := dialControl("tcp", "[2606:4700:4700::1111]:443", nil); err != nil {
		t.Fatalf("public err = %v", err)
	}
}
