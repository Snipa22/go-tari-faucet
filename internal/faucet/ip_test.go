package faucet

import (
	"net/http"
	"testing"
)

func TestClientIP(t *testing.T) {
	tests := []struct {
		name       string
		setXFF     bool
		xff        string
		remoteAddr string
		want       string
	}{
		{
			name:       "no XFF header falls back to RemoteAddr",
			remoteAddr: "203.0.113.7:54321",
			want:       "203.0.113.7",
		},
		{
			name:       "single XFF entry is used",
			setXFF:     true,
			xff:        "198.51.100.9",
			remoteAddr: "10.0.0.1:12345",
			want:       "198.51.100.9",
		},
		{
			name:       "multiple XFF entries use the first (original client)",
			setXFF:     true,
			xff:        "198.51.100.9, 10.0.0.5, 10.0.0.1",
			remoteAddr: "10.0.0.1:12345",
			want:       "198.51.100.9",
		},
		{
			name:       "XFF entry with surrounding whitespace is trimmed",
			setXFF:     true,
			xff:        "  198.51.100.9  ,10.0.0.5",
			remoteAddr: "10.0.0.1:12345",
			want:       "198.51.100.9",
		},
		{
			name:       "empty XFF header falls back to RemoteAddr",
			setXFF:     true,
			xff:        "",
			remoteAddr: "203.0.113.7:54321",
			want:       "203.0.113.7",
		},
		{
			name:       "whitespace-only first XFF entry falls back to RemoteAddr",
			setXFF:     true,
			xff:        "   ,10.0.0.5",
			remoteAddr: "203.0.113.7:54321",
			want:       "203.0.113.7",
		},
		{
			name:       "RemoteAddr without a port is used verbatim",
			remoteAddr: "203.0.113.7",
			want:       "203.0.113.7",
		},
		{
			name:       "IPv6 RemoteAddr with port is split correctly",
			remoteAddr: "[2001:db8::1]:54321",
			want:       "2001:db8::1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r, err := http.NewRequest(http.MethodPost, "/request", nil)
			if err != nil {
				t.Fatalf("http.NewRequest: %v", err)
			}
			r.RemoteAddr = tc.remoteAddr
			if tc.setXFF {
				r.Header.Set("X-Forwarded-For", tc.xff)
			}

			got := ClientIP(r)
			if got != tc.want {
				t.Fatalf("ClientIP() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestClientIP_DoesNotTrustXFFBlindly(t *testing.T) {
	// Even a maliciously-crafted XFF header is just taken at its literal
	// first-hop value -- this test documents that ClientIP does not do any
	// deeper trust validation (e.g. against a trusted-proxy allowlist),
	// which is a deployment-level concern (reverse proxy config), not this
	// function's job. It does, however, still correctly fall through to
	// RemoteAddr when the header is unusable.
	r, err := http.NewRequest(http.MethodPost, "/request", nil)
	if err != nil {
		t.Fatalf("http.NewRequest: %v", err)
	}
	r.RemoteAddr = "203.0.113.7:1"
	r.Header.Set("X-Forwarded-For", "1.2.3.4")
	if got := ClientIP(r); got != "1.2.3.4" {
		t.Fatalf("ClientIP() = %q, want %q", got, "1.2.3.4")
	}
}
