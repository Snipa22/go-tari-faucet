package faucet

import (
	"net"
	"net/http"
	"strings"
)

// ClientIP extracts the real client IP from r, preferring the first entry of
// a X-Forwarded-For header (this service may sit behind a reverse proxy at
// deploy time) but falling back to r.RemoteAddr when the header is absent or
// unparseable. It does not blindly trust X-Forwarded-For: an empty or
// whitespace-only first entry is treated as absent and falls through to
// RemoteAddr, same as a missing header.
func ClientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		// X-Forwarded-For is a comma-separated list, closest-proxy-appended;
		// the first entry is the original client as seen by the first proxy
		// in the chain.
		parts := strings.Split(xff, ",")
		first := strings.TrimSpace(parts[0])
		if first != "" {
			return first
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		// RemoteAddr wasn't in host:port form (e.g. a bare test-provided
		// address) -- use it verbatim rather than dropping the request.
		return r.RemoteAddr
	}
	return host
}
