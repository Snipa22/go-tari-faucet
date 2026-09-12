package faucet

import "testing"

// TestFormatXTM covers the shared microMinotari -> XTM display
// conversion used by both indexTemplate's balance display and the
// Request handler's success message. All internal representations stay
// raw microMinotari uint64 -- this only exercises the display-layer
// formatting.
func TestFormatXTM(t *testing.T) {
	tests := []struct {
		name string
		n    uint64
		want string
	}{
		{
			name: "exact whole XTM amount has no fractional remainder",
			n:    100_000_000,
			want: "100 XTM",
		},
		{
			name: "small fractional amount is zero-padded to 6 digits",
			n:    5,
			want: "0.000005 XTM",
		},
		{
			name: "large realistic balance keeps whole-part comma grouping",
			n:    799_178_469_391,
			want: "799,178.469391 XTM",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := formatXTM(tt.n); got != tt.want {
				t.Fatalf("formatXTM(%d) = %q, want %q", tt.n, got, tt.want)
			}
		})
	}
}
