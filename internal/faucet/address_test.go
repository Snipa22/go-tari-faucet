package faucet

import (
	"errors"
	"testing"

	"github.com/Snipa22/go-tari-lib/address"
)

func TestValidateAddress_AcceptsWellFormedAddress(t *testing.T) {
	valid := validTestnetAddress(t)
	addr, base58, err := ValidateAddress(valid)
	if err != nil {
		t.Fatalf("ValidateAddress(%q) returned unexpected error: %v", valid, err)
	}
	if base58 == "" {
		t.Fatal("ValidateAddress returned an empty base58 string for a valid address")
	}
	if addr.Kind() != address.KindSingle {
		t.Fatalf("ValidateAddress returned kind %v, want KindSingle", addr.Kind())
	}
}

func TestValidateAddress_TrimsWhitespace(t *testing.T) {
	valid := validTestnetAddress(t)
	_, base58, err := ValidateAddress("  " + valid + "\n")
	if err != nil {
		t.Fatalf("ValidateAddress with surrounding whitespace returned unexpected error: %v", err)
	}
	if base58 != valid {
		t.Fatalf("ValidateAddress trimmed base58 = %q, want %q", base58, valid)
	}
}

func TestValidateAddress_RejectsMalformedAddress(t *testing.T) {
	cases := []string{
		"",
		"   ",
		"not-a-tari-address-at-all",
		"12345",
		"!!!invalid-base58-chars-@#$%",
	}
	for _, tc := range cases {
		t.Run(tc, func(t *testing.T) {
			_, _, err := ValidateAddress(tc)
			if err == nil {
				t.Fatalf("ValidateAddress(%q) = nil error, want a validation error", tc)
			}
		})
	}
}

func TestValidateAddress_RejectsTruncatedValidAddress(t *testing.T) {
	valid := validTestnetAddress(t)
	truncated := valid[:len(valid)-5]
	_, _, err := ValidateAddress(truncated)
	if err == nil {
		t.Fatalf("ValidateAddress(%q) = nil error, want a validation error for a truncated address", truncated)
	}
	if !errors.Is(err, address.ErrInvalidAddressString) && !errors.Is(err, address.ErrInvalidSize) {
		// Any address package error is acceptable here, just make sure we
		// got one that describeAddressError can render.
		t.Logf("truncated address rejected with: %v", err)
	}
}
