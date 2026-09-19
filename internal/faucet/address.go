package faucet

import (
	"strings"

	"github.com/Snipa22/go-tari-lib/v2/address"
)

// ValidateAddress checks that raw is a well-formed Tari address (base58,
// hex, or emoji encoding -- anything address.Parse accepts) using go-tari-
// lib's DammSum/Ristretto255-based validation, and returns the parsed
// address plus its canonical base58 form. Malformed input returns
// address.Address{} and the underlying parse error unchanged, so callers
// can render it directly rather than needing their own validation logic.
func ValidateAddress(raw string) (address.Address, string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return address.Address{}, "", address.ErrInvalidAddressString
	}
	addr, err := address.Parse(trimmed)
	if err != nil {
		return address.Address{}, "", err
	}
	return addr, addr.Base58(), nil
}
