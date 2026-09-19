package faucet

import (
	"crypto/rand"
	"testing"

	"github.com/Snipa22/go-tari-lib/v2/address"
	"github.com/gtank/ristretto255"
)

// validTestnetAddress generates a real, valid Esmeralda single-address
// (a genuine Ristretto255 point, canonically encoded via
// address.PublicKeyFromCanonicalBytes -- same technique go-tari-lib's own
// address_test.go uses for randomPublicKey), then returns its base58
// encoding for use as a "well-formed address" fixture across tests. This
// deliberately doesn't hand-roll validation: it exercises the exact same
// address.NewSingleInteractiveOnly/Base58 path a real client would.
func validTestnetAddress(t *testing.T) string {
	t.Helper()
	var seed [64]byte
	if _, err := rand.Read(seed[:]); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	scalar, err := new(ristretto255.Scalar).SetUniformBytes(seed[:])
	if err != nil {
		t.Fatalf("SetUniformBytes: %v", err)
	}
	elem := new(ristretto255.Element).ScalarBaseMult(scalar)
	key, err := address.PublicKeyFromCanonicalBytes(elem.Bytes())
	if err != nil {
		t.Fatalf("PublicKeyFromCanonicalBytes: %v", err)
	}
	addr := address.NewSingleInteractiveOnly(key, address.Esmeralda)
	return addr.Base58()
}
