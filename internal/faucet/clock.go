// Package faucet implements the Tari testnet faucet's HTTP service: address
// validation, rate limiting, wallet dispensing, and the small persistence
// surface backing it. It follows the go-crypto-pool convention of narrow,
// consumer-scoped interfaces (Repository, WalletClient, Clock) so unit tests
// can supply fakes instead of talking to a live Postgres/wallet GRPC daemon.
package faucet

import "time"

// Clock is the narrow time source Service depends on, so tests can freeze
// "now" instead of racing a real rate-limit window.
type Clock interface {
	Now() time.Time
}

// RealClock is the production Clock, backed by time.Now.
type RealClock struct{}

// Now returns the current wall-clock time.
func (RealClock) Now() time.Time { return time.Now() }
