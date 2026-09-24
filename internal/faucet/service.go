package faucet

import (
	"context"
	"errors"
	"time"

	"github.com/Snipa22/go-tari-grpc-lib/v3/tari_generated"
	"github.com/sirupsen/logrus"
)

// Outcome classifies the result of a Dispense call so handlers can render
// an appropriate response without inspecting error strings.
type Outcome int

const (
	// OutcomeSuccess means the wallet accepted the transfer.
	OutcomeSuccess Outcome = iota
	// OutcomeInvalidAddress means the submitted address failed
	// address.Parse validation.
	OutcomeInvalidAddress
	// OutcomeRateLimited means the address or IP has dispensed
	// successfully within the configured rate-limit window.
	OutcomeRateLimited
	// OutcomeError means something else went wrong (DB or wallet GRPC
	// failure) -- the request should still get a clean error, not a 500.
	OutcomeError
)

// Result is the outcome of a single Dispense call.
type Result struct {
	Outcome    Outcome
	RetryAfter time.Time // valid only when Outcome == OutcomeRateLimited
	Err        error     // non-nil for OutcomeInvalidAddress/OutcomeError
	TxID       uint64    // valid only when Outcome == OutcomeSuccess
}

// Config holds Service's tunables, all of which are wired from main.go's
// flags.
type Config struct {
	DispenseAmount  uint64
	RateLimitWindow time.Duration
}

// Service is the faucet's core business logic: address validation, rate
// limiting, and wallet dispensing. It depends only on the narrow
// Repository/WalletClient/Clock interfaces, so it can be fully unit tested
// without a live Postgres or wallet GRPC daemon.
type Service struct {
	Repo   Repository
	Wallet WalletClient
	Clock  Clock
	Config Config
	Logger *logrus.Logger
}

// Dispense validates rawAddress, checks the rate limit for both the address
// and ip, and -- if allowed -- sends Config.DispenseAmount to the address
// via the wallet, recording the attempt in Repository regardless of
// outcome.
func (s *Service) Dispense(ctx context.Context, rawAddress, ip string) Result {
	logger := s.Logger
	if logger == nil {
		logger = logrus.StandardLogger()
	}

	_, base58Addr, err := ValidateAddress(rawAddress)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"address": rawAddress,
			"ip":      ip,
		}).Warn("faucet: rejected malformed address")
		return Result{Outcome: OutcomeInvalidAddress, Err: err}
	}

	now := s.now()
	since := now.Add(-s.Config.RateLimitWindow)
	// ReserveDispense does the rate-limit check AND reserves a
	// placeholder audit row atomically (see its doc comment) -- this is
	// the fix for the race that let concurrent requests for the same
	// address/ip all pass a plain "check" SELECT before any of their
	// "record" INSERTs landed. Nothing below this point may re-derive
	// the rate-limit decision from a separate, unsynchronized read.
	reservation, err := s.Repo.ReserveDispense(ctx, base58Addr, ip, s.Config.DispenseAmount, since, now)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"address": base58Addr,
			"ip":      ip,
			"error":   err,
		}).Warn("faucet: rate-limit reservation failed")
		return Result{Outcome: OutcomeError, Err: err}
	}
	if reservation.RateLimited {
		retryAfter := reservation.LastSuccessAt.Add(s.Config.RateLimitWindow)
		logger.WithFields(logrus.Fields{
			"address":     base58Addr,
			"ip":          ip,
			"retry_after": retryAfter,
		}).Info("faucet: rate limited")
		return Result{Outcome: OutcomeRateLimited, RetryAfter: retryAfter}
	}

	// the faucet only ever sends a single recipient per dispense call, so
	// single_tx is semantically a no-op here — pass false deliberately
	// rather than leaving it implicit.
	resp, sendErr := s.Wallet.SendTransactions([]*tari_generated.PaymentRecipient{
		{
			Address:     base58Addr,
			Amount:      s.Config.DispenseAmount,
			FeePerGram:  5,
			PaymentType: tari_generated.PaymentRecipient_ONE_SIDED,
		},
	}, false)

	success, errMsg, txID, result := s.buildResult(resp, sendErr)

	// Deliberately outside of ReserveDispense's advisory-lock
	// transaction -- see FinalizeDispense's doc comment for why this
	// (potentially slow, wallet-RPC-dependent) write must never hold
	// that lock.
	if finalizeErr := s.Repo.FinalizeDispense(ctx, reservation.ID, success, errMsg, txID); finalizeErr != nil {
		logger.WithFields(logrus.Fields{
			"address": base58Addr,
			"ip":      ip,
			"error":   finalizeErr,
		}).Error("faucet: failed to finalize dispense audit row")
	}

	logFields := logrus.Fields{
		"address": base58Addr,
		"ip":      ip,
		"amount":  s.Config.DispenseAmount,
		"success": success,
	}
	if errMsg != "" {
		logFields["error"] = errMsg
	}
	if success {
		logger.WithFields(logFields).Info("faucet: dispense attempt")
	} else {
		logger.WithFields(logFields).Warn("faucet: dispense attempt")
	}

	return result
}

// buildResult interprets the wallet's SendTransactions response, returning
// the success/error/txID values the caller records via FinalizeDispense
// alongside the Result to send back to the handler.
func (s *Service) buildResult(resp *tari_generated.TransferResponse, sendErr error) (success bool, errMsg string, txID uint64, result Result) {
	if sendErr != nil {
		return false, sendErr.Error(), 0, Result{Outcome: OutcomeError, Err: sendErr}
	}
	if resp == nil || len(resp.GetResults()) == 0 {
		err := errors.New("wallet returned no transfer result")
		return false, err.Error(), 0, Result{Outcome: OutcomeError, Err: err}
	}
	txResult := resp.GetResults()[0]
	if !txResult.GetIsSuccess() {
		err := errors.New(txResult.GetFailureMessage())
		return false, txResult.GetFailureMessage(), 0, Result{Outcome: OutcomeError, Err: err}
	}
	return true, "", txResult.GetTransactionId(), Result{Outcome: OutcomeSuccess, TxID: txResult.GetTransactionId()}
}

func (s *Service) now() time.Time {
	if s.Clock == nil {
		return time.Now()
	}
	return s.Clock.Now()
}

// CurrentBalance returns the wallet's currently spendable (available)
// balance, in microMinotari, via a live WalletClient.GetBalance call.
// ctx is accepted for symmetry with HealthCheck/Dispense's context-
// shaped signatures, even though the underlying WalletClient.GetBalance
// call (like SendTransactions/GetWalletConnectivity) doesn't take one
// yet.
//
// Handler.Index does NOT call this directly -- it reads StatusCache
// instead, so the balance display never blocks a request on a live
// wallet GRPC call (WalletClient.GetBalance has no timeout and has been
// observed to occasionally hang for 10+ seconds). StatusCache's
// background poll loop calls the wallet directly rather than through
// this method (Repo isn't involved in the balance/connectivity poll at
// all), so this method today exists as the live/synchronous primitive
// this package's tests exercise directly; it remains available for any
// caller that genuinely needs a fresh, uncached read.
func (s *Service) CurrentBalance(_ context.Context) (uint64, error) {
	resp, err := s.Wallet.GetBalance()
	if err != nil {
		return 0, err
	}
	if resp == nil {
		return 0, errors.New("wallet returned no balance response")
	}
	return resp.GetAvailableBalance(), nil
}

// HealthCheck reports whether both Postgres (via Repository.Ping) and the
// wallet GRPC connection (via WalletClient.GetWalletConnectivity) are
// reachable. A non-nil error means /healthz should return 503.
//
// Handler.Healthz does NOT call this directly -- it pings Postgres live
// (via Service.Repo.Ping, same as here) but reads the wallet's
// connectivity from StatusCache instead of calling
// WalletClient.GetWalletConnectivity synchronously per request, for the
// same no-request-should-block-on-a-live-wallet-call reason
// CurrentBalance's doc comment describes. This method remains available
// as the live/synchronous primitive.
func (s *Service) HealthCheck(ctx context.Context) error {
	if err := s.Repo.Ping(ctx); err != nil {
		return err
	}
	resp, err := s.Wallet.GetWalletConnectivity()
	if err != nil {
		return err
	}
	if resp == nil || resp.GetStatus() != tari_generated.CheckConnectivityResponse_Online {
		return errors.New("wallet GRPC connectivity is not online")
	}
	return nil
}
