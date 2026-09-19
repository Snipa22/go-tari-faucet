package faucet

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Snipa22/go-tari-grpc-lib/v3/tari_generated"
)

// fakeRepo is an in-memory Repository test double, following the same
// fakeXxx convention (and shape) as go-crypto-pool/internal/backend/
// unlocker's fakeRepo test double: plain maps/slices, no mocking
// framework, with error-injection knobs and a call log for assertions.
type fakeRepo struct {
	lastByKey map[string]time.Time // keyed by "address|ip"
	lastErr   error
	pingErr   error
	recorded  []DispenseRecord
	recordErr error
	// lookupCalls counts LastSuccessfulDispense invocations, so tests can
	// assert the rate-limit lookup was never reached (e.g. a
	// honeypot-tripped request must short-circuit before it).
	lookupCalls int
}

func (f *fakeRepo) LastSuccessfulDispense(_ context.Context, address, ip string, since time.Time) (time.Time, bool, error) {
	f.lookupCalls++
	if f.lastErr != nil {
		return time.Time{}, false, f.lastErr
	}
	var latest time.Time
	var found bool
	for _, key := range []string{"addr:" + address, "ip:" + ip} {
		if t, ok := f.lastByKey[key]; ok && !t.Before(since) {
			if !found || t.After(latest) {
				latest = t
				found = true
			}
		}
	}
	return latest, found, nil
}

func (f *fakeRepo) RecordDispense(_ context.Context, rec DispenseRecord) error {
	if f.recordErr != nil {
		return f.recordErr
	}
	f.recorded = append(f.recorded, rec)
	if rec.Success {
		if f.lastByKey == nil {
			f.lastByKey = map[string]time.Time{}
		}
		f.lastByKey["addr:"+rec.Address] = rec.CreatedAt
		f.lastByKey["ip:"+rec.IP] = rec.CreatedAt
	}
	return nil
}

func (f *fakeRepo) Ping(_ context.Context) error {
	return f.pingErr
}

// fakeWallet is an in-memory WalletClient test double.
type fakeWallet struct {
	sendResp        *tari_generated.TransferResponse
	sendErr         error
	connectivity    *tari_generated.CheckConnectivityResponse
	connectivityErr error
	sentRecipients  [][]*tari_generated.PaymentRecipient
	sentSingleTx    []bool
	balanceResp     *tari_generated.GetBalanceResponse
	balanceErr      error
}

func (f *fakeWallet) SendTransactions(transactions []*tari_generated.PaymentRecipient, singleTx bool) (*tari_generated.TransferResponse, error) {
	f.sentRecipients = append(f.sentRecipients, transactions)
	f.sentSingleTx = append(f.sentSingleTx, singleTx)
	if f.sendErr != nil {
		return nil, f.sendErr
	}
	return f.sendResp, nil
}

func (f *fakeWallet) GetWalletConnectivity() (*tari_generated.CheckConnectivityResponse, error) {
	if f.connectivityErr != nil {
		return nil, f.connectivityErr
	}
	return f.connectivity, nil
}

func (f *fakeWallet) GetBalance() (*tari_generated.GetBalanceResponse, error) {
	if f.balanceErr != nil {
		return nil, f.balanceErr
	}
	return f.balanceResp, nil
}

// fakeClock is a fixed/steppable Clock test double.
type fakeClock struct {
	now time.Time
}

func (f *fakeClock) Now() time.Time { return f.now }

func successResponse(txID uint64) *tari_generated.TransferResponse {
	return &tari_generated.TransferResponse{
		Results: []*tari_generated.TransferResult{
			{IsSuccess: true, TransactionId: txID},
		},
	}
}

func TestService_Dispense_RejectsInvalidAddress(t *testing.T) {
	repo := &fakeRepo{}
	wallet := &fakeWallet{sendResp: successResponse(1)}
	svc := &Service{
		Repo:   repo,
		Wallet: wallet,
		Clock:  &fakeClock{now: time.Now()},
		Config: Config{DispenseAmount: 1000000, RateLimitWindow: time.Hour},
	}

	result := svc.Dispense(context.Background(), "not-a-real-address", "10.0.0.1")
	if result.Outcome != OutcomeInvalidAddress {
		t.Fatalf("Outcome = %v, want OutcomeInvalidAddress", result.Outcome)
	}
	if result.Err == nil {
		t.Fatal("expected a non-nil error for an invalid address")
	}
	if len(wallet.sentRecipients) != 0 {
		t.Fatal("wallet should never be called for an invalid address")
	}
	if len(repo.recorded) != 0 {
		t.Fatal("no audit row should be recorded for an invalid address (it never reached dispensing)")
	}
}

func TestService_Dispense_SucceedsAndRecordsAuditRow(t *testing.T) {
	validAddr := validTestnetAddress(t)
	repo := &fakeRepo{}
	wallet := &fakeWallet{sendResp: successResponse(42)}
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	svc := &Service{
		Repo:   repo,
		Wallet: wallet,
		Clock:  &fakeClock{now: now},
		Config: Config{DispenseAmount: 1000000, RateLimitWindow: time.Hour},
	}

	result := svc.Dispense(context.Background(), validAddr, "10.0.0.1")
	if result.Outcome != OutcomeSuccess {
		t.Fatalf("Outcome = %v, want OutcomeSuccess (err=%v)", result.Outcome, result.Err)
	}
	if result.TxID != 42 {
		t.Fatalf("TxID = %d, want 42", result.TxID)
	}
	if len(repo.recorded) != 1 {
		t.Fatalf("expected exactly one recorded dispense, got %d", len(repo.recorded))
	}
	rec := repo.recorded[0]
	if !rec.Success || rec.Amount != 1000000 || rec.IP != "10.0.0.1" {
		t.Fatalf("recorded dispense = %+v, unexpected values", rec)
	}
	if len(wallet.sentSingleTx) != 1 || wallet.sentSingleTx[0] != false {
		t.Fatalf("wallet.SendTransactions singleTx = %v, want [false] (faucet dispenses one recipient per call, so single_tx must be passed false)", wallet.sentSingleTx)
	}
}

func TestService_Dispense_RateLimitsSameAddressWithinWindow(t *testing.T) {
	validAddr := validTestnetAddress(t)
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	repo := &fakeRepo{}
	wallet := &fakeWallet{sendResp: successResponse(1)}
	clock := &fakeClock{now: base}
	svc := &Service{
		Repo:   repo,
		Wallet: wallet,
		Clock:  clock,
		Config: Config{DispenseAmount: 1000000, RateLimitWindow: 24 * time.Hour},
	}

	// First request from this address/IP succeeds.
	first := svc.Dispense(context.Background(), validAddr, "10.0.0.1")
	if first.Outcome != OutcomeSuccess {
		t.Fatalf("first Dispense Outcome = %v, want OutcomeSuccess", first.Outcome)
	}

	// A second request for the same address, a few minutes later but well
	// inside the 24h window, from a *different* IP must still be blocked
	// (rate limit checks address OR ip).
	clock.now = base.Add(10 * time.Minute)
	second := svc.Dispense(context.Background(), validAddr, "10.0.0.2")
	if second.Outcome != OutcomeRateLimited {
		t.Fatalf("second Dispense Outcome = %v, want OutcomeRateLimited", second.Outcome)
	}
	wantRetry := base.Add(24 * time.Hour)
	if !second.RetryAfter.Equal(wantRetry) {
		t.Fatalf("RetryAfter = %v, want %v", second.RetryAfter, wantRetry)
	}
	if len(wallet.sentRecipients) != 1 {
		t.Fatalf("wallet.SendTransactions should have been called exactly once (not on the rate-limited attempt), got %d calls", len(wallet.sentRecipients))
	}
}

func TestService_Dispense_RateLimitsSameIPDifferentAddress(t *testing.T) {
	addrOne := validTestnetAddress(t)
	addrTwo := validTestnetAddress(t)
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	repo := &fakeRepo{}
	wallet := &fakeWallet{sendResp: successResponse(1)}
	clock := &fakeClock{now: base}
	svc := &Service{
		Repo:   repo,
		Wallet: wallet,
		Clock:  clock,
		Config: Config{DispenseAmount: 1000000, RateLimitWindow: time.Hour},
	}

	first := svc.Dispense(context.Background(), addrOne, "10.0.0.5")
	if first.Outcome != OutcomeSuccess {
		t.Fatalf("first Dispense Outcome = %v, want OutcomeSuccess", first.Outcome)
	}

	// Different address, same IP, still within the window -> blocked.
	clock.now = base.Add(time.Minute)
	second := svc.Dispense(context.Background(), addrTwo, "10.0.0.5")
	if second.Outcome != OutcomeRateLimited {
		t.Fatalf("second Dispense Outcome = %v, want OutcomeRateLimited", second.Outcome)
	}
}

func TestService_Dispense_AllowsAfterWindowExpires(t *testing.T) {
	validAddr := validTestnetAddress(t)
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	repo := &fakeRepo{}
	wallet := &fakeWallet{sendResp: successResponse(1)}
	clock := &fakeClock{now: base}
	svc := &Service{
		Repo:   repo,
		Wallet: wallet,
		Clock:  clock,
		Config: Config{DispenseAmount: 1000000, RateLimitWindow: time.Hour},
	}

	first := svc.Dispense(context.Background(), validAddr, "10.0.0.1")
	if first.Outcome != OutcomeSuccess {
		t.Fatalf("first Dispense Outcome = %v, want OutcomeSuccess", first.Outcome)
	}

	// Exactly at window expiry (base + window) the request should once
	// again be allowed: LastSuccessfulDispense's "since" boundary is
	// exclusive going forward in time from the perspective of a fresh
	// request one full window later.
	clock.now = base.Add(time.Hour + time.Second)
	third := svc.Dispense(context.Background(), validAddr, "10.0.0.1")
	if third.Outcome != OutcomeSuccess {
		t.Fatalf("Dispense after window expiry Outcome = %v, want OutcomeSuccess (err=%v)", third.Outcome, third.Err)
	}
}

func TestService_Dispense_WalletFailureIsRecordedAndReturnsError(t *testing.T) {
	validAddr := validTestnetAddress(t)
	repo := &fakeRepo{}
	wallet := &fakeWallet{sendErr: errors.New("grpc: connection refused")}
	svc := &Service{
		Repo:   repo,
		Wallet: wallet,
		Clock:  &fakeClock{now: time.Now()},
		Config: Config{DispenseAmount: 1000000, RateLimitWindow: time.Hour},
	}

	result := svc.Dispense(context.Background(), validAddr, "10.0.0.1")
	if result.Outcome != OutcomeError {
		t.Fatalf("Outcome = %v, want OutcomeError", result.Outcome)
	}
	if len(repo.recorded) != 1 || repo.recorded[0].Success {
		t.Fatalf("expected exactly one failed audit row, got %+v", repo.recorded)
	}
}

func TestService_Dispense_WalletRejectionIsRecordedAndReturnsError(t *testing.T) {
	validAddr := validTestnetAddress(t)
	repo := &fakeRepo{}
	wallet := &fakeWallet{sendResp: &tari_generated.TransferResponse{
		Results: []*tari_generated.TransferResult{
			{IsSuccess: false, FailureMessage: "insufficient funds"},
		},
	}}
	svc := &Service{
		Repo:   repo,
		Wallet: wallet,
		Clock:  &fakeClock{now: time.Now()},
		Config: Config{DispenseAmount: 1000000, RateLimitWindow: time.Hour},
	}

	result := svc.Dispense(context.Background(), validAddr, "10.0.0.1")
	if result.Outcome != OutcomeError {
		t.Fatalf("Outcome = %v, want OutcomeError", result.Outcome)
	}
	if repo.recorded[0].Error != "insufficient funds" {
		t.Fatalf("recorded error = %q, want %q", repo.recorded[0].Error, "insufficient funds")
	}
}

func TestService_Dispense_RepositoryLookupFailureDoesNotDispense(t *testing.T) {
	validAddr := validTestnetAddress(t)
	repo := &fakeRepo{lastErr: errors.New("connection reset")}
	wallet := &fakeWallet{sendResp: successResponse(1)}
	svc := &Service{
		Repo:   repo,
		Wallet: wallet,
		Clock:  &fakeClock{now: time.Now()},
		Config: Config{DispenseAmount: 1000000, RateLimitWindow: time.Hour},
	}

	result := svc.Dispense(context.Background(), validAddr, "10.0.0.1")
	if result.Outcome != OutcomeError {
		t.Fatalf("Outcome = %v, want OutcomeError", result.Outcome)
	}
	if len(wallet.sentRecipients) != 0 {
		t.Fatal("wallet must not be called when the rate-limit lookup itself fails")
	}
}

func TestService_HealthCheck(t *testing.T) {
	t.Run("healthy when both dependencies are up", func(t *testing.T) {
		svc := &Service{
			Repo: &fakeRepo{},
			Wallet: &fakeWallet{connectivity: &tari_generated.CheckConnectivityResponse{
				Status: tari_generated.CheckConnectivityResponse_Online,
			}},
		}
		if err := svc.HealthCheck(context.Background()); err != nil {
			t.Fatalf("HealthCheck() = %v, want nil", err)
		}
	})

	t.Run("unhealthy when postgres ping fails", func(t *testing.T) {
		svc := &Service{
			Repo: &fakeRepo{pingErr: errors.New("db down")},
			Wallet: &fakeWallet{connectivity: &tari_generated.CheckConnectivityResponse{
				Status: tari_generated.CheckConnectivityResponse_Online,
			}},
		}
		if err := svc.HealthCheck(context.Background()); err == nil {
			t.Fatal("HealthCheck() = nil, want an error when postgres is down")
		}
	})

	t.Run("unhealthy when wallet GRPC is unreachable", func(t *testing.T) {
		svc := &Service{
			Repo:   &fakeRepo{},
			Wallet: &fakeWallet{connectivityErr: errors.New("grpc: no connection")},
		}
		if err := svc.HealthCheck(context.Background()); err == nil {
			t.Fatal("HealthCheck() = nil, want an error when the wallet GRPC is down")
		}
	})

	t.Run("unhealthy when wallet reports offline", func(t *testing.T) {
		svc := &Service{
			Repo: &fakeRepo{},
			Wallet: &fakeWallet{connectivity: &tari_generated.CheckConnectivityResponse{
				Status: tari_generated.CheckConnectivityResponse_Offline,
			}},
		}
		if err := svc.HealthCheck(context.Background()); err == nil {
			t.Fatal("HealthCheck() = nil, want an error when the wallet reports Offline")
		}
	})
}

func TestService_CurrentBalance_ReturnsAvailableBalance(t *testing.T) {
	svc := &Service{
		Repo: &fakeRepo{},
		Wallet: &fakeWallet{balanceResp: &tari_generated.GetBalanceResponse{
			AvailableBalance: 1234567,
		}},
	}

	got, err := svc.CurrentBalance(context.Background())
	if err != nil {
		t.Fatalf("CurrentBalance() error = %v, want nil", err)
	}
	if got != 1234567 {
		t.Fatalf("CurrentBalance() = %d, want 1234567", got)
	}
}

func TestService_CurrentBalance_PropagatesWalletError(t *testing.T) {
	svc := &Service{
		Repo:   &fakeRepo{},
		Wallet: &fakeWallet{balanceErr: errors.New("grpc: unavailable")},
	}

	_, err := svc.CurrentBalance(context.Background())
	if err == nil {
		t.Fatal("CurrentBalance() error = nil, want an error when the wallet call fails")
	}
}

func TestService_CurrentBalance_ErrorsOnNilResponse(t *testing.T) {
	svc := &Service{
		Repo:   &fakeRepo{},
		Wallet: &fakeWallet{},
	}

	_, err := svc.CurrentBalance(context.Background())
	if err == nil {
		t.Fatal("CurrentBalance() error = nil, want an error when the wallet returns a nil response")
	}
}
