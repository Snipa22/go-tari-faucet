package faucet

// Integration tests against a real Postgres instance, proving
// PGRepository.ReserveDispense's advisory-lock atomicity end-to-end --
// this is the authoritative regression test for the live incident (5
// concurrent requests for the same address all dispensing) that
// TestService_Dispense_ConcurrentSameAddressAndIP in service_test.go can
// only approximate with an in-memory fake.
//
// They are deliberately NOT run by default: `go test ./...` in a sandbox
// with no Postgres available must not fail. Set FAUCET_TEST_DSN to a
// Postgres connection string (pointing at a scratch/throwaway database --
// this file DROPs and recreates the `dispenses` table) to run them, e.g.:
//
//	export FAUCET_TEST_DSN="postgres://postgres:postgres@127.0.0.1:5432/tari_faucet_test?sslmode=disable"
//	go test ./internal/faucet/... -run Integration -race -v
//
// Verified manually in the implementing sandbox against a local
// Postgres 17 instance (see the PR description for the actual command
// transcript). CI/dev environments without Postgres will skip these
// tests (not fail them) -- same convention go-crypto-pool's
// internal/backend/db/integration_test.go uses for the same reason.
import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
)

// testPGPool connects to FAUCET_TEST_DSN, skipping the test entirely if
// it isn't set (rather than failing -- see package doc above).
func testPGPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("FAUCET_TEST_DSN")
	if dsn == "" {
		t.Skip("FAUCET_TEST_DSN not set; skipping Postgres integration test (see package doc)")
	}
	ctx := context.Background()
	pool, err := pgxpool.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.Connect(%q): %v", dsn, err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// resetAndApplySchema drops the dispenses table if present, then
// recreates it (and its indexes) by executing the real, unmodified
// migrations/tables.sql this service ships with -- not a hand-rolled
// approximation of the schema, so this test genuinely exercises the
// same table/index shape production runs against.
func resetAndApplySchema(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	if _, err := pool.Exec(ctx, "drop table if exists dispenses cascade"); err != nil {
		t.Fatalf("dropping dispenses table: %v", err)
	}
	schema, err := os.ReadFile("../../migrations/tables.sql")
	if err != nil {
		t.Fatalf("reading migrations/tables.sql: %v", err)
	}
	if _, err := pool.Exec(ctx, string(schema)); err != nil {
		t.Fatalf("applying migrations/tables.sql: %v", err)
	}
}

// TestIntegrationReserveDispense_ConcurrentSameAddressAndIP is the
// direct reproduction (and regression test) of the live incident this
// branch fixes: N concurrent HTTP requests -- modeled here as N
// concurrent Service.Dispense calls -- for the exact same address and
// exact same ip, racing against a REAL Postgres connection running the
// real migrations/tables.sql schema. Before this fix, PGRepository's
// separate, unsynchronized LastSuccessfulDispense SELECT + RecordDispense
// INSERT let all N pass the check before any audit row existed for the
// others to see, so all N dispensed. With ReserveDispense's advisory
// lock, exactly one must win.
func TestIntegrationReserveDispense_ConcurrentSameAddressAndIP(t *testing.T) {
	pool := testPGPool(t)
	resetAndApplySchema(t, pool)

	validAddr := validTestnetAddress(t)
	repo := NewPGRepository(pool)
	wallet := &fakeWallet{sendResp: successResponse(99)}
	svc := &Service{
		Repo:   repo,
		Wallet: wallet,
		Clock:  RealClock{},
		Config: Config{DispenseAmount: 1000000, RateLimitWindow: time.Hour},
	}

	const n = 20
	results := make([]Result, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			results[i] = svc.Dispense(context.Background(), validAddr, "203.0.113.42")
		}(i)
	}
	wg.Wait()

	var successes, rateLimited, other int
	for _, r := range results {
		switch r.Outcome {
		case OutcomeSuccess:
			successes++
		case OutcomeRateLimited:
			rateLimited++
		default:
			other++
			t.Errorf("unexpected Outcome %v (err=%v) among %d concurrent real-Postgres calls", r.Outcome, r.Err, n)
		}
	}
	if successes != 1 {
		t.Errorf("got %d OutcomeSuccess results across %d concurrent real-Postgres calls, want exactly 1 (this is the live incident's exact bug if it regresses: 500 XTM sent instead of 100)", successes, n)
	}
	if rateLimited != n-1 {
		t.Errorf("got %d OutcomeRateLimited results, want exactly %d", rateLimited, n-1)
	}
	if other != 0 {
		t.Errorf("got %d unexpected (non-success/non-rate-limited) outcomes, want 0", other)
	}
	if len(wallet.sentRecipients) != 1 {
		t.Errorf("wallet.SendTransactions called %d times, want exactly 1 (only the winning reservation should ever reach the wallet)", len(wallet.sentRecipients))
	}

	// Cross-check directly against the table: only the one winning
	// call ever inserts a placeholder row at all (every rate-limited
	// call returns before ever touching the INSERT), so there must be
	// exactly one row for this address total, and it must be the
	// success=true one.
	var totalRows, successRows int
	if err := pool.QueryRow(context.Background(), "select count(*), count(*) filter (where success) from dispenses where address = $1", validAddr).Scan(&totalRows, &successRows); err != nil {
		t.Fatalf("querying dispenses table directly: %v", err)
	}
	if totalRows != 1 {
		t.Errorf("dispenses table has %d rows for this address, want exactly 1 (only the winning reservation should ever insert a row)", totalRows)
	}
	if successRows != 1 {
		t.Errorf("dispenses table has %d success=true rows for this address, want exactly 1", successRows)
	}
}

// TestIntegrationReserveDispense_FailedDispenseAllowsImmediateRetry
// proves ReserveDispense's "or error is null" check correctly stops
// blocking once a reservation is finalized as a FAILURE (as opposed to
// a success, which should keep blocking for the full window): a wallet
// error must not permanently -- or even temporarily -- lock a real user
// out of retrying, only a genuine success should.
func TestIntegrationReserveDispense_FailedDispenseAllowsImmediateRetry(t *testing.T) {
	pool := testPGPool(t)
	resetAndApplySchema(t, pool)

	validAddr := validTestnetAddress(t)
	repo := NewPGRepository(pool)
	failingWallet := &fakeWallet{sendErr: context.DeadlineExceeded}
	svc := &Service{
		Repo:   repo,
		Wallet: failingWallet,
		Clock:  RealClock{},
		Config: Config{DispenseAmount: 1000000, RateLimitWindow: time.Hour},
	}

	first := svc.Dispense(context.Background(), validAddr, "203.0.113.99")
	if first.Outcome != OutcomeError {
		t.Fatalf("first Dispense (failing wallet) Outcome = %v, want OutcomeError", first.Outcome)
	}

	succeedingWallet := &fakeWallet{sendResp: successResponse(1)}
	svc.Wallet = succeedingWallet
	second := svc.Dispense(context.Background(), validAddr, "203.0.113.99")
	if second.Outcome != OutcomeSuccess {
		t.Fatalf("second Dispense (same address/ip, after a FAILED first attempt) Outcome = %v, want OutcomeSuccess -- a failed dispense must not rate-limit the retry", second.Outcome)
	}
}

// TestIntegrationReserveDispense_NoDeadlockWithSwappedAddressIPRoles is
// the direct regression test for the deadlock-avoidance requirement in
// ReserveDispense's lock-ordering doc comment: two distinct
// address/ip pairs, concurrently hammered with their roles swapped
// (goroutine group A: address=X ip=Y; group B: address=Y ip=X), must
// never deadlock against each other's advisory locks. If the lock
// order were naively "always address's lock first, then ip's", this
// would deadlock. A bounded overall timeout turns a real deadlock into
// a clear test failure instead of an indefinite hang.
func TestIntegrationReserveDispense_NoDeadlockWithSwappedAddressIPRoles(t *testing.T) {
	pool := testPGPool(t)
	resetAndApplySchema(t, pool)

	addrX := validTestnetAddress(t)
	addrY := validTestnetAddress(t)
	repo := NewPGRepository(pool)

	svcFor := func(w WalletClient) *Service {
		return &Service{
			Repo:   repo,
			Wallet: w,
			Clock:  RealClock{},
			Config: Config{DispenseAmount: 1000000, RateLimitWindow: time.Millisecond}, // short window: lets repeated iterations re-dispense instead of just rate-limiting after the first
		}
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		var wg sync.WaitGroup
		const iterations = 100
		wg.Add(2)
		go func() {
			defer wg.Done()
			svc := svcFor(&fakeWallet{sendResp: successResponse(1)})
			for i := 0; i < iterations; i++ {
				svc.Dispense(context.Background(), addrX, addrY) // address=X, ip=Y
			}
		}()
		go func() {
			defer wg.Done()
			svc := svcFor(&fakeWallet{sendResp: successResponse(1)})
			for i := 0; i < iterations; i++ {
				svc.Dispense(context.Background(), addrY, addrX) // address=Y, ip=X -- swapped roles
			}
		}()
		wg.Wait()
	}()

	select {
	case <-done:
		// no deadlock
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for swapped-role concurrent Dispense calls to finish -- likely a deadlock in ReserveDispense's advisory-lock ordering")
	}
}
