package faucet

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

// DispenseRecord is one row of the dispenses table -- an audit entry for a
// single dispense attempt, successful or not.
type DispenseRecord struct {
	ID        int64
	Address   string
	IP        string
	Amount    uint64
	Success   bool
	Error     string
	TxID      uint64
	CreatedAt time.Time
}

// Reservation is the result of ReserveDispense.
//
// If RateLimited is false, ID identifies the placeholder audit row
// ReserveDispense already inserted (success=false) -- the caller must
// follow up with exactly one FinalizeDispense(ctx, ID, ...) once the
// wallet call completes, to fill in the row's real outcome.
//
// If RateLimited is true, no row was inserted; LastSuccessAt is the
// timestamp ReserveDispense's rolling-window check found (mirroring the
// old LastSuccessfulDispense's return value), so callers can compute a
// retry-after time the same way they did before. Note this timestamp can
// come from either a confirmed successful dispense OR another request's
// still-in-flight reservation (see ReserveDispense's doc comment on why
// pending reservations must block too) -- in the latter case it's the
// reservation time, a close-enough approximation for a user-facing
// "try again after" message.
type Reservation struct {
	ID            int64
	RateLimited   bool
	LastSuccessAt time.Time
}

// Repository is the narrow persistence surface Service depends on, so tests
// can supply an in-memory fake instead of a live Postgres connection (same
// convention as go-crypto-pool/internal/backend/unlocker's Repository
// interface).
type Repository interface {
	// ReserveDispense atomically checks the rolling-window rate limit for
	// address and ip (whichever last dispensed successfully more
	// recently, at or after since) and, if neither is currently rate
	// limited, inserts a placeholder (success=false) audit row and
	// returns its ID -- all within a single transaction serialized by a
	// Postgres advisory lock, so two concurrent calls for the same
	// address or ip can never both pass the check before either's
	// reservation row is visible to the other (the TOCTOU race the
	// advisory lock exists to close). See PGRepository.ReserveDispense
	// for the exact locking mechanism.
	//
	// The returned Reservation's ID is only valid (non-zero) when
	// RateLimited is false; callers must not call FinalizeDispense at
	// all when RateLimited is true (no row was inserted for it to
	// finalize).
	ReserveDispense(ctx context.Context, address, ip string, amount uint64, since, now time.Time) (Reservation, error)
	// FinalizeDispense updates the placeholder row ReserveDispense
	// inserted (identified by id) with the wallet call's real outcome.
	// It must be called at most once per successful ReserveDispense
	// call, after the wallet RPC completes -- deliberately outside the
	// advisory-lock transaction, so a slow wallet call never holds the
	// lock and serializes unrelated requests behind it.
	FinalizeDispense(ctx context.Context, id int64, success bool, errMsg string, txID uint64) error
	// Ping verifies the underlying database connection is reachable.
	Ping(ctx context.Context) error
}

// PGRepository is the production Repository, backed by a pgx connection
// pool (the same milieu.GetRawPGXPool() pool payoutDaemon's sql package
// uses, just wrapped behind the narrower Repository interface here).
type PGRepository struct {
	pool *pgxpool.Pool
}

// NewPGRepository builds a PGRepository around an already-connected pool.
func NewPGRepository(pool *pgxpool.Pool) *PGRepository {
	return &PGRepository{pool: pool}
}

// ReserveDispense is the atomic check-and-reserve step that closes the
// TOCTOU race between the old LastSuccessfulDispense (a plain SELECT) and
// RecordDispense (a plain INSERT after the wallet call): with those as two
// separate, unsynchronized round-trips, N concurrent requests for the same
// address/ip could all observe "not rate limited" before any of their
// audit rows existed for the others to see, and all N would dispense.
//
// This method instead does the whole check-then-reserve inside one
// transaction, serialized by a Postgres session-level advisory lock keyed
// on address and on ip (pg_advisory_xact_lock(hashtext(...))), so a second
// concurrent call for the same address or ip blocks until the first
// call's transaction (and thus its lock) is released -- and by then the
// first call's placeholder row is already committed and visible, so the
// second call's rate-limit re-check inside its own lock sees it and
// correctly reports RateLimited.
//
// The two advisory locks (address, ip) are always acquired in an order
// derived purely from the pair of raw strings involved -- the smaller of
// the two (by Go string comparison) first -- never in "address always
// before ip" order. That matters because two concurrent requests can have
// their address/ip roles swapped (request A: address=X ip=Y; request B:
// address=Y ip=X); locking in "address first" order would then have A
// waiting on ip=Y's lock while holding address=X's, and B waiting on
// address=Y's lock while holding ip=X's -- a classic deadlock. Sorting by
// the string pair itself instead of by field role means both requests
// agree on which of {X, Y} gets locked first regardless of which one each
// happens to call "address", so there is no circular wait.
//
// The transaction commits (releasing the advisory lock) immediately after
// the placeholder row is inserted -- it does NOT stay open for the wallet
// RPC. Holding the lock across a slow wallet call would serialize every
// unrelated request behind whichever one is currently mid-wallet-call,
// which is an unacceptable latency regression; only the check+reserve
// step itself needs to be atomic. See FinalizeDispense for the follow-up
// update, made after the wallet call, with no lock held.
func (r *PGRepository) ReserveDispense(ctx context.Context, address, ip string, amount uint64, since, now time.Time) (Reservation, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return Reservation{}, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()

	first, second := address, ip
	if first > second {
		first, second = second, first
	}
	if _, err := tx.Exec(ctx, "select pg_advisory_xact_lock(hashtext($1))", first); err != nil {
		return Reservation{}, err
	}
	if second != first {
		if _, err := tx.Exec(ctx, "select pg_advisory_xact_lock(hashtext($1))", second); err != nil {
			return Reservation{}, err
		}
	}

	// Almost the same rolling-window rate-limit query
	// LastSuccessfulDispense used, with one deliberate addition:
	// `or error is null`. `error` is only ever NULL for a row that
	// hasn't been finalized yet (FinalizeDispense always writes a
	// literal string -- possibly "" for success, never NULL -- see its
	// doc comment); a finalized failure has a non-null error and
	// (correctly) stops blocking future requests, same as before, but
	// a still-*pending* reservation (another Dispense call's wallet
	// RPC is in flight right now) must ALSO block concurrent callers,
	// even though its success column is still false. Without this,
	// this whole method degenerates back into the original bug: two
	// concurrent callers would each insert their own placeholder and
	// neither would ever see the other's, because neither's `success`
	// is true yet. Each half stays unioned separately so each still
	// hits its own trailing-created_at index (see migrations/
	// tables.sql) instead of table-scanning.
	const lookupQuery = `
(select created_at from dispenses where (success or error is null) and address = $1 and created_at >= $3 order by created_at desc limit 1)
union all
(select created_at from dispenses where (success or error is null) and ip = $2 and created_at >= $3 order by created_at desc limit 1)
order by created_at desc
limit 1`
	var last time.Time
	scanErr := tx.QueryRow(ctx, lookupQuery, address, ip, since).Scan(&last)
	if scanErr != nil && !errors.Is(scanErr, pgx.ErrNoRows) {
		return Reservation{}, scanErr
	}
	if scanErr == nil {
		// Rate limited: commit (there's nothing to roll back -- this
		// transaction never wrote anything) purely to release the
		// advisory lock promptly rather than waiting on Rollback.
		if err := tx.Commit(ctx); err != nil {
			return Reservation{}, err
		}
		committed = true
		return Reservation{RateLimited: true, LastSuccessAt: last}, nil
	}

	const insertQuery = `
insert into dispenses (address, ip, amount, success, error, created_at)
values ($1, $2, $3, false, null, $4)
returning id`
	var id int64
	if err := tx.QueryRow(ctx, insertQuery, address, ip, amount, now).Scan(&id); err != nil {
		return Reservation{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Reservation{}, err
	}
	committed = true
	return Reservation{ID: id}, nil
}

// FinalizeDispense updates the placeholder row ReserveDispense inserted
// (by id) with the wallet call's real outcome. Deliberately a plain,
// lock-free UPDATE -- by this point the row already exists and is only
// ever touched by the one Dispense call that reserved it, so there is no
// concurrent-access hazard here to guard against.
//
// error is always written as a literal string -- "" for a successful
// dispense, never NULL -- specifically so ReserveDispense's "or error is
// null" clause can treat "error IS NULL" as an unambiguous "still
// pending, not yet finalized" signal (see its doc comment). Writing NULL
// here for the success case would make a finalized success
// indistinguishable from a still-in-flight reservation.
func (r *PGRepository) FinalizeDispense(ctx context.Context, id int64, success bool, errMsg string, txID uint64) error {
	const query = `update dispenses set success = $2, error = $3, tx_id = $4 where id = $1`
	_, err := r.pool.Exec(ctx, query, id, success, errMsg, txID)
	return err
}

// Ping runs a trivial round-trip query against Postgres for /healthz.
func (r *PGRepository) Ping(ctx context.Context) error {
	var one int
	return r.pool.QueryRow(ctx, "select 1").Scan(&one)
}
