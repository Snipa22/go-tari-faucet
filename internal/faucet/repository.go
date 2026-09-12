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
	Address   string
	IP        string
	Amount    uint64
	Success   bool
	Error     string
	TxID      uint64
	CreatedAt time.Time
}

// Repository is the narrow persistence surface Service depends on, so tests
// can supply an in-memory fake instead of a live Postgres connection (same
// convention as go-crypto-pool/internal/backend/unlocker's Repository
// interface).
type Repository interface {
	// LastSuccessfulDispense returns the most recent successful dispense
	// timestamp for either address or ip, whichever is more recent, at or
	// after since. ok is false if neither has dispensed successfully in
	// that window.
	LastSuccessfulDispense(ctx context.Context, address, ip string, since time.Time) (t time.Time, ok bool, err error)
	// RecordDispense inserts an audit row for a dispense attempt.
	RecordDispense(ctx context.Context, rec DispenseRecord) error
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

// LastSuccessfulDispense looks up the most recent successful dispense for
// address and for ip independently (each via its own trailing-created_at
// index, see migrations/tables.sql), then returns whichever is more recent
// so the rate-limit check never has to table-scan.
func (r *PGRepository) LastSuccessfulDispense(ctx context.Context, address, ip string, since time.Time) (time.Time, bool, error) {
	const query = `
(select created_at from dispenses where success and address = $1 and created_at >= $3 order by created_at desc limit 1)
union all
(select created_at from dispenses where success and ip = $2 and created_at >= $3 order by created_at desc limit 1)
order by created_at desc
limit 1`
	row := r.pool.QueryRow(ctx, query, address, ip, since)
	var t time.Time
	if err := row.Scan(&t); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return time.Time{}, false, nil
		}
		return time.Time{}, false, err
	}
	return t, true, nil
}

// RecordDispense inserts an audit row for a dispense attempt.
func (r *PGRepository) RecordDispense(ctx context.Context, rec DispenseRecord) error {
	const query = `
insert into dispenses (address, ip, amount, success, error, tx_id, created_at)
values ($1, $2, $3, $4, $5, $6, $7)`
	var errCol *string
	if rec.Error != "" {
		errCol = &rec.Error
	}
	_, err := r.pool.Exec(ctx, query, rec.Address, rec.IP, rec.Amount, rec.Success, errCol, rec.TxID, rec.CreatedAt)
	return err
}

// Ping runs a trivial round-trip query against Postgres for /healthz.
func (r *PGRepository) Ping(ctx context.Context) error {
	var one int
	return r.pool.QueryRow(ctx, "select 1").Scan(&one)
}
