package sql

import (
	"context"
	"errors"
	core "github.com/Snipa22/core-go-lib/milieu"
	"github.com/jackc/pgx/v4"
	"time"
)

// Manage all Balance related SQL requests, no logic, just query and structs
// Error management is lifted up and out despite access to sentry here.

type BalanceSqlRow struct {
	ID                   uint64
	DateAdded            time.Time
	DateBalanceIncreased time.Time
	DateLastUpdated      time.Time
	Balance              uint64
	Valid                bool
	Address              string
	PayoutMinimum        uint64
}

func GetAllBalances(milieu *core.Milieu) ([]BalanceSqlRow, error) {
	rows, err := milieu.GetRawPGXPool().Query(context.Background(), "select id, date_added, date_balance_increased, date_last_updated, balance, valid, address, payout_minimum from balances order by balance desc")
	if err != nil {
		return nil, err
	}
	result := make([]BalanceSqlRow, 0)
	for rows.Next() {
		var id, balance, payoutMinimum uint64
		var valid bool
		var address string
		var dateAdded, dateBalanceIncreased, dateLastUpdated time.Time
		if err = rows.Scan(
			&id, &dateAdded, &dateBalanceIncreased, &dateLastUpdated, &balance, &valid, &address, &payoutMinimum,
		); err != nil {
			// If we can't decode a row, we need to report it, but continue, lift it to Sentry though.
			milieu.Info(err.Error())
			milieu.CaptureException(err)
			continue
		}
		result = append(result, BalanceSqlRow{
			ID:                   id,
			DateAdded:            dateAdded,
			DateBalanceIncreased: dateBalanceIncreased,
			DateLastUpdated:      dateLastUpdated,
			Balance:              balance,
			Valid:                valid,
			Address:              address,
			PayoutMinimum:        payoutMinimum,
		})
	}
	return result, nil
}

func GetBalanceIDByAddress(milieu *core.Milieu, address string) (uint64, error) {
	row := milieu.GetRawPGXPool().QueryRow(context.Background(), "select id from balances where address = $1", address)
	if row == nil {
		return 0, errors.New("balance not found")
	}
	var id uint64
	err := row.Scan(&id)
	if err != nil {
		return 0, err
	}
	return id, nil
}

func DecreaseBalance(txn pgx.Tx, balanceID uint64, amount uint64) error {
	_, err := txn.Exec(context.Background(), "update balances set balance = balance - $1, date_last_updated = now() where id = $2", amount, balanceID)
	return err
}

func IncreaseBalance(txn pgx.Tx, balanceID uint64, amount uint64) error {
	_, err := txn.Exec(context.Background(), "update balances set balance = balance + $1, date_last_updated = now(), date_balance_increased = now() where id = $2", amount, balanceID)
	return err
}
