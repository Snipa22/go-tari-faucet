package sql

import (
	"context"
	core "github.com/Snipa22/core-go-lib/milieu"
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
	rows, err := milieu.GetRawPGXPool().Query(context.Background(), "select id, date_added, date_balance_increased, date_last_updated, balance, valid, address, payout_minimum from balances")
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
