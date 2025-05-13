package sql

import (
	"context"
	"errors"
	core "github.com/Snipa22/core-go-lib/milieu"
)

// Manage all Batch related SQL requests, no logic, just query and structs
// Error management is lifted up and out despite access to sentry here.

// CreateNewBatch takes the transaction account and amount, and returns the ID for the batch for fkey work
func CreateNewBatch(milieu *core.Milieu, txCount int, amount uint64) (int, error) {
	row := milieu.GetRawPGXPool().QueryRow(context.Background(), "insert into payment_batch (count, amount) values ($1, $2) returning id", txCount, amount)
	if row == nil {
		return 0, errors.New("unable to create new batch")
	}
	var id int
	err := row.Scan(&id)
	if err != nil {
		return 0, err
	}
	return id, nil
}

// UpdateBatchAmounts sets the amounts success/failed
func UpdateBatchAmounts(milieu *core.Milieu, batchID int, successAmount uint64, failedAmount uint64) error {
	_, err := milieu.GetRawPGXPool().Exec(context.Background(), "update payment_batch set amount_success = $1, amount_fail = $2 where id = $3", successAmount, failedAmount, batchID)
	return err
}
