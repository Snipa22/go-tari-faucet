package sql

import (
	"context"
	"errors"
	core "github.com/Snipa22/core-go-lib/milieu"
)

// Manage all Batch related SQL requests, no logic, just query and structs
// Error management is lifted up and out despite access to sentry here.

// CreateNewBatch takes the transaction account and amount, and returns the ID for the batch for fkey work
func CreateNewBatch(milieu core.Milieu, txCount int, amount uint64) (int, error) {
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
