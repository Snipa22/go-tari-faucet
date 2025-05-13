package sql

import (
	"context"
	"github.com/jackc/pgx/v4"
)

func CreateNewTransaction(psqlTx pgx.Tx, txID uint64, success bool, errorString string, balanceID uint64, batchID int) error {
	_, err := psqlTx.Exec(context.Background(), "insert into transactions (id, success, error, balance_id, batch_id) values ($1, $2, $3, $4, $5)", txID, success, errorString, balanceID, batchID)
	return err
}
