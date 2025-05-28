package sql

import (
	"context"
	"github.com/jackc/pgx/v4"
)

func CreateNewTransaction(psqlTx pgx.Tx, txID uint64, success bool, errorString string, balanceID uint64, batchID int, amount uint64) error {
	_, err := psqlTx.Exec(context.Background(), "insert into transactions (id, success, error, balance_id, batch_id, amount) values ($1, $2, $3, $4, $5, $6) ON CONFLICT ON CONSTRAINT transactions_pk DO UPDATE SET success = $1, error = $2, balance_id = $3, batch_id = $4, amount = $5", txID, success, errorString, balanceID, batchID, amount)
	return err
}
