package sql

import (
	"context"
	"github.com/jackc/pgx/v4"
)

func CreateNewTransaction(psqlTx pgx.Tx, txID uint64, success bool, errorString string, balanceID uint64, batchID int, amount uint64) error {
	_, err := psqlTx.Exec(context.Background(), "insert into transactions (id, success, error, balance_id, batch_id, amount) values ($1, $2, $3, $4, $5, $6) ON CONFLICT ON CONSTRAINT transactions_pk DO UPDATE SET success = $2, error = $3, balance_id = $4, batch_id = $5, amount = $6", txID, success, errorString, balanceID, batchID, amount)
	return err
}
