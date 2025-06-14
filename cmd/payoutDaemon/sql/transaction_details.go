package sql

import (
	"context"
	core "github.com/Snipa22/core-go-lib/milieu"
	"github.com/Snipa22/go-tari-grpc-lib/v2/tari_generated"
	"time"
)

type TransactionDetail struct {
	ID            uint64
	Status        uint64
	Amount        uint64
	Fee           uint64
	IsCancelled   bool
	ExcessSig     []byte
	Timestamp     time.Time
	RawPaymentID  []byte
	MinedAtHeight uint64
	UserPaymentID []byte
	DestAddress   []byte
	Rechecked     bool
	Repaid        bool
}

func CreateTransactionDetail(milieu *core.Milieu, txnDetail *tari_generated.TransactionInfo) error {
	_, err := milieu.GetRawPGXPool().Exec(context.Background(),
		"insert into transaction_details (id, status, amount, fee, is_cancelled, excess_sig, timestamp, raw_payment_id, mined_at_height, user_payment_id, dest_address, rechecked, repaid) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, false, false)",
		txnDetail.TxId, txnDetail.Status.Number(), txnDetail.Amount, txnDetail.Fee, txnDetail.IsCancelled, txnDetail.ExcessSig, time.Unix(int64(txnDetail.Timestamp), 0), txnDetail.RawPaymentId,
		txnDetail.MinedInBlockHeight, txnDetail.UserPaymentId, txnDetail.DestAddress)
	return err
}

func UpdateMinedAtHeight(milieu *core.Milieu, txnDetail *tari_generated.TransactionInfo) error {
	_, err := milieu.GetRawPGXPool().Exec(context.Background(), "update transaction_details set mined_at_height = $1 where id = $2", txnDetail.MinedInBlockHeight, txnDetail.TxId)
	return err
}
