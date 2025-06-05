package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/Snipa22/core-go-lib/helpers"
	core "github.com/Snipa22/core-go-lib/milieu"
	"github.com/Snipa22/go-tari-faucet/cmd/payoutDaemon/sql"
	"github.com/Snipa22/go-tari-grpc-lib/v2/tari_generated"
	"github.com/Snipa22/go-tari-grpc-lib/v2/walletGRPC"
	"github.com/jackc/pgx/v4"
)

func main() {
	psqlURL := helpers.GetEnv("PSQL_SERVER", "postgres://postgres@localhost/postgres?sslmode=disable")
	sentryURI := helpers.GetEnv("SENTRY_SERVER", "")

	// Build Milieu
	milieu, err := core.NewMilieu(&psqlURL, nil, &sentryURI)
	if err != nil {
		milieu.CaptureException(err)
		milieu.Fatal(err.Error())
	}

	walletGRPCAddressPtr := flag.String("wallet-grpc-address", "127.0.0.1:18143", "Tari wallet GRPC address")
	flag.Parse()
	walletGRPC.InitWalletGRPC(*walletGRPCAddressPtr)

	walletTransactions, err := walletGRPC.GetTransactionsInBlock(0)
	if err != nil {
		milieu.CaptureException(err)
		milieu.Fatal(err.Error())
	}

	rows, err := milieu.GetRawPGXPool().Query(context.Background(), "select id from transactions where success is true")
	if err != nil {
		milieu.CaptureException(err)
		milieu.Fatal(err.Error())
	}
	defer rows.Close()
	idList := make([]uint64, 0)
	for rows.Next() {
		var id uint64
		if err = rows.Scan(&id); err != nil {
			milieu.CaptureException(err)
			milieu.Info(err.Error())
			continue
		}
		idList = append(idList, id)
	}

	txnToBackfill := make([]uint64, 0)
	for _, id := range idList {
		row := milieu.GetRawPGXPool().QueryRow(context.Background(), "select id from transaction_details where id in ($1)", id)
		idScan := new(uint64)
		if err := row.Scan(&idScan); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				txnToBackfill = append(txnToBackfill, id)
				continue
			}
			milieu.CaptureException(err)
			milieu.Fatal(err.Error())
		}
	}

	fmt.Printf("Backfilling %d/%d transactions, with %d from the wallet\n", len(txnToBackfill), len(idList), len(walletTransactions))

	for _, id := range txnToBackfill {
		var txnData *tari_generated.TransactionInfo
		for _, txn := range walletTransactions {
			if txn.TxId == id {
				txnData = txn
				break
			}
		}
		if txnData == nil {
			continue
		}
		if err = sql.CreateTransactionDetail(milieu, txnData); err != nil {
			milieu.CaptureException(err)
			milieu.Info(err.Error())
			continue
		}
	}
}
