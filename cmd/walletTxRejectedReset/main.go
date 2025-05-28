package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"github.com/Snipa22/core-go-lib/helpers"
	core "github.com/Snipa22/core-go-lib/milieu"
	sql2 "github.com/Snipa22/go-tari-faucet/cmd/payoutDaemon/sql"
	"github.com/Snipa22/go-tari-grpc-lib/v2/walletGRPC"
	_ "github.com/mattn/go-sqlite3"
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
	walletSqliteDBPtr := flag.String("wallet-sqlite-db", "", "Path to the source tari wallet sqlite DB")
	flag.Parse()
	walletGRPC.InitWalletGRPC(*walletGRPCAddressPtr)

	if *walletSqliteDBPtr == "" {
		milieu.Fatal("No wallet sqlite DB provided")
	}

	sqliteDSN := fmt.Sprintf("file:%s?cache=shared&mode=ro", *walletSqliteDBPtr)
	db, err := sql.Open("sqlite3", sqliteDSN)
	if err != nil {
		milieu.CaptureException(err)
		milieu.Fatal(err.Error())
	}
	defer db.Close()
	rows, err := db.Query("select tx_id from completed_transactions where status = 7")
	if err != nil {
		milieu.CaptureException(err)
		milieu.Fatal(err.Error())
	}
	defer rows.Close()
	for rows.Next() {
		var txID uint64
		err = rows.Scan(&txID)
		if err != nil {
			milieu.CaptureException(err)
			milieu.Info(err.Error())
			continue
		}
		psqlRow := milieu.GetRawPGXPool().QueryRow(context.Background(), "select success, amount, balance_id from transactions where id = $1", txID)
		if psqlRow == nil {
			milieu.CaptureException(fmt.Errorf("no transaction found with id %d", txID))
			milieu.Info(fmt.Sprintf("No transaction found with id %d", txID))
			continue
		}
		success := false
		var amount, balanceID uint64
		err = psqlRow.Scan(&success, &amount, &balanceID)
		if err != nil {
			milieu.CaptureException(err)
			milieu.Info(err.Error())
			continue
		}
		if !success {
			milieu.Info(fmt.Sprintf("transaction found with id %d, but it's been procesed, skipping", txID))
			continue
		}
		milieu.Info(fmt.Sprintf("transaction found with id %d to increase balance, doing so, and locking old txn", txID))
		txn, err := milieu.GetTransaction()
		if err != nil {
			milieu.CaptureException(err)
			milieu.Info(err.Error())
			continue
		}
		defer milieu.CleanupTxn()
		err = sql2.IncreaseBalance(txn, balanceID, amount)
		if err != nil {
			milieu.CaptureException(err)
			milieu.Info(err.Error())
			milieu.CleanupTxn()
			continue
		}
		_, err = txn.Exec(context.Background(), "update transactions set success = false, error = 'Transaction detected as double-spend, increased balance' where id = $1", txID)
		if err != nil {
			milieu.CaptureException(err)
			milieu.Info(err.Error())
			milieu.CleanupTxn()
			continue
		}
		txn.Commit(context.Background())
		milieu.Info(fmt.Sprintf("processed txn ID %d and incremented balance for %v by %v", txID, balanceID, amount))
	}
	db.Close()
}
