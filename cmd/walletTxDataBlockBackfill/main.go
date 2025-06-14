package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/Snipa22/core-go-lib/helpers"
	core "github.com/Snipa22/core-go-lib/milieu"
	"github.com/Snipa22/go-tari-faucet/cmd/payoutDaemon/sql"
	"github.com/Snipa22/go-tari-grpc-lib/v2/tari_generated"
	"github.com/Snipa22/go-tari-grpc-lib/v2/walletGRPC"
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

	rows, err := milieu.GetRawPGXPool().Query(context.Background(), "select id from transaction_details where mined_at_height = 0")
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

	fmt.Printf("Backfilling %d transactions with mined_at_height, with %d from the wallet\n", len(idList), len(walletTransactions))

	for _, id := range idList {
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
		if err = sql.UpdateMinedAtHeight(milieu, txnData); err != nil {
			milieu.CaptureException(err)
			milieu.Info(err.Error())
			continue
		}
	}
}
