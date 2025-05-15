package main

import (
	"context"
	"flag"
	"fmt"
	core "github.com/Snipa22/core-go-lib/milieu"
	"github.com/Snipa22/go-tari-faucet/cmd/payoutDaemon/sql"
	"github.com/Snipa22/go-tari-grpc-lib/v2/tari_generated"
	"github.com/Snipa22/go-tari-grpc-lib/v2/walletGRPC"
	"github.com/robfig/cron/v3"
	"github.com/sirupsen/logrus"
	"math/rand"
	"os"
)

/* payoutDaemon does the following steps, on a cron schedule set by a flag, or on the hour by default:

Scans the `balances` postgresql table to build a list of valid payouts - this uses redis to check the full balance list
	for a bypass for an address in case it should be paid out still, using the key `bal_bypass_<address` with a val of 1
This gets compiled into transaction objects, which is submitted to `walletGRPCAddress`
	We'll go into a PSQL txn state at this time, then do the following:
	1. Add the transfer to `transfers`
	2-fail. Then we'll commit the txn and continue
	2-success. Subtract the balance of the transfer from `balances`
	3. Unset the redis key.
	4. Add data to the `payments` struct so we can log it to the `payment_batch` table
	5. Commit the txn.
Once the above is processed for every TXN, we'll go into the payments struct and commit it to the `payments` table, then
	sleep until the next cron pass

payoutDaemon is /not/ designed to perform any additional GRPC calls/etc, it is /very/ light and dedicated exclusively to
	transactions.  Check grpcWalletData for a more generic set of interfaces
*/

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func performPayouts(milieu *core.Milieu) {
	milieu.Info("Starting payouts")

	milieu.Debug("Starting balance fetch")
	balances, err := sql.GetAllBalances(milieu)
	if err != nil {
		milieu.Info(err.Error())
		milieu.CaptureException(err)
		return
	}
	if len(balances) == 0 {
		milieu.Info("No balances found, exiting run")
		return
	}
	milieu.Info(fmt.Sprintf("%v balances found", len(balances)))

	// Cache the address -> ID map for later use, as well as the address -> amount map
	addressCache := make(map[string]uint64)
	balanceCache := make(map[string]uint64)

	// With balances found, lets start the real processing
	payments := make([]*tari_generated.PaymentRecipient, 0)
	var totalAmount uint64 = 0
	for _, sqlBalance := range balances {
		milieu.Debug(fmt.Sprintf("Starting payout check for %v", sqlBalance.ID))
		if sqlBalance.Balance < sqlBalance.PayoutMinimum {
			// Check to see if there's a bypass in redis
			client := milieu.GetRedis()
			val := client.Exists(context.Background(), fmt.Sprintf("bal_bypass_%v", sqlBalance.Address))
			if val.Val() == 0 {
				milieu.Debug(fmt.Sprintf("Balance for %v does not get a bypass and is under payout minimum, "+
					"skipping", sqlBalance.ID))
				continue
			}
		}
		milieu.Debug(fmt.Sprintf("Adding %v to payment ready for %v", sqlBalance.ID, sqlBalance.Balance))
		totalAmount += sqlBalance.Balance
		payments = append(payments, &tari_generated.PaymentRecipient{
			Address:     sqlBalance.Address,
			Amount:      sqlBalance.Balance,
			FeePerGram:  5,
			PaymentType: 2,
			PaymentId:   nil,
		})
		addressCache[sqlBalance.Address] = sqlBalance.ID
		balanceCache[sqlBalance.Address] = sqlBalance.Balance
	}
	if len(payments) == 0 {
		milieu.Debug(fmt.Sprintf("No payments found, exiting run"))
		return
	}
	milieu.Info(fmt.Sprintf("%v/%v payments prepared for %v, inserting batch data", len(payments), len(balances), totalAmount))

	batchID, err := sql.CreateNewBatch(milieu, len(payments), totalAmount)
	if err != nil {
		milieu.CaptureException(err)
		milieu.Info(err.Error())
		return
	}

	milieu.Info(fmt.Sprintf("Batch ID: %v, starting txn send", batchID))

	txResults, err := walletGRPC.SendTransactions(payments)
	if err != nil {
		milieu.CaptureException(err)
		milieu.Info(err.Error())
		return
	}
	var successAmount uint64 = 0
	var failedAmount uint64 = 0

	milieu.Debug("Processing transaction results")
	for _, v := range txResults.GetResults() {
		// Each result needs to be handled cleanly
		milieu.Debug(fmt.Sprintf("Processing transaction: %v for %v", v.TransactionId, addressCache[v.Address]))
		if v.IsSuccess {
			successAmount += balanceCache[v.Address]
		} else {
			failedAmount += balanceCache[v.Address]
		}
		txn, err := milieu.GetTransaction()
		if err != nil {
			milieu.CaptureException(err)
			milieu.Info(err.Error())
			continue
		}
		if v.TransactionId == 0 {
			// 0 TXN ID's are a problem.  Replace them with something more valid-esque, we shouldn't have any collisions
			// given how big the uint64 size is.
			v.TransactionId = rand.Uint64()
		}
		txn.Begin(context.Background())
		defer milieu.CleanupTxn()
		err = sql.CreateNewTransaction(txn, v.TransactionId, v.IsSuccess, v.FailureMessage, addressCache[v.Address], batchID, balanceCache[v.Address])
		if err != nil {
			milieu.CaptureException(err)
			milieu.Info(err.Error())
			milieu.CleanupTxn()
			continue
		}
		if !v.IsSuccess {
			txn.Commit(context.Background())
			milieu.CleanupTxn()
			continue
		}
		err = sql.DecreaseBalance(txn, addressCache[v.Address], balanceCache[v.Address])
		if err != nil {
			milieu.CaptureException(err)
			milieu.Info(err.Error())
			milieu.CleanupTxn()
			continue
		}
		txn.Commit(context.Background())
		milieu.CleanupTxn()
		client := milieu.GetRedis()
		client.Del(context.Background(), fmt.Sprintf("bal_bypass_%v", v.Address))
	}
	milieu.Info("Done processing transaction results, updating batch data")
	err = sql.UpdateBatchAmounts(milieu, batchID, successAmount, failedAmount)
	if err != nil {
	}
}

func main() {
	psqlURL := getEnv("PSQL_SERVER", "postgres://postgres@localhost/postgres?sslmode=disable")
	redisURI := getEnv("REDIS_SERVER", "redis://redis:6379/0")
	sentryURI := getEnv("SENTRY_SERVER", "")

	// Build Milieu
	milieu, err := core.NewMilieu(&psqlURL, &redisURI, &sentryURI)
	if err != nil {
		milieu.CaptureException(err)
		milieu.Fatal(err.Error())
	}

	// Load config flags
	walletGRPCAddressPtr := flag.String("wallet-grpc-address", "100.96.247.35:18143", "Tari wallet GRPC address")
	walletGRPC.InitWalletGRPC(*walletGRPCAddressPtr)

	debugEnabledPtr := flag.Bool("debug-enabled", false, "Enable debug logging")
	if *debugEnabledPtr {
		milieu.SetLogLevel(logrus.DebugLevel)
	}

	// Everything is setup, lets get to work.
	payoutOnBootPtr := flag.Bool("payout-on-boot", true, "Perform payout on boot")

	if *payoutOnBootPtr {
		performPayouts(milieu)
	}

	// Cron time!
	cronTimePtr := flag.String("cron-time", "0 * * * *", "Cron time for payouts, runs every hour")

	// Build the cron spinner
	c := cron.New()
	_, _ = c.AddFunc(*cronTimePtr, func() {
		performPayouts(milieu)
	})
	c.Run()

	// Idle loop!
	for {
		select {}
	}
}
