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

var isDryRun bool
var txnMsg string
var running = false
var txnsPerBatch = 50
var haltTxnKey = "payout-daemon-halt-batching"
var balanceSortOrder = 0

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func atomicBalanceUpdates(milieu *core.Milieu, daemonResponse *tari_generated.TransferResponse, addressCache map[string]uint64, balanceCache map[string]uint64, batchID int) (successAmount uint64, failedAmount uint64, err error) {
	for _, v := range daemonResponse.GetResults() {
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
	return
}

func performPayouts(milieu *core.Milieu) {
	if running {
		return
	}
	running = true
	defer func() {
		running = false
	}()
	blocked := milieu.GetRedis().Exists(context.Background(), haltTxnKey)
	if blocked.Val() != 0 {
		// We're blocked by the halt txn key in redis, report and return.
		milieu.Info("Payout system halted due to redis key set, check with your local admin!")
		return
	}

	milieu.Info("Starting payouts")

	milieu.Debug("Starting balance fetch")
	balances, err := sql.GetAllBalances(milieu, balanceSortOrder)
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
		if !sqlBalance.Valid {
			// Balance is tagged as invalid, do not process
			milieu.Debug(fmt.Sprintf("%v is set to invalid", sqlBalance.ID))
			continue
		}
		if sqlBalance.Balance < sqlBalance.PayoutMinimum {
			// Check to see if there's a bypass in redis
			val := milieu.GetRedis().Exists(context.Background(), fmt.Sprintf("bal_bypass_%v", sqlBalance.Address))
			if val.Val() == 0 {
				milieu.Debug(fmt.Sprintf("Balance for %v does not get a bypass and is under payout minimum, "+
					"skipping", sqlBalance.ID))
				continue
			}
		}
		milieu.Debug(fmt.Sprintf("Adding %v to payment ready for %v", sqlBalance.ID, sqlBalance.Balance))
		totalAmount += sqlBalance.Balance
		paymentRecipient := &tari_generated.PaymentRecipient{
			Address:       sqlBalance.Address,
			Amount:        sqlBalance.Balance - 5000,
			FeePerGram:    5,
			PaymentType:   1,
			UserPaymentId: nil,
		}
		if len(txnMsg) > 0 {
			paymentRecipient.UserPaymentId = &tari_generated.UserPaymentId{
				Utf8String: txnMsg,
			}
		}
		payments = append(payments, paymentRecipient)
		addressCache[sqlBalance.Address] = sqlBalance.ID
		balanceCache[sqlBalance.Address] = sqlBalance.Balance
	}
	if len(payments) == 0 {
		milieu.Info(fmt.Sprintf("No payments found, exiting run"))
		return
	}

	if isDryRun {
		milieu.Info("In dry run mode, not inserting batch or executing wallet, dumping txn list for debugging")
		for i, v := range payments {
			milieu.Info(fmt.Sprintf("Index: %v, data: %v", i, v))
		}
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

	sentTransactions := make([]*tari_generated.TransferResult, 0)
	paymentShortList := make([]*tari_generated.PaymentRecipient, 0)
	batchCount := 0
	var successAmount uint64 = 0
	var failedAmount uint64 = 0

	for _, payment := range payments {
		if blocked.Val() != 0 {
			// We're blocked by the halt txn key in redis, report and return.
			milieu.Info("Payout system halted due to redis key set")
			paymentShortList = make([]*tari_generated.PaymentRecipient, 0)
			break
		}
		paymentShortList = append(paymentShortList, payment)
		if len(paymentShortList) == txnsPerBatch {
			txResults, err := walletGRPC.SendTransactions(paymentShortList)
			if err != nil {
				milieu.CaptureException(err)
				milieu.Info(err.Error())
				milieu.Error("Dumping all data in the transaction struct for debugging")
				for i, v := range paymentShortList {
					milieu.Error(fmt.Sprintf("Batch: %v Index: %v, data: %v", batchCount, i, v))
				}
				paymentShortList = make([]*tari_generated.PaymentRecipient, 0)
				milieu.Info(fmt.Sprintf("Processed batch WITH ERROR: %v/%v", batchCount, len(payments)/txnsPerBatch))
				batchCount += 1
				continue
			}
			localSuccess, localFailure, err := atomicBalanceUpdates(milieu, txResults, addressCache, balanceCache, batchID)
			if err != nil {
				milieu.CaptureException(err)
				milieu.Info(err.Error())
				milieu.Error("Dumping all data in the transaction struct for debugging")
				for i, v := range paymentShortList {
					milieu.Error(fmt.Sprintf("Batch: %v Index: %v, data: %v", batchCount, i, v))
				}
				paymentShortList = make([]*tari_generated.PaymentRecipient, 0)
				milieu.Info(fmt.Sprintf("Processed batch WITH ERROR: %v/%v", batchCount, len(payments)/txnsPerBatch))
				batchCount += 1
				continue
			}
			for _, v := range txResults.GetResults() {
				sentTransactions = append(sentTransactions, v)
			}
			successAmount += localSuccess
			failedAmount += localFailure
			paymentShortList = make([]*tari_generated.PaymentRecipient, 0)
			milieu.Info(fmt.Sprintf("Processed batch: %v/%v", batchCount, len(payments)/txnsPerBatch))
			batchCount += 1
		}
	}

	if len(paymentShortList) > 0 {
		txResults, err := walletGRPC.SendTransactions(paymentShortList)
		if err != nil {
			milieu.CaptureException(err)
			milieu.Info(err.Error())
			milieu.Error("Dumping all data in the transaction struct for debugging")
			for i, v := range paymentShortList {
				milieu.Error(fmt.Sprintf("Batch: %v Index: %v, data: %v", batchCount, i, v))
			}
			return
		}
		localSuccess, localFailure, err := atomicBalanceUpdates(milieu, txResults, addressCache, balanceCache, batchID)
		if err != nil {
			milieu.CaptureException(err)
			milieu.Info(err.Error())
			milieu.Error("Dumping all data in the transaction struct for debugging")
			for i, v := range paymentShortList {
				milieu.Error(fmt.Sprintf("Batch: %v Index: %v, data: %v", batchCount, i, v))
			}
			return
		}
		for _, v := range txResults.GetResults() {
			sentTransactions = append(sentTransactions, v)
		}
		successAmount += localSuccess
		failedAmount += localFailure
	}
	milieu.Info("Done processing transaction results, updating batch data")
	err = sql.UpdateBatchAmounts(milieu, batchID, successAmount, failedAmount)
	if err != nil {
		milieu.CaptureException(err)
		milieu.Info(err.Error())
	}
	milieu.Info("Done updating batch data, starting TX repeat scan.")

	for _, v := range sentTransactions {
		if !v.IsSuccess {
			continue
		}
		txInfo, err := walletGRPC.GetTransactionInfoByID(v.TransactionId)
		if err != nil {
			milieu.CaptureException(err)
			milieu.Info(err.Error())
			continue
		}
		if txInfo.Status == 11 {
			continue
		}
		if err = sql.CreateTransactionDetail(milieu, txInfo); err != nil {
			milieu.CaptureException(err)
			milieu.Info(err.Error())
			continue
		}
	}
	milieu.Info("Done updating batch data, stored excess TX data, payout complete")
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
	walletGRPCAddressPtr := flag.String("wallet-grpc-address", "127.0.0.1:18143", "Tari wallet GRPC address")
	debugEnabledPtr := flag.Bool("debug-enabled", false, "Enable debug logging")
	payoutOnBootPtr := flag.Bool("payout-on-boot", false, "Perform payout on boot")
	cronTimePtr := flag.String("cron-time", "0 * * * *", "Cron time for payouts, runs every hour")
	runOncePtr := flag.Bool("run-once", false, "Run once and exit")
	dryRunPtr := flag.Bool("dry-run", false, "Puts the system into dry-run mode, exits right before batch insert")
	txnMsgPtr := flag.String("txn-msg", "", "Transaction message to attach to a send, max length 256 characters")
	batchSizePtr := flag.Int("batch-size", 50, "How many TXNs to submit to the wallet in a batch")
	settxnHalt := flag.Bool("set-txn-halt", false, "Set transaction halt flag in redis")
	unsetTxnHalt := flag.Bool("unset-txn-halt", false, "Unset transaction halt flag in redis")
	balanceSelectOrder := flag.Int("balance-select-order", 0, "Select balance order by, 0 for unsorted, 1 for highest, 2 for lowest")

	flag.Parse()
	txnMsg = *txnMsgPtr
	walletGRPC.InitWalletGRPC(*walletGRPCAddressPtr)
	balanceSortOrder = *balanceSelectOrder

	txnsPerBatch = *batchSizePtr

	if *debugEnabledPtr {
		milieu.SetLogLevel(logrus.DebugLevel)
	}

	if *settxnHalt {
		milieu.Info("Setting transaction halt flag in redis and exiting")
		milieu.GetRedis().Set(context.Background(), haltTxnKey, 1, 0)
		return
	}

	if *unsetTxnHalt {
		milieu.Info("Unsetting transaction halt flag in redis and exiting")
		milieu.GetRedis().Del(context.Background(), haltTxnKey)
		return
	}

	isDryRun = *dryRunPtr

	// Everything is setup, lets get to work.
	if *payoutOnBootPtr || *runOncePtr {
		performPayouts(milieu)
		if *runOncePtr {
			milieu.Info("Dry-run mode is enabled, exiting")
			os.Exit(0)
		}
	}

	// Cron time!

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
