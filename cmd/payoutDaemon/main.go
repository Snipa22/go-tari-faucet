package main

import (
	core "github.com/Snipa22/core-go-lib/milieu"
	"os"
)

/* payoutDaemon does the following steps, on a cron schedule set by a flag, or on the hour by default:

Scans the `balances` postgresql table to build a list of valid payouts - this can use redis to flag transactions that
	do not meet `payoutBalanceMin`, set via the flag, this is an override for the system
This gets compiled into transaction objects which are packaged into `transfersPerTxnSubmit` transfers set via a flag, or
	200, which is submitted to `walletGRPCAddress`
	We'll go into a PSQL txn state at this time, then do the following:
	1. Add the transfer to `transfers`
	2-fail. Then we'll commit the txn and continue
	2-success. Subtract the balance of the transfer from `balances`
	3. Unset the redis key.
	4. Add data to the `payments` struct so we can log it to the `payments` table
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
}
