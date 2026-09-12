# go-tari-faucet
Faucet for the Tari network

An HTTP service that dispenses a small, fixed amount of test Tari to a
submitted testnet address, rate limited per-address and per-IP. It does not
perform payouts itself (see `go-tari-tools/cmd/payoutDaemon` for that); it
owns only its own `dispenses` audit/rate-limit table.

## Running

```
go build ./...
PSQL_SERVER='postgres://user:pass@host:5432/dbname?sslmode=disable' \
  ./go-tari-faucet -wallet-grpc-address=127.0.0.1:18143
```

Apply `migrations/tables.sql` against the target Postgres database/role
before first run.

### Flags

| Flag                   | Env             | Default                  | Description                                             |
|-------------------------|-----------------|---------------------------|-----------------------------------------------------------|
| `-listen-addr`          | -               | `0.0.0.0:8091`             | HTTP listen address                                        |
| `-wallet-grpc-address`  | -               | `100.88.139.119:12345`     | Tari wallet GRPC address                                    |
| `-psql-server`          | `PSQL_SERVER`   | *(required)*               | Postgres DSN for this faucet's own database                |
| `-sentry-server`        | `SENTRY_SERVER` | *(empty, disabled)*        | Sentry DSN, optional                                        |
| `-dispense-amount`      | -               | `1000000`                  | microMinotari dispensed per successful request              |
| `-rate-limit-window`    | -               | `24h`                      | Minimum time between successful dispenses per address/IP    |
| `-debug-enabled`        | -               | `false`                    | Enable debug logging                                        |

Routes: `GET /` (request form), `POST /request` (submit an address),
`GET /healthz` (200 if Postgres + wallet GRPC are both reachable, 503
otherwise).

See `deploy/tari-faucet.service` for a reference (documentation-only)
systemd unit.
