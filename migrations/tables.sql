-- dispenses tracks every faucet dispense attempt (successful or not), and is
-- the sole bookkeeping table this service owns. It is deliberately separate
-- from go-tari-tools/cmd/payoutDaemon's `balances`/`payment_batch`/
-- `transactions` schema -- this faucet does not do payouts, it just needs an
-- audit trail plus a fast lookup for the address+IP rate limit.
create table dispenses
(
    id         bigserial
        constraint dispenses_pk
            primary key,
    address    text                                   not null,
    ip         text                                   not null,
    amount     bigint                   default 0     not null,
    success    boolean                  default false not null,
    error      text,
    tx_id      numeric,
    created_at timestamp with time zone default now() not null
);

-- Rate limiting checks "has this address dispensed successfully in the last
-- <window>" and "has this IP dispensed successfully in the last <window>"
-- independently, so each needs its own index with created_at trailing for an
-- efficient "most recent row" lookup instead of a table scan.
create index dispenses_address_created_at_idx
    on dispenses (address, created_at desc);

create index dispenses_ip_created_at_idx
    on dispenses (ip, created_at desc);
