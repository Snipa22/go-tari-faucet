create table balances
(
    id                     bigserial
        constraint balances_pk
            primary key,
    date_added             timestamp with time zone default now()    not null,
    date_balance_increased timestamp with time zone default now()    not null,
    date_last_updated      timestamp with time zone default now()    not null,
    balance                bigint                   default 0        not null,
    valid                  boolean                  default true     not null,
    address                text                                      not null,
    payout_minimum         bigint                   default 10000000 not null
);

create unique index balances_address_uindex
    on balances (address);

create table payment_batch
(
    id             bigserial
        constraint payment_batch_pk
            primary key,
    count          integer                  default 0     not null,
    amount         bigint                   default 0     not null,
    date_added     timestamp with time zone default now() not null,
    amount_success bigint                   default 0     not null,
    amount_fail    bigint                   default 0     not null
);

create index payment_batch_id_index
    on payment_batch (id);

create table transactions
(
    id         numeric               not null
        constraint transactions_pk
            primary key,
    success    boolean default false not null,
    error      text,
    balance_id bigint                not null
        constraint transactions_balances_id_fk
            references balances,
    batch_id   bigint                not null
        constraint transactions_payment_batch_id_fk
            references payment_batch,
    amount     bigint  default 0     not null
);

create index transactions_batch_id_index
    on transactions (batch_id);
create index transactions_balance_id_index
    on transactions (balance_id);
