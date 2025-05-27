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

create table public.transaction_details
(
    id              numeric                  not null
        constraint transaction_details_pk
            primary key,
    status          integer default 0        not null,
    amount          numeric default 0        not null,
    fee             integer default 0        not null,
    is_cancelled    boolean default false    not null,
    excess_sig      bytea                    not null,
    timestamp       timestamp with time zone not null,
    raw_payment_id  bytea                    not null,
    mined_at_height numeric                  not null,
    user_payment_id bytea                    not null,
    dest_address    bytea                    not null,
    rechecked       boolean default false,
    repaid          boolean
);

alter table public.transaction_details
    owner to postgres;

create index transaction_details_dest_address_index
    on public.transaction_details (dest_address);
