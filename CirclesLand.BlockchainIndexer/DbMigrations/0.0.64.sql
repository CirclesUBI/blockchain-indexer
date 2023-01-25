--
-- Indexer role and user
--
CREATE ROLE indexer;
GRANT USAGE ON SCHEMA public TO indexer;

CREATE USER indexer_user WITH PASSWORD 'postgres';
GRANT indexer TO indexer_user;

--
-- Pathfinder role and user
--
CREATE ROLE pathfinder;

CREATE USER pathfinder_user WITH PASSWORD 'postgres';
GRANT pathfinder TO pathfinder_user;

--
-- Readonly role and user
--
CREATE ROLE readonly;

CREATE USER readonly_user WITH PASSWORD 'postgres';
GRANT readonly TO readonly_user;



create table if not exists _block_staging
(
    number                  bigint,
    hash                    text,
    timestamp               timestamp,
    total_transaction_count integer,
    selected_at             timestamp,
    imported_at             timestamp,
    already_available       boolean
);



create index if not exists ix_block_staging_imported_at
    on _block_staging (imported_at) include (hash, number, total_transaction_count);

create index if not exists ix_block_staging_selected_at_
    on _block_staging (selected_at) include (hash, number, total_transaction_count);

create index if not exists ix_block_staging_number
    on _block_staging (number) include (hash, selected_at, total_transaction_count);

grant delete, insert, select, update on _block_staging to indexer;

grant select on _block_staging to readonly;

create table if not exists _crc_hub_transfer_staging
(
    hash         text,
    index        integer,
    timestamp    timestamp,
    block_number bigint,
    "from"       text,
    "to"         text,
    value        text
);



create index if not exists ix_crc_hub_transfer_staging_hash
    on _crc_hub_transfer_staging (hash) include (block_number);

grant delete, insert, select, update on _crc_hub_transfer_staging to indexer;

grant select on _crc_hub_transfer_staging to readonly;

create table if not exists _crc_organisation_signup_staging
(
    hash         text,
    index        integer,
    timestamp    timestamp,
    block_number bigint,
    organisation text,
    owners       text[]
);



create index if not exists ix_crc_organisation_signup_staging_hash
    on _crc_organisation_signup_staging (hash) include (block_number);

grant delete, insert, select, update on _crc_organisation_signup_staging to indexer;

grant select on _crc_organisation_signup_staging to readonly;

create table if not exists _crc_signup_staging
(
    hash         text,
    index        integer,
    timestamp    timestamp,
    block_number bigint,
    "user"       text,
    token        text,
    owners       text[]
);



create index if not exists ix_crc_signup_staging_hash
    on _crc_signup_staging (hash) include (block_number);

grant delete, insert, select, update on _crc_signup_staging to indexer;

grant select on _crc_signup_staging to readonly;

create table if not exists _crc_trust_staging
(
    hash         text,
    index        integer,
    timestamp    timestamp,
    block_number bigint,
    address      text,
    can_send_to  text,
    "limit"      numeric
);



create index if not exists ix_crc_trust_staging_hash
    on _crc_trust_staging (hash) include (block_number);

grant delete, insert, select, update on _crc_trust_staging to indexer;

grant select on _crc_trust_staging to readonly;

create table if not exists _erc20_transfer_staging
(
    hash         text,
    index        integer,
    timestamp    timestamp,
    block_number bigint,
    "from"       text,
    "to"         text,
    token        text,
    value        text
);



create index if not exists ix_erc20_transfer_staging_from
    on _erc20_transfer_staging ("from");

create index if not exists ix_erc20_transfer_staging_hash
    on _erc20_transfer_staging (hash) include (block_number);

create index if not exists ix_erc20_transfer_staging_to
    on _erc20_transfer_staging ("to");

grant delete, insert, select, update on _erc20_transfer_staging to indexer;

grant select on _erc20_transfer_staging to readonly;

create table if not exists _eth_transfer_staging
(
    hash         text,
    index        integer,
    timestamp    timestamp,
    block_number bigint,
    "from"       text,
    "to"         text,
    value        text
);



create index if not exists ix_eth_transfer_staging_from
    on _eth_transfer_staging ("from");

create index if not exists ix_eth_transfer_staging_hash
    on _eth_transfer_staging (hash) include (block_number);

create index if not exists ix_eth_transfer_staging_to
    on _eth_transfer_staging ("to");

grant delete, insert, select, update on _eth_transfer_staging to indexer;

grant select on _eth_transfer_staging to readonly;

create table if not exists _gnosis_safe_eth_transfer_staging
(
    hash         text,
    index        integer,
    timestamp    timestamp,
    block_number bigint,
    initiator    text,
    "from"       text,
    "to"         text,
    value        text
);



create index if not exists ix_gnosis_safe_eth_transfer_staging_from
    on _gnosis_safe_eth_transfer_staging ("from");

create index if not exists ix_gnosis_safe_eth_transfer_staging_hash
    on _gnosis_safe_eth_transfer_staging (hash) include (block_number);

create index if not exists ix_gnosis_safe_eth_transfer_staging_to
    on _gnosis_safe_eth_transfer_staging ("to");

grant delete, insert, select, update on _gnosis_safe_eth_transfer_staging to indexer;

grant select on _gnosis_safe_eth_transfer_staging to readonly;

create table if not exists _transaction_staging
(
    block_number   bigint,
    "from"         text,
    "to"           text,
    hash           text,
    index          integer,
    timestamp      timestamp,
    value          text,
    input          text,
    nonce          text,
    type           text,
    classification text[]
);



create index if not exists ix_transaction_staging_block_number
    on _transaction_staging (block_number) include (hash);

create index if not exists ix_transaction_staging_hash
    on _transaction_staging (hash) include (block_number);

grant delete, insert, select, update on _transaction_staging to indexer;

grant select on _transaction_staging to readonly;

create table if not exists block
(
    number                    bigserial
        primary key,
    hash                      text      not null
        unique,
    timestamp                 timestamp not null,
    total_transaction_count   integer   not null,
    indexed_transaction_count integer   not null
);



create unique index if not exists idx_block_timestamp
    on block (timestamp) include (number);

create unique index if not exists ux_block_number
    on block (number) include (timestamp);

grant delete, insert, select on block to indexer;

grant select on block to readonly;

create table if not exists cache_all_addresses
(
    id      serial
        primary key,
    type    text,
    address text
        unique
);



grant select, usage on sequence cache_all_addresses_id_seq to indexer;

grant delete, insert, select on cache_all_addresses to indexer;

grant select on cache_all_addresses to readonly;

create table if not exists cache_crc_balances_by_safe_and_token
(
    safe_address           text,
    token                  text,
    token_owner            text,
    balance                numeric,
    last_change_at         timestamp,
    last_change_at_block   numeric,
    safe_address_id        integer,
    token_address_id       integer,
    token_owner_address_id integer
);



create index if not exists ix_cache_crc_balances_by_safe_and_token_token
    on cache_crc_balances_by_safe_and_token (token) include (safe_address, balance, last_change_at_block);

create index if not exists ix_cache_crc_balances_by_safe_and_token_safe_address
    on cache_crc_balances_by_safe_and_token (safe_address, token_owner) include (balance, last_change_at_block);

create index if not exists ix_cache_crc_balances_by_safe_and_token_last_change_on_block
    on cache_crc_balances_by_safe_and_token (last_change_at_block);

create index if not exists ix_cache_crc_balances_by_safe_and_token_last_change_at
    on cache_crc_balances_by_safe_and_token (last_change_at);

create index if not exists ix_cache_crc_balances_by_safe_and_token_token_owner
    on cache_crc_balances_by_safe_and_token (token_owner);

grant delete, insert, select on cache_crc_balances_by_safe_and_token to indexer;

grant select on cache_crc_balances_by_safe_and_token to pathfinder;

grant select on cache_crc_balances_by_safe_and_token to readonly;

create table if not exists cache_crc_current_trust
(
    "user"                       text,
    user_token                   text,
    can_send_to                  text,
    can_send_to_token            text,
    "limit"                      numeric,
    history_count                bigint,
    last_change                  timestamp,
    last_change_at_block         numeric,
    user_address_id              integer,
    user_token_address_id        integer,
    can_send_to_address_id       integer,
    can_send_to_token_address_id integer
);



create index if not exists ix_cache_crc_current_trust_user
    on cache_crc_current_trust ("user");

create index if not exists ix_cache_crc_current_trust_last_change
    on cache_crc_current_trust (last_change);

create index if not exists ix_cache_crc_current_trust_can_send_to
    on cache_crc_current_trust (can_send_to) include (user_token, "limit", last_change_at_block);

create index if not exists ix_cache_crc_current_trust_limit
    on cache_crc_current_trust ("limit");

create index if not exists ix_cache_crc_current_trust_last_change_at_block
    on cache_crc_current_trust (last_change_at_block desc);

create index if not exists ix_cache_crc_current_trust_user_token
    on cache_crc_current_trust (user_token);

grant delete, insert, select on cache_crc_current_trust to indexer;

grant select on cache_crc_current_trust to pathfinder;

grant select on cache_crc_current_trust to readonly;

create table if not exists transaction_2
(
    block_number   bigint
        constraint fk_transaction_2_block_number
            references block,
    "from"         text,
    "to"           text,
    hash           text not null
        constraint pk_transaction_2
            primary key,
    index          integer,
    timestamp      timestamp,
    value          numeric,
    input          text,
    nonce          text,
    type           text,
    classification text[]
);



create table if not exists crc_signup_2
(
    hash         text
        constraint fk_signup_transaction_2
            references transaction_2,
    index        integer,
    timestamp    timestamp,
    block_number bigint
        constraint fk_signup_block_2
            references block,
    "user"       text,
    token        text,
    owners       text[]
);



create index if not exists ix_crc_signup_2_hash
    on crc_signup_2 (hash) include (block_number, index, timestamp);

create index if not exists ix_crc_signup_2_block_number
    on crc_signup_2 (block_number) include (index, timestamp);

create index if not exists ix_gin_crc_signup_2_owners
    on crc_signup_2 using gin (owners);

create unique index if not exists ux_crc_signup_2_token
    on crc_signup_2 (token) include ("user");

create index if not exists ix_crc_signup_2_timestamp
    on crc_signup_2 (timestamp) include (hash, block_number, index, timestamp);

create unique index if not exists ux_crc_signup_2_user
    on crc_signup_2 ("user") include (token);

grant delete, insert, select on crc_signup_2 to indexer;

grant select on crc_signup_2 to readonly;

create table if not exists erc20_transfer_2
(
    hash         text
        constraint fk_erc20_transfer_transaction_2
            references transaction_2,
    index        integer,
    timestamp    timestamp,
    block_number bigint
        constraint fk_erc20_transfer_block_2
            references block,
    "from"       text,
    "to"         text,
    token        text,
    value        numeric
);



create index if not exists ix_erc20_transfer_2_hash
    on erc20_transfer_2 (hash) include (block_number, index, timestamp);

create index if not exists ix_erc20_transfer_2_block_number
    on erc20_transfer_2 (block_number) include (index, timestamp);

create index if not exists ix_erc20_transfer_2_from
    on erc20_transfer_2 ("from") include ("to", token, value);

create index if not exists ix_erc20_transfer_2_to
    on erc20_transfer_2 ("to") include ("from", token, value);

create index if not exists ix_erc20_transfer_2_token
    on erc20_transfer_2 (token);

create index if not exists ix_erc20_transfer_2_timestamp
    on erc20_transfer_2 (timestamp) include (hash, block_number, index, timestamp);

create unique index if not exists ux_erc20_transfer_2_hash_from_to_token_value
    on erc20_transfer_2 (hash, "from", "to", token, value);

grant delete, insert, select on erc20_transfer_2 to indexer;

grant select on erc20_transfer_2 to readonly;

create index if not exists ix_transaction_2_timestamp
    on transaction_2 (timestamp) include (hash, block_number, index, timestamp);

create index if not exists ix_transaction_2_from
    on transaction_2 ("from") include ("to", value);

create index if not exists ix_transaction_2_to
    on transaction_2 ("to") include ("from", value);

create unique index if not exists ux_transaction_2_block_number_index
    on transaction_2 (block_number, index);

grant delete, insert, select on transaction_2 to indexer;

grant select on transaction_2 to pathfinder;

grant select on transaction_2 to readonly;

create table if not exists crc_organisation_signup_2
(
    hash         text
        constraint fk_organisation_signup_transaction_2
            references transaction_2,
    index        integer,
    timestamp    timestamp,
    block_number bigint
        constraint fk_organisation_signup_block_2
            references block,
    organisation text,
    owners       text[]
);



create index if not exists ix_crc_organisation_signup_2_block_number
    on crc_organisation_signup_2 (block_number) include (index, timestamp);

create index if not exists ix_crc_organisation_signup_2_timestamp
    on crc_organisation_signup_2 (timestamp) include (hash, block_number, index, timestamp);

create index if not exists ix_crc_organisation_signup_2_hash
    on crc_organisation_signup_2 (hash) include (block_number, index, timestamp);

create index if not exists ix_gin_crc_organisation_signup_2_owners
    on crc_organisation_signup_2 using gin (owners);

create unique index if not exists ux_crc_organisation_signup_2_organisation
    on crc_organisation_signup_2 (organisation);

grant delete, insert, select on crc_organisation_signup_2 to indexer;

grant select on crc_organisation_signup_2 to readonly;

create table if not exists crc_trust_2
(
    hash         text
        constraint fk_trust_transaction_2
            references transaction_2,
    index        integer,
    timestamp    timestamp,
    block_number bigint
        constraint fk_trust_block_2
            references block,
    address      text,
    can_send_to  text,
    "limit"      numeric
);



create index if not exists ix_crc_trust_2_hash
    on crc_trust_2 (hash) include (block_number, index, timestamp);

create index if not exists ix_crc_trust_2_block_number
    on crc_trust_2 (block_number) include (index, timestamp);

create unique index if not exists ux_crc_trust_2_hash_address_can_send_to_limit
    on crc_trust_2 (hash, address, can_send_to, "limit");

create index if not exists ix_crc_trust_2_timestamp
    on crc_trust_2 (timestamp) include (hash, block_number, index, timestamp);

create index if not exists ix_crc_trust_2_address
    on crc_trust_2 (address) include (can_send_to, "limit");

create index if not exists ix_crc_trust_2_can_send_to
    on crc_trust_2 (can_send_to) include (address, "limit");

grant delete, insert, select on crc_trust_2 to indexer;

grant select on crc_trust_2 to readonly;

create table if not exists crc_hub_transfer_2
(
    hash         text
        constraint fk_hub_transfer_transaction_2
            references transaction_2,
    index        integer,
    timestamp    timestamp,
    block_number bigint
        constraint fk_hub_transfer_block_2
            references block,
    "from"       text,
    "to"         text,
    value        numeric
);



create index if not exists ix_crc_hub_transfer_2_hash
    on crc_hub_transfer_2 (hash) include (block_number, index, timestamp);

create index if not exists ix_crc_hub_transfer_2_from
    on crc_hub_transfer_2 ("from") include ("to", value);

create index if not exists ix_crc_hub_transfer_2_block_number
    on crc_hub_transfer_2 (block_number) include (index, timestamp);

create index if not exists ix_crc_hub_transfer_2_to
    on crc_hub_transfer_2 ("to") include ("from", value);

create index if not exists ix_crc_hub_transfer_2_timestamp
    on crc_hub_transfer_2 (timestamp) include (hash, block_number, index, timestamp);

create unique index if not exists ux_crc_hub_transfer_2_hash_from_to_value
    on crc_hub_transfer_2 (hash, "from", "to", value);

grant delete, insert, select on crc_hub_transfer_2 to indexer;

grant select on crc_hub_transfer_2 to readonly;

create table if not exists eth_transfer_2
(
    hash         text
        constraint fk_eth_transfer_transaction_2
            references transaction_2,
    index        integer,
    timestamp    timestamp,
    block_number bigint
        constraint fk_eth_transfer_block_2
            references block,
    "from"       text,
    "to"         text,
    value        numeric
);



create index if not exists ix_eth_transfer_2_to
    on eth_transfer_2 ("to") include ("from", value);

create index if not exists ix_eth_transfer_2_hash
    on eth_transfer_2 (hash) include (block_number, index, timestamp);

create index if not exists ix_eth_transfer_2_timestamp
    on eth_transfer_2 (timestamp) include (hash, block_number, index, timestamp);

create index if not exists ix_eth_transfer_2_from
    on eth_transfer_2 ("from") include ("to", value);

create index if not exists ix_eth_transfer_2_block_number
    on eth_transfer_2 (block_number) include (index, timestamp);

create unique index if not exists ux_eth_transfer_2_hash_from_to_value
    on eth_transfer_2 (hash, "from", "to", value);

grant delete, insert, select on eth_transfer_2 to indexer;

grant select on eth_transfer_2 to readonly;

create table if not exists gnosis_safe_eth_transfer_2
(
    hash         text
        constraint fk_gnosis_safe_eth_transfer_transaction_2
            references transaction_2,
    index        integer,
    timestamp    timestamp,
    block_number bigint
        constraint fk_gnosis_safe_eth_transfer_block_2
            references block,
    initiator    text,
    "from"       text,
    "to"         text,
    value        numeric
);



create index if not exists ix_gnosis_safe_eth_transfer_2_block_number
    on gnosis_safe_eth_transfer_2 (block_number) include (index, timestamp);

create index if not exists ix_gnosis_safe_eth_transfer_2_hash
    on gnosis_safe_eth_transfer_2 (hash) include (block_number, index, timestamp);

create index if not exists ix_gnosis_safe_eth_transfer_2_to
    on gnosis_safe_eth_transfer_2 ("to") include ("from", value);

create index if not exists ix_gnosis_safe_eth_transfer_2_initiator
    on gnosis_safe_eth_transfer_2 (initiator);

create index if not exists ix_gnosis_safe_eth_transfer_2_from
    on gnosis_safe_eth_transfer_2 ("from") include ("to", value);

create index if not exists ix_gnosis_safe_eth_transfer_2_timestamp
    on gnosis_safe_eth_transfer_2 (timestamp) include (hash, block_number, index, timestamp);

create unique index if not exists ux_gnosis_safe_eth_transfer_2_hash_from_to_value
    on gnosis_safe_eth_transfer_2 (hash, initiator, "from", "to", value);

grant delete, insert, select on gnosis_safe_eth_transfer_2 to indexer;

grant select on gnosis_safe_eth_transfer_2 to readonly;

create table if not exists requested_blocks
(
    block_no numeric
);



create unique index if not exists ux_requested_blocks_block_no
    on requested_blocks (block_no);

grant delete, insert, select on requested_blocks to indexer;

grant select on requested_blocks to readonly;

create or replace view crc_token_transfer_2(timestamp, block_number, index, hash, "from", "to", token, value) as
SELECT t."timestamp",
       t.block_number,
       t.index,
       t.hash,
       t."from",
       t."to",
       t.token,
       t.value
FROM erc20_transfer_2 t
         JOIN crc_signup_2 s ON t.token = s.token;



grant select on crc_token_transfer_2 to readonly;

create or replace view crc_alive_accounts("to") as
SELECT tt."to"
FROM crc_token_transfer_2 tt
         JOIN transaction_2 t ON tt.hash = t.hash
         JOIN block b ON t.block_number = b.number
GROUP BY tt."to"
HAVING max(b."timestamp") > (now() - '90 days'::interval);



grant select on crc_alive_accounts to readonly;

create or replace view crc_all_signups(hash, block_number, index, timestamp, "user", token) as
SELECT c.hash,
       c.block_number,
       c.index,
       c."timestamp",
       c."user",
       c.token
FROM crc_signup_2 c
UNION ALL
SELECT c.hash,
       c.block_number,
       c.index,
       c."timestamp",
       c.organisation AS "user",
       NULL::text AS token
FROM crc_organisation_signup_2 c;



grant select on crc_all_signups to indexer;

grant select on crc_all_signups to pathfinder;

grant select on crc_all_signups to readonly;

create or replace view crc_ledger_2
            (timestamp, transaction_id, verb, value, token, token_owner, predicate, safe_address, block_number) as
WITH ledger AS (
    SELECT t_1.hash,
           t_1.block_number,
           t_1."timestamp",
           'add'::text AS verb,
           sum(t_1.value) AS value,
           t_1.token,
           cs."user" AS token_owner,
           'to'::text AS predicate,
           t_1."to" AS safe_address
    FROM erc20_transfer_2 t_1
             JOIN crc_signup_2 cs ON t_1.token = cs.token
    GROUP BY t_1.hash, t_1.block_number, t_1."timestamp", t_1."to", t_1.token, cs."user"
    UNION
    SELECT t_1.hash,
           t_1.block_number,
           t_1."timestamp",
           'remove'::text AS verb,
           - sum(t_1.value) AS value,
           t_1.token,
           cs."user" AS token_owner,
           'from'::text AS predicate,
           t_1."from" AS safe_address
    FROM erc20_transfer_2 t_1
             JOIN crc_signup_2 cs ON t_1.token = cs.token
    GROUP BY t_1.hash, t_1.block_number, t_1."timestamp", t_1."from", t_1.token, cs."user"
)
SELECT l."timestamp",
       l.hash AS transaction_id,
       l.verb,
       l.value,
       l.token,
       l.token_owner,
       l.predicate,
       l.safe_address,
       l.block_number
FROM ledger l
ORDER BY l.block_number, l.token, l.verb DESC;



grant select on crc_ledger_2 to readonly;

create or replace view crc_balances_by_safe_2(safe_address, balance) as
SELECT crc_ledger_2.safe_address,
       sum(crc_ledger_2.value) AS balance
FROM crc_ledger_2
GROUP BY crc_ledger_2.safe_address
ORDER BY crc_ledger_2.safe_address;



grant select on crc_balances_by_safe_2 to readonly;

create or replace view crc_balances_by_safe_and_token_2(safe_address, token, token_owner, balance, last_change_at) as
SELECT crc_ledger_2.safe_address,
       crc_ledger_2.token,
       crc_ledger_2.token_owner,
       sum(crc_ledger_2.value) AS balance,
       max(crc_ledger_2."timestamp") AS last_change_at
FROM crc_ledger_2
GROUP BY crc_ledger_2.safe_address, crc_ledger_2.token, crc_ledger_2.token_owner
ORDER BY crc_ledger_2.safe_address, (sum(crc_ledger_2.value)) DESC;



grant select on crc_balances_by_safe_and_token_2 to indexer;

grant select on crc_balances_by_safe_and_token_2 to pathfinder;

grant select on crc_balances_by_safe_and_token_2 to readonly;

create or replace view crc_capacity_graph
            (token_holder, token, token_owner, balance, can_send_to, can_send_to_is_orga, capacity) as
WITH accepted_tokens AS (
    SELECT cache_crc_current_trust.can_send_to AS potential_token_receiver,
           cache_crc_current_trust.user_token AS accepted_token,
           cache_crc_current_trust."user" AS accepted_token_owner,
           cas.token AS potential_token_receivers_own_token,
           cas.token IS NULL AS potential_token_receiver_is_orga,
           cache_crc_current_trust."limit"
    FROM cache_crc_current_trust
             JOIN crc_all_signups cas ON cache_crc_current_trust.can_send_to = cas."user"
    WHERE cache_crc_current_trust."limit" > 0::numeric
), total_holdings AS (
    SELECT balances.safe_address AS token_holder,
           balances.balance,
           accepted_tokens.accepted_token AS token,
           accepted_tokens.accepted_token_owner AS token_owner,
           accepted_tokens.potential_token_receiver AS can_send_to,
           accepted_tokens.potential_token_receiver_is_orga AS can_send_to_is_orga,
           accepted_tokens.potential_token_receivers_own_token = balances.token AS is_receivers_own_token,
           accepted_tokens."limit"
    FROM accepted_tokens
             JOIN cache_crc_balances_by_safe_and_token balances ON accepted_tokens.accepted_token = balances.token
    WHERE balances.safe_address <> '0x0000000000000000000000000000000000000000'::text AND balances.safe_address <> '0x0000000000000000000000000000000000000001'::text AND balances.balance > 0::numeric AND balances.safe_address <> accepted_tokens.potential_token_receiver
), with_token_owners AS (
    SELECT h.token_holder,
           h.balance,
           h.token,
           h.can_send_to,
           h.can_send_to_is_orga,
           h.is_receivers_own_token,
           h."limit",
           s."user" AS token_owner
    FROM total_holdings h
             LEFT JOIN crc_all_signups s ON s.token = h.token
), with_token_owner_balance AS (
    SELECT h.token_holder,
           h.balance,
           h.token,
           h.can_send_to,
           h.can_send_to_is_orga,
           h.is_receivers_own_token,
           h."limit",
           h.token_owner,
           COALESCE(b.balance, 0::numeric) AS token_owners_own_balance
    FROM with_token_owners h
             LEFT JOIN cache_crc_balances_by_safe_and_token b ON h.token_owner = b.safe_address AND h.token = b.token
), with_max_transferable_amount AS (
    SELECT with_token_owner_balance.token_holder,
           with_token_owner_balance.balance,
           with_token_owner_balance.token,
           with_token_owner_balance.can_send_to,
           with_token_owner_balance.can_send_to_is_orga,
           with_token_owner_balance.is_receivers_own_token,
           with_token_owner_balance."limit",
           with_token_owner_balance.token_owner,
           with_token_owner_balance.token_owners_own_balance,
           with_token_owner_balance.token_owners_own_balance * with_token_owner_balance."limit" / 100::numeric AS max_transferable_amount
    FROM with_token_owner_balance
), with_receiver_balance AS (
    SELECT h.token_holder,
           h.balance,
           h.token,
           h.can_send_to,
           h.can_send_to_is_orga,
           h.is_receivers_own_token,
           h."limit",
           h.token_owner,
           h.token_owners_own_balance,
           h.max_transferable_amount,
           COALESCE(b.balance, 0::numeric) AS receiver_token_balance,
           COALESCE(b.balance, 0::numeric) * (100::numeric - h."limit") / 100::numeric AS receiver_token_balance_scaled
    FROM with_max_transferable_amount h
             LEFT JOIN cache_crc_balances_by_safe_and_token b ON h.can_send_to = b.safe_address AND h.token = b.token
), max_capacity AS (
    SELECT with_receiver_balance.token_holder,
           with_receiver_balance.balance,
           with_receiver_balance.token,
           with_receiver_balance.can_send_to,
           with_receiver_balance.can_send_to_is_orga,
           with_receiver_balance.is_receivers_own_token,
           with_receiver_balance."limit",
           with_receiver_balance.token_owner,
           with_receiver_balance.token_owners_own_balance,
           with_receiver_balance.max_transferable_amount,
           with_receiver_balance.receiver_token_balance,
           with_receiver_balance.receiver_token_balance_scaled,
           with_receiver_balance.max_transferable_amount - with_receiver_balance.receiver_token_balance_scaled AS max_capacity
    FROM with_receiver_balance
), final AS (
    SELECT max_capacity.token_holder,
           max_capacity.balance,
           max_capacity.token,
           max_capacity.can_send_to,
           max_capacity.can_send_to_is_orga,
           max_capacity.is_receivers_own_token,
           max_capacity."limit",
           max_capacity.token_owner,
           max_capacity.token_owners_own_balance,
           max_capacity.max_transferable_amount,
           max_capacity.receiver_token_balance,
           max_capacity.receiver_token_balance_scaled,
           max_capacity.max_capacity,
           max_capacity.receiver_token_balance > 0::numeric AND max_capacity.max_transferable_amount < max_capacity.receiver_token_balance AS zero,
           CASE
               WHEN max_capacity.max_capacity < max_capacity.balance THEN max_capacity.max_capacity
               ELSE max_capacity.balance
               END AS actual_capacity
    FROM max_capacity
)
SELECT final.token_holder,
       final.token,
       final.token_owner,
       final.balance,
       final.can_send_to,
       final.can_send_to_is_orga,
       CASE
           WHEN final.is_receivers_own_token OR final.can_send_to_is_orga THEN final.balance
           ELSE
               CASE
                   WHEN final.zero THEN 0::numeric
                   ELSE final.actual_capacity
                   END
           END AS capacity
FROM final;



grant select on crc_capacity_graph to readonly;

create or replace view crc_capacity_graph_2("from", "to", token_owner, capacity, "limit") as
WITH a AS (
    SELECT crc_current_trust_2.user_token,
           crc_current_trust_2.can_send_to,
           crc_current_trust_2.can_send_to_token,
           crc_current_trust_2."limit"
    FROM cache_crc_current_trust crc_current_trust_2
), b AS (
    SELECT bal.safe_address AS "from",
           a.can_send_to AS "to",
           a."limit",
           bal.token_owner,
           bal.balance AS from_balance
    FROM a
             JOIN cache_crc_balances_by_safe_and_token bal ON bal.token = a.user_token
    WHERE bal.balance >= 0::numeric
), c AS (
    SELECT b."from",
           b."to",
           b."limit",
           b.token_owner,
           b.from_balance,
           bal.balance AS to_own_token_holdings
    FROM b
             LEFT JOIN cache_crc_balances_by_safe_and_token bal ON bal.safe_address = b."to" AND bal.token_owner = b."to"
), d AS (
    SELECT c."from",
           c."to",
           c."limit",
           c.token_owner,
           c.from_balance,
           c.to_own_token_holdings,
           CASE
               WHEN bal.balance IS NOT NULL THEN bal.balance
               ELSE 0::numeric
               END AS to_already_holds_balance,
           os.organisation IS NOT NULL AS to_is_orga,
           c."to" = c.token_owner AS is_to_own_token
    FROM c
             LEFT JOIN cache_crc_balances_by_safe_and_token bal ON bal.safe_address = c."to" AND bal.token_owner = c.token_owner
             LEFT JOIN crc_organisation_signup_2 os ON os.organisation = c."to"
), e AS (
    SELECT d."from",
           d."to",
           d."limit",
           d.token_owner,
           d.from_balance,
           d.to_own_token_holdings,
           d.to_already_holds_balance,
           d.to_is_orga,
           d.is_to_own_token,
           CASE
               WHEN d.is_to_own_token OR d.to_is_orga THEN d.from_balance
               ELSE d.to_own_token_holdings * d."limit" / 100::numeric
               END AS max_transfer_amount
    FROM d
), f AS (
    SELECT e."from",
           e."to",
           e."limit",
           e.token_owner,
           e.from_balance,
           e.to_own_token_holdings,
           e.to_already_holds_balance,
           e.to_is_orga,
           e.is_to_own_token,
           CASE
               WHEN e."limit" > 0::numeric THEN e.max_transfer_amount
               ELSE 0::numeric
               END AS max_transfer_amount,
           e.to_already_holds_balance * (100::numeric - e."limit") / 100::numeric AS dest_balance_scaled
    FROM e
), g AS (
    SELECT f."from",
           f."to",
           f."limit",
           f.token_owner,
           f.from_balance,
           f.to_own_token_holdings,
           f.to_already_holds_balance,
           f.to_is_orga,
           f.is_to_own_token,
           f.max_transfer_amount,
           f.dest_balance_scaled,
           CASE
               WHEN f.max_transfer_amount < f.to_already_holds_balance THEN 0::numeric
               ELSE
                   CASE
                       WHEN f."limit" > 0::numeric THEN f.max_transfer_amount - f.dest_balance_scaled
                       ELSE 0::numeric
                       END
               END AS capacity
    FROM f
), h AS (
    SELECT g."from",
           g."to",
           g.token_owner,
           CASE
               WHEN g.to_is_orga OR g.is_to_own_token THEN g.max_transfer_amount
               ELSE
                   CASE
                       WHEN g.capacity < 0::numeric THEN 0::numeric
                       ELSE
                           CASE
                               WHEN g.from_balance < g.capacity THEN g.from_balance - 1::numeric
                               ELSE g.capacity - 1::numeric
                               END
                       END
               END AS capacity,
           g."limit"
    FROM g
    WHERE g."from" <> g."to"
)
SELECT h."from",
       h."to",
       h.token_owner,
       CASE
           WHEN h.capacity < 0::numeric THEN 0::numeric
           ELSE h.capacity
           END AS capacity,
       h."limit"
FROM h;



grant select on crc_capacity_graph_2 to pathfinder;

grant select on crc_capacity_graph_2 to readonly;

create or replace view crc_current_trust_2
            ("user", user_token, can_send_to, can_send_to_token, "limit", history_count, last_change) as
WITH cte AS (
    SELECT t.address AS "user",
           cs_a.token AS user_token,
           t.can_send_to,
           cs_b.token AS can_send_to_token,
           t."limit",
           0::bigint AS history_count,
           t."timestamp" AS last_change,
           row_number() OVER (PARTITION BY t.address, t.can_send_to ORDER BY t.block_number DESC, t.index DESC) AS row_no
    FROM crc_trust_2 t
             JOIN crc_all_signups cs_a ON t.address = cs_a."user"
             JOIN crc_all_signups cs_b ON t.can_send_to = cs_b."user"
)
SELECT cte."user",
       cte.user_token,
       cte.can_send_to,
       cte.can_send_to_token,
       cte."limit",
       cte.history_count,
       cte.last_change
FROM cte
WHERE cte.row_no = 1;



grant select on crc_current_trust_2 to indexer;

grant select on crc_current_trust_2 to pathfinder;

grant select on crc_current_trust_2 to readonly;

create or replace view crc_capacity_graph_3
            ("from", "to", token_owner, capacity, trust_last_change, from_balance_last_change,
             to_already_holds_balance_last_change, to_own_token_holdings_last_change)
as
WITH a AS (
    SELECT t.user_token,
           t.can_send_to,
           t.can_send_to_token,
           t."limit",
           b.number AS trust_last_change,
           os.organisation IS NOT NULL AS to_is_orga
    FROM crc_current_trust_2 t
             JOIN block b ON b."timestamp" = t.last_change
             LEFT JOIN crc_organisation_signup_2 os ON os.organisation = t.can_send_to
), b AS (
    SELECT bal.safe_address AS "from",
           a.can_send_to AS "to",
           a."limit",
           a.trust_last_change,
           bal.token_owner,
           bal.balance AS from_balance,
           bal.last_change_at_block AS from_balance_last_change,
           a.to_is_orga
    FROM a
             JOIN cache_crc_balances_by_safe_and_token bal ON bal.token = a.user_token
    WHERE bal.balance >= 0::numeric
), c AS (
    SELECT b."from",
           b."to",
           b."limit",
           b.trust_last_change,
           b.token_owner,
           b.from_balance,
           b.from_balance_last_change,
           b.to_is_orga,
           bal.balance AS to_own_token_holdings,
           bal.last_change_at_block AS to_own_token_holdings_last_change
    FROM b
             LEFT JOIN cache_crc_balances_by_safe_and_token bal ON bal.safe_address = b."to" AND bal.token_owner = b."to"
), d AS (
    SELECT c."from",
           c."to",
           c."limit",
           c.trust_last_change,
           c.token_owner,
           c.from_balance,
           c.from_balance_last_change,
           c.to_is_orga,
           c.to_own_token_holdings,
           c.to_own_token_holdings_last_change,
           CASE
               WHEN bal.balance IS NOT NULL THEN bal.balance
               ELSE 0::numeric
               END AS to_already_holds_balance,
           c."to" = c.token_owner AS is_to_own_token,
           bal.last_change_at_block AS to_already_holds_balance_last_change
    FROM c
             LEFT JOIN cache_crc_balances_by_safe_and_token bal ON bal.safe_address = c."to" AND bal.token_owner = c.token_owner
), e AS (
    SELECT d."from",
           d."to",
           d."limit",
           d.trust_last_change,
           d.token_owner,
           d.from_balance,
           d.from_balance_last_change,
           d.to_is_orga,
           d.to_own_token_holdings,
           d.to_own_token_holdings_last_change,
           d.to_already_holds_balance,
           d.is_to_own_token,
           d.to_already_holds_balance_last_change,
           CASE
               WHEN d.is_to_own_token OR d.to_is_orga THEN d.from_balance
               ELSE d.to_own_token_holdings * d."limit" / 100::numeric
               END AS max_transfer_amount
    FROM d
), f AS (
    SELECT e."from",
           e."to",
           e."limit",
           e.trust_last_change,
           e.token_owner,
           e.from_balance,
           e.from_balance_last_change,
           e.to_is_orga,
           e.to_own_token_holdings,
           e.to_own_token_holdings_last_change,
           e.to_already_holds_balance,
           e.is_to_own_token,
           e.to_already_holds_balance_last_change,
           e.max_transfer_amount,
           e.to_already_holds_balance * (100::numeric - e."limit") / 100::numeric AS dest_balance_scaled
    FROM e
), g AS (
    SELECT f."from",
           f."to",
           f."limit",
           f.trust_last_change,
           f.token_owner,
           f.from_balance,
           f.from_balance_last_change,
           f.to_is_orga,
           f.to_own_token_holdings,
           f.to_own_token_holdings_last_change,
           f.to_already_holds_balance,
           f.is_to_own_token,
           f.to_already_holds_balance_last_change,
           f.max_transfer_amount,
           f.dest_balance_scaled,
           CASE
               WHEN (f.max_transfer_amount - f.dest_balance_scaled) > f.from_balance THEN f.from_balance
               ELSE f.max_transfer_amount - f.dest_balance_scaled
               END AS capacity
    FROM f
)
SELECT g."from",
       g."to",
       g.token_owner,
       CASE
           WHEN g.capacity < 0::numeric THEN 0::numeric
           ELSE g.capacity
           END AS capacity,
       g.trust_last_change,
       g.from_balance_last_change,
       g.to_already_holds_balance_last_change,
       g.to_own_token_holdings_last_change
FROM g
WHERE g."from" <> g."to";



grant select on crc_capacity_graph_3 to readonly;

create or replace view crc_dead_accounts("to") as
SELECT tt."to"
FROM crc_token_transfer_2 tt
         JOIN transaction_2 t ON tt.hash = t.hash
         JOIN block b ON t.block_number = b.number
GROUP BY tt."to"
HAVING max(b."timestamp") < (now() - '90 days'::interval);



grant select on crc_dead_accounts to readonly;

create or replace view crc_hub_transfers_per_day(timestamp, transfers) as
SELECT b."timestamp"::date AS "timestamp",
       count(*) AS transfers
FROM crc_hub_transfer_2 s
         JOIN transaction_2 t ON s.hash = t.hash
         JOIN block b ON t.block_number = b.number
GROUP BY (b."timestamp"::date);



grant select on crc_hub_transfers_per_day to readonly;

create or replace view erc20_minting_2(timestamp, block_number, index, hash, "from", "to", token, value) as
SELECT erc20_transfer_2."timestamp",
       erc20_transfer_2.block_number,
       erc20_transfer_2.index,
       erc20_transfer_2.hash,
       erc20_transfer_2."from",
       erc20_transfer_2."to",
       erc20_transfer_2.token,
       erc20_transfer_2.value
FROM erc20_transfer_2
WHERE erc20_transfer_2."from" = '0x0000000000000000000000000000000000000000'::text;



grant select on erc20_minting_2 to readonly;

create or replace view crc_minting_2(timestamp, block_number, index, hash, "from", "to", token, value) as
SELECT tm."timestamp",
       tm.block_number,
       tm.index,
       tm.hash,
       tm."from",
       tm."to",
       tm.token,
       tm.value
FROM erc20_minting_2 tm
         JOIN crc_signup_2 s ON tm.token = s.token;


grant select on crc_minting_2 to readonly;

create or replace view crc_safe_accepted_crc (timestamp, safe_address, accepted_token, accepted_token_owner, "limit") as
WITH all_events AS (
    SELECT t_1."timestamp",
           t_1.can_send_to AS safe_address,
           s.token AS accepted_token,
           s."user" AS accepted_token_owner,
           t_1."limit"
    FROM crc_trust_2 t_1
             JOIN crc_signup_2 s ON s."user" = t_1.address
), latest_events AS (
    SELECT max(all_events."timestamp") AS last_change,
           all_events.safe_address,
           all_events.accepted_token,
           all_events.accepted_token_owner
    FROM all_events
    GROUP BY all_events.safe_address, all_events.accepted_token, all_events.accepted_token_owner
)
SELECT t."timestamp",
       l.safe_address,
       l.accepted_token,
       l.accepted_token_owner,
       t."limit"
FROM latest_events l
         JOIN crc_trust_2 t ON l.last_change = t."timestamp" AND t.can_send_to = l.safe_address AND t.address = l.accepted_token_owner;


grant select on crc_safe_accepted_crc to readonly;

create or replace view formatted_crc_hub_transfer(hash, index, timestamp, block_number, "from", "to", value) as
SELECT crc_hub_transfer_2.hash,
       crc_hub_transfer_2.index,
       crc_hub_transfer_2."timestamp",
       crc_hub_transfer_2.block_number,
       crc_hub_transfer_2."from",
       crc_hub_transfer_2."to",
       crc_hub_transfer_2.value::text AS value
FROM crc_hub_transfer_2;



grant select on formatted_crc_hub_transfer to readonly;

create or replace view formatted_crc_minting(timestamp, block_number, index, hash, "from", "to", token, value) as
SELECT crc_minting_2."timestamp",
       crc_minting_2.block_number,
       crc_minting_2.index,
       crc_minting_2.hash,
       crc_minting_2."from",
       crc_minting_2."to",
       crc_minting_2.token,
       crc_minting_2.value::text AS value
FROM crc_minting_2;



grant select on formatted_crc_minting to readonly;

create or replace view formatted_erc20_transfer(hash, index, timestamp, block_number, "from", "to", token, value) as
SELECT erc20_transfer_2.hash,
       erc20_transfer_2.index,
       erc20_transfer_2."timestamp",
       erc20_transfer_2.block_number,
       erc20_transfer_2."from",
       erc20_transfer_2."to",
       erc20_transfer_2.token,
       erc20_transfer_2.value::text AS value
FROM erc20_transfer_2;



grant select on formatted_erc20_transfer to readonly;

create or replace view formatted_eth_transfer(hash, index, timestamp, block_number, "from", "to", value) as
SELECT eth_transfer_2.hash,
       eth_transfer_2.index,
       eth_transfer_2."timestamp",
       eth_transfer_2.block_number,
       eth_transfer_2."from",
       eth_transfer_2."to",
       eth_transfer_2.value::text AS value
FROM eth_transfer_2;



grant select on formatted_eth_transfer to readonly;

create or replace view formatted_gnosis_safe_eth_transfer(hash, index, timestamp, block_number, initiator, "from", "to", value) as
SELECT gnosis_safe_eth_transfer_2.hash,
       gnosis_safe_eth_transfer_2.index,
       gnosis_safe_eth_transfer_2."timestamp",
       gnosis_safe_eth_transfer_2.block_number,
       gnosis_safe_eth_transfer_2.initiator,
       gnosis_safe_eth_transfer_2."from",
       gnosis_safe_eth_transfer_2."to",
       gnosis_safe_eth_transfer_2.value::text AS value
FROM gnosis_safe_eth_transfer_2;



grant select on formatted_gnosis_safe_eth_transfer to readonly;

create or replace view crc_safe_timeline_2
            (timestamp, block_number, transaction_index, transaction_hash, type, safe_address, contact_address,
             direction, value, obj)
as
WITH safe_timeline AS (
    SELECT cs."timestamp",
           cs.block_number,
           cs.index,
           cs.hash,
           'CrcSignup'::text AS type,
           cs."user",
           cs."user" AS contact_address,
           'self'::text AS direction,
           0::text AS value,
           row_to_json(cs.*) AS obj
    FROM crc_all_signups cs
    UNION ALL
    SELECT cht."timestamp",
           cht.block_number,
           cht.index,
           cht.hash,
           'CrcHubTransfer'::text AS type,
           crc_all_signups."user",
           CASE
               WHEN cht."from" = crc_all_signups."user" AND cht."to" = crc_all_signups."user" THEN cht."to"
               WHEN cht."from" = crc_all_signups."user" THEN cht."to"
               ELSE cht."from"
               END AS contact_address,
           CASE
               WHEN cht."from" = crc_all_signups."user" AND cht."to" = crc_all_signups."user" THEN 'self'::text
               WHEN cht."from" = crc_all_signups."user" THEN 'out'::text
               ELSE 'in'::text
               END AS direction,
           cht.value,
           ( SELECT json_agg(_steps.*) AS row_to_json
             FROM ( SELECT t_1.hash AS "transactionHash",
                           t_1."from",
                           t_1."to",
                           t_1.value::text AS flow,
                           ( SELECT json_agg(steps.*) AS transfers
                             FROM ( SELECT e20t."from",
                                           e20t."to",
                                           e20t.token,
                                           e20t.value::text AS value
                                    FROM crc_token_transfer_2 e20t
                                    WHERE e20t.hash = t_1.hash) steps) AS transfers
                    FROM crc_hub_transfer_2 t_1
                    WHERE t_1.hash = cht.hash) _steps) AS transitive_path
    FROM formatted_crc_hub_transfer cht
             JOIN crc_all_signups ON crc_all_signups."user" = cht."from" OR crc_all_signups."user" = cht."to"
    UNION ALL
    SELECT ct."timestamp",
           ct.block_number,
           ct.index,
           ct.hash,
           'CrcTrust'::text AS type,
           crc_all_signups."user",
           CASE
               WHEN ct.can_send_to = crc_all_signups."user" AND ct.address = crc_all_signups."user" THEN crc_all_signups."user"
               WHEN ct.can_send_to = crc_all_signups."user" THEN ct.address
               ELSE ct.can_send_to
               END AS contact_address,
           CASE
               WHEN ct.can_send_to = crc_all_signups."user" AND ct.address = crc_all_signups."user" THEN 'self'::text
               WHEN ct.can_send_to = crc_all_signups."user" THEN 'out'::text
               ELSE 'in'::text
               END AS direction,
           ct."limit"::text AS "limit",
           row_to_json(ct.*) AS obj
    FROM crc_trust_2 ct
             JOIN crc_all_signups ON crc_all_signups."user" = ct.address OR crc_all_signups."user" = ct.can_send_to
    UNION ALL
    SELECT ct."timestamp",
           ct.block_number,
           ct.index,
           ct.hash,
           'CrcMinting'::text AS type,
           crc_all_signups."user",
           ct."from" AS contact_address,
           'in'::text AS direction,
           ct.value,
           row_to_json(ct.*) AS obj
    FROM formatted_crc_minting ct
             JOIN crc_all_signups ON ct.token = crc_all_signups.token
    UNION ALL
    SELECT eth."timestamp",
           eth.block_number,
           eth.index,
           eth.hash,
           'EthTransfer'::text AS type,
           crc_all_signups."user",
           CASE
               WHEN eth."from" = crc_all_signups."user" AND eth."to" = crc_all_signups."user" THEN crc_all_signups."user"
               WHEN eth."from" = crc_all_signups."user" THEN eth."to"
               ELSE eth."from"
               END AS contact_address,
           CASE
               WHEN eth."from" = crc_all_signups."user" AND eth."to" = crc_all_signups."user" THEN 'self'::text
               WHEN eth."from" = crc_all_signups."user" THEN 'out'::text
               ELSE 'in'::text
               END AS direction,
           eth.value,
           row_to_json(eth.*) AS obj
    FROM formatted_eth_transfer eth
             JOIN crc_all_signups ON crc_all_signups."user" = eth."from" OR crc_all_signups."user" = eth."to"
    UNION ALL
    SELECT erc20."timestamp",
           erc20.block_number,
           erc20.index,
           erc20.hash,
           'Erc20Transfer'::text AS type,
           crc_all_signups."user",
           CASE
               WHEN erc20."from" = crc_all_signups."user" AND erc20."to" = crc_all_signups."user" THEN crc_all_signups."user"
               WHEN erc20."from" = crc_all_signups."user" THEN erc20."to"
               ELSE erc20."from"
               END AS contact_address,
           CASE
               WHEN erc20."from" = crc_all_signups."user" AND erc20."to" = crc_all_signups."user" THEN 'self'::text
               WHEN erc20."from" = crc_all_signups."user" THEN 'out'::text
               ELSE 'in'::text
               END AS direction,
           erc20.value,
           row_to_json(erc20.*) AS obj
    FROM formatted_erc20_transfer erc20
             JOIN crc_all_signups ON crc_all_signups."user" = erc20."from" OR crc_all_signups."user" = erc20."to"
             LEFT JOIN crc_signup_2 s ON s.token = erc20.token
    WHERE s.token IS NULL
    UNION ALL
    SELECT seth."timestamp",
           seth.block_number,
           seth.index,
           seth.hash,
           'GnosisSafeEthTransfer'::text AS type,
           crc_all_signups."user",
           CASE
               WHEN seth."from" = crc_all_signups."user" AND seth."to" = crc_all_signups."user" THEN crc_all_signups."user"
               WHEN seth."from" = crc_all_signups."user" THEN seth."to"
               ELSE seth."from"
               END AS contact_address,
           CASE
               WHEN seth."from" = crc_all_signups."user" AND seth."to" = crc_all_signups."user" THEN 'self'::text
               WHEN seth."from" = crc_all_signups."user" THEN 'out'::text
               ELSE 'in'::text
               END AS direction,
           seth.value,
           row_to_json(seth.*) AS obj
    FROM formatted_gnosis_safe_eth_transfer seth
             JOIN crc_all_signups ON crc_all_signups."user" = seth."from" OR crc_all_signups."user" = seth."to"
)
SELECT st."timestamp",
       st.block_number,
       st.index AS transaction_index,
       st.hash AS transaction_hash,
       st.type,
       st."user" AS safe_address,
       st.contact_address,
       st.direction,
       st.value,
       st.obj
FROM safe_timeline st;



grant select on crc_safe_timeline_2 to readonly;

create or replace view crc_signups_per_day(timestamp, signups) as
SELECT b."timestamp"::date AS "timestamp",
       count(*) AS signups
FROM crc_signup_2 s
         JOIN transaction_2 t ON s.hash = t.hash
         JOIN block b ON t.block_number = b.number
GROUP BY (b."timestamp"::date);



grant select on crc_signups_per_day to readonly;

create or replace view crc_total_minted_amount(total_crc_amount) as
SELECT sum(crc_token_transfer_2.value) AS total_crc_amount
FROM crc_token_transfer_2
WHERE crc_token_transfer_2."from" = '0x0000000000000000000000000000000000000000'::text;



grant select on crc_total_minted_amount to readonly;

create or replace view erc20_balances_by_safe_and_token(safe_address, token, balance, last_changed_at) as
WITH non_circles_transfers AS (
    SELECT et."timestamp",
           et.block_number,
           t.index AS transaction_index,
           t.hash AS transaction_hash,
           'Erc20Transfer'::text AS type,
           et.token,
           et."from",
           et."to",
           et.value
    FROM erc20_transfer_2 et
             JOIN crc_all_signups alls ON alls."user" = et."from" OR alls."user" = et."to"
             LEFT JOIN crc_signup_2 s ON s.token = et.token
             JOIN transaction_2 t ON et.hash = t.hash
    WHERE s.token IS NULL
), non_circles_ledger AS (
    SELECT nct."timestamp",
           nct.block_number,
           nct.transaction_index,
           nct.transaction_hash,
           nct.type,
           alls."user" AS safe_address,
           CASE
               WHEN nct."from" = alls."user" THEN nct."to"
               ELSE nct."from"
               END AS contact_address,
           CASE
               WHEN nct."from" = alls."user" THEN 'out'::text
               ELSE 'in'::text
               END AS direction,
           nct.token,
           nct."from",
           nct."to",
           nct.value
    FROM crc_all_signups alls
             JOIN non_circles_transfers nct ON alls."user" = nct."from" OR alls."user" = nct."to"
), erc20_balances AS (
    SELECT non_circles_ledger.safe_address,
           non_circles_ledger.token,
           sum(
                   CASE
                       WHEN non_circles_ledger.direction = 'in'::text THEN non_circles_ledger.value
                       ELSE non_circles_ledger.value * '-1'::integer::numeric
                       END) AS balance,
           max(non_circles_ledger."timestamp") AS last_changed_at
    FROM non_circles_ledger
    GROUP BY non_circles_ledger.safe_address, non_circles_ledger.token
)
SELECT erc20_balances.safe_address,
       erc20_balances.token,
       erc20_balances.balance,
       erc20_balances.last_changed_at
FROM erc20_balances;



grant select on erc20_balances_by_safe_and_token to readonly;

create or replace view first_incomplete_block(block_no) as
SELECT min(block.number) AS block_no
FROM block
WHERE block.total_transaction_count > block.indexed_transaction_count;



grant select on first_incomplete_block to readonly;

create or replace procedure delete_incomplete_blocks()
    language plpgsql
as
$$
declare
    first_corrupt_block bigint;
begin
    select block_no into first_corrupt_block  from first_incomplete_block;
    delete from crc_hub_transfer where transaction_id in (select id from transaction where block_number >= first_corrupt_block);
    delete from crc_organisation_signup where transaction_id in (select id from transaction where block_number >= first_corrupt_block);
    delete from crc_signup where transaction_id in (select id from transaction where block_number >= first_corrupt_block);
    delete from crc_trust where transaction_id in (select id from transaction where block_number >= first_corrupt_block);
    delete from erc20_transfer where transaction_id in (select id from transaction where block_number >= first_corrupt_block);
    delete from eth_transfer where transaction_id in (select id from transaction where block_number >= first_corrupt_block);
    delete from gnosis_safe_eth_transfer where transaction_id in (select id from transaction where block_number >= first_corrupt_block);
    delete from transaction where block_number >= first_corrupt_block;
    delete from block where number >= first_corrupt_block;
end
$$;



create or replace function get_capacity("from" text, "to" text, token_owner text)
    returns TABLE(id integer, label text, value numeric)
    language plpgsql
as
$$
declare
begin
    return query
        with args as (
            select 1 as id, 'token_owner''s balance of token_owner''s own tokens' as label, balance as value
            from cache_crc_balances_by_safe_and_token b
            where b.safe_address = get_capacity.token_owner
              and b.token_owner = get_capacity.token_owner
            union all
            select 2, 'receiver''s balance of token_owner''s tokens', balance
            from cache_crc_balances_by_safe_and_token b
            where b.safe_address = get_capacity.to
              and b.token_owner = get_capacity.token_owner
            union all
            select 3, 'senders''s balance of token_owner''s tokens', balance
            from cache_crc_balances_by_safe_and_token b
            where b.safe_address = get_capacity.from
              and b.token_owner = get_capacity.token_owner
            union all
            select 4, 'receiver''s trust in token_owner''s token (%)', "limit"
            from cache_crc_current_trust t
            where t.can_send_to = get_capacity.to
              and t."user" = get_capacity.token_owner
            union all
            select 5, 'receiver''s balance of receiver''s own tokens', balance
            from cache_crc_balances_by_safe_and_token b
            where b.safe_address = get_capacity.to
              and b.token_owner = get_capacity.to
        ), calc_1 as (
            select *
            from args
            union all
            select 6 as id, 'max' as label
                 , (select args.value from args where args.id  = 5)
                       * (select args.value from args where args.id  = 4)
                / 100
            union all
            select 7, 'destBalanceScaled'
                 , (select args.value from args where args.id  = 2)
                       * (100 - (select args.value from args where args.id  = 4))
                / 100
        ), calc_2 as (
            select *
            from calc_1
            union all
            select 8 as id, 'max < receiver''s balance of token_owner''s tokens'
                 , case when ((select calc_1.value from calc_1 where calc_1.id  = 6)
                < (select args.value from args where args.id  = 2)) then 1 else 0 end as value
            union all
            select 9 as id, 'sendLimit'
                 , case when ((select calc_1.value from calc_1 where calc_1.id  = 6)
                < (select args.value from args where args.id  = 2))
                            then 0
                        else (select calc_1.value from calc_1 where calc_1.id  = 6)
                            - (select calc_1.value from calc_1 where calc_1.id  = 7)
                end as value
        )
        select *
        from calc_2;
end;
$$;



create or replace function get_capacity_changes_since_block(since_block numeric)
    returns TABLE(token_holder text, token text, token_owner text, balance numeric, can_send_to text, can_send_to_is_orga boolean, capacity numeric)
    language plpgsql
as
$$
declare
begin
    RETURN QUERY
        WITH accepted_tokens AS (
            SELECT ct.can_send_to AS potential_token_receiver,
                   ct.user_token  AS accepted_token,
                   ct."user"      AS accepted_token_owner,
                   cas.token                           AS potential_token_receivers_own_token,
                   cas.token IS NULL                   AS potential_token_receiver_is_orga,
                   ct."limit",
                   ct.last_change_at_block
            FROM cache_crc_current_trust ct
                     JOIN crc_all_signups cas ON ct.can_send_to = cas."user"
        ), total_holdings AS (
            SELECT balances.safe_address                                                AS token_holder,
                   balances.balance,
                   accepted_tokens.accepted_token                                       AS token,
                   accepted_tokens.accepted_token_owner                                 AS token_owner,
                   accepted_tokens.potential_token_receiver                             AS can_send_to,
                   accepted_tokens.potential_token_receiver_is_orga                     AS can_send_to_is_orga,
                   accepted_tokens.potential_token_receivers_own_token = balances.token AS is_receivers_own_token,
                   accepted_tokens."limit",
                   balances.last_change_at_block                                        AS balance_last_change_at_block,
                   accepted_tokens.last_change_at_block                                 AS trust_last_change_at_block
            FROM accepted_tokens
                     JOIN cache_crc_balances_by_safe_and_token balances ON accepted_tokens.accepted_token = balances.token
            WHERE balances.safe_address <> '0x0000000000000000000000000000000000000000'::text
              AND balances.safe_address <> '0x0000000000000000000000000000000000000001'::text
              AND balances.balance > 0::numeric
              AND balances.safe_address <> accepted_tokens.potential_token_receiver
        ), with_token_owners AS (
            SELECT h.token_holder,
                   h.balance,
                   h.token,
                   h.can_send_to,
                   h.can_send_to_is_orga,
                   h.is_receivers_own_token,
                   h."limit",
                   s."user" AS token_owner,
                   h.balance_last_change_at_block,
                   h.trust_last_change_at_block
            FROM total_holdings h
                     LEFT JOIN crc_all_signups s ON s.token = h.token
        ), with_token_owner_balance AS (
            SELECT h.token_holder,
                   h.balance,
                   h.token,
                   h.can_send_to,
                   h.can_send_to_is_orga,
                   h.is_receivers_own_token,
                   h."limit",
                   h.token_owner,
                   h.balance_last_change_at_block,
                   h.trust_last_change_at_block,
                   COALESCE(b.balance, 0::numeric) AS token_owners_own_balance
            FROM with_token_owners h
                     LEFT JOIN cache_crc_balances_by_safe_and_token b ON h.token_owner = b.safe_address AND h.token = b.token
        ), with_max_transferable_amount AS (
            SELECT with_token_owner_balance.token_holder,
                   with_token_owner_balance.balance,
                   with_token_owner_balance.token,
                   with_token_owner_balance.can_send_to,
                   with_token_owner_balance.can_send_to_is_orga,
                   with_token_owner_balance.is_receivers_own_token,
                   with_token_owner_balance."limit",
                   with_token_owner_balance.token_owner,
                   with_token_owner_balance.balance_last_change_at_block,
                   with_token_owner_balance.trust_last_change_at_block,
                   with_token_owner_balance.token_owners_own_balance,
                   with_token_owner_balance.token_owners_own_balance *
                   with_token_owner_balance."limit" / 100::numeric AS max_transferable_amount
            FROM with_token_owner_balance
        ), with_receiver_balance AS (
            SELECT h.token_holder,
                   h.balance,
                   h.token,
                   h.can_send_to,
                   h.can_send_to_is_orga,
                   h.is_receivers_own_token,
                   h."limit",
                   h.token_owner,
                   h.balance_last_change_at_block,
                   h.trust_last_change_at_block,
                   h.token_owners_own_balance,
                   h.max_transferable_amount,
                   COALESCE(b.balance, 0::numeric)                                             AS receiver_token_balance,
                   COALESCE(b.balance, 0::numeric) * (100::numeric - h."limit") /
                   100::numeric                                                                AS receiver_token_balance_scaled
            FROM with_max_transferable_amount h
                     LEFT JOIN cache_crc_balances_by_safe_and_token b ON h.can_send_to = b.safe_address AND h.token = b.token
        ), max_capacity AS (
            SELECT with_receiver_balance.token_holder,
                   with_receiver_balance.balance,
                   with_receiver_balance.token,
                   with_receiver_balance.can_send_to,
                   with_receiver_balance.can_send_to_is_orga,
                   with_receiver_balance.is_receivers_own_token,
                   with_receiver_balance."limit",
                   with_receiver_balance.token_owner,
                   with_receiver_balance.balance_last_change_at_block,
                   with_receiver_balance.trust_last_change_at_block,
                   with_receiver_balance.token_owners_own_balance,
                   with_receiver_balance.max_transferable_amount,
                   with_receiver_balance.receiver_token_balance,
                   with_receiver_balance.receiver_token_balance_scaled,
                   with_receiver_balance.max_transferable_amount -
                   with_receiver_balance.receiver_token_balance_scaled AS max_capacity
            FROM with_receiver_balance
        ), final AS (
            SELECT max_capacity.token_holder,
                   max_capacity.balance,
                   max_capacity.token,
                   max_capacity.can_send_to,
                   max_capacity.can_send_to_is_orga,
                   max_capacity.is_receivers_own_token,
                   max_capacity."limit",
                   max_capacity.token_owner,
                   max_capacity.balance_last_change_at_block,
                   max_capacity.trust_last_change_at_block,
                   max_capacity.token_owners_own_balance,
                   max_capacity.max_transferable_amount,
                   max_capacity.receiver_token_balance,
                   max_capacity.receiver_token_balance_scaled,
                   max_capacity.max_capacity,
                   max_capacity.receiver_token_balance > 0::numeric AND
                   max_capacity.max_transferable_amount < max_capacity.receiver_token_balance AS zero,
                   CASE WHEN max_capacity.max_capacity < max_capacity.balance
                            THEN max_capacity.max_capacity
                        ELSE max_capacity.balance
                       END                                                                    AS actual_capacity
            FROM max_capacity
        )
        SELECT final.token_holder,
               final.token,
               final.token_owner,
               final.balance,
               final.can_send_to,
               final.can_send_to_is_orga,
               CASE
                   WHEN final.is_receivers_own_token OR final.can_send_to_is_orga THEN final.balance
                   ELSE
                       CASE WHEN final.zero
                                THEN 0::numeric
                            ELSE final.actual_capacity
                           END
                   END AS capacity
        FROM final
        where balance_last_change_at_block >= since_block
           or trust_last_change_at_block >= since_block;
end;
$$;



create or replace function get_capacity_changes_since_block_2(since_block numeric)
    returns TABLE(token_holder text, token_owner text, can_send_to text, capacity numeric)
    language plpgsql
as
$$
declare
begin
    return query
        WITH a AS (
            SELECT crc_current_trust_2.user_token,
                   crc_current_trust_2.can_send_to,
                   crc_current_trust_2.can_send_to_token,
                   crc_current_trust_2."limit",
                   crc_current_trust_2.last_change_at_block as trust_last_change
            FROM cache_crc_current_trust crc_current_trust_2
        ), b AS (
            SELECT bal.safe_address AS "from",
                   a.can_send_to AS "to",
                   a."limit",
                   a.trust_last_change,
                   bal.token_owner,
                   bal.balance AS from_balance,
                   bal.last_change_at_block as from_balance_last_change
            FROM a
                     JOIN cache_crc_balances_by_safe_and_token bal ON bal.token = a.user_token
            WHERE bal.balance >= 0::numeric
        ), c AS (
            SELECT b."from",
                   b."to",
                   b."limit",
                   b.token_owner,
                   b.from_balance,
                   bal.balance AS to_own_token_holdings
            FROM b
                     LEFT JOIN cache_crc_balances_by_safe_and_token bal ON bal.safe_address = b."to" AND bal.token_owner = b."to"
            where b.trust_last_change >= since_block
               or b.from_balance_last_change >= since_block
        ), d AS (
            SELECT c."from",
                   c."to",
                   c."limit",
                   c.token_owner,
                   c.from_balance,
                   c.to_own_token_holdings,
                   CASE
                       WHEN bal.balance IS NOT NULL THEN bal.balance
                       ELSE 0::numeric
                       END AS to_already_holds_balance,
                   os.organisation IS NOT NULL AS to_is_orga,
                   c."to" = c.token_owner AS is_to_own_token
            FROM c
                     LEFT JOIN cache_crc_balances_by_safe_and_token bal ON bal.safe_address = c."to" AND bal.token_owner = c.token_owner
                     LEFT JOIN crc_organisation_signup_2 os ON os.organisation = c."to"
        ), e AS (
            SELECT d."from",
                   d."to",
                   d."limit",
                   d.token_owner,
                   d.from_balance,
                   d.to_own_token_holdings,
                   d.to_already_holds_balance,
                   d.to_is_orga,
                   d.is_to_own_token,
                   CASE
                       WHEN d.is_to_own_token OR d.to_is_orga THEN d.from_balance
                       ELSE d.to_own_token_holdings * d."limit" / 100::numeric
                       END AS max_transfer_amount
            FROM d
        ), f AS (
            SELECT e."from",
                   e."to",
                   e."limit",
                   e.token_owner,
                   e.from_balance,
                   e.to_own_token_holdings,
                   e.to_already_holds_balance,
                   e.to_is_orga,
                   e.is_to_own_token,
                   CASE
                       WHEN e."limit" > 0::numeric THEN e.max_transfer_amount
                       ELSE 0::numeric
                       END AS max_transfer_amount,
                   e.to_already_holds_balance * (100::numeric - e."limit") / 100::numeric AS dest_balance_scaled
            FROM e
        ), g AS (
            SELECT f."from",
                   f."to",
                   f."limit",
                   f.token_owner,
                   f.from_balance,
                   f.to_own_token_holdings,
                   f.to_already_holds_balance,
                   f.to_is_orga,
                   f.is_to_own_token,
                   f.max_transfer_amount,
                   f.dest_balance_scaled,
                   CASE
                       WHEN f.max_transfer_amount < f.to_already_holds_balance THEN 0::numeric
                       ELSE
                           CASE
                               WHEN f."limit" > 0::numeric THEN f.max_transfer_amount - f.dest_balance_scaled
                               ELSE 0::numeric
                               END
                       END AS capacity
            FROM f
        ), h as (
            -- (token_holder text, token_owner text, can_send_to text, capacity numeric)
            SELECT g."from",
                   g.token_owner,
                   g."to",
                   CASE
                       WHEN g.to_is_orga OR g.is_to_own_token THEN g.max_transfer_amount
                       ELSE
                           CASE
                               WHEN g.capacity < 0::numeric THEN 0::numeric
                               ELSE
                                   CASE
                                       WHEN g.from_balance < g.capacity THEN g.from_balance - 1::numeric
                                       ELSE g.capacity - 1::numeric
                                       END
                               END
                       END AS capacity
            FROM g
            WHERE g."from" <> g."to"
        )
        select h."from"
             , h.token_owner
             , h."to"
             , case when h.capacity < 0 then 0 else h.capacity end as capactiy
        from h;

end;
$$;



grant execute on function get_capacity_changes_since_block_2(numeric) to pathfinder;

create or replace procedure import_from_staging_2()
    language plpgsql
as
$$
declare
    selected_at_ts timestamp;
begin

    -- Cleanup all duplicate blocks (duplicate number, different hash)
    -- and leave only the newer blocks.
    drop table if exists disambiguated_blocks;
    create temp table disambiguated_blocks
    as
    select number, max(distinct timestamp) as timestamp
    from _block_staging
    group by number
    having count(distinct timestamp) > 1;

    delete from _crc_hub_transfer_staging b
        using disambiguated_blocks d
    where b.block_number = d.number
      and b.timestamp < d.timestamp;

    delete from _crc_organisation_signup_staging b
        using disambiguated_blocks d
    where b.block_number = d.number
      and b.timestamp < d.timestamp;

    delete from _crc_signup_staging b
        using disambiguated_blocks d
    where b.block_number = d.number
      and b.timestamp < d.timestamp;

    delete from _crc_trust_staging b
        using disambiguated_blocks d
    where b.block_number = d.number
      and b.timestamp < d.timestamp;

    delete from _erc20_transfer_staging b
        using disambiguated_blocks d
    where b.block_number = d.number
      and b.timestamp < d.timestamp;

    delete from _eth_transfer_staging b
        using disambiguated_blocks d
    where b.block_number = d.number
      and b.timestamp < d.timestamp;

    delete from _gnosis_safe_eth_transfer_staging b
        using disambiguated_blocks d
    where b.block_number = d.number
      and b.timestamp < d.timestamp;

    delete from _transaction_staging b
        using disambiguated_blocks d
    where b.block_number = d.number
      and b.timestamp < d.timestamp;

    delete from _block_staging b
        using disambiguated_blocks d
    where b.number = d.number
      and b.timestamp < d.timestamp;

    select now() into selected_at_ts;

-- Set 'selected_at' of all complete staging blocks
    with complete_staging_blocks as (
        select bs.number, bs.total_transaction_count, count(distinct ts.hash)
        from _block_staging bs
                 left join _transaction_staging ts on bs.number = ts.block_number
        group by bs.number, bs.total_transaction_count
        having count(distinct ts.hash) = bs.total_transaction_count
    )
    update _block_staging bs
    set selected_at = selected_at_ts
    from complete_staging_blocks csb
    where bs.selected_at is null
      and bs.already_available is null
      and bs.imported_at is null
      and bs.number = csb.number;

    -- Set 'already_available' and remove 'selected_at' on all selected entries which are already
-- completely imported.
    with completed_blocks as (
        select b.number, b.total_transaction_count, count(distinct t.hash)
        from _block_staging bs
                 join block b on bs.number = b.number
                 left join transaction_2 t on b.number = t.block_number
        group by b.number, b.total_transaction_count
        having count(distinct t.hash) = b.total_transaction_count
    )
    update _block_staging bs
    set already_available = true,
        selected_at = null
    from completed_blocks
    where bs.number = completed_blocks.number;

-- insert all selected blocks
    insert into block
    select distinct sb.number, sb.hash, sb.timestamp, sb.total_transaction_count, 0 as indexed_transaction_count
    from _block_staging sb
    where sb.selected_at = selected_at_ts
    on conflict do nothing;

-- insert all transactions of all selected blocks
    insert into transaction_2
    select distinct ts2.block_number
                  , ts2."from"
                  , ts2."to"
                  , ts2.hash
                  , ts2.index
                  , ts2.timestamp
                  , ts2.value::numeric
                  , ts2.input
                  , ts2.nonce
                  , ts2.type
                  , ts2.classification
    from _block_staging sb
             join _transaction_staging ts2 on sb.number= ts2.block_number
    where sb.selected_at = selected_at_ts
    on conflict do nothing;

    insert into crc_hub_transfer_2
    select distinct ts2.hash
                  , ts2.index
                  , ts2.timestamp
                  , ts2.block_number
                  , ts2."from"
                  , ts2."to"
                  , ts2.value::numeric
    from _block_staging sb
             join _crc_hub_transfer_staging ts2 on sb.number= ts2.block_number
        and sb.selected_at = selected_at_ts
    on conflict do nothing;

    insert into crc_organisation_signup_2
    select distinct ts2.hash
                  , ts2.index
                  , ts2.timestamp
                  , ts2.block_number
                  , ts2.organisation
                  , ts2.owners
    from _block_staging sb
             join _crc_organisation_signup_staging ts2 on sb.number = ts2.block_number
        and sb.selected_at = selected_at_ts
    on conflict do nothing;

    insert into crc_signup_2
    select distinct ts2.hash
                  , ts2.index
                  , ts2.timestamp
                  , ts2.block_number
                  , ts2."user"
                  , ts2.token
                  , ts2.owners
    from _block_staging sb
             join _crc_signup_staging ts2 on sb.number = ts2.block_number
        and sb.selected_at = selected_at_ts
    on conflict do nothing;

    insert into cache_all_addresses  (type, address)
    select 'token', token
    from _crc_signup_staging
    union all
    select 'safe', "user"
    from _crc_signup_staging
    union all
    select 'safe', organisation
    from _crc_organisation_signup_staging
    on conflict do nothing;

    insert into crc_trust_2
    select distinct ts2.hash
                  , ts2.index
                  , ts2.timestamp
                  , ts2.block_number
                  , ts2.address
                  , ts2.can_send_to
                  , ts2."limit"
    from _block_staging sb
             join _crc_trust_staging ts2 on sb.number = ts2.block_number
        and sb.selected_at = selected_at_ts
    on conflict do nothing;

    insert into erc20_transfer_2
    select distinct ts2.hash
                  , ts2.index
                  , ts2.timestamp
                  , ts2.block_number
                  , ts2."from"
                  , ts2."to"
                  , ts2.token
                  , ts2.value::numeric
    from _block_staging sb
             join _erc20_transfer_staging ts2 on sb.number = ts2.block_number
        and sb.selected_at = selected_at_ts
    on conflict do nothing;

    insert into eth_transfer_2
    select distinct ts2.hash
                  , ts2.index
                  , ts2.timestamp
                  , ts2.block_number
                  , ts2."from"
                  , ts2."to"
                  , ts2.value::numeric
    from _block_staging sb
             join _eth_transfer_staging ts2 on sb.number = ts2.block_number
        and sb.selected_at = selected_at_ts
    on conflict do nothing;

    insert into gnosis_safe_eth_transfer_2
    select distinct ts2.hash
                  , ts2.index
                  , ts2.timestamp
                  , ts2.block_number
                  , ts2.initiator
                  , ts2."from"
                  , ts2."to"
                  , ts2.value::numeric
    from _block_staging sb
             join _gnosis_safe_eth_transfer_staging ts2 on sb.number = ts2.block_number
        and sb.selected_at = selected_at_ts
    on conflict do nothing;

    update _block_staging
    set
        imported_at = now()
      , selected_at = null
    where selected_at = selected_at_ts
       or already_available is not null;

    --
-- Take care of possibly outdated cached crc balances
--
    create temporary table stale_cached_balances as
    select "from" as safe_address
    from _erc20_transfer_staging
             join crc_all_signups cas on _erc20_transfer_staging."from" = cas."user"
    union
    select "to" as safe_address
    from _erc20_transfer_staging
             join crc_all_signups cas on _erc20_transfer_staging."to" = cas."user"
    union
    select "from" as safe_address
    from _crc_hub_transfer_staging
             join crc_all_signups cas on _crc_hub_transfer_staging."from" = cas."user"
    union
    select "to" as safe_address
    from _crc_hub_transfer_staging
             join crc_all_signups cas on _crc_hub_transfer_staging."to" = cas."user";

    create unique index t_ux_stale_cached_balances_safe_address on stale_cached_balances(safe_address);

    delete from cache_crc_balances_by_safe_and_token c
        using stale_cached_balances s
    where s.safe_address = c.safe_address;

    insert into cache_crc_balances_by_safe_and_token (
                                                       safe_address
                                                     , token
                                                     , token_owner
                                                     , balance
                                                     , last_change_at
                                                     , safe_address_id
                                                     , token_address_id
                                                     , token_owner_address_id
                                                     , last_change_at_block
    )
    select b.safe_address
         , token
         , token_owner
         , balance
         , b.last_change_at
         , a1.id as safe_address_id
         , a2.id as token_address_id
         , a3.id as token_owner_address_id
         , bl.number as last_change_at_block
    from crc_balances_by_safe_and_token_2 b
             left join cache_all_addresses a1 on a1.address = b.safe_address
             left join cache_all_addresses a2 on a2.address = b.token
             left join cache_all_addresses a3 on a3.address = b.token_owner
             join block bl on bl.timestamp = b.last_change_at
    where b.safe_address = ANY((select array_agg(safe_address) from stale_cached_balances)::text[]);

    drop table stale_cached_balances;

    create temporary table stale_cached_trust_relations as
    select address safe_address
    from _crc_trust_staging
    union
    select can_send_to safe_address
    from _crc_trust_staging;

    create unique index t_ux_stale_cached_trust_relations_safe_address on stale_cached_trust_relations(safe_address);

    delete from cache_crc_current_trust c
        using stale_cached_trust_relations s
    where s.safe_address = c."user"
       or  s.safe_address = c."can_send_to";

    insert into cache_crc_current_trust (
                                          "user"
                                        , user_token
                                        , can_send_to
                                        , can_send_to_token
                                        , "limit"
                                        , history_count
                                        , last_change
                                        , user_address_id
                                        , user_token_address_id
                                        , can_send_to_address_id
                                        , can_send_to_token_address_id
                                        , last_change_at_block
    )
    select b."user"
         , b.user_token
         , b.can_send_to
         , b.can_send_to_token
         , b."limit"
         , b.history_count
         , b.last_change
         , a1.id as user_address_id
         , a2.id as user_token_address_id
         , a3.id as can_send_to_address_id
         , a4.id as can_send_to_token_address_id
         , bl.number as last_change_at_block
    from crc_current_trust_2 b
             left join cache_all_addresses a1 on a1.address = b."user"
             left join cache_all_addresses a2 on a2.address = b."user_token"
             left join cache_all_addresses a3 on a3.address = b."can_send_to"
             left join cache_all_addresses a4 on a4.address = b."can_send_to_token"
             join block bl on bl.timestamp = b.last_change
    where b."user" = ANY((select array_agg(safe_address) from stale_cached_trust_relations)::text[])
       or b."can_send_to" = ANY((select array_agg(safe_address) from stale_cached_trust_relations)::text[]);

    drop table stale_cached_trust_relations;

end;
$$;


grant execute on procedure import_from_staging_2() to indexer;

create or replace procedure publish_event(IN topic text, IN message text)
    language plpgsql
as
$$
begin
    perform pg_notify(topic, message::text);
end
$$;

create table version (
                         date timestamp with time zone not null,
                         version text not null,
                         comment text not null
);

insert into version (date, version, comment) values (now(), '0.0.64', 'Initial schema version');
