begin transaction;

create table db_version (
                            version text unique primary key,
                            description text
);

create table _block_staging
(
    number                  bigint,
    hash                    text,
    timestamp               timestamp,
    total_transaction_count integer,
    selected_at             timestamp,
    imported_at             timestamp,
    already_available       boolean
);

create index ix_block_staging_imported_at
    on _block_staging (imported_at) include (hash, number, total_transaction_count);

create index ix_block_staging_number
    on _block_staging (number) include (hash, selected_at, total_transaction_count);

create index ix_block_staging_selected_at_
    on _block_staging (selected_at) include (hash, number, total_transaction_count);


create table _crc_hub_transfer_staging
(
    hash         text,
    index        integer,
    timestamp    timestamp,
    block_number bigint,
    "from"       text,
    "to"         text,
    value        text
);

create index ix_crc_hub_transfer_staging_hash
    on _crc_hub_transfer_staging (hash) include (block_number);

create table _crc_organisation_signup_staging
(
    hash         text,
    index        integer,
    timestamp    timestamp,
    block_number bigint,
    organisation text,
    owners       text[]
);

create index ix_crc_organisation_signup_staging_hash
    on _crc_organisation_signup_staging (hash) include (block_number);

create table _crc_signup_staging
(
    hash         text,
    index        integer,
    timestamp    timestamp,
    block_number bigint,
    "user"       text,
    token        text,
    owners       text[]
);

create index ix_crc_signup_staging_hash
    on _crc_signup_staging (hash) include (block_number);

create table _crc_trust_staging
(
    hash         text,
    index        integer,
    timestamp    timestamp,
    block_number bigint,
    address      text,
    can_send_to  text,
    "limit"      numeric
);

create index ix_crc_trust_staging_hash
    on _crc_trust_staging (hash) include (block_number);

create table _erc20_transfer_staging
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

create index ix_erc20_transfer_staging_from
    on _erc20_transfer_staging ("from");

create index ix_erc20_transfer_staging_hash
    on _erc20_transfer_staging (hash) include (block_number);

create index ix_erc20_transfer_staging_to
    on _erc20_transfer_staging ("to");

create table _eth_transfer_staging
(
    hash         text,
    index        integer,
    timestamp    timestamp,
    block_number bigint,
    "from"       text,
    "to"         text,
    value        text
);

create index ix_eth_transfer_staging_from
    on _eth_transfer_staging ("from");

create index ix_eth_transfer_staging_hash
    on _eth_transfer_staging (hash) include (block_number);

create index ix_eth_transfer_staging_to
    on _eth_transfer_staging ("to");

create table _gnosis_safe_eth_transfer_staging
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

create index ix_gnosis_safe_eth_transfer_staging_from
    on _gnosis_safe_eth_transfer_staging ("from");

create index ix_gnosis_safe_eth_transfer_staging_hash
    on _gnosis_safe_eth_transfer_staging (hash) include (block_number);

create index ix_gnosis_safe_eth_transfer_staging_to
    on _gnosis_safe_eth_transfer_staging ("to");

create table _transaction_staging
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

create index ix_transaction_staging_block_number
    on _transaction_staging (block_number) include (hash);

create index ix_transaction_staging_hash
    on _transaction_staging (hash) include (block_number);

create table block
(
    number                    bigserial
        primary key,
    hash                      text      not null
        unique,
    timestamp                 timestamp not null,
    total_transaction_count   integer   not null,
    indexed_transaction_count integer   not null
);

create unique index idx_block_timestamp
    on block (timestamp) include (number);

create unique index ux_block_number
    on block (number) include (timestamp);


create table transaction_2
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

create table crc_signup_2
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

create index ix_crc_signup_2_block_number
    on crc_signup_2 (block_number) include (index, timestamp);

create index ix_crc_signup_2_hash
    on crc_signup_2 (hash) include (block_number, index, timestamp);

create index ix_crc_signup_2_timestamp
    on crc_signup_2 (timestamp) include (hash, block_number, index, timestamp);

create index ix_gin_crc_signup_2_owners
    on crc_signup_2 using gin (owners);

create unique index ux_crc_signup_2_token
    on crc_signup_2 (token) include ("user");

create unique index ux_crc_signup_2_user
    on crc_signup_2 ("user") include (token);

create table erc20_transfer_2
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

create index ix_erc20_transfer_2_block_number
    on erc20_transfer_2 (block_number) include (index, timestamp);

create index ix_erc20_transfer_2_from
    on erc20_transfer_2 ("from") include ("to", token, value);

create index ix_erc20_transfer_2_hash
    on erc20_transfer_2 (hash) include (block_number, index, timestamp);

create index ix_erc20_transfer_2_timestamp
    on erc20_transfer_2 (timestamp) include (hash, block_number, index, timestamp);

create index ix_erc20_transfer_2_to
    on erc20_transfer_2 ("to") include ("from", token, value);

create index ix_erc20_transfer_2_token
    on erc20_transfer_2 (token);

create unique index ux_erc20_transfer_2_hash_from_to_token_value
    on erc20_transfer_2 (hash, "from", "to", token, value);


create index ix_transaction_2_from
    on transaction_2 ("from") include ("to", value);

create index ix_transaction_2_timestamp
    on transaction_2 (timestamp) include (hash, block_number, index, timestamp);

create index ix_transaction_2_to
    on transaction_2 ("to") include ("from", value);

create unique index ux_transaction_2_block_number_index
    on transaction_2 (block_number, index);

create table crc_organisation_signup_2
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

create index ix_crc_organisation_signup_2_block_number
    on crc_organisation_signup_2 (block_number) include (index, timestamp);

create index ix_crc_organisation_signup_2_hash
    on crc_organisation_signup_2 (hash) include (block_number, index, timestamp);

create index ix_crc_organisation_signup_2_timestamp
    on crc_organisation_signup_2 (timestamp) include (hash, block_number, index, timestamp);

create index ix_gin_crc_organisation_signup_2_owners
    on crc_organisation_signup_2 using gin (owners);

create unique index ux_crc_organisation_signup_2_organisation
    on crc_organisation_signup_2 (organisation);

create table crc_trust_2
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

create index ix_crc_trust_2_address
    on crc_trust_2 (address) include (can_send_to, "limit");

create index ix_crc_trust_2_block_number
    on crc_trust_2 (block_number) include (index, timestamp);

create index ix_crc_trust_2_can_send_to
    on crc_trust_2 (can_send_to) include (address, "limit");

create index ix_crc_trust_2_hash
    on crc_trust_2 (hash) include (block_number, index, timestamp);

create index ix_crc_trust_2_timestamp
    on crc_trust_2 (timestamp) include (hash, block_number, index, timestamp);

create unique index ux_crc_trust_2_hash_address_can_send_to_limit
    on crc_trust_2 (hash, address, can_send_to, "limit");

create table crc_hub_transfer_2
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

create index ix_crc_hub_transfer_2_block_number
    on crc_hub_transfer_2 (block_number) include (index, timestamp);

create index ix_crc_hub_transfer_2_from
    on crc_hub_transfer_2 ("from") include ("to", value);

create index ix_crc_hub_transfer_2_hash
    on crc_hub_transfer_2 (hash) include (block_number, index, timestamp);

create index ix_crc_hub_transfer_2_timestamp
    on crc_hub_transfer_2 (timestamp) include (hash, block_number, index, timestamp);

create index ix_crc_hub_transfer_2_to
    on crc_hub_transfer_2 ("to") include ("from", value);

create unique index ux_crc_hub_transfer_2_hash_from_to_value
    on crc_hub_transfer_2 (hash, "from", "to", value);

create table eth_transfer_2
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

create index ix_eth_transfer_2_block_number
    on eth_transfer_2 (block_number) include (index, timestamp);

create index ix_eth_transfer_2_from
    on eth_transfer_2 ("from") include ("to", value);

create index ix_eth_transfer_2_hash
    on eth_transfer_2 (hash) include (block_number, index, timestamp);

create index ix_eth_transfer_2_timestamp
    on eth_transfer_2 (timestamp) include (hash, block_number, index, timestamp);

create index ix_eth_transfer_2_to
    on eth_transfer_2 ("to") include ("from", value);

create unique index ux_eth_transfer_2_hash_from_to_value
    on eth_transfer_2 (hash, "from", "to", value);

create table gnosis_safe_eth_transfer_2
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

create index ix_gnosis_safe_eth_transfer_2_block_number
    on gnosis_safe_eth_transfer_2 (block_number) include (index, timestamp);

create index ix_gnosis_safe_eth_transfer_2_from
    on gnosis_safe_eth_transfer_2 ("from") include ("to", value);

create index ix_gnosis_safe_eth_transfer_2_hash
    on gnosis_safe_eth_transfer_2 (hash) include (block_number, index, timestamp);

create index ix_gnosis_safe_eth_transfer_2_initiator
    on gnosis_safe_eth_transfer_2 (initiator);

create index ix_gnosis_safe_eth_transfer_2_timestamp
    on gnosis_safe_eth_transfer_2 (timestamp) include (hash, block_number, index, timestamp);

create index ix_gnosis_safe_eth_transfer_2_to
    on gnosis_safe_eth_transfer_2 ("to") include ("from", value);

create unique index ux_gnosis_safe_eth_transfer_2_hash_from_to_value
    on gnosis_safe_eth_transfer_2 (hash, initiator, "from", "to", value);

create table requested_blocks
(
    block_no numeric
);

create unique index ux_requested_blocks_block_no
    on requested_blocks (block_no);

create table cache_crc_current_trust
(
    "user"                       text,
    user_token                   text,
    can_send_to                  text,
    can_send_to_token            text,
    "limit"                      numeric,
    history_count                bigint,
    last_change                  timestamp,
    last_change_at_block         numeric
);

create index ix_cache_crc_current_trust_user
    on cache_crc_current_trust ("user");

create index ix_cache_crc_current_trust_last_change
    on cache_crc_current_trust (last_change);

create index ix_cache_crc_current_trust_user_token
    on cache_crc_current_trust (user_token);

create index ix_cache_crc_current_trust_limit
    on cache_crc_current_trust ("limit");

create index ix_cache_crc_current_trust_can_send_to
    on cache_crc_current_trust (can_send_to) include (user_token, "limit", last_change_at_block);

create index ix_cache_crc_current_trust_last_change_at_block
    on cache_crc_current_trust (last_change_at_block desc);



create table cache_crc_balances_by_safe_and_token
(
    safe_address           text,
    token                  text,
    token_owner            text,
    balance                numeric,
    last_change_at         timestamp,
    last_change_at_block   numeric
);

create index ix_cache_crc_balances_by_safe_and_token_safe_address
    on cache_crc_balances_by_safe_and_token (safe_address, token_owner) include (balance, last_change_at_block);

create index ix_cache_crc_balances_by_safe_and_token_token_owner
    on cache_crc_balances_by_safe_and_token (token_owner);

create index ix_cache_crc_balances_by_safe_and_token_last_change_at
    on cache_crc_balances_by_safe_and_token (last_change_at);

create index ix_cache_crc_balances_by_safe_and_token_last_change_on_block
    on cache_crc_balances_by_safe_and_token (last_change_at_block);

create index ix_cache_crc_balances_by_safe_and_token_token
    on cache_crc_balances_by_safe_and_token (token) include (safe_address, token_owner, balance, last_change_at_block);


create view crc_token_transfer_2(timestamp, block_number, index, hash, "from", "to", token, value) as
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

create view crc_alive_accounts("to") as
SELECT tt."to"
FROM crc_token_transfer_2 tt
         JOIN transaction_2 t ON tt.hash = t.hash
         JOIN block b ON t.block_number = b.number
GROUP BY tt."to"
HAVING max(b."timestamp") > (now() - '90 days'::interval);


create view crc_all_signups(hash, block_number, index, timestamp, "user", token) as
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
       NULL::text     AS token
FROM crc_organisation_signup_2 c;

create view crc_ledger_2 (timestamp, transaction_id, verb, value, token, token_owner, predicate, safe_address) as
WITH ledger AS (SELECT t_1.hash,
                       t_1.block_number,
                       t_1."timestamp",
                       'add'::text    AS verb,
                       sum(t_1.value) AS value,
                       t_1.token,
                       cs."user"      AS token_owner,
                       'to'::text     AS predicate,
                       t_1."to"       AS safe_address
                FROM erc20_transfer_2 t_1
                         JOIN crc_signup_2 cs ON t_1.token = cs.token
                GROUP BY t_1.hash, t_1.block_number, t_1."timestamp", t_1."to", t_1.token, cs."user"
                UNION
                SELECT t_1.hash,
                       t_1.block_number,
                       t_1."timestamp",
                       'remove'::text   AS verb,
                       - sum(t_1.value) AS value,
                       t_1.token,
                       cs."user"        AS token_owner,
                       'from'::text     AS predicate,
                       t_1."from"       AS safe_address
                FROM erc20_transfer_2 t_1
                         JOIN crc_signup_2 cs ON t_1.token = cs.token
                GROUP BY t_1.hash, t_1.block_number, t_1."timestamp", t_1."from", t_1.token, cs."user")
SELECT l."timestamp",
       l.hash AS transaction_id,
       l.verb,
       l.value,
       l.token,
       l.token_owner,
       l.predicate,
       l.safe_address
FROM ledger l
ORDER BY l."timestamp", l.token, l.verb DESC;


create view crc_balances_by_safe_2(safe_address, balance) as
SELECT crc_ledger_2.safe_address,
       sum(crc_ledger_2.value) AS balance
FROM crc_ledger_2
GROUP BY crc_ledger_2.safe_address
ORDER BY crc_ledger_2.safe_address;


create view crc_balances_by_safe_and_token_2(safe_address, token, token_owner, balance, last_change_at) as
SELECT crc_ledger_2.safe_address,
       crc_ledger_2.token,
       crc_ledger_2.token_owner,
       sum(crc_ledger_2.value)       AS balance,
       max(crc_ledger_2."timestamp") AS last_change_at
FROM crc_ledger_2
GROUP BY crc_ledger_2.safe_address, crc_ledger_2.token, crc_ledger_2.token_owner
ORDER BY crc_ledger_2.safe_address, (sum(crc_ledger_2.value)) DESC;


create view crc_current_trust_2
            ("user", user_token, can_send_to, can_send_to_token, "limit", history_count, last_change) as
WITH cte AS (SELECT t.address                                                                               AS "user",
                    cs_a.token                                                                              AS user_token,
                    t.can_send_to,
                    cs_b.token                                                                              AS can_send_to_token,
                    t."limit",
                    0::bigint                                                                               AS history_count,
                    t."timestamp"                                                                           AS last_change,
                    row_number()
                    OVER (PARTITION BY t.address, t.can_send_to ORDER BY t.block_number DESC, t.index DESC) AS row_no
             FROM crc_trust_2 t
                      JOIN crc_all_signups cs_a ON t.address = cs_a."user"
                      JOIN crc_all_signups cs_b ON t.can_send_to = cs_b."user")
SELECT cte."user",
       cte.user_token,
       cte.can_send_to,
       cte.can_send_to_token,
       cte."limit",
       cte.history_count,
       cte.last_change
FROM cte
WHERE cte.row_no = 1;



create view crc_dead_accounts("to") as
SELECT tt."to"
FROM crc_token_transfer_2 tt
         JOIN transaction_2 t ON tt.hash = t.hash
         JOIN block b ON t.block_number = b.number
GROUP BY tt."to"
HAVING max(b."timestamp") < (now() - '90 days'::interval);

create view erc20_minting_2(timestamp, block_number, index, hash, "from", "to", token, value) as
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



create view crc_minting_2(timestamp, block_number, index, hash, "from", "to", token, value) as
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

create view formatted_crc_hub_transfer(hash, index, timestamp, block_number, "from", "to", value) as
SELECT crc_hub_transfer_2.hash,
       crc_hub_transfer_2.index,
       crc_hub_transfer_2."timestamp",
       crc_hub_transfer_2.block_number,
       crc_hub_transfer_2."from",
       crc_hub_transfer_2."to",
       crc_hub_transfer_2.value::text AS value
FROM crc_hub_transfer_2;

create view formatted_crc_minting(timestamp, block_number, index, hash, "from", "to", token, value) as
SELECT crc_minting_2."timestamp",
       crc_minting_2.block_number,
       crc_minting_2.index,
       crc_minting_2.hash,
       crc_minting_2."from",
       crc_minting_2."to",
       crc_minting_2.token,
       crc_minting_2.value::text AS value
FROM crc_minting_2;

create view formatted_erc20_transfer(hash, index, timestamp, block_number, "from", "to", token, value) as
SELECT erc20_transfer_2.hash,
       erc20_transfer_2.index,
       erc20_transfer_2."timestamp",
       erc20_transfer_2.block_number,
       erc20_transfer_2."from",
       erc20_transfer_2."to",
       erc20_transfer_2.token,
       erc20_transfer_2.value::text AS value
FROM erc20_transfer_2;

create view formatted_eth_transfer(hash, index, timestamp, block_number, "from", "to", value) as
SELECT eth_transfer_2.hash,
       eth_transfer_2.index,
       eth_transfer_2."timestamp",
       eth_transfer_2.block_number,
       eth_transfer_2."from",
       eth_transfer_2."to",
       eth_transfer_2.value::text AS value
FROM eth_transfer_2;

create view formatted_gnosis_safe_eth_transfer(hash, index, timestamp, block_number, initiator, "from", "to", value) as
SELECT gnosis_safe_eth_transfer_2.hash,
       gnosis_safe_eth_transfer_2.index,
       gnosis_safe_eth_transfer_2."timestamp",
       gnosis_safe_eth_transfer_2.block_number,
       gnosis_safe_eth_transfer_2.initiator,
       gnosis_safe_eth_transfer_2."from",
       gnosis_safe_eth_transfer_2."to",
       gnosis_safe_eth_transfer_2.value::text AS value
FROM gnosis_safe_eth_transfer_2;


create view crc_safe_timeline_2
            (timestamp, block_number, transaction_index, transaction_hash, type, safe_address, contact_address,
             direction, value, obj)
as
WITH safe_timeline AS (SELECT cs."timestamp",
                              cs.block_number,
                              cs.index,
                              cs.hash,
                              'CrcSignup'::text AS type,
                              cs."user",
                              cs."user"         AS contact_address,
                              'self'::text      AS direction,
                              0::text           AS value,
                              row_to_json(cs.*) AS obj
                       FROM crc_all_signups cs
                       UNION ALL
                       SELECT cht."timestamp",
                              cht.block_number,
                              cht.index,
                              cht.hash,
                              'CrcHubTransfer'::text                    AS type,
                              crc_all_signups."user",
                              CASE
                                  WHEN cht."from" = crc_all_signups."user" AND cht."to" = crc_all_signups."user"
                                      THEN cht."to"
                                  WHEN cht."from" = crc_all_signups."user" THEN cht."to"
                                  ELSE cht."from"
                                  END                                   AS contact_address,
                              CASE
                                  WHEN cht."from" = crc_all_signups."user" AND cht."to" = crc_all_signups."user"
                                      THEN 'self'::text
                                  WHEN cht."from" = crc_all_signups."user" THEN 'out'::text
                                  ELSE 'in'::text
                                  END                                   AS direction,
                              cht.value,
                              (SELECT json_agg(_steps.*) AS row_to_json
                               FROM (SELECT t_1.hash                                  AS "transactionHash",
                                            t_1."from",
                                            t_1."to",
                                            t_1.value::text                           AS flow,
                                            (SELECT json_agg(steps.*) AS transfers
                                             FROM (SELECT e20t."from",
                                                          e20t."to",
                                                          e20t.token,
                                                          e20t.value::text AS value
                                                   FROM crc_token_transfer_2 e20t
                                                   WHERE e20t.hash = t_1.hash) steps) AS transfers
                                     FROM crc_hub_transfer_2 t_1
                                     WHERE t_1.hash = cht.hash) _steps) AS transitive_path
                       FROM formatted_crc_hub_transfer cht
                                JOIN crc_all_signups
                                     ON crc_all_signups."user" = cht."from" OR crc_all_signups."user" = cht."to"
                       UNION ALL
                       SELECT ct."timestamp",
                              ct.block_number,
                              ct.index,
                              ct.hash,
                              'CrcTrust'::text  AS type,
                              crc_all_signups."user",
                              CASE
                                  WHEN ct.can_send_to = crc_all_signups."user" AND ct.address = crc_all_signups."user"
                                      THEN crc_all_signups."user"
                                  WHEN ct.can_send_to = crc_all_signups."user" THEN ct.address
                                  ELSE ct.can_send_to
                                  END           AS contact_address,
                              CASE
                                  WHEN ct.can_send_to = crc_all_signups."user" AND ct.address = crc_all_signups."user"
                                      THEN 'self'::text
                                  WHEN ct.can_send_to = crc_all_signups."user" THEN 'out'::text
                                  ELSE 'in'::text
                                  END           AS direction,
                              ct."limit"::text  AS "limit",
                              row_to_json(ct.*) AS obj
                       FROM crc_trust_2 ct
                                JOIN crc_all_signups
                                     ON crc_all_signups."user" = ct.address OR crc_all_signups."user" = ct.can_send_to
                       UNION ALL
                       SELECT ct."timestamp",
                              ct.block_number,
                              ct.index,
                              ct.hash,
                              'CrcMinting'::text AS type,
                              crc_all_signups."user",
                              ct."from"          AS contact_address,
                              'in'::text         AS direction,
                              ct.value,
                              row_to_json(ct.*)  AS obj
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
                                  WHEN eth."from" = crc_all_signups."user" AND eth."to" = crc_all_signups."user"
                                      THEN crc_all_signups."user"
                                  WHEN eth."from" = crc_all_signups."user" THEN eth."to"
                                  ELSE eth."from"
                                  END             AS contact_address,
                              CASE
                                  WHEN eth."from" = crc_all_signups."user" AND eth."to" = crc_all_signups."user"
                                      THEN 'self'::text
                                  WHEN eth."from" = crc_all_signups."user" THEN 'out'::text
                                  ELSE 'in'::text
                                  END             AS direction,
                              eth.value,
                              row_to_json(eth.*)  AS obj
                       FROM formatted_eth_transfer eth
                                JOIN crc_all_signups
                                     ON crc_all_signups."user" = eth."from" OR crc_all_signups."user" = eth."to"
                       UNION ALL
                       SELECT erc20."timestamp",
                              erc20.block_number,
                              erc20.index,
                              erc20.hash,
                              'Erc20Transfer'::text AS type,
                              crc_all_signups."user",
                              CASE
                                  WHEN erc20."from" = crc_all_signups."user" AND erc20."to" = crc_all_signups."user"
                                      THEN crc_all_signups."user"
                                  WHEN erc20."from" = crc_all_signups."user" THEN erc20."to"
                                  ELSE erc20."from"
                                  END               AS contact_address,
                              CASE
                                  WHEN erc20."from" = crc_all_signups."user" AND erc20."to" = crc_all_signups."user"
                                      THEN 'self'::text
                                  WHEN erc20."from" = crc_all_signups."user" THEN 'out'::text
                                  ELSE 'in'::text
                                  END               AS direction,
                              erc20.value,
                              row_to_json(erc20.*)  AS obj
                       FROM formatted_erc20_transfer erc20
                                JOIN crc_all_signups
                                     ON crc_all_signups."user" = erc20."from" OR crc_all_signups."user" = erc20."to"
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
                                  WHEN seth."from" = crc_all_signups."user" AND seth."to" = crc_all_signups."user"
                                      THEN crc_all_signups."user"
                                  WHEN seth."from" = crc_all_signups."user" THEN seth."to"
                                  ELSE seth."from"
                                  END                       AS contact_address,
                              CASE
                                  WHEN seth."from" = crc_all_signups."user" AND seth."to" = crc_all_signups."user"
                                      THEN 'self'::text
                                  WHEN seth."from" = crc_all_signups."user" THEN 'out'::text
                                  ELSE 'in'::text
                                  END                       AS direction,
                              seth.value,
                              row_to_json(seth.*)           AS obj
                       FROM formatted_gnosis_safe_eth_transfer seth
                                JOIN crc_all_signups
                                     ON crc_all_signups."user" = seth."from" OR crc_all_signups."user" = seth."to")
SELECT st."timestamp",
       st.block_number,
       st.index  AS transaction_index,
       st.hash   AS transaction_hash,
       st.type,
       st."user" AS safe_address,
       st.contact_address,
       st.direction,
       st.value,
       st.obj
FROM safe_timeline st;


create view crc_total_minted_amount(total_crc_amount) as
SELECT sum(crc_token_transfer_2.value) AS total_crc_amount
FROM crc_token_transfer_2
WHERE crc_token_transfer_2."from" = '0x0000000000000000000000000000000000000000'::text;



create view erc20_balances_by_safe_and_token(safe_address, token, balance, last_changed_at) as
WITH non_circles_transfers AS (SELECT et."timestamp",
                                      et.block_number,
                                      t.index               AS transaction_index,
                                      t.hash                AS transaction_hash,
                                      'Erc20Transfer'::text AS type,
                                      et.token,
                                      et."from",
                                      et."to",
                                      et.value
                               FROM erc20_transfer_2 et
                                        JOIN crc_all_signups alls ON alls."user" = et."from" OR alls."user" = et."to"
                                        LEFT JOIN crc_signup_2 s ON s.token = et.token
                                        JOIN transaction_2 t ON et.hash = t.hash
                               WHERE s.token IS NULL),
     non_circles_ledger AS (SELECT nct."timestamp",
                                   nct.block_number,
                                   nct.transaction_index,
                                   nct.transaction_hash,
                                   nct.type,
                                   alls."user" AS safe_address,
                                   CASE
                                       WHEN nct."from" = alls."user" THEN nct."to"
                                       ELSE nct."from"
                                       END     AS contact_address,
                                   CASE
                                       WHEN nct."from" = alls."user" THEN 'out'::text
                                       ELSE 'in'::text
                                       END     AS direction,
                                   nct.token,
                                   nct."from",
                                   nct."to",
                                   nct.value
                            FROM crc_all_signups alls
                                     JOIN non_circles_transfers nct
                                          ON alls."user" = nct."from" OR alls."user" = nct."to"),
     erc20_balances AS (SELECT non_circles_ledger.safe_address,
                               non_circles_ledger.token,
                               sum(
                                       CASE
                                           WHEN non_circles_ledger.direction = 'in'::text THEN non_circles_ledger.value
                                           ELSE non_circles_ledger.value * '-1'::integer::numeric
                                           END)                    AS balance,
                               max(non_circles_ledger."timestamp") AS last_changed_at
                        FROM non_circles_ledger
                        GROUP BY non_circles_ledger.safe_address, non_circles_ledger.token)
SELECT erc20_balances.safe_address,
       erc20_balances.token,
       erc20_balances.balance,
       erc20_balances.last_changed_at
FROM erc20_balances;

create view crc_capacity_graph_2("from", "to", token_owner, capacity) as
WITH a AS (
    SELECT crc_current_trust_2.user_token,
           crc_current_trust_2.can_send_to,
           crc_current_trust_2.can_send_to_token,
           crc_current_trust_2."limit"
    FROM crc_current_trust_2
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
           e.max_transfer_amount,
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
           END AS capacity
FROM g
WHERE g."from" <> g."to";

create procedure import_from_staging_2()
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
                                                     , last_change_at_block
    )
    select b.safe_address
         , token
         , token_owner
         , balance
         , b.last_change_at
         , bl.number as last_change_at_block
    from crc_balances_by_safe_and_token_2 b
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
                                        , last_change_at_block
    )
    select b."user"
         , b.user_token
         , b.can_send_to
         , b.can_send_to_token
         , b."limit"
         , b.history_count
         , b.last_change
         , bl.number as last_change_at_block
    from crc_current_trust_2 b
             join block bl on bl.timestamp = b.last_change
    where b."user" = ANY((select array_agg(safe_address) from stale_cached_trust_relations)::text[])
       or b."can_send_to" = ANY((select array_agg(safe_address) from stale_cached_trust_relations)::text[]);

    drop table stale_cached_trust_relations;

end;
$$;



create procedure publish_event(IN topic text, IN message text)
    language plpgsql
as
$$
begin
    perform pg_notify(topic, message::text);
end
$$;


create function get_capacity_changes_since_block_2(since_block numeric)
    returns TABLE(token_holder text, token_owner text, can_send_to text, capacity numeric)
    language plpgsql
as
$$
declare
begin
    return query
        --explain (verbose, costs, timing, analyze, buffers, wal, settings, summary)
        with a as (
            -- Get all accepted token ('user_token') for each 'can_send_to'
            select t.user_token
                 , t.can_send_to
                 , t."limit"
                 , t.last_change_at_block as trust_last_change
                 , os.organisation is not null as to_is_orga
            from cache_crc_current_trust t
                     left join crc_organisation_signup_2 os on os.organisation = t."can_send_to"
        ), b as (
            -- Find all safes that hold accepted tokens
            select bal.safe_address as "from"       -- a potential sender
                 , a.can_send_to as "to"            -- the receiver
                 , a."limit"                        -- the trust limit betwen "from" and "to"
                 , a.trust_last_change
                 , bal.token_owner as "token_owner" -- the owner of the sendable token
                 , bal.balance as "from_balance"    -- the sendable amount
                 , bal.last_change_at_block as from_balance_last_change
                 , a.to_is_orga
            from a
                     join cache_crc_balances_by_safe_and_token bal on bal.token = a.user_token
            where bal.balance >= 0
        ), c as (
            -- Get the receiver's balance of the own token
            select b.*
                 , bal.balance as to_own_token_holdings
                 , bal.last_change_at_block as to_own_token_holdings_last_change
            from b
                     left join cache_crc_balances_by_safe_and_token bal on bal.safe_address = b."to"
                and bal.token_owner = b."to"
            where b.trust_last_change >= since_block
               or b.from_balance_last_change >= since_block
        ), d as (
            -- Get the receiver's holdings of token_owner's tokens
            select c.*
                 , case when bal.balance is not null then bal.balance else 0 end as to_already_holds_balance
                 , c."to" = c.token_owner as is_to_own_token
                 , bal.last_change_at_block as to_already_holds_balance_last_change
            from c
                     left join cache_crc_balances_by_safe_and_token bal on bal.safe_address = c."to"
                and bal.token_owner = c.token_owner
        ), e as (
            -- orgas take any amount of trusted tokens
            -- token_owners take any amount of their own tokens
            -- if none of the above: d.to_own_token_holdings * d."limit" / 100
            select d.*
                 , case when (d.is_to_own_token or d.to_is_orga)
                            then d.from_balance
                        else (d.to_own_token_holdings * d."limit" / 100)
                end as max_transfer_amount
            from d
        ), f as (
            -- calculate the 'destBalanceScaled'
            select e.*
                 , e.to_already_holds_balance * (100 - e."limit") / 100 as dest_balance_scaled
            from e
        ), g as (
            select f.*
                 , case when f.max_transfer_amount - f.dest_balance_scaled > f.from_balance
                            then f.from_balance
                        else f.max_transfer_amount - f.dest_balance_scaled
                end as capacity
            from f
        )
        select g."from"
             , g.token_owner
             , g."to"
             , case when g.capacity < 0 then 0 else g.capacity end as capacity
        from g
        where g."from" <> g."to";
end;
$$;

insert into db_version (version, description) values ('1.0.0', 'The initial release-db');

commit;