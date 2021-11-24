create table block (
                       number bigserial primary key,
                       hash text not null unique ,
                       timestamp timestamp not null,
                       total_transaction_count int not null,
                       indexed_transaction_count int not null
);

create unique index idx_block_timestamp on block(timestamp) include (number);

create view first_incomplete_block as
select min(number) block_no from block
where total_transaction_count > indexed_transaction_count;

create table transaction (
                             id bigserial primary key,
                             block_number bigint not null references block(number),
                             "from" text not null,
                             "to" text null, -- Todo: NULL happens only on contract creation. Get the address of the deployed contact.
                             index int not null,
                             gas numeric not null,
                             hash text unique not null,
                             value numeric not null,
                             input text null,
                             nonce text null,
                             type text null,
                             gas_price numeric null,
                             classification text[] not null
);

create index idx_transaction_fk_block_number on transaction(block_number) include (id);

create table crc_organisation_signup (
                                         id bigserial primary key,
                                         transaction_id bigint not null references transaction (id),
                                         organisation text not null
);

create unique index idx_crc_organisation_signup_organisation on crc_organisation_signup(organisation) include (transaction_id);
create index idx_crc_organisation_signup_fk_transaction_id on crc_organisation_signup(transaction_id);

create table crc_signup (
                            id bigserial primary key,
                            transaction_id bigint not null references transaction (id),
                            "user" text unique not null unique,
                            token text not null unique
);

create unique index idx_crc_signup_user on crc_signup("user") include (transaction_id, token);
create unique index idx_crc_signup_token on crc_signup(token) include (transaction_id, "user");
create index idx_crc_signup_fk_transaction_id on crc_signup (transaction_id);

create table crc_hub_transfer (
                                  id bigserial primary key,
                                  transaction_id bigint not null references transaction (id),
                                  "from" text not null,
                                  "to" text not null,
                                  value numeric not null
);

create index idx_crc_hub_transfer_from on crc_hub_transfer("from") include (transaction_id);
create index idx_crc_hub_transfer_to on crc_hub_transfer("to") include (transaction_id);
create index idx_crc_hub_transfer_fk_transaction_id on crc_hub_transfer(transaction_id);

create table erc20_transfer (
                                id bigserial primary key,
                                transaction_id bigint not null references transaction (id),
                                "from" text not null,
                                "to" text not null,
                                token text not null,
                                value numeric not null
);

create index idx_erc20_transfer_from on erc20_transfer("from") include (transaction_id);
create index idx_erc20_transfer_to on erc20_transfer("to") include (transaction_id);
create index idx_erc20_transfer_token on erc20_transfer("token") include (transaction_id);
create index idx_erc20_transfer_fk_transaction_id on erc20_transfer(transaction_id);

create view crc_token_transfer
as
select t.*
from erc20_transfer t
         join crc_signup s on t.token = s.token;

create view erc20_minting
as
select *
from erc20_transfer
where "from" = '0x0000000000000000000000000000000000000000';

create view crc_minting
as
select tm.*
from erc20_minting tm
         join crc_signup s on tm.token = s.token;

create view crc_ledger
as
with ledger as (
    select t.transaction_id
         , 'add' as verb
         , sum(t.value) as value
         , t.token
         , cs."user" token_owner
         , 'to' predicate
         , t."to" as safe_address
    from erc20_transfer t -- includes minting and every transfer of all crc-tokens
             join crc_signup cs on t.token = cs.token
    group by t.transaction_id, t."to", t.token, cs."user"
    union
    select t.transaction_id,
           'remove'        as verb,
           -(sum(t.value)) as value,
           t.token,
           cs."user" token_owner,
           'from'             predicate,
           t."from"        as safe_address
    from erc20_transfer t -- includes minting and every transfer of all crc-tokens
             join crc_signup cs on t.token = cs.token
    group by t.transaction_id, t."from", t.token, cs."user"
)
select b.timestamp, l.*
from ledger l
         join transaction t on t.id = l.transaction_id
         join block b on t.block_number = b.number
order by b.timestamp, t.index, l.token, l.verb desc /* TODO: The log index is gone */;

create view crc_balances_by_safe
as
select safe_address, sum(value) balance
from crc_ledger
group by safe_address
order by safe_address;

create view crc_balances_by_safe_and_token
as
select safe_address, token, token_owner, sum(value) balance
from crc_ledger
group by safe_address, token, token_owner
order by safe_address, balance desc;

create table crc_trust (
                           id bigserial primary key,
                           transaction_id bigint not null references transaction (id),
                           address text not null,
                           can_send_to text not null,
                           "limit" numeric not null
);

create index idx_crc_trust_address on crc_trust(address) include (transaction_id);
create index idx_crc_trust_can_send_to on crc_trust(can_send_to) include (transaction_id);
create index idx_crc_trust_fk_transaction_id on crc_trust(transaction_id);

create view crc_current_trust
as
select lte.address as "user",
       cs_a.id as user_id,
       cs_a.token user_token,
       lte.can_send_to,
       cs_b.id can_send_to_id,
       cs_b.token can_send_to_token,
       ct."limit",
       lte.history_count
from (
         select max(transaction_id) transaction_id,
                count(transaction_id) history_count,
                address,
                can_send_to
         from crc_trust
         group by address,
                  can_send_to) lte
         join crc_trust ct on lte.transaction_id = ct.transaction_id
         join crc_signup cs_a on lte.address = cs_a."user"
         join crc_signup cs_b on lte.can_send_to = cs_b."user";

create table eth_transfer (
                              id bigserial primary key,
                              transaction_id bigint not null references transaction (id),
                              "from" text not null,
                              "to" text not null,
                              value numeric not null
);

create index idx_eth_transfer_from on eth_transfer("from") include (transaction_id);
create index idx_eth_transfer_to on eth_transfer("to") include (transaction_id);
create index idx_eth_transfer_fk_transaction_id on eth_transfer(transaction_id);

create table gnosis_safe_eth_transfer (
                                          id bigserial primary key,
                                          transaction_id bigint not null references transaction (id),
                                          initiator text not null,
                                          "from" text not null,
                                          "to" text not null,
                                          value numeric not null
);

create index idx_gnosis_safe_eth_transfer_initiator on gnosis_safe_eth_transfer(initiator) include (transaction_id);
create index idx_gnosis_safe_eth_transfer_from on gnosis_safe_eth_transfer("from") include (transaction_id);
create index idx_gnosis_safe_eth_transfer_to on gnosis_safe_eth_transfer("to") include (transaction_id);

create or replace procedure delete_incomplete_blocks()
as
$yolo$
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
$yolo$
    language plpgsql;

create view crc_safe_timeline
as
with safe_timeline as (
    select t.id
         , b.timestamp
         , b.number
         , t.index
         , t.hash
         , 'crc_signup' as type
         , cs."user"
         , 'self'       as direction
         , 0            as value
         , row_to_json(cs) obj
    from crc_signup cs
             join transaction t on cs.transaction_id = t.id
             join block b on t.block_number = b.number
    union all
    select t.id
         , b.timestamp
         , b.number
         , t.index
         , t.hash
         , 'crc_hub_transfer' as type
         , crc_signup."user"
         , case
               when cht."from" = crc_signup."user" and cht."to" = crc_signup."user" then 'self'
               when cht."from" = crc_signup."user" then 'out'
               else 'in' end  as direction
         , cht.value
         , (
        select row_to_json(_steps)
        from (
                 select cht.id,
                        t.id as          transaction_id,
                        t."hash"         "transactionHash",
                        ht."from"        "from",
                        ht."to"          "to",
                        ht."value"::text flow,
                        (select json_agg(steps) transfers
                         from (
                                  select E20T."from"           "from",
                                         E20T."to"             "to",
                                         E20T."token"          "token",
                                         E20T."value"::text as "value"
                                  from crc_token_transfer E20T
                                  where E20T.transaction_id = t.id
                              ) steps)
                 from transaction t
                          join crc_hub_transfer ht on t.id = ht.transaction_id
                 where t.id = cht.transaction_id
             ) _steps
    )                         as transitive_path
    from crc_hub_transfer cht
             join crc_signup on crc_signup."user" = cht."from" or crc_signup."user" = cht."to"
             join transaction t on cht.transaction_id = t.id
             join block b on t.block_number = b.number
    union all
    select t.id
         , b.timestamp
         , b.number
         , t.index
         , t.hash
         , 'crc_trust'       as type
         , crc_signup."user"
         , case
               when ct.can_send_to = crc_signup."user" and ct.address = crc_signup."user" then 'self'
               when ct.can_send_to = crc_signup."user" then 'out'
               else 'in' end as direction
         , ct."limit"
         , row_to_json(ct)      obj
    from crc_trust ct
             join crc_signup on crc_signup."user" = ct.address or crc_signup."user" = ct.can_send_to
             join transaction t on ct.transaction_id = t.id
             join block b on t.block_number = b.number
    union all
    select t.id
         , b.timestamp
         , b.number
         , t.index
         , t.hash
         , 'crc_minting' as type
         , crc_signup."user"
         , 'in'          as direction
         , ct.value
         , row_to_json(ct)  obj
    from crc_minting ct
             join crc_signup on ct.token = crc_signup.token
             join transaction t on ct.transaction_id = t.id
             join block b on t.block_number = b.number
    union all
    select t.id
         , b.timestamp
         , b.number
         , t.index
         , t.hash
         , 'eth_transfer'    as type
         , crc_signup."user"
         , case
               when eth."from" = crc_signup."user" and eth."to" = crc_signup."user" then 'self'
               when eth."from" = crc_signup."user" then 'out'
               else 'in' end as direction
         , eth.value
         , row_to_json(eth)     obj
    from eth_transfer eth
             join crc_signup on crc_signup."user" = eth."from" or crc_signup."user" = eth."to"
             join transaction t on eth.transaction_id = t.id
             join block b on t.block_number = b.number
    union all
    select t.id
         , b.timestamp
         , b.number
         , t.index
         , t.hash
         , 'gnosis_safe_eth_transfer' as type
         , crc_signup."user"
         , case
               when seth."from" = crc_signup."user" and seth."to" = crc_signup."user" then 'self'
               when seth."from" = crc_signup."user" then 'out'
               else 'in' end          as direction
         , seth.value
         , row_to_json(seth)             obj
    from gnosis_safe_eth_transfer seth
             join crc_signup on crc_signup."user" = seth."from" or crc_signup."user" = seth."to"
             join transaction t on seth.transaction_id = t.id
             join block b on t.block_number = b.number
)
select id transaction_id
     , timestamp
     , number block_number
     , index transaction_index
     , hash transaction_hash
     , type
     , "user" safe_address
     , direction
     , value
     , obj
from safe_timeline st;

/*
create table transaction_log (
    id bigserial primary key,
    transaction_id bigint not null references transaction (id),
    logIndex int not null,
    address text not null,
    data text not null
);

create table log_topic (
    topic text primary key
);

create table transaction_log_topic (
    id bigserial primary key,
    transactionLogId bigint not null references transaction_log (id),
    topic text not null references log_topic (topic)
);
*/

-- V2


-- alter table eth_transfer_2 add column id serial;
-- with a as (
--     select array_agg(id) as ids
--     from eth_transfer_2
--     group by hash, "from", "to", "value"
--     having count(*) > 1
-- )
-- delete from eth_transfer_2 t
--     using (
--         select unnest(ids[2:]) as id
--         from a
--     ) b
-- where t.id = b.id;
-- alter table eth_transfer_2 drop column id;

create unique index ux_block_number on block(number) include (timestamp);

select cs.block_number
     ,cs.from
     ,cs.to
     ,cs.hash
     ,cs.index
     ,b.timestamp
     ,cs.value
     ,cs.input
     ,cs.nonce
     ,cs.type
     ,cs.classification
into transaction_2
from transaction cs
         join block b on cs.block_number = b.number;

alter table transaction_2 add constraint pk_transaction_2 primary key (hash);
create unique index ux_transaction_2_block_number_index on transaction_2(block_number, index);
alter table transaction_2 add constraint fk_transaction_2_block_number foreign key(block_number) references block(number);
create index ix_transaction_2_timestamp on transaction_2(timestamp) include (hash, block_number, index, timestamp);
create index ix_transaction_2_from on transaction_2("from") include ("to", value);
create index ix_transaction_2_to on transaction_2("to") include ("from", value);

select t.hash, t.index, t_2.timestamp, t.block_number, "user", token
into crc_signup_2
from crc_signup cs
         join transaction t on cs.transaction_id = t.id
         join transaction_2 t_2 on t_2.hash = t.hash;

alter table crc_signup_2 add constraint fk_signup_transaction_2 foreign key(hash) references transaction_2(hash);
alter table crc_signup_2 add constraint fk_signup_block_2 foreign key(block_number) references block(number);
create unique index ux_crc_signup_2_user on crc_signup_2("user") include (token);
create unique index ux_crc_signup_2_token on crc_signup_2(token) include ("user");
create index ix_crc_signup_2_timestamp on crc_signup_2(timestamp) include (hash, block_number, index, timestamp);
create index ix_crc_signup_2_hash on crc_signup_2(hash) include (block_number, index, timestamp);
create index ix_crc_signup_2_block_number on crc_signup_2(block_number) include (index, timestamp);


select t.hash, t.index, t_2.timestamp, t.block_number, cs.organisation
into crc_organisation_signup_2
from crc_organisation_signup cs
         join transaction t on cs.transaction_id = t.id
         join transaction_2 t_2 on t_2.hash = t.hash;

alter table crc_organisation_signup_2 add constraint fk_organisation_signup_transaction_2 foreign key(hash) references transaction_2(hash);
alter table crc_organisation_signup_2 add constraint fk_organisation_signup_block_2 foreign key(block_number) references block(number);
create unique index ux_crc_organisation_signup_2_organisation on crc_organisation_signup_2(organisation);
create index ix_crc_organisation_signup_2_timestamp on crc_organisation_signup_2(timestamp) include (hash, block_number, index, timestamp);
create index ix_crc_organisation_signup_2_hash on crc_organisation_signup_2(hash) include (block_number, index, timestamp);
create index ix_crc_organisation_signup_2_block_number on crc_organisation_signup_2(block_number) include (index, timestamp);


select t.hash, t.index, t_2.timestamp, t.block_number, cs.address, cs.can_send_to, cs."limit"
into crc_trust_2
from crc_trust cs
         join transaction t on cs.transaction_id = t.id
         join transaction_2 t_2 on t_2.hash = t.hash;

alter table crc_trust_2 add constraint fk_trust_transaction_2 foreign key(hash) references transaction_2(hash);
alter table crc_trust_2 add constraint fk_trust_block_2 foreign key(block_number) references block(number);
create unique index ux_crc_trust_2_hash_address_can_send_to_limit on crc_trust_2(hash, address, can_send_to, "limit");
create index ix_crc_trust_2_timestamp on crc_trust_2(timestamp) include (hash, block_number, index, timestamp);
create index ix_crc_trust_2_hash on crc_trust_2(hash) include (block_number, index, timestamp);
create index ix_crc_trust_2_block_number on crc_trust_2(block_number) include (index, timestamp);
create index ix_crc_trust_2_address on crc_trust_2(address) include (can_send_to, "limit");
create index ix_crc_trust_2_can_send_to on crc_trust_2(can_send_to) include (address, "limit");

select t.hash, t.index, t_2.timestamp, t.block_number, cs.from, cs.to, cs.value
into crc_hub_transfer_2
from crc_hub_transfer cs
         join transaction t on cs.transaction_id = t.id
         join transaction_2 t_2 on t_2.hash = t.hash;

alter table crc_hub_transfer_2 add constraint fk_hub_transfer_transaction_2 foreign key(hash) references transaction_2(hash);
alter table crc_hub_transfer_2 add constraint fk_hub_transfer_block_2 foreign key(block_number) references block(number);
create unique index ux_crc_hub_transfer_2_hash_from_to_value on crc_hub_transfer_2(hash, "from", "to", "value");
create index ix_crc_hub_transfer_2_timestamp on crc_hub_transfer_2(timestamp) include (hash, block_number, index, timestamp);
create index ix_crc_hub_transfer_2_hash on crc_hub_transfer_2(hash) include (block_number, index, timestamp);
create index ix_crc_hub_transfer_2_block_number on crc_hub_transfer_2(block_number) include (index, timestamp);
create index ix_crc_hub_transfer_2_from on crc_hub_transfer_2("from") include ("to", value);
create index ix_crc_hub_transfer_2_to on crc_hub_transfer_2("to") include ("from", value);

select t.hash, t.index, t_2.timestamp, t.block_number, cs.from, cs.to, cs.token, cs.value
into erc20_transfer_2
from erc20_transfer cs
         join transaction t on cs.transaction_id = t.id
         join transaction_2 t_2 on t_2.hash = t.hash;

alter table erc20_transfer_2 add constraint fk_erc20_transfer_transaction_2 foreign key(hash) references transaction_2(hash);
alter table erc20_transfer_2 add constraint fk_erc20_transfer_block_2 foreign key(block_number) references block(number);
create unique index ux_erc20_transfer_2_hash_from_to_token_value on erc20_transfer_2 (hash, "from", "to", token, value);
create index ix_erc20_transfer_2_timestamp on erc20_transfer_2(timestamp) include (hash, block_number, index, timestamp);
create index ix_erc20_transfer_2_hash on erc20_transfer_2(hash) include (block_number, index, timestamp);
create index ix_erc20_transfer_2_block_number on erc20_transfer_2(block_number) include (index, timestamp);
create index ix_erc20_transfer_2_token on erc20_transfer_2(token);
create index ix_erc20_transfer_2_from on erc20_transfer_2("from") include ("to", token, value);
create index ix_erc20_transfer_2_to on erc20_transfer_2("to") include ("from", token, value);


select t.hash, t.index, t_2.timestamp, t.block_number, cs.from, cs.to, cs.value
into eth_transfer_2
from eth_transfer cs
         join transaction t on cs.transaction_id = t.id
         join transaction_2 t_2 on t_2.hash = t.hash;

alter table eth_transfer_2 add constraint fk_eth_transfer_transaction_2 foreign key(hash) references transaction_2(hash);
alter table eth_transfer_2 add constraint fk_eth_transfer_block_2 foreign key(block_number) references block(number);
create unique index ux_eth_transfer_2_hash_from_to_value on eth_transfer_2(hash, "from", "to", "value");
create index ix_eth_transfer_2_timestamp on eth_transfer_2(timestamp) include (hash, block_number, index, timestamp);
create index ix_eth_transfer_2_hash on eth_transfer_2(hash) include (block_number, index, timestamp);
create index ix_eth_transfer_2_block_number on eth_transfer_2(block_number) include (index, timestamp);
create index ix_eth_transfer_2_from on eth_transfer_2("from") include ("to", value);
create index ix_eth_transfer_2_to on eth_transfer_2("to") include ("from", value);


select t.hash, t.index, t_2.timestamp, t.block_number, cs.initiator, cs.from, cs.to, cs.value
into gnosis_safe_eth_transfer_2
from gnosis_safe_eth_transfer cs
         join transaction t on cs.transaction_id = t.id
         join transaction_2 t_2 on t_2.hash = t.hash;

alter table gnosis_safe_eth_transfer_2 add constraint fk_gnosis_safe_eth_transfer_transaction_2 foreign key(hash) references transaction_2(hash);
alter table gnosis_safe_eth_transfer_2 add constraint fk_gnosis_safe_eth_transfer_block_2 foreign key(block_number) references block(number);
create unique index ux_gnosis_safe_eth_transfer_2_hash_from_to_value on gnosis_safe_eth_transfer_2(hash, initiator, "from", "to", "value");
create index ix_gnosis_safe_eth_transfer_2_timestamp on gnosis_safe_eth_transfer_2(timestamp) include (hash, block_number, index, timestamp);
create index ix_gnosis_safe_eth_transfer_2_hash on gnosis_safe_eth_transfer_2(hash) include (block_number, index, timestamp);
create index ix_gnosis_safe_eth_transfer_2_block_number on gnosis_safe_eth_transfer_2(block_number) include (index, timestamp);
create index ix_gnosis_safe_eth_transfer_2_from on gnosis_safe_eth_transfer_2("from") include ("to", value);
create index ix_gnosis_safe_eth_transfer_2_to on gnosis_safe_eth_transfer_2("to") include ("from", value);
create index ix_gnosis_safe_eth_transfer_2_initiator on gnosis_safe_eth_transfer_2(initiator);


create view crc_ledger_2 (timestamp, transaction_id, verb, value, token, token_owner, predicate, safe_address)
as
WITH ledger AS (
    SELECT t_1.hash,
           t_1.block_number,
           t_1.timestamp,
           'add'::text    AS verb,
           sum(t_1.value) AS value,
           t_1.token,
           cs."user"      AS token_owner,
           'to'::text     AS predicate,
           t_1."to"       AS safe_address
    FROM erc20_transfer_2 t_1
             JOIN crc_signup_2 cs ON t_1.token = cs.token
    GROUP BY t_1.hash, t_1.block_number, t_1.timestamp, t_1."to", t_1.token, cs."user"
    UNION
    SELECT t_1.hash,
           t_1.block_number,
           t_1.timestamp,
           'remove'::text   AS verb,
           - sum(t_1.value) AS value,
           t_1.token,
           cs."user"        AS token_owner,
           'from'::text     AS predicate,
           t_1."from"       AS safe_address
    FROM erc20_transfer_2 t_1
             JOIN crc_signup_2 cs ON t_1.token = cs.token
    GROUP BY t_1.hash, t_1.block_number, t_1.timestamp, t_1."from", t_1.token, cs."user"
)
SELECT l."timestamp",
       l.hash,
       l.verb,
       l.value,
       l.token,
       l.token_owner,
       l.predicate,
       l.safe_address
FROM ledger l
ORDER BY l."timestamp", l.token, l.verb DESC;


create view crc_balances_by_safe_2(safe_address, balance)
as
SELECT crc_ledger_2.safe_address,
       sum(crc_ledger_2.value) AS balance
FROM crc_ledger_2
GROUP BY crc_ledger_2.safe_address
ORDER BY crc_ledger_2.safe_address;

create view crc_balances_by_safe_and_token_2(safe_address, token, token_owner, balance)
as
SELECT crc_ledger_2.safe_address,
       crc_ledger_2.token,
       crc_ledger_2.token_owner,
       sum(crc_ledger_2.value) AS balance
FROM crc_ledger_2
GROUP BY crc_ledger_2.safe_address, crc_ledger_2.token, crc_ledger_2.token_owner
ORDER BY crc_ledger_2.safe_address, (sum(crc_ledger_2.value)) DESC;

create or replace view crc_balances_by_safe_and_token_2(safe_address, token, token_owner, balance) as
SELECT crc_ledger_2.safe_address,
       crc_ledger_2.token,
       crc_ledger_2.token_owner,
       sum(crc_ledger_2.value) AS balance,
       max(crc_ledger_2.timestamp) AS last_change_at
FROM crc_ledger_2
GROUP BY crc_ledger_2.safe_address, crc_ledger_2.token, crc_ledger_2.token_owner
ORDER BY crc_ledger_2.safe_address, (sum(crc_ledger_2.value)) DESC;

create view crc_current_trust_2 ("user", user_token, can_send_to, can_send_to_token, "limit", history_count)
as
SELECT lte.address AS "user",
       cs_a.token  AS user_token,
       lte.can_send_to,
       cs_b.token  AS can_send_to_token,
       ct."limit",
       lte.history_count
FROM (SELECT max(crc_trust_2.hash)   AS hash,
             count(crc_trust_2.hash) AS history_count,
             crc_trust_2.address,
             crc_trust_2.can_send_to
      FROM crc_trust_2
      GROUP BY crc_trust_2.address, crc_trust_2.can_send_to) lte
         JOIN crc_trust_2 ct ON lte.hash = ct.hash
         JOIN crc_signup_2 cs_a ON lte.address = cs_a."user"
         JOIN crc_signup_2 cs_b ON lte.can_send_to = cs_b."user";

create view erc20_minting_2(timestamp, block_number, index, hash, "from", "to", token, value)
as
SELECT erc20_transfer_2.timestamp,
       erc20_transfer_2.block_number,
       erc20_transfer_2.index,
       erc20_transfer_2.hash,
       erc20_transfer_2."from",
       erc20_transfer_2."to",
       erc20_transfer_2.token,
       erc20_transfer_2.value
FROM erc20_transfer_2
WHERE erc20_transfer_2."from" = '0x0000000000000000000000000000000000000000'::text;

create view crc_minting_2(timestamp, block_number, index, hash, "from", "to", token, value)
as
SELECT tm.timestamp,
       tm.block_number,
       tm.index,
       tm.hash,
       tm."from",
       tm."to",
       tm.token,
       tm.value
FROM erc20_minting_2 tm
         JOIN crc_signup_2 s ON tm.token = s.token;

create view crc_token_transfer_2(timestamp, block_number, index, hash, "from", "to", token, value)
as
SELECT t.timestamp,
       t.block_number,
       t.index,
       t.hash,
       t."from",
       t."to",
       t.token,
       t.value
FROM erc20_transfer_2 t
         JOIN crc_signup_2 s ON t.token = s.token;

create view crc_safe_timeline_2
            (timestamp, block_number, transaction_index, transaction_hash, type, safe_address,
             direction, value, obj)
as
WITH safe_timeline AS (
    SELECT cs."timestamp",
           cs.block_number,
           cs.index,
           cs.hash,
           'CrcSignup'::text AS type,
           cs."user",
           'self'::text       AS direction,
           0                  AS value,
           row_to_json(cs.*)  AS obj
    FROM crc_signup_2 cs
    UNION ALL
    SELECT cht."timestamp",
           cht.block_number,
           cht.index,
           cht.hash,
           'CrcHubTransfer'::text                  AS type,
           crc_signup_2."user",
           CASE
               WHEN cht."from" = crc_signup_2."user" AND cht."to" = crc_signup_2."user" THEN 'self'::text
               WHEN cht."from" = crc_signup_2."user" THEN 'out'::text
               ELSE 'in'::text
               END                                   AS direction,
           cht.value,
           (SELECT json_agg(_steps.*) AS row_to_json
            FROM (SELECT t_1.hash                                  AS "transactionHash",
                         t_1."from",
                         t_1."to",
                         t_1.value::text                            AS flow,
                         (SELECT json_agg(steps.*) AS transfers
                          FROM (SELECT e20t."from",
                                       e20t."to",
                                       e20t.token,
                                       e20t.value::text AS value
                                FROM crc_token_transfer_2 e20t
                                WHERE e20t.hash = t_1.hash) steps) AS transfers
                  FROM crc_hub_transfer_2 t_1
                  WHERE t_1.hash = cht.hash) _steps) AS transitive_path
    FROM crc_hub_transfer_2 cht
             JOIN crc_signup_2 ON crc_signup_2."user" = cht."from"
        OR crc_signup_2."user" = cht."to"
    UNION ALL
    SELECT ct."timestamp",
           ct.block_number,
           ct.index,
           ct.hash,
           'CrcTrust'::text AS type,
           crc_signup_2."user",
           CASE
               WHEN ct.can_send_to = crc_signup_2."user" AND ct.address = crc_signup_2."user" THEN 'self'::text
               WHEN ct.can_send_to = crc_signup_2."user" THEN 'out'::text
               ELSE 'in'::text
               END           AS direction,
           ct."limit",
           row_to_json(ct.*) AS obj
    FROM crc_trust_2 ct
             JOIN crc_signup_2 ON crc_signup_2."user" = ct.address OR crc_signup_2."user" = ct.can_send_to
    UNION ALL
    SELECT ct."timestamp",
           ct.block_number,
           ct.index,
           ct.hash,
           'CrcMinting'::text AS type,
           crc_signup_2."user",
           'in'::text          AS direction,
           ct.value,
           row_to_json(ct.*)   AS obj
    FROM crc_minting_2 ct
             JOIN crc_signup_2 ON ct.token = crc_signup_2.token
    UNION ALL
    SELECT eth."timestamp",
           eth.block_number,
           eth.index,
           eth.hash,
           'EthTransfer'::text AS type,
           crc_signup_2."user",
           CASE
               WHEN eth."from" = crc_signup_2."user" AND eth."to" = crc_signup_2."user" THEN 'self'::text
               WHEN eth."from" = crc_signup_2."user" THEN 'out'::text
               ELSE 'in'::text
               END              AS direction,
           eth.value,
           row_to_json(eth.*)   AS obj
    FROM eth_transfer_2 eth
             JOIN crc_signup_2 ON crc_signup_2."user" = eth."from" OR crc_signup_2."user" = eth."to"
    UNION ALL
    SELECT seth."timestamp",
           seth.block_number,
           seth.index,
           seth.hash,
           'GnosisSafeEthTransfer'::text AS type,
           crc_signup_2."user",
           CASE
               WHEN seth."from" = crc_signup_2."user" AND seth."to" = crc_signup_2."user" THEN 'self'::text
               WHEN seth."from" = crc_signup_2."user" THEN 'out'::text
               ELSE 'in'::text
               END                          AS direction,
           seth.value,
           row_to_json(seth.*)              AS obj
    FROM gnosis_safe_eth_transfer_2 seth
             JOIN crc_signup_2 ON crc_signup_2."user" = seth."from" OR crc_signup_2."user" = seth."to"
)
SELECT st."timestamp",
       st.block_number,
       st.index  AS transaction_index,
       st.hash   AS transaction_hash,
       st.type,
       st."user" AS safe_address,
       st.direction,
       st.value,
       st.obj
FROM safe_timeline st;


drop view crc_safe_timeline_2;

create or replace view crc_safe_timeline_2 (timestamp, block_number, transaction_index, transaction_hash, type, safe_address, contact_address, direction, value, obj) as
WITH safe_timeline AS (
    SELECT cs."timestamp",
           cs.block_number,
           cs.index,
           cs.hash,
           'CrcSignup'::text AS type,
           cs."user",
           cs."user" as contact_address,
           'self'::text      AS direction,
           0                 AS value,
           row_to_json(cs.*) AS obj
    FROM crc_signup_2 cs
    UNION ALL
    SELECT cht."timestamp",
           cht.block_number,
           cht.index,
           cht.hash,
           'CrcHubTransfer'::text                    AS type,
           crc_signup_2."user",
           CASE
               WHEN cht."from" = crc_signup_2."user" AND cht."to" = crc_signup_2."user" THEN cht."to"
               WHEN cht."from" = crc_signup_2."user" THEN cht."to"
               ELSE cht."from"
               END                                   AS contact_address,
           CASE
               WHEN cht."from" = crc_signup_2."user" AND cht."to" = crc_signup_2."user" THEN 'self'::text
               WHEN cht."from" = crc_signup_2."user" THEN 'out'::text
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
    FROM crc_hub_transfer_2 cht
             JOIN crc_signup_2 ON crc_signup_2."user" = cht."from" OR crc_signup_2."user" = cht."to"
    UNION ALL
    SELECT ct."timestamp",
           ct.block_number,
           ct.index,
           ct.hash,
           'CrcTrust'::text  AS type,
           crc_signup_2."user",
           CASE
               WHEN ct.can_send_to = crc_signup_2."user" AND ct.address = crc_signup_2."user" THEN crc_signup_2."user"
               WHEN ct.can_send_to = crc_signup_2."user" THEN ct.address
               ELSE ct.can_send_to
               END           AS contact_address,
           CASE
               WHEN ct.can_send_to = crc_signup_2."user" AND ct.address = crc_signup_2."user" THEN 'self'::text
               WHEN ct.can_send_to = crc_signup_2."user" THEN 'out'::text
               ELSE 'in'::text
               END           AS direction,
           ct."limit",
           row_to_json(ct.*) AS obj
    FROM crc_trust_2 ct
             JOIN crc_signup_2 ON crc_signup_2."user" = ct.address OR crc_signup_2."user" = ct.can_send_to
    UNION ALL
    SELECT ct."timestamp",
           ct.block_number,
           ct.index,
           ct.hash,
           'CrcMinting'::text AS type,
           crc_signup_2."user",
           ct."from" as contact_address,
           'in'::text         AS direction,
           ct.value,
           row_to_json(ct.*)  AS obj
    FROM crc_minting_2 ct
             JOIN crc_signup_2 ON ct.token = crc_signup_2.token
    UNION ALL
    SELECT eth."timestamp",
           eth.block_number,
           eth.index,
           eth.hash,
           'EthTransfer'::text AS type,
           crc_signup_2."user",
           CASE
               WHEN eth."from" = crc_signup_2."user" AND eth."to" = crc_signup_2."user" THEN crc_signup_2."user"
               WHEN eth."from" = crc_signup_2."user" THEN eth."to"
               ELSE eth."from"
               END             AS contact_address,
           CASE
               WHEN eth."from" = crc_signup_2."user" AND eth."to" = crc_signup_2."user" THEN 'self'::text
               WHEN eth."from" = crc_signup_2."user" THEN 'out'::text
               ELSE 'in'::text
               END             AS direction,
           eth.value,
           row_to_json(eth.*)  AS obj
    FROM eth_transfer_2 eth
             JOIN crc_signup_2 ON crc_signup_2."user" = eth."from" OR crc_signup_2."user" = eth."to"
    UNION ALL
    SELECT seth."timestamp",
           seth.block_number,
           seth.index,
           seth.hash,
           'GnosisSafeEthTransfer'::text AS type,
           crc_signup_2."user",
           CASE
               WHEN seth."from" = crc_signup_2."user" AND seth."to" = crc_signup_2."user" THEN crc_signup_2."user"
               WHEN seth."from" = crc_signup_2."user" THEN seth."to"
               ELSE seth."from"
               END             AS contact_address,
           CASE
               WHEN seth."from" = crc_signup_2."user" AND seth."to" = crc_signup_2."user" THEN 'self'::text
               WHEN seth."from" = crc_signup_2."user" THEN 'out'::text
               ELSE 'in'::text
               END                       AS direction,
           seth.value,
           row_to_json(seth.*)           AS obj
    FROM gnosis_safe_eth_transfer_2 seth
             JOIN crc_signup_2 ON crc_signup_2."user" = seth."from" OR crc_signup_2."user" = seth."to"
)
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

-- v3
select number, hash, timestamp, total_transaction_count, null::timestamp as selected_at, null::timestamp as imported_at
into _block_staging
from block
limit 0;

create index ix_block_staging_number on _block_staging(number) include (hash, selected_at, total_transaction_count);
create index ix_block_staging_selected_at_ on _block_staging(selected_at) include (hash, number, total_transaction_count);
create index ix_block_staging_imported_at on _block_staging(imported_at) include (hash, number, total_transaction_count);

select hash, index, timestamp, block_number, "from", "to", value::text
into _crc_hub_transfer_staging
from crc_hub_transfer_2
limit 0;

create index ix_crc_hub_transfer_staging_hash on _crc_hub_transfer_staging(hash) include (block_number);

select *
into _crc_organisation_signup_staging
from crc_organisation_signup_2
limit 0;

create index ix_crc_organisation_signup_staging_hash on _crc_organisation_signup_staging(hash) include (block_number);

select *
into _crc_signup_staging
from crc_signup_2
limit 0;

create index ix_crc_signup_staging_hash on _crc_signup_staging(hash) include (block_number);

select *
into _crc_trust_staging
from crc_trust_2
limit 0;

create index ix_crc_trust_staging_hash on _crc_trust_staging(hash) include (block_number);

select hash, index, timestamp, block_number, "from", "to", token, value::text
into _erc20_transfer_staging
from erc20_transfer_2
limit 0;

create index ix_erc20_transfer_staging_hash on _erc20_transfer_staging(hash) include (block_number);
create index ix_erc20_transfer_staging_from on _erc20_transfer_staging("from");
create index ix_erc20_transfer_staging_to on _erc20_transfer_staging("to");

select hash, index, timestamp, block_number, "from", "to", value::text
into _eth_transfer_staging
from eth_transfer_2
limit 0;

create index ix_eth_transfer_staging_hash on _eth_transfer_staging(hash) include (block_number);
create index ix_eth_transfer_staging_from on _eth_transfer_staging("from");
create index ix_eth_transfer_staging_to on _eth_transfer_staging("to");

select hash, index, timestamp, block_number, initiator, "from", "to", value::text
into _gnosis_safe_eth_transfer_staging
from gnosis_safe_eth_transfer_2
limit 0;

create index ix_gnosis_safe_eth_transfer_staging_hash on _gnosis_safe_eth_transfer_staging(hash) include (block_number);
create index ix_gnosis_safe_eth_transfer_staging_from on _gnosis_safe_eth_transfer_staging("from");
create index ix_gnosis_safe_eth_transfer_staging_to on _gnosis_safe_eth_transfer_staging("to");

select block_number, "from", "to", hash, index, timestamp, value::text, input, nonce, type, classification
into _transaction_staging
from transaction_2
limit 0;

create index ix_transaction_staging_hash on _transaction_staging(hash) include (block_number);
create index ix_transaction_staging_block_number on _transaction_staging(block_number) include (hash);

create table requested_blocks (
    block_no numeric
);
create unique index ux_requested_blocks_block_no on requested_blocks(block_no);


alter table crc_signup_2 add column owners text[];
create index ix_gin_crc_signup_2_owners on crc_signup_2 using GIN (owners);

alter table crc_organisation_signup_2 add column owners text[];
create index ix_gin_crc_organisation_signup_2_owners on crc_organisation_signup_2 using GIN (owners);

alter table _crc_signup_staging add column owners text[];
alter table _crc_organisation_signup_staging add column owners text[];

/*
-- Delete duplicates:
alter table requested_blocks add column pk serial;
with a as (
    select array_agg(pk) agg
    from requested_blocks
    group by block_no
    having count(block_no) > 1
), b as (
    select unnest(a.agg[2:]) as pk
    from a
)
delete from requested_blocks
using b
where b.pk = requested_blocks.pk;
alter table requested_blocks drop column pk;
*/

------------------------------------------------------------------------
-- Check how many and which blocks haven't been imported.
------------------------------------------------------------------------
-- The importer writes all block numbers it intends to import to the
-- 'requested_blocks' table.
-- This function compares the values from the 'requested_blocks' table
-- with the actually imported blocks in the 'blocks' table.
------------------------------------------------------------------------
with max_imported as (
    select max(number) as number from block
), max_staging as (
    select max(number) as number from _block_staging
), min_missing as (
    select min(block_no) -1 missing_block_begin
    from requested_blocks rb
             left join block b on rb.block_no = b.number and b.number < (select number from max_imported)
    where b.number is null
), c as (
    select (select number from max_staging) - (select number from max_imported) as staging_distance
         , (select number from max_imported) - missing_block_begin              as imported_distance
    from min_missing
)
select *
from c ;

--or (slower with more detail):
with c as (
    select a.block_no as requested, b.number as actual
    from requested_blocks a
             left join block b on a.block_no = b.number
    order by block_no
), d as (
    select max(requested) max_requested, max(actual) - 1 as max_imported, max(requested) - max(actual) as distance
    from c
), e as (
    select d.*, rb.block_no as missing_block_no, d.max_imported - rb.block_no distance_from_last_imported
    from d
             join requested_blocks rb on rb.block_no < d.max_imported
             left join block bb on bb.number = rb.block_no
    where bb.number is null
    order by d.max_imported - rb.block_no desc
)
select *
from e;

explain with common as (
    select number
    from block b
             join requested_blocks rb on (rb.block_no = b.number)
)
        select max(common.number), min(common.number)
        from common;

------------------------------------------------------------------------
-- checks for blocks with missing transactions
------------------------------------------------------------------------
with a as (
    select b.number as block_no, b.total_transaction_count, count(t.hash), b.total_transaction_count - count(t.hash) as distance
    from block b
             left join transaction_2 t on b.number = t.block_number
    group by b.number, b.total_transaction_count
    having b.total_transaction_count - count(t.hash) > 0
)
select *
from a;


------------------------------------------------------------------------
-- show a summary of the staging tables contents
------------------------------------------------------------------------
select '_block_staging' as type, count(distinct number) from _block_staging
union all
select '_crc_hub_transfer_staging' as type, count(distinct hash) from _crc_hub_transfer_staging
union all
select '_crc_organisation_signup_staging' as type, count(distinct hash) from _crc_organisation_signup_staging
union all
select '_crc_signup_staging' as type, count(distinct hash) from _crc_signup_staging
union all
select '_erc20_transfer_staging' as type, count(distinct hash) from _erc20_transfer_staging
union all
select '_eth_transfer_staging' as type, count(distinct hash) from _eth_transfer_staging
union all
select '_gnosis_safe_eth_transfer_staging' as type, count(distinct hash) from _gnosis_safe_eth_transfer_staging
union all
select '_transaction_staging' as type, count(distinct hash) from _transaction_staging;


create or replace procedure import_from_staging_2()
    language plpgsql
as
$$
declare
    selected_at_ts timestamp;
begin
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

end;
$$;

create or replace procedure publish_event(topic text, message text)
as
$yolo$
begin
    perform pg_notify(topic, message::text);
end
$yolo$
    language plpgsql;

create view crc_alive_accounts
as
select tt."to"
from crc_token_transfer_2 tt
         join transaction_2 t on tt.hash = t.hash
         join block b on t.block_number = b.number
group by tt."to"
having max(b.timestamp) > now() - interval '90 days';

create view crc_dead_accounts
as
select tt."to"
from crc_token_transfer_2 tt
         join transaction_2 t on tt.hash = t.hash
         join block b on t.block_number = b.number
group by tt."to"
having max(b.timestamp) < now() - interval '90 days';

create view crc_hub_transfers_per_day
as
select b.timestamp::date, count(*) as transfers
from crc_hub_transfer_2 s
         join transaction_2 t on s.hash = t.hash
         join block b on t.block_number = b.number
group by b.timestamp::date;

create view crc_signups_per_day
as
select b.timestamp::date, count(*) as signups
from crc_signup_2 s
         join transaction_2 t on s.hash = t.hash
         join block b on t.block_number = b.number
group by b.timestamp::date;

create view crc_total_minted_amount
as
select sum(value) total_crc_amount
from crc_token_transfer_2
where "from" = '0x0000000000000000000000000000000000000000';

drop view crc_balances_by_safe;
drop view crc_balances_by_safe_and_token;
drop view crc_current_trust;
drop view crc_ledger;
drop view crc_safe_timeline;
drop view crc_minting;
drop view crc_token_transfer;
drop view erc20_minting;

drop table crc_hub_transfer;
drop table crc_signup;
drop table crc_organisation_signup;
drop table crc_trust;
drop table eth_transfer;
drop table erc20_transfer;


create view crc_all_signups as
select c.hash, c.block_number, c.index, c.timestamp, c."user" as "user", c.token as "token"
from crc_signup_2 c
union all
select c.hash, c.block_number, c.index, c.timestamp, c.organisation as "user", null as "token"
from crc_organisation_signup_2 c;

create or replace view crc_safe_timeline_2 (timestamp, block_number, transaction_index, transaction_hash, type, safe_address, contact_address, direction, value, obj) as
WITH safe_timeline AS (
    SELECT cs."timestamp",
           cs.block_number,
           cs.index,
           cs.hash,
           'CrcSignup'::text AS type,
           cs."user",
           cs."user" as contact_address,
           'self'::text      AS direction,
           0                 AS value,
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
               WHEN cht."from" = crc_all_signups."user" AND cht."to" = crc_all_signups."user" THEN cht."to"
               WHEN cht."from" = crc_all_signups."user" THEN cht."to"
               ELSE cht."from"
               END                                   AS contact_address,
           CASE
               WHEN cht."from" = crc_all_signups."user" AND cht."to" = crc_all_signups."user" THEN 'self'::text
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
    FROM crc_hub_transfer_2 cht
             JOIN crc_all_signups ON crc_all_signups."user" = cht."from" OR crc_all_signups."user" = cht."to"
    UNION ALL
    SELECT ct."timestamp",
           ct.block_number,
           ct.index,
           ct.hash,
           'CrcTrust'::text  AS type,
           crc_all_signups."user",
           CASE
               WHEN ct.can_send_to = crc_all_signups."user" AND ct.address = crc_all_signups."user" THEN crc_all_signups."user"
               WHEN ct.can_send_to = crc_all_signups."user" THEN ct.address
               ELSE ct.can_send_to
               END           AS contact_address,
           CASE
               WHEN ct.can_send_to = crc_all_signups."user" AND ct.address = crc_all_signups."user" THEN 'self'::text
               WHEN ct.can_send_to = crc_all_signups."user" THEN 'out'::text
               ELSE 'in'::text
               END           AS direction,
           ct."limit",
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
           ct."from" as contact_address,
           'in'::text         AS direction,
           ct.value,
           row_to_json(ct.*)  AS obj
    FROM crc_minting_2 ct
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
               END             AS contact_address,
           CASE
               WHEN eth."from" = crc_all_signups."user" AND eth."to" = crc_all_signups."user" THEN 'self'::text
               WHEN eth."from" = crc_all_signups."user" THEN 'out'::text
               ELSE 'in'::text
               END             AS direction,
           eth.value,
           row_to_json(eth.*)  AS obj
    FROM eth_transfer_2 eth
             JOIN crc_all_signups ON crc_all_signups."user" = eth."from" OR crc_all_signups."user" = eth."to"
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
               END             AS contact_address,
           CASE
               WHEN seth."from" = crc_all_signups."user" AND seth."to" = crc_all_signups."user" THEN 'self'::text
               WHEN seth."from" = crc_all_signups."user" THEN 'out'::text
               ELSE 'in'::text
               END                       AS direction,
           seth.value,
           row_to_json(seth.*)           AS obj
    FROM gnosis_safe_eth_transfer_2 seth
             JOIN crc_all_signups ON crc_all_signups."user" = seth."from" OR crc_all_signups."user" = seth."to"
)
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


create or replace view crc_current_trust_2 ("user", user_token, can_send_to, can_send_to_token, "limit", history_count)
as
SELECT lte.address AS "user",
       cs_a.token  AS user_token,
       lte.can_send_to,
       cs_b.token  AS can_send_to_token,
       ct."limit",
       lte.history_count
FROM (SELECT max(crc_trust_2.hash)   AS hash,
             count(crc_trust_2.hash) AS history_count,
             crc_trust_2.address,
             crc_trust_2.can_send_to
      FROM crc_trust_2
      GROUP BY crc_trust_2.address, crc_trust_2.can_send_to) lte
         JOIN crc_trust_2 ct ON lte.hash = ct.hash
         JOIN crc_all_signups cs_a ON lte.address = cs_a."user"
         JOIN crc_all_signups cs_b ON lte.can_send_to = cs_b."user";


create or replace view crc_safe_timeline_2 (timestamp, block_number, transaction_index, transaction_hash, type, safe_address, contact_address, direction, value, obj) as
WITH safe_timeline AS (
    SELECT cs."timestamp",
           cs.block_number,
           cs.index,
           cs.hash,
           'CrcSignup'::text AS type,
           cs."user",
           cs."user" as contact_address,
           'self'::text      AS direction,
           0                 AS value,
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
               WHEN cht."from" = crc_all_signups."user" AND cht."to" = crc_all_signups."user" THEN cht."to"
               WHEN cht."from" = crc_all_signups."user" THEN cht."to"
               ELSE cht."from"
               END                                   AS contact_address,
           CASE
               WHEN cht."from" = crc_all_signups."user" AND cht."to" = crc_all_signups."user" THEN 'self'::text
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
    FROM crc_hub_transfer_2 cht
             JOIN crc_all_signups ON crc_all_signups."user" = cht."from" OR crc_all_signups."user" = cht."to"
    UNION ALL
    SELECT ct."timestamp",
           ct.block_number,
           ct.index,
           ct.hash,
           'CrcTrust'::text  AS type,
           crc_all_signups."user",
           CASE
               WHEN ct.can_send_to = crc_all_signups."user" AND ct.address = crc_all_signups."user" THEN crc_all_signups."user"
               WHEN ct.can_send_to = crc_all_signups."user" THEN ct.address
               ELSE ct.can_send_to
               END           AS contact_address,
           CASE
               WHEN ct.can_send_to = crc_all_signups."user" AND ct.address = crc_all_signups."user" THEN 'self'::text
               WHEN ct.can_send_to = crc_all_signups."user" THEN 'out'::text
               ELSE 'in'::text
               END           AS direction,
           ct."limit",
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
           ct."from" as contact_address,
           'in'::text         AS direction,
           ct.value,
           row_to_json(ct.*)  AS obj
    FROM crc_minting_2 ct
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
               END             AS contact_address,
           CASE
               WHEN eth."from" = crc_all_signups."user" AND eth."to" = crc_all_signups."user" THEN 'self'::text
               WHEN eth."from" = crc_all_signups."user" THEN 'out'::text
               ELSE 'in'::text
               END             AS direction,
           eth.value,
           row_to_json(eth.*)  AS obj
    FROM eth_transfer_2 eth
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
               END             AS contact_address,
           CASE
               WHEN erc20."from" = crc_all_signups."user" AND erc20."to" = crc_all_signups."user" THEN 'self'::text
               WHEN erc20."from" = crc_all_signups."user" THEN 'out'::text
               ELSE 'in'::text
               END             AS direction,
           erc20.value,
           row_to_json(erc20.*)  AS obj
    FROM erc20_transfer_2 erc20
             JOIN crc_all_signups ON crc_all_signups."user" = erc20."from" OR crc_all_signups."user" = erc20."to"
             LEFT JOIN crc_Signup_2 s on s.token = erc20.token
    WHERE s.token is null
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
               END             AS contact_address,
           CASE
               WHEN seth."from" = crc_all_signups."user" AND seth."to" = crc_all_signups."user" THEN 'self'::text
               WHEN seth."from" = crc_all_signups."user" THEN 'out'::text
               ELSE 'in'::text
               END                       AS direction,
           seth.value,
           row_to_json(seth.*)           AS obj
    FROM gnosis_safe_eth_transfer_2 seth
             JOIN crc_all_signups ON crc_all_signups."user" = seth."from" OR crc_all_signups."user" = seth."to"
)
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

create or replace view erc20_balances_by_safe_and_token as
    with non_circles_transfers as (
        -- Todo: Filter all tokens which have been minted before the circles-hub inception
        select et.timestamp
             , et.block_number
             , t.index as transaction_index
             , t.hash as transaction_hash
             , 'Erc20Transfer' as type
             , et.token
             , et."from"
             , et."to"
             , et."value"
        from erc20_transfer_2 et
                 join crc_all_signups alls on alls."user" = et."from" or alls."user" = et."to"
                 left join crc_signup_2 s on s.token = et.token
                 join transaction_2 t on et.hash = t.hash
        where s.token is null -- only non-circles tokens
    ), non_circles_ledger as (
        select nct.timestamp
             , nct.block_number
             , nct.transaction_index
             , nct.transaction_hash
             , nct.type
             , alls."user" as safe_address
             , case when nct."from" = alls."user" then nct."to" else nct."from" end as contact_address
             , case when nct."from" = alls."user" then 'out' else 'in' end as direction
             , nct.token
             , nct."from"
             , nct."to"
             , nct.value
        from crc_all_signups alls
                 join non_circles_transfers nct on alls."user" = nct."from" or alls."user" = nct."to"
    ), erc20_balances as (
        select safe_address
             , token
             , sum(case when direction = 'in' then value else value * -1 end) as balance
             , max(timestamp) as last_changed_at
        from non_circles_ledger
        group by safe_address, token
    )
    select *
    from erc20_balances;

create or replace view crc_current_trust_2
as
SELECT lte.address AS "user",
       cs_a.token  AS user_token,
       lte.can_send_to,
       cs_b.token  AS can_send_to_token,
       ct."limit",
       lte.history_count
FROM (SELECT max(crc_trust_2.block_number) AS block_number, -- Todo: This must be max. block_number and max. index within that block
             count(crc_trust_2.hash) AS history_count,
             crc_trust_2.address,
             crc_trust_2.can_send_to
      FROM crc_trust_2
      GROUP BY crc_trust_2.address, crc_trust_2.can_send_to) lte
         JOIN crc_trust_2 ct ON lte.block_number = ct.block_number
         JOIN crc_all_signups cs_a ON lte.address = cs_a."user"
         JOIN crc_all_signups cs_b ON lte.can_send_to = cs_b."user";



