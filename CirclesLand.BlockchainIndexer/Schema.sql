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

create view crc_signups_per_day
as
    select b.timestamp::date, count(*) as signups
    from crc_signup s
             join transaction t on s.transaction_id = t.id
             join block b on t.block_number = b.number
    group by b.timestamp::date;

create table crc_hub_transfer (
    id bigserial primary key,
    transaction_id bigint not null references transaction (id),
    "from" text not null,
    "to" text not null,
    value numeric not null
);

create view crc_alive_accounts
as
    select tt."to"
    from crc_token_transfer tt
             join transaction t on tt.transaction_id = t.id
             join block b on t.block_number = b.number
    group by tt."to"
    having max(b.timestamp) > now() - interval '90 days';

create index idx_crc_hub_transfer_from on crc_hub_transfer("from") include (transaction_id);
create index idx_crc_hub_transfer_to on crc_hub_transfer("to") include (transaction_id);
create index idx_crc_hub_transfer_fk_transaction_id on crc_hub_transfer(transaction_id);

create view crc_hub_transfers_per_day
as
    select b.timestamp::date, count(*) as transfers
    from crc_hub_transfer s
             join transaction t on s.transaction_id = t.id
             join block b on t.block_number = b.number
    group by b.timestamp::date;

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

create view crc_total_minted_amount
as
    select sum(value) total_crc_amount
    from crc_token_transfer
    where "from" = '0x0000000000000000000000000000000000000000';

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
               'crc_signup'::text AS type,
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
               'crc_hub_transfer'::text                  AS type,
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
               'crc_trust'::text AS type,
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
               'crc_minting'::text AS type,
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
               'eth_transfer'::text AS type,
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
               'gnosis_safe_eth_transfer'::text AS type,
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

-- v3
select number, hash, timestamp, total_transaction_count, null::timestamp as selected_at, null::timestamp as imported_at
into block_staging
from block
limit 0;

create index ix_block_staging_selected_at on _block_staging(number) include (selected_at);

select hash, hash, index, timestamp, block_number, "from", "to", value::text
into crc_hub_transfer_staging
from crc_hub_transfer_2
limit 0;

create index ix_crc_hub_transfer_staging_hash on _crc_hub_transfer_staging(hash) include (block_number);

select *
into crc_organisation_signup_staging
from crc_organisation_signup_2
limit 0;

create index ix_crc_organisation_signup_staging_hash on _crc_organisation_signup_staging(hash) include (block_number);

select *
into crc_signup_staging
from crc_signup_2
limit 0;

create index ix_crc_signup_staging_hash on _crc_signup_staging(hash) include (block_number);

select *
into crc_trust_staging
from crc_trust_2
limit 0;

create index ix_crc_trust_staging_hash on _crc_trust_staging(hash) include (block_number);

select hash, index, timestamp, block_number, "from", "to", token, value::text
into erc20_transfer_staging
from erc20_transfer_2
limit 0;

create index ix_erc20_transfer_staging_hash on _erc20_transfer_staging(hash) include (block_number);
create index ix_erc20_transfer_staging_from on _erc20_transfer_staging("from");
create index ix_erc20_transfer_staging_to on _erc20_transfer_staging("to");

select hash, index, timestamp, block_number, "from", "to", value::text
into eth_transfer_staging
from eth_transfer_2
limit 0;

create index ix_eth_transfer_staging_hash on _eth_transfer_staging(hash) include (block_number);
create index ix_eth_transfer_staging_from on _eth_transfer_staging("from");
create index ix_eth_transfer_staging_to on _eth_transfer_staging("to");

select hash, hash, index, timestamp, block_number, initiator, "from", "to", value::text
into gnosis_safe_eth_transfer_staging
from gnosis_safe_eth_transfer_2
limit 0;

create index ix_gnosis_safe_eth_transfer_staging_hash on _gnosis_safe_eth_transfer_staging(hash) include (block_number);
create index ix_gnosis_safe_eth_transfer_staging_from on _gnosis_safe_eth_transfer_staging("from");
create index ix_gnosis_safe_eth_transfer_staging_to on _gnosis_safe_eth_transfer_staging("to");

select block_number, "from", "to", hash, index, timestamp, value::text, input, nonce, type, classification
into transaction_staging
from transaction_2
limit 0;

create index ix_transaction_staging_hash on _transaction_staging(hash) include (block_number);

create or replace view selected_staging_blocks
as
select distinct *
from _block_staging
where selected_at is not null
  and imported_at is null;

create or replace view imported_staging_blocks
as
select distinct *
from _block_staging
where (selected_at is not null
  and imported_at is not null)
  or already_available is not null;

create or replace view selected_staging_transactions
as
select distinct s.selected_at, ts.*
from _transaction_staging ts
         join selected_staging_blocks s on ts.block_number = s.number;

create or replace view imported_staging_transactions
as
select distinct s.imported_at, ts.*
from _transaction_staging ts
         join imported_staging_blocks s on ts.block_number = s.number;

create or replace view selected_staging_hub_transfers
as
select distinct ts.selected_at, s.*
from selected_staging_transactions ts
         join _crc_hub_transfer_staging s on ts.hash = s.hash;

create or replace view selected_staging_organisation_signups
as
select distinct ts.selected_at, s.*
from selected_staging_transactions ts
         join _crc_organisation_signup_staging s on ts.hash = s.hash;

create or replace view selected_staging_signups
as
select distinct ts.selected_at, s.*
from selected_staging_transactions ts
         join _crc_signup_staging s on ts.hash = s.hash;

create or replace view selected_staging_trusts
as
select distinct ts.selected_at, s.*
from selected_staging_transactions ts
         join _crc_trust_staging s on ts.hash = s.hash;

create or replace view selected_staging_erc20_transfers
as
select distinct ts.selected_at, s.*
from selected_staging_transactions ts
         join _erc20_transfer_staging s on ts.hash = s.hash;

create or replace view selected_staging_eth_transfers
as
select distinct ts.selected_at, s.*
from selected_staging_transactions ts
         join _eth_transfer_staging s on ts.hash = s.hash;

create or replace view selected_staging_safe_eth_transfers
as
select distinct ts.selected_at, s.*
from selected_staging_transactions ts
         join _gnosis_safe_eth_transfer_staging s on ts.hash = s.hash;


create or replace procedure import_from_staging()
as
$yolo$
declare
    selected_at_ts timestamp;
    imported_at_ts timestamp;
begin
    select now() into selected_at_ts;
        
    update _block_staging b
    set already_available = true
    from
        (
            select b.number, b.total_transaction_count, count(distinct t.hash)
            from _block_staging b
                     join transaction_2 t on b.number = t.block_number
            group by b.number, b.total_transaction_count
            having b.total_transaction_count = count(distinct t.hash)
        ) a
    where a.number = b.number;
    
    update _block_staging b
    set selected_at = selected_at_ts
    from
        (
            select b.number, b.total_transaction_count, count(distinct t.hash)
            from _block_staging b
                     join _transaction_staging t on b.number = t.block_number
            where b.already_available is null
            group by b.number, b.total_transaction_count
            having b.total_transaction_count = count(distinct t.hash)
        ) a
    where a.number = b.number;

    insert into block
    select sb.number, sb.hash, sb.timestamp, sb.total_transaction_count, 0
    from selected_staging_blocks sb
             left join block b on sb.number = b.number
    where b.number is null
      and selected_at = selected_at_ts;

    insert into transaction_2
    select sb.block_number
         , sb."from"
         , sb."to"
         , sb.hash
         , sb.index
         , sb.timestamp
         , sb.value::numeric
         , sb.input
         , sb.nonce
         , sb.type
         , sb.classification
    from selected_staging_transactions sb
             left join transaction_2 b on sb.hash = b.hash
    where b.hash is null
      and selected_at = selected_at_ts;

    insert into crc_hub_transfer_2
    select sb.hash
         , sb.index
         , sb.timestamp
         , sb.block_number
         , sb."from"
         , sb."to"
         , sb.value::numeric
    from selected_staging_hub_transfers sb
             left join crc_hub_transfer_2 b
                       on sb.hash = b.hash
                           and sb."from" = b."from"
                           and sb."to" = b."to"
                           and sb.value::numeric = b.value
    where b.hash is null
      and selected_at = selected_at_ts;

    insert into crc_organisation_signup_2
    select sb.hash
         , sb.index
         , sb.timestamp
         , sb.block_number
         , sb.organisation
    from selected_staging_organisation_signups sb
             left join crc_organisation_signup_2 b on sb."organisation" = b."organisation"
    where b.hash is null
      and selected_at = selected_at_ts;

    insert into crc_signup_2
    select sb.hash
         , sb.index
         , sb.timestamp
         , sb.block_number
         , sb.user
         , sb.token
    from selected_staging_signups sb
             left join crc_signup_2 b on sb."user" = b."user"
    where b.hash is null
      and selected_at = selected_at_ts;

    insert into crc_trust_2
    select sb.hash
         , sb.index
         , sb.timestamp
         , sb.block_number
         , sb.address
         , sb.can_send_to
         , sb."limit"
    from selected_staging_trusts sb
             left join crc_trust_2 b on sb.hash = b.hash
        and sb.address = b.address
        and sb.can_send_to = b.can_send_to
        and sb."limit" = b."limit"
    where b.hash is null
      and selected_at = selected_at_ts;

    insert into erc20_transfer_2
    select sb.hash
         , sb.index
         , sb.timestamp
         , sb.block_number
         , sb."from"
         , sb."to"
         , sb.token
         , sb.value::numeric
    from selected_staging_erc20_transfers sb
             left join erc20_transfer_2 b on sb.hash = b.hash
        and sb."from" = b."from"
        and sb."to" = b."to"
        and sb.token = b.token
        and sb.value::numeric = b.value
    where b.hash is null
      and selected_at = selected_at_ts;

    insert into eth_transfer_2
    select sb.hash
         , sb.index
         , sb.timestamp
         , sb.block_number
         , sb."from"
         , sb."to"
         , sb.value::numeric
    from selected_staging_eth_transfers sb
             left join eth_transfer_2 b on sb.hash = b.hash
        and sb."from" = b."from"
        and sb."to" = b."to"
        and sb.value::numeric = b.value
    where b.hash is null
      and selected_at = selected_at_ts;

    insert into gnosis_safe_eth_transfer_2
    select sb.hash
         , sb.index
         , sb.timestamp
         , sb.block_number
         , sb.initiator
         , sb."from"
         , sb."to"
         , sb.value::numeric
    from selected_staging_safe_eth_transfers sb
             left join gnosis_safe_eth_transfer_2 b on sb.hash = b.hash
        and sb."from" = b."from"
        and sb."to" = b."to"
        and sb.initiator = b.initiator
        and sb.value::numeric = b.value
    where b.hash is null
      and selected_at = selected_at_ts;

    select now() into imported_at_ts;

    update _block_staging b
    set imported_at = imported_at_ts
    where selected_at = selected_at_ts;

    delete from _crc_hub_transfer_staging s
        using imported_staging_transactions i
    where s.hash = i.hash;

    delete from _crc_organisation_signup_staging s
        using imported_staging_transactions i
    where s.hash = i.hash;

    delete from _crc_signup_staging s
        using imported_staging_transactions i
    where s.hash = i.hash;

    delete from _crc_trust_staging s
        using imported_staging_transactions i
    where s.hash = i.hash;

    delete from _erc20_transfer_staging s
        using imported_staging_transactions i
    where s.hash = i.hash;

    delete from _eth_transfer_staging s
        using imported_staging_transactions i
    where s.hash = i.hash;

    delete from _gnosis_safe_eth_transfer_staging s
        using imported_staging_transactions i
    where s.hash = i.hash;

    delete from _transaction_staging s
        using imported_staging_transactions i
    where s.hash = i.hash;

    delete from _block_staging s
        using imported_staging_blocks i
    where s.number = i.number;
end;
$yolo$
    language plpgsql;
