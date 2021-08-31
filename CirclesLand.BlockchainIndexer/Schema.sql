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
    "to" text null, -- Todo: This happens only on contract creation. Get the address of the deployed contact.
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

create index idx_transaction_fk_block_number on transaction(block_number); 

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

create view crc_balances
as
with crc_users as (
    select "user", token
    from crc_signup
), all_events as (
    select cht.transaction_id
         , 'crc_hub_transfer' as type
         , u."user" as subject
         , case
               when cht."from" = u."user" and cht."to" = u."user" then 'self'
               when cht."from" = u."user" then 'out'
               else 'in' end  as direction
         , cht.value
    from crc_hub_transfer cht
             join crc_users u on cht."from" = u."user" or cht."to" = u."user"
    union all
    select cht.transaction_id
         , 'crc_minting' as type
         , u."user" as subject
         , 'in' as direction
         , cht.value
    from crc_minting cht
            join crc_users u on cht."to" = u."user" and u.token = cht.token
)
select e.subject
     , sum(case when e.direction = 'out' then -(e.value) else e.value end) as balance
from all_events e
         join transaction t on e.transaction_id = t.id
         join block b on b.number = t.block_number
group by e.subject;

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

create view crc_balances_by_token
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

create view current_trust
as
    select lte.address,
           lte.can_send_to,
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
             join crc_trust ct on lte.transaction_id = ct.transaction_id;

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
begin
    delete from crc_hub_transfer where transaction_id in (select id from transaction where block_number >= (select block_no from first_incomplete_block limit 1));
    delete from crc_organisation_signup where transaction_id in (select id from transaction where block_number >= (select block_no from first_incomplete_block limit 1));
    delete from crc_signup where transaction_id in (select id from transaction where block_number >= (select block_no from first_incomplete_block limit 1));
    delete from crc_trust where transaction_id in (select id from transaction where block_number >= (select block_no from first_incomplete_block limit 1));
    delete from erc20_transfer where transaction_id in (select id from transaction where block_number >= (select block_no from first_incomplete_block limit 1));
    delete from eth_transfer where transaction_id in (select id from transaction where block_number >= (select block_no from first_incomplete_block limit 1));
    delete from gnosis_safe_eth_transfer where transaction_id in (select id from transaction where block_number >= (select block_no from first_incomplete_block limit 1));
    delete from transaction where block_number >= (select block_no from first_incomplete_block limit 1);
    delete from block where number >= (select block_no from first_incomplete_block limit 1);
end
$yolo$
language plpgsql;

create function safe_timeline(safe_address text)
    returns table(
                     block_number bigint
        , "timestamp" timestamp
        , type text
        , direction text
        , value numeric
        , transaction_hash text
        , obj json
    )
    language plpgsql
as
$yolo$
begin
    return query with my_token as (
        select token
        from crc_signup
        where "user" = safe_address
    ), my_events as (
        select cht.transaction_id
             , 'crc_hub_transfer' as type
             , case when cht."from" = safe_address and cht."to" = safe_address then 'self'
                    when cht."from" = safe_address then 'out'
                    else 'in' end as direction
             , cht.value
             , row_to_json(cht) obj
        from crc_hub_transfer cht
        where (cht."from" = safe_address
            or cht."to" = safe_address)
        union all
        select ct.transaction_id
             , 'crc_trust' as type
             , case when ct.can_send_to = safe_address then 'out' else 'in' end as direction
             , ct."limit"
             , row_to_json(ct) obj
        from crc_trust ct
        where (ct.address = safe_address
            or ct.can_send_to = safe_address)
        union all
        select ct.transaction_id
             , 'crc_minting' as type
             , 'in' as direction
             , ct.value
             , row_to_json(ct) obj
        from crc_minting ct
        where (ct.token = (select token from my_token))
        union all
        select eth.transaction_id
             , 'eth_transfer' as type
             , case when eth."from" = safe_address and eth."to" = safe_address then 'self'
                    when eth."from" = safe_address then 'out'
                    else 'in' end as direction
             , eth.value
             , row_to_json(eth) obj
        from eth_transfer eth
        where (eth."from" = safe_address
            or eth."to" = safe_address)
        union all
        select seth.transaction_id
             , 'gnosis_safe_eth_transfer' as type
             , case when seth."from" = safe_address and seth."to" = safe_address then 'self'
                    when seth."from" = safe_address then 'out'
                    else 'in' end as direction
             , seth.value
             , row_to_json(seth) obj
        from gnosis_safe_eth_transfer seth
        where (seth."from" = safe_address
            or seth."to" = safe_address)
    )
                 select b.number block_number
                      , b.timestamp
                      , e.type
                      , e.direction
                      , e.value
                      , t.hash transaction_hash
                      , e.obj
                 from my_events e
                          join transaction t on e.transaction_id = t.id
                          join block b on b.number = t.block_number
                 order by timestamp, t.index;
end
$yolo$;

create or replace procedure publish_event(topic text, message text)
as
$yolo$
begin
    perform pg_notify(topic, message::text);
end
$yolo$
language plpgsql;
/*
create or replace function on_crc_hub_transfer()
    returns trigger as
$yolo$
begin
    call publish_event('new_event', (jsonb '{"$type":"crc_hub_transfer"}' || row_to_json(new)::jsonb)::text);
    return new;
end
$yolo$
language plpgsql;

create trigger crc_hub_transfer_trigger
    after insert
    on crc_hub_transfer
    for each row
execute procedure on_crc_hub_transfer();

create or replace function on_crc_organisation_signup()
    returns trigger as
$yolo$
begin
    call publish_event('new_event', (jsonb '{"$type":"crc_organisation_signup"}' || row_to_json(new)::jsonb)::text);
    return new;
end;
$yolo$
language plpgsql;

create trigger crc_organisation_signup_trigger
    after insert
    on crc_organisation_signup
    for each row
execute procedure on_crc_organisation_signup();

create or replace function on_crc_signup()
    returns trigger as
$yolo$
begin
    call publish_event('new_event', (jsonb '{"$type":"crc_signup"}' || row_to_json(new)::jsonb)::text);
    return new;
end;
$yolo$
language plpgsql;

create trigger crc_signup_trigger
    after insert
    on crc_signup
    for each row
execute procedure on_crc_signup();

create or replace function on_erc20_transfer()
    returns trigger as
$yolo$
begin
    if ((select count(*) from crc_signup where token = new.token limit 1) = 1
        and new."from" = '0x0000000000000000000000000000000000000000') then
        call publish_event('new_event', (jsonb '{"$type":"crc_minting"}' || row_to_json(new)::jsonb)::text);
    end if;
    return new;
end
$yolo$
language plpgsql;

create trigger erc20_transfer_trigger
    after insert
    on erc20_transfer
    for each row
    execute procedure on_erc20_transfer();

create or replace function on_eth_transfer()
    returns trigger as
$yolo$
begin
    call publish_event('new_event', (jsonb '{"$type":"eth_transfer"}' || row_to_json(new)::jsonb)::text);
    return new;
end;
$yolo$
language plpgsql;

create trigger eth_transfer_trigger
    after insert
    on eth_transfer
    for each row
execute procedure on_eth_transfer();

create or replace function on_gnosis_safe_eth_transfer()
    returns trigger as
$yolo$
begin
    call publish_event('new_event', (jsonb '{"$type":"gnosis_safe_eth_transfer"}' || row_to_json(new)::jsonb)::text);
    return new;
end;
$yolo$
language plpgsql;

create trigger gnosis_safe_eth_transfer_trigger
    after insert
    on gnosis_safe_eth_transfer
    for each row
execute procedure on_gnosis_safe_eth_transfer();

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


CREATE OR REPLACE FUNCTION employee_insert_trigger_fnc()
  RETURNS trigger AS
$$
BEGIN

    INSERT INTO "Employee_Audit" ( "EmployeeId", "LastName", "FirstName","UserName" ,"EmpAdditionTime")
         VALUES(NEW."EmployeeId",NEW."LastName",NEW."FirstName",current_user,current_date);

RETURN NEW;
END;
$$
LANGUAGE 'plpgsql';

CREATE TRIGGER employee_insert_trigger
  AFTER INSERT
  ON "Employee"
  FOR EACH ROW
  EXECUTE PROCEDURE employee_insert_trigger_fnc();
*/