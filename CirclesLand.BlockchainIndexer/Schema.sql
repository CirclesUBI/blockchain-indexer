create table block (
    number bigserial primary key,
    hash text not null unique ,
    timestamp timestamp not null,
    total_transaction_count int not null,
    indexed_transaction_count int not null 
);

create unique index idx_block_timestamp on block(timestamp) include (number);

create view last_incomplete_block as
    select max(number) block_no from block
    where total_transaction_count > indexed_transaction_count;

create table transaction (
    id bigserial primary key,
    block_number bigint not null references block(number),
    "from" text not null,
    "to" text null, -- Todo: This happens only on contract creation. Get the address of the deployed contact.
    index int not null,
    gas text not null,
    hash text unique not null,
    value text not null,
    input text null,
    nonce text null,
    type text null,
    gas_price text null,
    classification text[] not null
);

create table crc_organisation_signup (
    id bigserial primary key,
    transaction_id bigint not null references transaction (id),
    organisation text not null
);

create unique index idx_crc_organisation_signup_organisation on crc_organisation_signup(organisation) include (transaction_id);

create table crc_signup (
    id bigserial primary key,
    transaction_id bigint not null references transaction (id),
    "user" text unique not null unique,
    token text not null unique 
);

create unique index idx_crc_signup_user on crc_signup("user") include (transaction_id, token);
create unique index idx_crc_signup_token on crc_signup(token) include (transaction_id, "user");

create table crc_hub_transfer (
    id bigserial primary key,
    transaction_id bigint not null references transaction (id),
    "from" text not null,
    "to" text not null,
    value text not null
);

create index idx_crc_hub_transfer_from on crc_hub_transfer("from") include (transaction_id);
create index idx_crc_hub_transfer_to on crc_hub_transfer("to") include (transaction_id);

create table erc20_transfer (
    id bigserial primary key,
    transaction_id bigint not null references transaction (id),
    "from" text not null,
    "to" text not null,
    token text not null,
    value text not null
);

create index idx_erc20_transfer_from on erc20_transfer("from") include (transaction_id);
create index idx_erc20_transfer_to on erc20_transfer("to") include (transaction_id);

create view erc20_minting as 
    select * 
    from erc20_transfer
    where "from" = '0x0000000000000000000000000000000000000000';

create table crc_trust (
    id bigserial primary key,
    transaction_id bigint not null references transaction (id),
    address text not null,
    can_send_to text not null,
    "limit" bigint not null
); 

create index idx_crc_trust_address on crc_trust(address) include (transaction_id);
create index idx_crc_trust_can_send_to on crc_trust(can_send_to) include (transaction_id);

create table eth_transfer (
    id bigserial primary key,
    transaction_id bigint not null references transaction (id),
    "from" text not null,
    "to" text not null,
    value text not null
);

create index idx_eth_transfer_from on eth_transfer("from") include (transaction_id);
create index idx_eth_transfer_to on eth_transfer("to") include (transaction_id);

create table gnosis_safe_eth_transfer (
    id bigserial primary key,
    transaction_id bigint not null references transaction (id),
    initiator text not null,
    "from" text not null,
    "to" text not null,
    value text not null
);

create index idx_gnosis_safe_eth_transfer_initiator on gnosis_safe_eth_transfer(initiator) include (transaction_id);
create index idx_gnosis_safe_eth_transfer_from on gnosis_safe_eth_transfer("from") include (transaction_id);
create index idx_gnosis_safe_eth_transfer_to on gnosis_safe_eth_transfer("to") include (transaction_id);

create or replace procedure delete_last_incomplete_block()
as
$yolo$
begin
    delete from crc_hub_transfer where transaction_id in (select id from transaction where block_number = (select block_no from last_incomplete_block));
    delete from crc_organisation_signup where transaction_id in (select id from transaction where block_number = (select block_no from last_incomplete_block));
    delete from crc_signup where transaction_id in (select id from transaction where block_number = (select block_no from last_incomplete_block));
    delete from crc_trust where transaction_id in (select id from transaction where block_number = (select block_no from last_incomplete_block));
    delete from erc20_transfer where transaction_id in (select id from transaction where block_number = (select block_no from last_incomplete_block));
    delete from eth_transfer where transaction_id in (select id from transaction where block_number = (select block_no from last_incomplete_block));
    delete from gnosis_safe_eth_transfer where transaction_id in (select id from transaction where block_number = (select block_no from last_incomplete_block));
    delete from transaction where id in (select id from transaction where block_number = (select block_no from last_incomplete_block));
    delete from block where number = (select block_no from last_incomplete_block);
end
$yolo$
language plpgsql;

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

create or replace procedure publish_event(topic text, message text)
as
$yolo$
begin
    perform pg_notify(topic, message::text);
end
$yolo$
language plpgsql;
*/