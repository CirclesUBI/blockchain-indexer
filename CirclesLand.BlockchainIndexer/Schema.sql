create table block (
    number bigserial primary key,
    hash text not null unique ,
    timestamp timestamp not null,
    total_transaction_count int not null,
    indexed_transaction_count int not null 
);

create index idx_block_timestamp on block(timestamp) include (number);

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

create table crc_organisation_signup (
    id bigserial primary key,
    transaction_id bigint not null references transaction (id),
    organisation text not null
);

create table crc_signup (
    id bigserial primary key,
    transaction_id bigint not null references transaction (id),
    "user" text unique not null,
    token text not null
);

create table crc_hub_transfer (
    id bigserial primary key,
    transaction_id bigint not null references transaction (id),
    "from" text not null,
    "to" text not null,
    value text not null
);

create table erc20_transfer (
    id bigserial primary key,
    transaction_id bigint not null references transaction (id),
    "from" text not null,
    "to" text not null,
    token text not null,
    value text not null
);

create table crc_trust (
    id bigserial primary key,
    transaction_id bigint not null references transaction (id),
    address text not null,
    can_send_to text not null,
    "limit" text not null
);

create table eth_transfer (
    id bigserial primary key,
    transaction_id bigint not null references transaction (id),
    "from" text not null,
    "to" text not null,
    value text not null
);

create table gnosis_safe_eth_transfer (
    id bigserial primary key,
    transaction_id bigint not null references transaction (id),
    "from" text not null,
    "to" text not null,
    value text not null
);

create table transaction_message (
    id bigserial primary key,
    transaction_id bigint not null references transaction (id),
    text text not null
);

create table token_minting (
    id bigserial primary key,
    transaction_id bigint not null references transaction (id),
    "to" text not null,
    token text not null,
    tokens text not null
);

-- 'invitation_eoas' are created empty.
-- To make them usable they have to be funded.
-- The funding and redemptions are tracked in
-- 'invitation_eoa_funding' and 'invitation_eoa_redemption'. 
create table invitation_eoa (
    id bigserial primary key,
    owner_id text not null,
    name text null,
    redeem_code text unique not null,
    eoa_address text unique not null,
    eoa_key text unique not null
);

create table invitation_eoa_funding (
    id bigserial primary key,
    transaction_id bigint not null references transaction (id),
    invitation_eoa_id bigint not null references invitation_eoa (id),
    eth_transfer_id bigint not null references eth_transfer (id)
);

create table invitation_eoa_redemption (
    id bigserial primary key,
    transaction_id bigint not null references transaction (id),
    invitation_eoa_id bigint not null references invitation_eoa (id),
    eth_transfer_id bigint not null references eth_transfer (id)
);

create or replace procedure publish_event(topic text, message text)
as
$yolo$
begin
    perform pg_notify(topic, message::text);
end
$yolo$
language plpgsql;