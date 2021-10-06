drop procedure delete_service(service_id text);
drop procedure  delete_service_signal(service_id text, name text);
drop procedure delete_signal(name text);
drop function is_service_signal_set(service_id text, name text);
drop procedure publish_event(topic text, message text);
drop procedure register_service(service_id text, timeout_at timestamp);
drop procedure set_signal(service_id text, is_primary bool, name text, value text);
drop function try_acquire_lock(service_id text, name text, timeout_at timestamp);
drop function try_get_service_signal_value(service_id text, name text);

drop view active_services;
drop view active_service_ids;

drop table signal;
drop table lock;
drop table service;

create table service (
    id text primary key,
    host_id text not null,
    created_at timestamp default now(),
    timeout_at timestamp
);
create index idx_service_timeout_at on service(timeout_at);
    
create table lock (
    id bigserial primary key,
    created_at timestamp default now(),
    timeout_at timestamp default now(),
    service_id text references service(id),
    name text not null
);
alter table lock add constraint uc_lock_name unique(name);
create index idx_lock_timeout_at on lock(timeout_at);
create index idx_lock_service_id on lock(service_id);
create index u_lock_name on lock(name) include (service_id, timeout_at);

create table signal (
    id bigserial primary key,
    created_at timestamp default now(),
    service_id text references service(id),
    is_primary bool not null,
    name text not null,
    value text null
);
alter table signal add constraint uc_signal_name_service_id unique (name, service_id);
create unique index u_idx_signal_name_service_id on signal(name, service_id);
create index u_idx_signal_service_id on signal(service_id);

create view active_service_ids
as
    select s.id as service_id
         , s.host_id
         , s.timeout_at
    from service s
    where s.timeout_at > now();

create view active_services
as
    select distinct * 
    from (
       select s.service_id
            , s.host_id
            , s.timeout_at
            , case
                  when l.id is not null then 'lock'
                  else null end as resource_type
            , case
                  when l.id is not null then l.id
                  else null end as resource_id
            , case
                  when l.id is not null then l.name
                  else null end as resource_name
            , case
                  when l.id is not null then null
                  else null end as resource_value
            , null as is_primary
       from active_service_ids s
                left join lock l on s.service_id = l.service_id
       where s.timeout_at > now()
         and l.timeout_at > now()
       union all
       select s.service_id
            , s.host_id
            , s.timeout_at
            , case
                  when s2.id is not null then 'signal'
                  else null end as resource_type
            , case
                  when s2.id is not null then s2.id
                  else null end as resource_id
            , case
                  when s2.id is not null then s2.name
                  else null end as resource_name
            , case
                  when s2.id is not null then s2.value
                  else null end as resource_value
            , case
                  when s2.id is not null then s2.is_primary
                  else null end as is_primary
       from active_service_ids s
                left join signal s2 on s.service_id = s2.service_id
       where s.timeout_at > now()
   ) activities;

create or replace procedure publish_event(topic text, message text)
as
$yolo$
begin
    perform pg_notify(topic, message::text);
end
$yolo$
language plpgsql;

create or replace function next_event_id()
returns bigint
as
$yolo$
declare 
begin
    update event_id set id = id + 1;
    return (select event_id.id as event_id from event_id limit 1);
end
$yolo$
    language plpgsql;

-- registers or updates a service entry
create or replace procedure register_service(service_id text, host_id text, timeout_at timestamp)
as
$yolo$
declare 
    event_json jsonb;
    now timestamp;
begin
    select now() into now;
    
    -- set the timeout of all locks to the service-timeout if
    -- it was reached in the mean time.
    update lock set timeout_at = s.timeout_at
    from service s 
    where lock.service_id = s.id
      and s.timeout_at < now;
    
    -- Register or update the service
    insert into service (id, host_id, timeout_at) 
    values (register_service.service_id, register_service.host_id, register_service.timeout_at)
    on conflict(id) do update set timeout_at = register_service.timeout_at;
    
    select json_build_object('timestamp', now
                           , 'id', extract(microseconds from now())
                           , 'type', 'connected'
                           , 'service_id', register_service.service_id
                           , 'host_id', register_service.host_id
                           , 'timeout_at', register_service.timeout_at) into event_json;
    -- TODO: Only publish on first connect, not on keepalive updates
    call publish_event('service', event_json::text);
end;
$yolo$
language plpgsql;

create or replace procedure delete_service(service_id text)
as
$yolo$
declare
    event_json jsonb;
begin
    delete from signal s
    where s.service_id = delete_service.service_id;
    
    delete from lock l
    where l.service_id = delete_service.service_id;
    
    delete from service s
    where s.id = delete_service.service_id;

    select json_build_object('timestamp', now()
                           , 'id', extract(microseconds from now())
                           , 'type', 'disconnected'
                           , 'service_id', delete_service.service_id) into event_json;

    call publish_event('service', event_json::text);
end
$yolo$
language plpgsql;


-- 
create or replace procedure set_signal(service_id text, is_primary bool, name text, value text)
as
$yolo$
declare
    now timestamp;
    event_json jsonb;
begin
    select now() into now;
        
    insert into signal (created_at, service_id, is_primary, name, value)
    values (now, set_signal.service_id, set_signal.is_primary, set_signal.name, set_signal.value)
    on conflict on constraint uc_signal_name_service_id  do update set value = set_signal.value;

    select json_build_object('timestamp', now
                           , 'id', extract(microseconds from now())
                           , 'is_primary', is_primary
                           , 'service_id', set_signal.service_id
                           , 'name', set_signal.name
                           , 'value', set_signal.value) into event_json;

    call publish_event('signal', event_json::text);
end
$yolo$
language plpgsql;

create or replace function try_get_lock_owner(name text) 
    returns text
as
$yolo$
declare 
    lock_owner text;
begin
    select l.service_id into lock_owner
    from lock l
             join service c on c.id = l.service_id
    where l.name = try_get_lock_owner.name
      and l.timeout_at > now()
      and c.timeout_at > now();
    
    return lock_owner;
end;
$yolo$
language plpgsql;

-- Tries to acquire a lock with the specified parameters and returns its 'id'.
-- If the lock couldn't be acquired then the current lock_owner (a service_id) is returned.
create or replace function try_acquire_lock(service_id text, name text, timeout_at timestamp)
    returns text
as
$yolo$
declare
    now timestamp;
begin
    select now() into now;
    
    -- Clear all timed-out locks
    delete from lock where lock.timeout_at <= now;
    
    -- If another process holds the lock nothing happens
    -- If this process holds the lock then the timeout is updated
    -- If nobody holds a lock then a new lock is inserted
    insert into lock(service_id, name, timeout_at)
        select locks.service_id
             , locks.name
             , locks.timeout_at 
        from (
                 select 'proposal'                      as type
                      , 0                               as weight
                      , try_acquire_lock.service_id     as service_id
                      , try_acquire_lock.name           as name
                      , try_acquire_lock.timeout_at     as timeout_at
                 union all
                 select 'fact'
                      , 1 as weight
                      , l.service_id
                      , l.name
                      , try_acquire_lock.timeout_at
                 from lock l
                    join service c on c.id = l.service_id
                 where l.name = try_acquire_lock.name
                   and l.timeout_at > now
                   and c.timeout_at > now
                 order by weight desc
                 limit 1
             ) locks
        where (locks.service_id = try_acquire_lock.service_id)
        limit 1
    on conflict 
        on constraint uc_lock_name 
        do update 
            set timeout_at = try_acquire_lock.timeout_at;

    return (
        select c.id
        from lock l
            join service c on c.id = l.service_id
                                     where l.name = try_acquire_lock.name
                                       and l.timeout_at > now
                                       and c.timeout_at > now
        limit 1);
end
$yolo$
language plpgsql;

create or replace function try_get_service_signal_value(service_id text, name text)
    returns text
as
$yolo$
declare
    result text;
begin
    select value into result 
    from signal s
    join service c on c.id = try_get_service_signal_value.service_id
     and c.id = s.service_id
    where s.name = try_get_service_signal_value.name
      and s.service_id = try_get_service_signal_value.service_id
      and c.timeout_at > now();
    
    return result;
end
$yolo$
language plpgsql;

create or replace function is_service_signal_set(service_id text, name text)
    returns bool
as
$yolo$
begin
    return exists (
            select s.id
            from signal s
            join service c on c.id = is_service_signal_set.service_id
                          and c.id = s.service_id
            where s.name = is_service_signal_set.name
              and c.timeout_at > now()
        );
end
$yolo$
language plpgsql;

create or replace procedure delete_service_signal(service_id text, name text)
as
$yolo$
begin
    delete from signal s
    where s.service_id = delete_service_signal.service_id 
      and s.name = delete_service_signal.name;
end
$yolo$
language plpgsql;

create or replace procedure delete_signal(name text)
as
$yolo$
begin
    delete from signal s
    where s.name = delete_signal.name;
end
$yolo$
language plpgsql;