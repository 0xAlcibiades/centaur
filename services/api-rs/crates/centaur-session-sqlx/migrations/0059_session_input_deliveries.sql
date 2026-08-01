alter table sessions
    add column if not exists sandbox_resource_uid text,
    add column if not exists sandbox_assignment_epoch text;

-- Older deployments introduced the trace assignment epoch before the trace
-- resource UID. Do not turn those historically valid partial trace identities
-- into a permanently partial canonical assignment identity.
update sessions
set sandbox_resource_uid = null,
    sandbox_assignment_epoch = null
where (sandbox_resource_uid is null) <> (sandbox_assignment_epoch is null);

update sessions
set sandbox_resource_uid = sandbox_metadata_trace_resource_uid,
    sandbox_assignment_epoch = sandbox_metadata_trace_assignment_epoch
where sandbox_id is not null
  and sandbox_resource_uid is null
  and sandbox_assignment_epoch is null
  and sandbox_metadata_trace_resource_uid is not null
  and sandbox_metadata_trace_assignment_epoch is not null;

alter table session_warm_sandboxes
    add column if not exists sandbox_resource_uid text,
    add column if not exists sandbox_assignment_epoch text;

do $$
begin
    if exists (
        select 1
        from session_executions
        where status in ('queued', 'running')
    ) then
        raise exception
            'cannot install session_input_deliveries while active executions exist; drain old api writers and wait for queued/running executions to reach zero before this cutover';
    end if;
end $$;

create table if not exists session_input_deliveries (
    delivery_id text primary key,
    thread_key text not null references sessions(thread_key) on delete cascade,
    execution_id text not null references session_executions(execution_id) on delete cascade,
    sequence bigint not null check (sequence >= 0),
    idempotency_key text not null check (length(idempotency_key) between 1 and 512),
    message_ids jsonb not null,
    input_lines jsonb not null,
    input_sha256 text not null check (input_sha256 ~ '^[0-9a-f]{64}$'),
    input_line_count integer not null check (input_line_count >= 0),
    boundary_fingerprint text not null check (length(boundary_fingerprint) between 1 and 512),
    state text not null default 'pending'
        check (state in ('pending', 'claimed', 'ambiguous', 'flushed', 'failed')),
    owner_id text,
    owner_generation bigint not null default 0 check (owner_generation >= 0),
    owner_lease_expires_at timestamptz,
    sandbox_id text,
    sandbox_resource_uid text,
    sandbox_assignment_epoch text,
    attempts integer not null default 0 check (attempts >= 0),
    last_error text,
    created_at timestamptz not null default now(),
    claimed_at timestamptz,
    flushed_at timestamptz,
    failed_at timestamptz,
    updated_at timestamptz not null default now(),
    constraint session_input_deliveries_owner_lease_check check (
        (state = 'claimed') = (owner_id is not null and owner_lease_expires_at is not null)
    ),
    constraint session_input_deliveries_flushed_check check (
        (state <> 'flushed') or flushed_at is not null
    ),
    constraint session_input_deliveries_failed_check check (
        (state <> 'failed') or failed_at is not null
    ),
    -- Exact input is retained only until the write is resolved. The digest and
    -- count are enough to audit an acknowledged or terminally failed delivery
    -- without retaining a replayable plaintext copy.
    constraint session_input_deliveries_input_payload_check check (
        (state in ('pending', 'claimed', 'ambiguous')
            and jsonb_typeof(input_lines) = 'array'
            and jsonb_array_length(input_lines) = input_line_count)
        or (state in ('flushed', 'failed') and input_lines = '[]'::jsonb)
    ),
    unique (execution_id, sequence),
    unique (thread_key, idempotency_key)
);

create index if not exists session_input_deliveries_recovery_idx
    on session_input_deliveries (state, owner_lease_expires_at, created_at, delivery_id)
    where state in ('pending', 'claimed', 'ambiguous');

create index if not exists session_input_deliveries_execution_state_idx
    on session_input_deliveries (execution_id, state, sequence);

create or replace function fence_unresolved_session_input_delivery()
returns trigger
language plpgsql
as $$
begin
    if old.status in ('queued', 'running')
       and new.status not in ('queued', 'running')
       and exists (
           select 1
           from session_input_deliveries delivery
           where delivery.execution_id = old.execution_id
             and delivery.state in ('pending', 'claimed', 'ambiguous')
       ) then
        raise exception 'execution % has unresolved input delivery', old.execution_id
            using errcode = '55000';
    end if;
    return new;
end
$$;

drop trigger if exists session_executions_input_delivery_fence on session_executions;
create trigger session_executions_input_delivery_fence
before update of status on session_executions
for each row execute function fence_unresolved_session_input_delivery();
