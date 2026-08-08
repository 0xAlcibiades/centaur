-- An execution receives one opaque Console-managed credential pin.  This is
-- deliberately separate from input delivery: input may be replayed, but a
-- credential generation must never change while that logical execution lives.
create table if not exists execution_credential_pins (
    execution_id text primary key references session_executions(execution_id) on delete cascade,
    acquire_operation_id text not null unique check (length(btrim(acquire_operation_id)) > 0),
    release_operation_id text not null unique check (length(btrim(release_operation_id)) > 0),
    state text not null check (state in ('acquiring', 'held', 'evidence', 'release_pending', 'released')),
    pin_id text unique,
    credential_version text,
    expires_at timestamptz,
    quota_evidence_operation_id text unique,
    quota_evidence_at timestamptz,
    quota_reported_at timestamptz,
    release_attempts integer not null default 0 check (release_attempts >= 0),
    last_release_error text,
    created_at timestamptz not null default clock_timestamp(),
    held_at timestamptz,
    released_at timestamptz,
    updated_at timestamptz not null default clock_timestamp(),
    constraint execution_credential_pins_held_metadata check (
        state = 'acquiring' or (
            pin_id is not null and credential_version is not null
            and expires_at is not null
        )
    ),
    constraint execution_credential_pins_released_at check (
        (state = 'released') = (released_at is not null)
    )
);

create index if not exists execution_credential_pins_release_outbox_idx
    on execution_credential_pins (updated_at, execution_id)
    where state = 'release_pending';

-- Every terminal CAS places a held/evidence pin in the durable release outbox
-- in the same transaction. A process crash cannot strand a credential merely
-- because it happened after the terminal event was persisted.
create or replace function enqueue_terminal_execution_credential_pin_release()
returns trigger
language plpgsql
as $$
begin
    if old.status in ('queued', 'running') and new.status not in ('queued', 'running') then
        update execution_credential_pins
        set state = 'release_pending', updated_at = clock_timestamp()
        where execution_id = new.execution_id
          and state in ('held', 'evidence');
    end if;
    return new;
end
$$;

drop trigger if exists session_executions_credential_pin_release on session_executions;
create trigger session_executions_credential_pin_release
after update of status on session_executions
for each row execute function enqueue_terminal_execution_credential_pin_release();

comment on table execution_credential_pins is
    'Opaque per-execution Autorotate credential pins and durable external-release outbox; never stores credentials.';
