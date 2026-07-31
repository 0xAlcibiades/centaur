alter table sessions
    add column if not exists sandbox_metadata_trace_config_generation bigint;

create table if not exists metadata_trace_config_state (
    singleton boolean primary key default true check (singleton),
    generation bigint not null check (generation > 0),
    config_fingerprint text not null,
    reconciler_owner_id text,
    reconciler_fence bigint not null default 0,
    reconciler_lease_expires_at timestamptz,
    activated_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);
