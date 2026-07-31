create table if not exists metadata_trace_consents (
    source text not null,
    workspace_id text not null,
    user_id text not null,
    enabled boolean not null default false,
    expires_at timestamptz,
    revision bigint not null default 0 check (revision >= 0),
    drain_pending boolean not null default false,
    updated_at timestamptz not null default now(),
    primary key (source, workspace_id, user_id),
    check ((enabled and expires_at is not null) or (not enabled and expires_at is null))
);

create table if not exists metadata_trace_consent_requests (
    source text not null,
    workspace_id text not null,
    user_id text not null,
    idempotency_key text not null check (length(idempotency_key) between 1 and 128),
    request_hash text not null,
    result_enabled boolean,
    result_expires_at timestamptz,
    result_revision bigint,
    result_drain_pending boolean,
    created_at timestamptz not null default now(),
    primary key (source, workspace_id, user_id, idempotency_key)
);

alter table sessions
    add column if not exists sandbox_metadata_trace_subject_hash text,
    add column if not exists sandbox_metadata_trace_source text,
    add column if not exists sandbox_metadata_trace_workspace_id text,
    add column if not exists sandbox_metadata_trace_user_id text,
    add column if not exists sandbox_metadata_trace_consent_revision bigint,
    add column if not exists sandbox_metadata_trace_assignment_epoch text;

create index if not exists sessions_metadata_trace_actor_idx
    on sessions (sandbox_metadata_trace_subject_hash)
    where sandbox_metadata_trace_enabled is true;
