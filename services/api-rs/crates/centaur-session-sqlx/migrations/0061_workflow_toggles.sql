create table if not exists workflow_toggles (
    workflow_name text primary key,
    enabled boolean not null,
    updated_by text not null default '',
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    check (workflow_name ~ '^[a-z0-9][a-z0-9._-]{0,127}$')
);

create index if not exists idx_workflow_toggles_updated
    on workflow_toggles (updated_at desc);
