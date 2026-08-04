create table if not exists workflow_audit_states (
    workflow_name text primary key,
    state jsonb not null default '{}'::jsonb,
    version bigint not null default 1,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint workflow_audit_states_name_nonempty
        check (length(btrim(workflow_name)) > 0),
    constraint workflow_audit_states_state_object
        check (jsonb_typeof(state) = 'object'),
    constraint workflow_audit_states_version_positive
        check (version > 0)
);

comment on table workflow_audit_states is
    'Cross-run state for bounded incremental workflow audits; updated with optimistic version checks.';
