alter table sessions
    add column if not exists sandbox_metadata_trace_resource_uid text;

create index if not exists sessions_metadata_trace_resource_uid_idx
    on sessions (sandbox_metadata_trace_resource_uid)
    where sandbox_metadata_trace_enabled is true;
