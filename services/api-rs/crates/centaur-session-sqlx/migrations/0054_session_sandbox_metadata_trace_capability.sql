alter table sessions
    add column if not exists sandbox_metadata_trace_enabled boolean,
    add column if not exists sandbox_metadata_trace_expires_at timestamptz,
    add column if not exists sandbox_metadata_trace_config_fingerprint text;
