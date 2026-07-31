alter table metadata_trace_consent_requests
    add column if not exists drain_assignment_revision bigint;
