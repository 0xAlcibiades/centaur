create table if not exists user_experience_scans (
    scan_id text primary key,
    thread_key text not null references sessions(thread_key) on delete cascade,
    -- Keep the snapshot identifier even if message-level retention later prunes
    -- the source row. Deleting the parent session still removes its scans.
    last_message_id text not null,
    last_message_created_at timestamptz not null,
    classifier_version text not null,
    model text not null,
    status text not null default 'pending',
    problem_detected boolean,
    experience text,
    severity text,
    user_emotion text,
    agent_contribution text,
    confidence double precision,
    failure_modes text[] not null default '{}',
    evidence_message_ids text[] not null default '{}',
    summary text,
    result jsonb not null default '{}'::jsonb,
    attempt_count integer not null default 0,
    last_error text not null default '',
    workflow_run_id text,
    eligible_after timestamptz not null,
    review_status text not null default 'unreviewed',
    reviewed_by text,
    reviewed_at timestamptz,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    completed_at timestamptz,
    constraint user_experience_scans_snapshot_unique
        unique (thread_key, last_message_id, classifier_version, model),
    constraint user_experience_scans_status_check
        check (status in ('pending', 'running', 'completed', 'failed', 'superseded')),
    constraint user_experience_scans_experience_check
        check (experience is null or experience in ('good', 'mixed', 'bad', 'unknown')),
    constraint user_experience_scans_severity_check
        check (severity is null or severity in ('none', 'low', 'medium', 'high', 'critical')),
    constraint user_experience_scans_user_emotion_check
        check (user_emotion is null or user_emotion in ('neutral', 'disappointed', 'frustrated', 'angry', 'unknown')),
    constraint user_experience_scans_agent_contribution_check
        check (agent_contribution is null or agent_contribution in ('none', 'possible', 'likely', 'unknown')),
    constraint user_experience_scans_confidence_check
        check (confidence is null or (confidence >= 0 and confidence <= 1)),
    constraint user_experience_scans_completed_result_check
        check (
            status <> 'completed'
            or (
                problem_detected is not null
                and experience is not null
                and severity is not null
                and user_emotion is not null
                and agent_contribution is not null
                and confidence is not null
                and summary is not null
                and completed_at is not null
            )
        ),
    constraint user_experience_scans_problem_severity_check
        check (
            problem_detected is null
            or (problem_detected and severity in ('low', 'medium', 'high', 'critical'))
            or (not problem_detected and severity = 'none')
        ),
    constraint user_experience_scans_attempt_count_check
        check (attempt_count >= 0),
    constraint user_experience_scans_review_status_check
        check (review_status in ('unreviewed', 'confirmed', 'false_positive', 'resolved')),
    constraint user_experience_scans_classifier_version_len
        check (octet_length(classifier_version) between 1 and 128),
    constraint user_experience_scans_scan_id_len
        check (octet_length(scan_id) between 1 and 128),
    constraint user_experience_scans_model_len
        check (octet_length(model) between 1 and 128),
    constraint user_experience_scans_summary_len
        check (summary is null or octet_length(summary) <= 4000),
    constraint user_experience_scans_error_len
        check (octet_length(last_error) <= 4000)
);

create index if not exists user_experience_scans_claim_idx
    on user_experience_scans (status, eligible_after, created_at)
    where status in ('pending', 'failed');

create index if not exists user_experience_scans_running_lease_idx
    on user_experience_scans (updated_at)
    where status = 'running';

create index if not exists user_experience_scans_problem_completed_idx
    on user_experience_scans (completed_at desc, severity)
    where status = 'completed' and problem_detected = true;

create index if not exists user_experience_scans_thread_created_idx
    on user_experience_scans (thread_key, created_at desc);
