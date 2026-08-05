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
    label text,
    confidence double precision,
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
        check (status in ('baseline', 'pending', 'running', 'completed', 'failed', 'superseded')),
    constraint user_experience_scans_label_check
        check (label is null or label in ('good', 'mixed', 'bad', 'unknown')),
    constraint user_experience_scans_confidence_check
        check (confidence is null or (confidence >= 0 and confidence <= 1)),
    constraint user_experience_scans_completed_result_check
        check (
            status <> 'completed'
            or (
                label is not null
                and confidence is not null
                and summary is not null
                and completed_at is not null
            )
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

-- Establish a durable rollout baseline in the same table as scan results. These
-- rows prevent the first scheduled run from classifying retained history. A
-- thread becomes eligible only after its latest message changes.
insert into user_experience_scans (
    scan_id,
    thread_key,
    last_message_id,
    last_message_created_at,
    classifier_version,
    model,
    status,
    eligible_after
)
select
    'uxs_baseline_' || md5(s.thread_key || chr(31) || latest.message_id),
    s.thread_key,
    latest.message_id,
    latest.created_at,
    'baseline',
    'baseline',
    'baseline',
    latest.created_at
from sessions s
join lateral (
    select m.message_id, m.created_at
    from session_messages m
    where m.thread_key = s.thread_key
    order by m.created_at desc, m.message_id desc
    limit 1
) latest on true
where coalesce(s.metadata ->> 'platform', '') in (
    'slack', 'discord', 'linear', 'github', 'msteams'
)
  and exists (
      select 1
      from session_messages user_message
      where user_message.thread_key = s.thread_key
        and user_message.role = 'user'
  )
on conflict do nothing;

create index if not exists user_experience_scans_claim_idx
    on user_experience_scans (status, eligible_after, created_at)
    where status in ('pending', 'failed');

create index if not exists user_experience_scans_running_lease_idx
    on user_experience_scans (updated_at)
    where status = 'running';

create index if not exists user_experience_scans_problem_completed_idx
    on user_experience_scans (completed_at desc, label)
    where status = 'completed' and label in ('mixed', 'bad');

create index if not exists user_experience_scans_thread_created_idx
    on user_experience_scans (thread_key, created_at desc);
