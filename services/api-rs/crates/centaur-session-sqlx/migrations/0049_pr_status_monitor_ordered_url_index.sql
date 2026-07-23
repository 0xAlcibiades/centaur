drop index if exists session_messages_slack_pr_url_trgm_idx;

create index if not exists session_messages_slack_pr_monitor_url_idx
    on session_messages (created_at desc, message_id desc)
    where role = 'user'
      and metadata @> '{"source":"slackbotv2","is_mention":true}'::jsonb
      and parts::text like '%github.com%/pull/%';
