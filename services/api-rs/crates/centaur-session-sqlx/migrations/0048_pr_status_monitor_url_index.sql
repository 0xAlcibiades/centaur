create extension if not exists pg_trgm;

create index if not exists session_messages_slack_pr_url_trgm_idx
    on session_messages using gin ((parts::text) gin_trgm_ops)
    where role = 'user'
      and metadata @> '{"source":"slackbotv2","is_mention":true}'::jsonb;
