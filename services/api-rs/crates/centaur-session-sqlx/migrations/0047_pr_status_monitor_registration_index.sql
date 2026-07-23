create index if not exists session_messages_slack_pr_monitor_idx
    on session_messages (created_at desc, message_id desc)
    where role = 'user'
      and metadata @> '{"source":"slackbotv2","is_mention":true}'::jsonb;
