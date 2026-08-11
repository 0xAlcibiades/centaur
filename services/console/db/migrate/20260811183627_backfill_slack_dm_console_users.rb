class BackfillSlackDmConsoleUsers < ActiveRecord::Migration[8.1]
  def up
    execute <<~SQL.squish
      WITH eligible_principals AS (
        SELECT id, slack_team_id, slack_user_id, slack_email
        FROM principals
        WHERE kind = 'slack_dm'
          AND console_user_id IS NULL
          AND slack_user_id ~ '^([UW][A-Z0-9]{8,}|USLACK)$'
          AND slack_team_id ~ '^[TE][A-Z0-9]{8,}$'
      ), candidates AS (
        SELECT principals.id AS principal_id, user_identities.user_id
        FROM eligible_principals principals
        INNER JOIN user_identities
          ON user_identities.provider = 'slack'
         AND user_identities.subject = principals.slack_user_id
         AND user_identities.team_id = principals.slack_team_id

        UNION

        SELECT principals.id AS principal_id, user_identities.user_id
        FROM eligible_principals principals
        INNER JOIN user_identities
          ON user_identities.email_verified = TRUE
         AND lower(trim(user_identities.email)) = lower(trim(principals.slack_email))

        UNION

        SELECT principals.id AS principal_id, broker_credentials.created_by_id AS user_id
        FROM eligible_principals principals
        INNER JOIN broker_credentials
          ON broker_credentials.provider_subject = principals.slack_user_id
         AND broker_credentials.labels->>'slack_team_id' = principals.slack_team_id
         AND broker_credentials.created_by_id IS NOT NULL
        INNER JOIN oauth_apps
          ON oauth_apps.id = broker_credentials.oauth_app_id
         AND oauth_apps.provider = 'slack'
      ), resolved AS (
        SELECT principal_id, MIN(user_id) AS user_id
        FROM candidates
        GROUP BY principal_id
        HAVING COUNT(DISTINCT user_id) = 1
      )
      UPDATE principals
      SET console_user_id = resolved.user_id,
          console_user_email = users.email,
          updated_at = CURRENT_TIMESTAMP
      FROM resolved
      INNER JOIN users ON users.id = resolved.user_id
      WHERE principals.id = resolved.principal_id
        AND principals.console_user_id IS NULL
    SQL
  end

  def down
    # One-way identity backfill. A derived link may become operationally
    # significant after deployment, so rollback must not remove it.
  end
end
