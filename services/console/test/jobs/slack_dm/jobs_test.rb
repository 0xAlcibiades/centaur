require "test_helper"

module SlackDm
  class JobsTest < ActiveJob::TestCase
    def slack_app(slug: "slack-dms")
      OauthApp.create!(
        provider: "slack",
        slug: slug,
        client_id: "slack-client-#{SecureRandom.hex(4)}",
        client_secret: "secret",
        allowed_scopes: SlackDm::SyncCredential::REQUIRED_SCOPES,
        created_by: users(:acme_admin)
      )
    end

    def slack_credential(
      app:,
      scopes: SlackDm::SyncCredential::REQUIRED_SCOPES,
      access_token: "xoxp-live",
      provider_subject: "U#{SecureRandom.hex(4).upcase}",
      labels: {}
    )
      BrokerCredential.create!(
        oauth_app: app,
        foreign_id: "slack-dms-#{SecureRandom.hex(6)}",
        token_endpoint: "https://slack.com/api/oauth.v2.access",
        access_token: access_token,
        refresh_token: "refresh",
        last_refresh: Time.current,
        expires_at: 1.hour.from_now,
        scopes: scopes,
        provider_subject: provider_subject,
        labels: labels
      )
    end

    test "PollSyncJob enqueues credentials with any supported private conversation scopes" do
      app = slack_app
      good = slack_credential(app: app, labels: { "slack_team_id" => "T123" })
      dm_only = slack_credential(
        app: app,
        scopes: SlackDm::SyncCredential::DM_REQUIRED_SCOPES,
        labels: { "slack_team_id" => "T123" }
      )
      missing_scope = slack_credential(app: app, scopes: %w[chat:write])
      no_token = slack_credential(app: app, access_token: nil)
      other_app = slack_app(slug: "other-slack")
      other = slack_credential(app: other_app)

      SlackDm::PollSyncJob.perform_now("slack-dms")

      sync_jobs = enqueued_jobs.select { |job| job[:job] == SlackDm::SyncCredentialJob }
      assert_equal 1, sync_jobs.length
      assert_equal "#{app.id}:T123", sync_jobs.first[:args].first
      assert_equal [ good.id, dm_only.id ].sort, sync_jobs.first[:args].second
      refute_includes sync_jobs.first[:args].second, missing_scope.id
      refute_includes sync_jobs.first[:args].second, no_token.id
      refute_includes sync_jobs.first[:args].second, other.id
    end

    test "credentials without a team label retain per-credential scopes" do
      app = slack_app
      first = slack_credential(app: app)
      second = slack_credential(app: app)

      refute_equal SlackDm::SyncCredential.sync_scope_for(first),
                   SlackDm::SyncCredential.sync_scope_for(second)
      assert_equal "#{app.id}:credential:#{first.id}", SlackDm::SyncCredential.sync_scope_for(first)
    end

    test "SyncCredentialJob deduplicates work for the same app and workspace" do
      app = slack_app
      first = slack_credential(app: app, labels: { "slack_team_id" => "T123" })
      second = slack_credential(app: app, labels: { "slack_team_id" => "T123" })
      other_team = slack_credential(app: app, labels: { "slack_team_id" => "T456" })

      first_job = SlackDm::SyncCredentialJob.new(
        SlackDm::SyncCredential.sync_scope_for(first),
        [ first.id, second.id ]
      )
      other_team_job = SlackDm::SyncCredentialJob.new(
        SlackDm::SyncCredential.sync_scope_for(other_team),
        [ other_team.id ]
      )

      refute_equal first_job.concurrency_key, other_team_job.concurrency_key
      assert_equal :discard, SlackDm::SyncCredentialJob.concurrency_on_conflict
      assert_equal 30.minutes, SlackDm::SyncCredentialJob.concurrency_duration
    end

    test "SyncCredentialJob rotates one credential per scope run" do
      app = slack_app
      first = slack_credential(app: app, labels: { "slack_team_id" => "T123" })
      second = slack_credential(app: app, labels: { "slack_team_id" => "T123" })
      ids = [ first.id, second.id ].sort
      scope = "#{app.id}:T123"
      cache = ActiveSupport::Cache::MemoryStore.new
      synced_ids = []
      fake_sync = Object.new
      fake_sync.define_singleton_method(:call) { true }
      factory = lambda do |credential|
        synced_ids << credential.id
        fake_sync
      end

      Rails.stub(:cache, cache) do
        SlackDm::SyncCredential.stub(:new, factory) do
          SlackDm::SyncCredentialJob.perform_now(scope, ids)
          SlackDm::SyncCredentialJob.perform_now(scope, ids)
        end
      end

      assert_equal ids, synced_ids
    end

    test "SyncCredentialJob is a no-op for missing credentials" do
      assert_nothing_raised { SlackDm::SyncCredentialJob.perform_now(-1) }
    end
  end
end
