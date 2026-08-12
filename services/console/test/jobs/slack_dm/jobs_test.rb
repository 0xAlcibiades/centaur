require "test_helper"

module SlackDm
  class JobsTest < ActiveJob::TestCase
    include ActiveSupport::Testing::TimeHelpers

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
      access_token: "xoxp-live"
    )
      BrokerCredential.create!(
        oauth_app: app,
        foreign_id: "slack-dms-#{SecureRandom.hex(6)}",
        token_endpoint: "https://slack.com/api/oauth.v2.access",
        access_token: access_token,
        scopes: scopes,
        provider_subject: "U#{SecureRandom.hex(4).upcase}"
      )
    end

    test "poll enqueues one inventory job per syncable credential" do
      app = slack_app
      good = slack_credential(app: app)
      dm_only = slack_credential(app: app, scopes: SlackDm::SyncCredential::DM_REQUIRED_SCOPES)
      slack_credential(app: app, scopes: %w[chat:write])
      slack_credential(app: app, access_token: nil)
      slack_credential(app: slack_app(slug: "other-slack"))

      SlackDm::PollSyncJob.perform_now("slack-dms")

      inventory_jobs = enqueued_jobs.select { |job| job[:job] == SlackDm::InventoryCredentialJob }
      assert_equal [ good.id, dm_only.id ].sort, inventory_jobs.map { |job| job[:args].first }.sort
    end

    test "inventory jobs deduplicate per credential" do
      job = SlackDm::InventoryCredentialJob.new(123)

      assert job.concurrency_key.end_with?("/slack_dm_inventory_123")
      assert_equal :discard, SlackDm::InventoryCredentialJob.concurrency_on_conflict
      assert_equal 30.minutes, SlackDm::InventoryCredentialJob.concurrency_duration
    end

    test "legacy credential jobs become inventory jobs" do
      SlackDm::SyncCredentialJob.perform_now("old-scope", [ 12, 34 ])

      inventory_jobs = enqueued_jobs.select { |job| job[:job] == SlackDm::InventoryCredentialJob }
      assert_equal [ 12, 34 ], inventory_jobs.map { |job| job[:args].first }
    end

    test "dispatcher claims oldest due rows and enqueues bounded unit jobs" do
      travel_to Time.zone.at(1_000), with_usec: true
      credential = slack_credential(app: slack_app)
      later = create_ledger(credential, "D2", next_sync_at: 2.minutes.ago)
      earlier = create_ledger(credential, "D1", next_sync_at: 3.minutes.ago)

      with_env("CENTAUR_CONSOLE_SLACK_DM_SYNC_DISPATCH_BATCH_SIZE" => "1") do
        SlackDm::DispatchSyncJob.perform_now("slack-dms")
      end

      jobs = enqueued_jobs.select { |job| job[:job] == SlackDm::SyncConversationJob }
      assert_equal 1, jobs.length
      assert_equal earlier.id, jobs.first[:args].first
      assert_equal earlier.reload.claim_token, jobs.first[:args].second
      assert_nil later.reload.claim_token
    end

    test "unit job records failures as row backoff without raising" do
      travel_to Time.zone.at(1_000), with_usec: true
      credential = slack_credential(app: slack_app)
      ledger = create_ledger(
        credential,
        "D1",
        claim_token: "claim",
        claimed_until: 1.hour.from_now
      )
      failing_sync = Object.new
      failing_sync.define_singleton_method(:call) { raise SlackDm::ApiClient::SlackApiError, "rate limited" }

      SlackDm::SyncConversation.stub(:new, ->(*) { failing_sync }) do
        assert_nothing_raised { SlackDm::SyncConversationJob.perform_now(ledger.id, "claim") }
      end

      ledger.reload
      assert_equal 1, ledger.backoff_level
      assert_match "SlackDm::ApiClient::SlackApiError: rate limited", ledger.last_error
      assert_equal 5.seconds.from_now, ledger.next_sync_at
      assert_nil ledger.claim_token
    end

    private

    def create_ledger(credential, conversation_id, **attributes)
      SlackDm::SyncLedger.create!(
        {
          broker_credential: credential,
          home_team_id: "T123",
          conversation_id: conversation_id,
          conversation_type: "im",
          raw_payload: { "id" => conversation_id, "is_im" => true }
        }.merge(attributes)
      )
    end
  end
end
