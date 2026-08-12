require "test_helper"

module SlackDm
  class SyncConversationTest < ActiveSupport::TestCase
    FakeApiClient = Struct.new(:batch) do
      def ingest_slack_dm_sync_batch(payload)
        self.batch = payload
        { "ok" => true }
      end
    end

    class FakeSlackClient
      attr_reader :history_params

      def conversations_history(params)
        @history_params = params
        {
          "messages" => [
            {
              "type" => "message",
              "ts" => "1700000000.000002",
              "thread_ts" => "1700000000.000002",
              "user" => "U_OTHER",
              "text" => "hel\u0000lo",
              "reply_count" => 1,
              "files" => [ { "id" => "F1", "name" => "fi\u0000le.txt" } ]
            }
          ],
          "response_metadata" => { "next_cursor" => "" }
        }
      end

      def conversations_replies(_params)
        {
          "messages" => [
            { "type" => "message", "ts" => "1700000000.000002", "text" => "hello" },
            { "type" => "message", "ts" => "1700000000.000003", "text" => "reply" }
          ],
          "response_metadata" => { "next_cursor" => "" }
        }
      end
    end

    def setup
      app = OauthApp.create!(
        provider: "slack",
        slug: "slack-unit-#{SecureRandom.hex(4)}",
        client_id: "client",
        client_secret: "secret",
        allowed_scopes: SlackDm::SyncCredential::REQUIRED_SCOPES,
        created_by: users(:acme_admin)
      )
      credential = BrokerCredential.create!(
        oauth_app: app,
        foreign_id: "slack-unit-#{SecureRandom.hex(4)}",
        token_endpoint: "https://slack.com/api/oauth.v2.access",
        access_token: "xoxp-live",
        scopes: SlackDm::SyncCredential::REQUIRED_SCOPES,
        provider_subject: "U_ME"
      )
      @ledger = SlackDm::SyncLedger.create!(
        broker_credential: credential,
        home_team_id: "T123",
        conversation_id: "D1",
        conversation_type: "im",
        raw_payload: { "id" => "D1", "is_im" => true, "user" => "U_OTHER" },
        watermark_ts: "1700000000.000001",
        claim_token: "claim",
        claimed_until: 2.hours.from_now
      )
    end

    test "syncs one ledger conversation without refreshing inventory" do
      api = FakeApiClient.new
      slack = FakeSlackClient.new

      SlackDm::SyncConversation.new(
        @ledger,
        claim_token: "claim",
        api_client: api,
        slack_client: slack
      ).call

      assert_equal "1700000000.000001", slack.history_params["oldest"]
      assert_equal %w[U_OTHER U_ME], api.batch[:members].map { |member| member[:user_id] }
      assert_equal %w[hello reply], api.batch[:messages].map { |message| message[:text] }
      assert_equal "file.txt", api.batch[:attachments].first[:name]
      assert_equal "1700000000.000002", api.batch[:checkpoints].first[:watermark_ts]
      assert_equal "1700000000.000002", @ledger.reload.watermark_ts
      assert_nil @ledger.claim_token
      assert_equal 0, @ledger.backoff_level
    end
  end
end
