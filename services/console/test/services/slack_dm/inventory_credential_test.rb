require "test_helper"

module SlackDm
  class InventoryCredentialTest < ActiveSupport::TestCase
    FakeApiClient = Struct.new(:batches) do
      def ingest_slack_dm_sync_batch(payload)
        batches << payload
        { "ok" => true }
      end
    end

    class FakeSlackClient
      attr_reader :list_params

      def initialize(pages)
        @pages = pages
        @list_params = []
      end

      def auth_test = { "ok" => true, "team_id" => "T123" }

      def conversations_list(params)
        @list_params << params
        @pages.shift
      end
    end

    def setup
      app = OauthApp.create!(
        provider: "slack",
        slug: "slack-inventory-#{SecureRandom.hex(4)}",
        client_id: "client",
        client_secret: "secret",
        allowed_scopes: SlackDm::SyncCredential::REQUIRED_SCOPES,
        created_by: users(:acme_admin)
      )
      @credential = BrokerCredential.create!(
        oauth_app: app,
        foreign_id: "slack-inventory-#{SecureRandom.hex(4)}",
        token_endpoint: "https://slack.com/api/oauth.v2.access",
        access_token: "xoxp-live",
        scopes: SlackDm::SyncCredential::REQUIRED_SCOPES,
        provider_subject: "U_ME"
      )
    end

    test "inventory pages conversations and refreshes durable ledger metadata" do
      slack = FakeSlackClient.new([
        {
          "channels" => [ { "id" => "D1", "is_im" => true, "user" => "U_OTHER" } ],
          "response_metadata" => { "next_cursor" => "next" }
        },
        {
          "channels" => [ { "id" => "G1", "is_private" => true, "is_ext_shared" => true } ],
          "response_metadata" => { "next_cursor" => "" }
        }
      ])
      api = FakeApiClient.new([])

      assert_equal 2, SlackDm::InventoryCredential.new(
        @credential,
        api_client: api,
        slack_client: slack
      ).call

      assert_equal "T123", @credential.reload.labels["slack_team_id"]
      assert_equal %w[D1 G1], SlackDm::SyncLedger.order(:conversation_id).pluck(:conversation_id)
      assert_equal "U_OTHER", SlackDm::SyncLedger.find_by!(conversation_id: "D1").raw_payload["user"]
      assert SlackDm::SyncLedger.find_by!(conversation_id: "G1").is_ext_shared?
      assert_nil slack.list_params.first["cursor"]
      assert_equal "next", slack.list_params.second["cursor"]
      assert_equal false, api.batches.first[:replace_memberships]
      assert_equal 2, api.batches.first[:conversations].length
    end

    test "inventory deactivates conversations that disappeared" do
      SlackDm::SyncLedger.refresh_inventory!(
        credential: @credential,
        home_team_id: "T123",
        conversations: [
          {
            conversation_id: "D_OLD",
            conversation_type: "im",
            is_archived: false,
            is_ext_shared: false,
            raw_payload: { "id" => "D_OLD", "is_im" => true }
          }
        ]
      )
      slack = FakeSlackClient.new([
        {
          "channels" => [ { "id" => "D_NEW", "is_im" => true, "user" => "U_OTHER" } ],
          "response_metadata" => { "next_cursor" => "" }
        }
      ])

      SlackDm::InventoryCredential.new(
        @credential,
        api_client: FakeApiClient.new([]),
        slack_client: slack
      ).call

      refute SlackDm::SyncLedger.find_by!(conversation_id: "D_OLD").active?
      assert SlackDm::SyncLedger.find_by!(conversation_id: "D_NEW").active?
    end
  end
end
