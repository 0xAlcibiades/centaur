require "test_helper"

module SlackDm
  class ApiClientTest < ActiveSupport::TestCase
    include ActiveSupport::Testing::TimeHelpers

    CapturingLogger = Struct.new(:warnings) do
      def warn(entry) = warnings << entry
    end

    def setup
      app = OauthApp.create!(
        provider: "slack",
        slug: "slack-api-#{SecureRandom.hex(4)}",
        client_id: "client",
        client_secret: "secret",
        allowed_scopes: SlackDm::SyncCredential::REQUIRED_SCOPES,
        created_by: users(:acme_admin)
      )
      @credential = BrokerCredential.create!(
        oauth_app: app,
        foreign_id: "slack-api-#{SecureRandom.hex(4)}",
        token_endpoint: "https://slack.com/api/oauth.v2.access",
        access_token: "xoxp-live",
        scopes: SlackDm::SyncCredential::REQUIRED_SCOPES,
        labels: { "slack_team_id" => "T123" }
      )
    end

    test "paces repeated methods through the database bucket" do
      travel_to Time.zone.parse("2030-01-01 00:00:00"), with_usec: true
      sleeps = []
      http = lambda do |endpoint:, params:, access_token:|
        assert_equal SlackDm::ApiClient::CONVERSATIONS_HISTORY_ENDPOINT, endpoint
        assert_equal "D1", params["channel"]
        assert_equal "xoxp-live", access_token
        { "ok" => true, "messages" => [] }
      end
      client = SlackDm::ApiClient.new(@credential, slack_api_http: http, sleeper: ->(seconds) { sleeps << seconds })

      2.times { client.conversations_history("channel" => "D1") }

      assert_equal 1, sleeps.length
      assert_in_delta 1.2, sleeps.first, 0.001
    end

    test "honors Retry-After and records one structured warning" do
      travel_to Time.zone.parse("2030-01-01 00:00:00"), with_usec: true
      sleeps = []
      attempts = 0
      logger = CapturingLogger.new([])
      http = lambda do |**|
        attempts += 1
        if attempts == 1
          HttpClient::Response.new(status: 429, body: "", headers: { "retry-after" => "2" })
        else
          { "ok" => true, "messages" => [] }
        end
      end

      Rails.stub(:logger, logger) do
        SlackDm::ApiClient.new(
          @credential,
          slack_api_http: http,
          sleeper: ->(seconds) { sleeps << seconds }
        ).conversations_history("channel" => "D1")
      end

      assert_equal 2, attempts
      assert_equal 1, logger.warnings.length
      assert_equal "slack_dm_sync_rate_limited", logger.warnings.first[:event]
      assert_in_delta 2.25, sleeps.first, 0.001
    end

    test "honors Retry-After before auth discovers the workspace" do
      @credential.update!(labels: {})
      sleeps = []
      attempts = 0
      http = lambda do |**|
        attempts += 1
        if attempts == 1
          HttpClient::Response.new(status: 429, body: "", headers: { "retry-after" => "2" })
        else
          { "ok" => true, "team_id" => "T123" }
        end
      end

      SlackDm::ApiClient.new(
        @credential,
        slack_api_http: http,
        sleeper: ->(seconds) { sleeps << seconds }
      ).auth_test

      assert_equal 2, attempts
      assert_equal [ 2.25 ], sleeps
    end
  end
end
