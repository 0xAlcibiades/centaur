require "test_helper"

module Api
  module V1
    class SandboxPermissionRequestsControllerTest < ActionDispatch::IntegrationTest
      include ActiveJob::TestHelper

      setup { @proxy = proxies(:acme_proxy) }
      teardown { clear_enqueued_jobs }

      test "requires sandbox auth" do
        post "/api/v1/sandbox/permission_requests", params: text_body.to_json, headers: json_headers

        assert_response :unauthorized
      end

      test "rejects non Slack channel principals" do
        with_permission_request_env do
          post "/api/v1/sandbox/permission_requests",
               params: text_body.to_json,
               headers: auth_headers(token_for(proxies(:globex_proxy)))
        end

        assert_response :unprocessable_entity
        assert_includes json_body.dig("error", "details", "requesting_principal"),
                        "must be a Slack channel principal"
      end

      test "rejects unsupported request kind" do
        with_permission_request_env do
          post "/api/v1/sandbox/permission_requests",
               params: { data: { kind: "slack", metadata: { requested_channel_ids: [ "C1111111111" ] } } }.to_json,
               headers: auth_headers(token_for(@proxy))
        end

        assert_response :unprocessable_entity
        assert_includes json_body.dig("error", "details", "kind"), "is not included in the list"
      end

      test "validates text metadata" do
        with_permission_request_env do
          post "/api/v1/sandbox/permission_requests",
               params: { data: { kind: "text", metadata: {} } }.to_json,
               headers: auth_headers(token_for(@proxy))
        end

        assert_response :unprocessable_entity
        assert json_body.dig("error", "details", "metadata").any? { |message| message.include?("request") }
      end

      test "requires Slack notification configuration" do
        with_env(
          "CENTAUR_JWT_SIGNING_SECRET" => "test-secret",
          "CENTAUR_CONSOLE_PERMISSION_REQUEST_APPROVAL_CHANNEL_ID" => nil,
          "CENTAUR_CONSOLE_SLACK_BOT_TOKEN" => nil,
          "SLACK_BOT_TOKEN" => nil
        ) do
          assert_no_enqueued_jobs only: PermissionRequestApproverNotificationJob do
            assert_no_difference -> { PermissionRequest.count } do
              post "/api/v1/sandbox/permission_requests",
                   params: text_body.to_json,
                   headers: auth_headers(token_for(@proxy))
            end
          end
        end

        assert_response :service_unavailable
      end

      test "creates text permission request and enqueues approver notification" do
        with_permission_request_env("CENTAUR_CONSOLE_PUBLIC_URL" => "https://console.test") do
          assert_enqueued_jobs 1, only: PermissionRequestApproverNotificationJob do
            assert_difference -> { PermissionRequest.count }, 1 do
              post "/api/v1/sandbox/permission_requests",
                   params: text_body.to_json,
                   headers: auth_headers(token_for(@proxy))
            end
          end
        end

        assert_response :created
        request = PermissionRequest.last
        assert_equal request.oid, json_body.dig("data", "id")
        assert_equal PermissionRequest::TEXT_KIND, request.kind
        assert_equal({ "request" => "Please authorize Gmail." }, request.metadata)
        assert_equal "pending", request.approver_notification_status
      end

      private

      def text_body
        {
          data: {
            kind: "text",
            requesting_slack_thread_ts: "170.123",
            metadata: { request: "Please authorize Gmail." }
          }
        }
      end

      def auth_headers(token)
        json_headers.merge("Authorization" => "Bearer #{token}")
      end

      def json_headers
        { "Content-Type" => "application/json" }
      end

      def token_for(proxy)
        with_env("CENTAUR_JWT_SIGNING_SECRET" => "test-secret") do
          SandboxEntitlements::Jwt.encode_for_proxy(proxy)
        end
      end

      def json_body
        JSON.parse(response.body)
      end

      def with_permission_request_env(values = {})
        with_env({
          "CENTAUR_JWT_SIGNING_SECRET" => "test-secret",
          "CENTAUR_CONSOLE_PERMISSION_REQUEST_APPROVAL_CHANNEL_ID" => "CAPPROVERS",
          "CENTAUR_CONSOLE_SLACK_BOT_TOKEN" => "xoxb-test-token"
        }.merge(values)) { yield }
      end
    end
  end
end
