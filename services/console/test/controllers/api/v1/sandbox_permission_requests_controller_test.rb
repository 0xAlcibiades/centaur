require "test_helper"

module Api
  module V1
    class SandboxPermissionRequestsControllerTest < ActionDispatch::IntegrationTest
      include ActiveJob::TestHelper

      setup do
        @proxy = proxies(:acme_proxy)
      end

      teardown do
        clear_enqueued_jobs
        clear_performed_jobs
      end

      test "rejects requests without a sandbox token" do
        post "/api/v1/sandbox/permission_requests", params: text_body.to_json, headers: json_headers

        assert_response :unauthorized
        assert_equal "invalid or missing sandbox token", json_body.dig("error", "message")
      end

      test "rejects invalid sandbox token" do
        with_env("CENTAUR_JWT_SIGNING_SECRET" => "test-secret") do
          post "/api/v1/sandbox/permission_requests",
               params: text_body.to_json,
               headers: auth_headers("not-a-jwt")
        end

        assert_response :unauthorized
      end

      test "rejects stale proxy principal claims" do
        token = nil
        with_env("CENTAUR_JWT_SIGNING_SECRET" => "test-secret") do
          token = token_for(@proxy)
          @proxy.update!(principal: principals(:acme_user_bob))
          post "/api/v1/sandbox/permission_requests",
               params: text_body.to_json,
               headers: auth_headers(token)
        end

        assert_response :unauthorized
        assert_equal "invalid sandbox token", json_body.dig("error", "message")
      end

      test "rejects unassigned proxy claims" do
        proxy = proxies(:unassigned_proxy)
        token = token_for_payload(
          "sandbox_id" => proxy.name,
          "proxy_id" => proxy.oid,
          "principal_id" => @proxy.principal.oid
        )

        with_env("CENTAUR_JWT_SIGNING_SECRET" => "test-secret") do
          post "/api/v1/sandbox/permission_requests",
               params: text_body.to_json,
               headers: auth_headers(token)
        end

        assert_response :unauthorized
      end

      test "rejects non Slack channel principal" do
        proxy = proxies(:globex_proxy)

        with_permission_request_env do
          post "/api/v1/sandbox/permission_requests",
               params: text_body.to_json,
               headers: auth_headers(token_for(proxy))
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

      test "rejects invalid request shape" do
        with_permission_request_env do
          post "/api/v1/sandbox/permission_requests",
               params: { data: { kind: "text", metadata: {} } }.to_json,
               headers: auth_headers(token_for(@proxy))
        end

        assert_response :unprocessable_entity
        assert json_body.dig("error", "details", "metadata").any? do |message|
          message.include?("missing required properties: request")
        end
      end

      test "rejects request when permission request Slack is not configured" do
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
        assert_equal "permission requests are not configured", json_body.dig("error", "message")
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
        assert_equal "pending", json_body.dig("data", "status")
        assert_equal @proxy.principal, request.requesting_principal
        assert_equal @proxy, request.requesting_proxy
        assert_equal "C0123456789", request.requesting_slack_channel_id
        assert_equal({ "request" => "Please authorize Gmail." }, request.metadata)
        assert_equal "pending", request.approver_notification_status
      end

      test "creates text permission request with arbitrary request text" do
        with_permission_request_env do
          assert_enqueued_jobs 1, only: PermissionRequestApproverNotificationJob do
            post "/api/v1/sandbox/permission_requests",
                 params: {
                   data: {
                     kind: "text",
                     metadata: {
                       request: "Please authorize Gmail and Google Drive for customer follow-up."
                     }
                   }
                 }.to_json,
                 headers: auth_headers(token_for(@proxy))
          end
        end

        assert_response :created
        request = PermissionRequest.last
        assert_equal PermissionRequest::TEXT_KIND, request.kind
        assert_equal({ "request" => "Please authorize Gmail and Google Drive for customer follow-up." }, request.metadata)
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

      def token_for(proxy, now: Time.current)
        SandboxEntitlements::Jwt.encode_for_proxy(proxy, now: now)
      end

      def token_for_payload(payload)
        CentaurJwt::Hs256.encode(
          payload.merge(
            "aud" => SandboxEntitlements::Jwt.audience,
            "iss" => SandboxEntitlements::Jwt.issuer,
            "iat" => Time.current.to_i,
            "exp" => 5.minutes.from_now.to_i
          ),
          signing_secret: "test-secret"
        )
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

      def with_env(values)
        previous = values.keys.to_h { |key| [ key, ENV[key] ] }
        values.each do |key, value|
          value.nil? ? ENV.delete(key) : ENV[key] = value
        end
        yield
      ensure
        previous.each do |key, value|
          value.nil? ? ENV.delete(key) : ENV[key] = value
        end
      end
    end
  end
end
