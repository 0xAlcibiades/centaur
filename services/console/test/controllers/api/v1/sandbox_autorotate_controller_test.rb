require "test_helper"

module Api
  module V1
    class SandboxAutorotateControllerTest < ActionDispatch::IntegrationTest
      setup do
        @proxy = proxies(:acme_proxy)
      end

      test "returns aggregate status without detailed account data" do
        client = Minitest::Mock.new
        client.expect(
          :status,
          {
            "generated_at" => "2026-07-29T12:00:00Z",
            "total" => 5,
            "healthy" => 4,
            "available" => 3,
            "limited" => 1,
            "login_required" => 1,
            "disabled" => 0,
            "leased" => 1,
            "removed" => 0,
            "next_available_at" => nil,
            "pending_enrollments" => 0
          }
        )

        with_sandbox_request(client) do
          get "/api/v1/sandbox/autorotate/status", headers: auth_headers
        end

        assert_response :ok
        assert_equal 3, json_body.dig("data", "available")
        refute json_body.fetch("data").key?("accounts")
        client.verify
      end

      test "rejects a stale sandbox assignment" do
        with_env("CENTAUR_JWT_SIGNING_SECRET" => "test-secret") do
          token = SandboxEntitlements::Jwt.encode_for_proxy(@proxy)
          @proxy.update!(principal: principals(:globex_user))

          get(
            "/api/v1/sandbox/autorotate/status",
            headers: { "Authorization" => "Bearer #{token}" }
          )
        end

        assert_response :unauthorized
      end

      private

      def with_sandbox_request(client)
        with_env("CENTAUR_JWT_SIGNING_SECRET" => "test-secret") do
          AutorotateClient.stub(:new, client) { yield }
        end
      end

      def auth_headers
        token = SandboxEntitlements::Jwt.encode_for_proxy(@proxy)
        { "Authorization" => "Bearer #{token}" }
      end

      def json_body
        JSON.parse(response.body)
      end
    end
  end
end
