require "test_helper"

module Api
  module V1
    class AutorotateParentLeasesControllerTest < ActionDispatch::IntegrationTest
      def auth_headers
        { "Authorization" => "Bearer iak_acme-ci-token", "Content-Type" => "application/json" }
      end

      test "pin response is opaque and never contains runtime credentials" do
        lease = AutorotateParentLease.create!(consumer: "bojack", lease_id: "lease-1", fence: 2,
                                              external_generation: 1, state: "active", expires_at: 30.minutes.from_now)
        AutorotateCredentialVersion.create!(parent_lease: lease, access_token: "runtime-secret",
                                            broker_lease_id: "lease-1", provider_account_id: "account-1", external_generation: 1,
                                            expires_at: 20.minutes.from_now)

        post "/api/v1/autorotate/parent-lease/pins", params: { data: { operation_id: "op-1", execution_id: "exe-1" } }.to_json,
                                                    headers: auth_headers

        assert_response :ok
        assert_equal %w[expires_at pin_id version_id], JSON.parse(response.body).fetch("data").keys.sort
        refute_includes response.body, "runtime-secret"
        refute_includes response.body, "account-1"
      end

      test "pin quota acknowledgment only begins the local drain" do
        lease = AutorotateParentLease.create!(consumer: "bojack", lease_id: "lease-1", fence: 2,
                                              external_generation: 1, state: "active", expires_at: 30.minutes.from_now)
        version = AutorotateCredentialVersion.create!(parent_lease: lease, access_token: "runtime-secret",
                                                      broker_lease_id: "lease-1", provider_account_id: "account-1", external_generation: 1,
                                                      expires_at: 20.minutes.from_now)
        pin = AutorotateExecutionPin.create!(parent_lease: lease, credential_version: version,
                                             operation_id: "pin-op", execution_id: "exe-1", request_hash: "request-hash",
                                             lease_id: "lease-1", fence: 2, expires_at: 5.minutes.from_now)

        post "/api/v1/autorotate/parent-lease/pins/#{pin.oid}/quota-exhausted",
             params: { data: { operation_id: "quota-op" } }.to_json, headers: auth_headers

        assert_response :no_content
        lease.reload
        assert_equal "draining", lease.state
        assert_equal "begun", lease.drain_phase
        assert_match(AutorotateProxyParentClient::REQUEST_ID, lease.drain_refresh_operation_id)
        assert_nil lease.pending_operation_id
      end
    end
  end
end
