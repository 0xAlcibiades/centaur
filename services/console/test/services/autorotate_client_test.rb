require "test_helper"

class AutorotateClientTest < ActiveSupport::TestCase
  def client_with_response(status:, body:, &assert_request)
    http = expect_http_call(status: status, body: body, &assert_request)
    [
      AutorotateClient.new(
        base_url: "https://autorotate.example.test/broker",
        observer_token: "observer-secret",
        http: http
      ),
      http
    ]
  end

  test "fetches only aggregate observer status fields" do
    client, http = client_with_response(
      status: 200,
      body: {
        generated_at: "2026-07-29T12:00:00Z",
        total: 8,
        healthy: 6,
        available: 5,
        limited: 1,
        login_required: 1,
        disabled: 0,
        leased: 1,
        removed: 1,
        next_available_at: "2026-07-29T13:00:00Z",
        pending_enrollments: 1,
        account_labels: [ "must-not-pass-through" ]
      }.to_json
    ) do |request|
      assert_equal :get, request[:method]
      assert_equal "https://autorotate.example.test/broker/v1/status", request[:url]
      assert_equal "Bearer observer-secret", request[:headers]["Authorization"]
    end

    status = client.status

    assert_equal 8, status.fetch("total")
    assert_equal 5, status.fetch("available")
    assert_equal 1, status.fetch("pending_enrollments")
    refute status.key?("account_labels")
    http.verify
  end

  test "does not expose upstream response bodies in errors" do
    client, http = client_with_response(
      status: 401,
      body: {
        error: {
          code: "invalid_observer_token",
          message: "observer-secret and provider body must stay private"
        }
      }.to_json
    )

    error = assert_raises(AutorotateClient::Error) { client.status }

    assert_equal 401, error.upstream_status
    assert_equal "invalid_observer_token", error.upstream_code
    refute_includes error.message, "observer-secret"
    refute_includes error.message, "provider body"
    http.verify
  end

  test "requires HTTPS for non-loopback brokers" do
    error = assert_raises(AutorotateClient::Error) do
      AutorotateClient.new(
        base_url: "http://autorotate.example.test",
        observer_token: "observer-secret"
      )
    end

    assert_match(/HTTPS/, error.message)
  end

  test "rejects credentials embedded in the broker URL" do
    error = assert_raises(AutorotateClient::Error) do
      AutorotateClient.new(
        base_url: "https://operator:secret@autorotate.example.test",
        observer_token: "observer-secret"
      )
    end

    assert_equal "Autorotate URL is not configured correctly", error.message
  end

  test "requires a server-side observer token" do
    error = assert_raises(AutorotateClient::Error) do
      AutorotateClient.new(
        base_url: "https://autorotate.example.test",
        observer_token: ""
      )
    end

    assert_match(/observer token/, error.message)
  end
end
