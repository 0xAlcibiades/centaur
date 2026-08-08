require "test_helper"

class AutorotateProxyParentClientTest < ActiveSupport::TestCase
  test "uses the completed acquire wire contract and dedicated runtime token" do
    http = expect_http_call(status: 200, body: { lease_id: "lease-1" }.to_json) do |request|
      assert_equal :post, request[:method]
      assert_equal "https://autorotate.example.test/runtime/v1/proxy-parent-leases/acquire", request[:url]
      assert_equal "Bearer runtime-secret", request[:headers]["Authorization"]
      assert_equal "no-store", request[:headers]["Cache-Control"]
      assert_equal "83fb2121-16ef-4eb1-9fb1-1cf819f83f6e", request[:headers]["X-Request-Id"]
      refute request[:headers].key?("Idempotency-Key")
      assert_equal({ "parent_key" => "bojack", "client_id" => "centaur-console", "ttl_seconds" => 300 }, JSON.parse(request[:body]))
      refute_includes request[:body], "runtime-secret"
    end
    client = AutorotateProxyParentClient.new(base_url: "https://autorotate.example.test/runtime",
                                             proxy_parent_token: "runtime-secret", http: http, timeout: 3)

    client.acquire(operation_id: "83fb2121-16ef-4eb1-9fb1-1cf819f83f6e")
    http.verify
  end

  test "uses fenced generation-aware requests and normalizes no-content exhaust and release" do
    http = Minitest::Mock.new
    expect_http_call(http, status: 200, body: { lease_id: "lease-1", fence: 4, expires_at: "2026-08-07T12:05:00Z" }.to_json) do |request|
      assert_equal :post, request[:method]
      assert_equal "https://autorotate.example.test/v1/proxy-parent-leases/lease-1/heartbeat", request[:url]
      assert_equal({ "fence" => 4, "expected_generation" => 9, "ttl_seconds" => 300 }, JSON.parse(request[:body]))
      refute request[:headers].key?("X-Request-Id")
    end
    expect_http_call(http, status: 200, body: { lease_id: "lease-1", fence: 4, expires_at: "2026-08-07T12:05:00Z", bundle: {} }.to_json) do |request|
      assert_equal :post, request[:method]
      assert_equal "https://autorotate.example.test/v1/proxy-parent-leases/lease-1/refresh", request[:url]
      assert_equal({ "fence" => 4, "expected_generation" => 9 }, JSON.parse(request[:body]))
      assert_equal "83fb2121-16ef-4eb1-9fb1-1cf819f83f6e", request[:headers]["X-Request-Id"]
    end
    expect_http_call(http, status: 204, body: "") do |request|
      assert_equal :post, request[:method]
      assert_equal "https://autorotate.example.test/v1/proxy-parent-leases/lease-1/exhausted", request[:url]
      assert_equal({ "fence" => 4, "expected_generation" => 9 }, JSON.parse(request[:body]))
    end
    expect_http_call(http, status: 204, body: "") do |request|
      assert_equal :delete, request[:method]
      assert_equal "https://autorotate.example.test/v1/proxy-parent-leases/lease-1", request[:url]
      assert_equal({ "fence" => 4, "expected_generation" => 9 }, JSON.parse(request[:body]))
    end
    client = AutorotateProxyParentClient.new(base_url: "https://autorotate.example.test",
                                             proxy_parent_token: "runtime-secret", http: http)

    assert_equal 4, client.heartbeat(lease_id: "lease-1", fence: 4, expected_generation: 9).fetch("fence")
    assert_equal 4, client.refresh(lease_id: "lease-1", fence: 4, expected_generation: 9,
                                   operation_id: "83fb2121-16ef-4eb1-9fb1-1cf819f83f6e").fetch("fence")
    assert_nil client.exhaust(lease_id: "lease-1", fence: 4, expected_generation: 9)
    assert_nil client.release(lease_id: "lease-1", fence: 4, expected_generation: 9)
    http.verify
  end

  test "redacts upstream error bodies" do
    http = expect_http_call(status: 401, body: {
      code: "invalid_runtime_token", message: "runtime-secret must not escape"
    }.to_json)
    client = AutorotateProxyParentClient.new(base_url: "https://autorotate.example.test",
                                             proxy_parent_token: "runtime-secret", http: http)

    error = assert_raises(AutorotateProxyParentClient::Error) do
      client.acquire(operation_id: "83fb2121-16ef-4eb1-9fb1-1cf819f83f6e")
    end
    assert_equal "invalid_runtime_token", error.upstream_code
    refute_includes error.message, "runtime-secret"
  end
end
