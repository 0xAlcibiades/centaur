require "test_helper"

class AutorotateParentLeaseServiceTest < ActiveSupport::TestCase
  class FakeClient
    attr_reader :calls

    def initialize(response, refresh_response: response, refresh_responses: nil, acquire_responses: nil, on_refresh: nil)
      @response = response
      @refresh_response = refresh_response
      @refresh_responses = refresh_responses
      @acquire_responses = acquire_responses
      @on_refresh = on_refresh
      @calls = []
      @fail_refresh_once = false
      @fail_refresh_on_call = nil
      @refresh_errors = {}
      @not_exhausted_once = false
    end

    def fail_refresh_once! = @fail_refresh_once = true
    def fail_refresh_on_call!(call) = @fail_refresh_on_call = call
    def refresh_error_on_call!(call, code:, status:) = @refresh_errors[call] = [ code, status ]
    def not_exhausted_once! = @not_exhausted_once = true

    def acquire(operation_id:)
      @calls << [ :acquire, operation_id ]
      @acquire_responses&.shift || @response
    end

    def heartbeat(lease_id:, fence:, expected_generation:)
      @calls << [ :heartbeat, lease_id, fence, expected_generation ]
      { "lease_id" => lease_id, "fence" => fence, "expires_at" => @response.fetch("expires_at") }
    end

    def refresh(lease_id:, fence:, expected_generation:, operation_id:)
      @calls << [ :refresh, lease_id, fence, expected_generation, operation_id ]
      @on_refresh&.call
      refresh_count = @calls.count { |call| call.first == :refresh }
      if (error = @refresh_errors.delete(refresh_count))
        code, status = error
        raise AutorotateProxyParentClient::Error.new("Autorotate returned HTTP #{status}", upstream_status: status,
                                                                                      upstream_code: code)
      end
      if @fail_refresh_once || refresh_count == @fail_refresh_on_call
        @fail_refresh_once = false
        @fail_refresh_on_call = nil
        raise Timeout::Error, "broker response was lost"
      end

      @refresh_responses&.shift || @refresh_response
    end

    def exhaust(lease_id:, fence:, expected_generation:)
      @calls << [ :exhaust, lease_id, fence, expected_generation ]
      return unless @not_exhausted_once

      @not_exhausted_once = false
      raise AutorotateProxyParentClient::Error.new("Autorotate returned HTTP 409", upstream_status: 409,
                                                                          upstream_code: "proxy_parent_not_exhausted")
    end
    def release(lease_id:, fence:, expected_generation:) = @calls << [ :release, lease_id, fence, expected_generation ]
  end

  test "persists only immutable encrypted credential versions" do
    now = Time.utc(2026, 8, 7, 12)
    client = FakeClient.new({
      "lease_id" => "lease-1", "fence" => 4, "expires_at" => (now + 30.minutes).iso8601,
      "bundle" => {
        "auth_generation" => 9, "access_token_expires_at" => (now + 20.minutes).iso8601,
        "provider_account_id" => "account-1", "access_token" => "runtime-secret"
      }
    })
    service = Autorotate::ParentLeaseService.new(client: client, now: -> { now })

    lease = service.acquire!(operation_id: "83fb2121-16ef-4eb1-9fb1-1cf819f83f6e")

    assert_equal "bojack", lease.consumer
    assert_equal "active", lease.state
    assert_equal 1, lease.credential_versions.count
    refute AutorotateParentLease.column_names.any? { |name| name.in?(%w[access_token refresh_token auth_json]) }
    raw = AutorotateCredentialVersion.connection.select_value(
      "SELECT access_token FROM autorotate_credential_versions WHERE id = #{lease.current_version.id}"
    )
    refute_includes raw, "runtime-secret"
    raw_account = AutorotateCredentialVersion.connection.select_value(
      "SELECT provider_account_id FROM autorotate_credential_versions WHERE id = #{lease.current_version.id}"
    )
    refute_includes raw_account, "account-1"
  end

  test "quota drain rejects new pins before broker refresh" do
    now = Time.utc(2026, 8, 7, 12)
    client = FakeClient.new({
      "lease_id" => "lease-1", "fence" => 4, "expires_at" => (now + 30.minutes).iso8601,
      "bundle" => {
        "auth_generation" => 9, "access_token_expires_at" => (now + 20.minutes).iso8601,
        "provider_account_id" => "account-1", "access_token" => "runtime-secret"
      }
    })
    service = Autorotate::ParentLeaseService.new(client: client, now: -> { now })
    service.acquire!(operation_id: "83fb2121-16ef-4eb1-9fb1-1cf819f83f6e")
    pin = Autorotate::ExecutionPinService.new(now: -> { now }).create!(operation_id: "pin-op", execution_id: "exe-1")

    service.begin_drain_from_pin!(pin_oid: pin.oid, operation_id: "quota-op")

    assert_equal "draining", pin.parent_lease.reload.state
    assert_equal "release_pending", pin.reload.state
    refute client.calls.any? { |call| call.first == :refresh }
    assert_raises(Autorotate::ExecutionPinService::Unavailable) do
      Autorotate::ExecutionPinService.new(now: -> { now }).create!(operation_id: "next", execution_id: "exe-2")
    end
  end

  test "a lost quota response does not drain the successor lease on retry" do
    now = Time.utc(2026, 8, 7, 12)
    source = lease_response(now, lease_id: "lease-1", fence: 4, generation: 9, account: "account-1")
    refreshed = lease_response(now, lease_id: "lease-1", fence: 4, generation: 10, account: "account-1")
    successor = lease_response(now, lease_id: "lease-2", fence: 5, generation: 1, account: "account-2")
    client = FakeClient.new(source, refresh_response: refreshed, acquire_responses: [ source, successor ])
    service = Autorotate::ParentLeaseService.new(client: client, now: -> { now })
    service.acquire!(operation_id: "83fb2121-16ef-4eb1-9fb1-1cf819f83f6e")
    pin = Autorotate::ExecutionPinService.new(now: -> { now }).create!(operation_id: "pin-op", execution_id: "exe-1")

    lease = service.begin_drain_from_pin!(pin_oid: pin.oid, operation_id: "quota-op")
    pin.release!
    service.reconcile!(acquire_operation_id: "710ec33e-c377-4b3c-a6e9-4c988bbdaaf1")
    calls_after_success = client.calls.dup

    service.begin_drain_from_pin!(pin_oid: pin.oid, operation_id: "quota-op")

    assert_equal calls_after_success, client.calls
    assert_equal 1, client.calls.count { |call| call.first == :exhaust }
    lease = AutorotateParentLease.for_bojack!
    assert_equal "completed", lease.drain_phase
    assert_equal "lease-2", lease.lease_id
    assert_equal "active", lease.state
    assert_equal 9, lease.drain_external_generation
    assert_equal "account-1", lease.drain_provider_account_id
  end

  test "ambiguous refresh retries the exact source lease after its durable reservation" do
    now = Time.utc(2026, 8, 7, 12)
    source = lease_response(now, lease_id: "lease-1", fence: 4, generation: 9, account: "account-1")
    refreshed = lease_response(now, lease_id: "lease-1", fence: 4, generation: 10, account: "account-1")
    client = FakeClient.new(source, refresh_response: refreshed)
    client.fail_refresh_once!
    service = Autorotate::ParentLeaseService.new(client: client, now: -> { now })
    service.acquire!(operation_id: "83fb2121-16ef-4eb1-9fb1-1cf819f83f6e")
    pin = Autorotate::ExecutionPinService.new(now: -> { now }).create!(operation_id: "pin-op", execution_id: "exe-1")

    lease = service.begin_drain_from_pin!(pin_oid: pin.oid, operation_id: "quota-op")
    assert_raises(Timeout::Error) do
      service.reconcile!(acquire_operation_id: "710ec33e-c377-4b3c-a6e9-4c988bbdaaf1")
    end
    lease.reload
    assert_equal "begun", lease.drain_phase
    assert_match(AutorotateProxyParentClient::REQUEST_ID, lease.pending_operation_id)
    assert_equal lease.drain_refresh_operation_id, lease.pending_operation_id

    service.reconcile!(acquire_operation_id: "710ec33e-c377-4b3c-a6e9-4c988bbdaaf1")

    lease.reload
    assert_equal "refreshed", lease.drain_phase
    assert_equal "lease-1", lease.lease_id
    assert_equal 2, client.calls.count { |call| call.first == :refresh }
    refresh_calls = client.calls.select { |call| call.first == :refresh }
    assert_equal [ "lease-1", 4, 9 ], refresh_calls.first[1, 3]
    assert_equal refresh_calls.first.last, refresh_calls.second.last
    assert_match(AutorotateProxyParentClient::REQUEST_ID, refresh_calls.first.last)
    assert_nil lease.pending_operation_id
  end

  test "a quota operation cannot be replayed against another pin or released pin" do
    now = Time.utc(2026, 8, 7, 12)
    source = lease_response(now, lease_id: "lease-1", fence: 4, generation: 9, account: "account-1")
    service = Autorotate::ParentLeaseService.new(client: FakeClient.new(source), now: -> { now })
    service.acquire!(operation_id: "83fb2121-16ef-4eb1-9fb1-1cf819f83f6e")
    pins = Autorotate::ExecutionPinService.new(now: -> { now })
    first = pins.create!(operation_id: "pin-op-1", execution_id: "exe-1")
    second = pins.create!(operation_id: "pin-op-2", execution_id: "exe-2")

    service.begin_drain_from_pin!(pin_oid: first.oid, operation_id: "quota-op")

    assert_raises(Autorotate::ParentLeaseService::StaleLease) do
      service.begin_drain_from_pin!(pin_oid: second.oid, operation_id: "quota-op")
    end
    second.release!
    assert_raises(Autorotate::ParentLeaseService::StaleLease) do
      service.begin_drain_from_pin!(pin_oid: second.oid, operation_id: "another-quota-op")
    end
  end

  test "does not acknowledge a new quota drain while a regular refresh is in flight" do
    now = Time.utc(2026, 8, 7, 12)
    source = lease_response(now, lease_id: "lease-1", fence: 4, generation: 9, account: "account-1")
    refreshed = lease_response(now, lease_id: "lease-1", fence: 4, generation: 10, account: "account-1")
    service = Autorotate::ParentLeaseService.new(client: FakeClient.new(source), now: -> { now })
    service.acquire!(operation_id: "83fb2121-16ef-4eb1-9fb1-1cf819f83f6e")
    pin = Autorotate::ExecutionPinService.new(now: -> { now }).create!(operation_id: "pin-op", execution_id: "exe-1")
    lease, refresh_operation_id = service.send(:reserve_refresh!)

    assert_raises(Autorotate::ParentLeaseService::Unavailable) do
      service.begin_drain_from_pin!(pin_oid: pin.oid, operation_id: "quota-op")
    end
    refute pin.reload.quota_exhausted_operation_id
    assert_nil lease.reload.drain_operation_id

    service.send(:persist_bundle!, lease.id, refresh_operation_id, refreshed, state: "active")
    service.begin_drain_from_pin!(pin_oid: pin.oid, operation_id: "quota-op")
    assert_equal "draining", lease.reload.state
  end

  test "coalesces a second pin quota report onto an ambiguous source refresh" do
    now = Time.utc(2026, 8, 7, 12)
    source = lease_response(now, lease_id: "lease-1", fence: 4, generation: 9, account: "account-1")
    refreshed = lease_response(now, lease_id: "lease-1", fence: 4, generation: 10, account: "account-1")
    client = FakeClient.new(source, refresh_response: refreshed)
    client.fail_refresh_once!
    service = Autorotate::ParentLeaseService.new(client: client, now: -> { now })
    service.acquire!(operation_id: "83fb2121-16ef-4eb1-9fb1-1cf819f83f6e")
    pins = Autorotate::ExecutionPinService.new(now: -> { now })
    first = pins.create!(operation_id: "pin-op-1", execution_id: "exe-1")
    second = pins.create!(operation_id: "pin-op-2", execution_id: "exe-2")
    lease = service.begin_drain_from_pin!(pin_oid: first.oid, operation_id: "quota-op-1")
    refresh_operation_id = lease.drain_refresh_operation_id

    assert_raises(Timeout::Error) { service.reconcile!(acquire_operation_id: SecureRandom.uuid) }
    service.begin_drain_from_pin!(pin_oid: second.oid, operation_id: "quota-op-2")

    lease.reload
    assert_equal "quota-op-1", lease.drain_operation_id
    assert_equal refresh_operation_id, lease.drain_refresh_operation_id
    assert_equal "quota-op-2", second.reload.quota_exhausted_operation_id
    assert_equal "release_pending", second.state

    service.reconcile!(acquire_operation_id: SecureRandom.uuid)
    refresh_calls = client.calls.select { |call| call.first == :refresh }
    assert_equal refresh_calls.first.last, refresh_calls.second.last
  end

  test "expired acquire replay retires the stale parent before using a new request id" do
    now = Time.utc(2026, 8, 7, 12)
    expired = lease_response(now - 31.minutes, lease_id: "lease-expired", fence: 4, generation: 9, account: "account-1")
    successor = lease_response(now, lease_id: "lease-healthy", fence: 5, generation: 1, account: "account-2")
    client = FakeClient.new(expired, acquire_responses: [ expired, successor ])
    service = Autorotate::ParentLeaseService.new(client: client, now: -> { now })

    lease = service.acquire!(operation_id: "83fb2121-16ef-4eb1-9fb1-1cf819f83f6e")

    acquire_calls = client.calls.select { |call| call.first == :acquire }
    assert_equal 2, acquire_calls.size
    refute_equal acquire_calls.first.last, acquire_calls.second.last
    assert_equal "lease-healthy", lease.lease_id
    assert_equal "active", lease.state
  end

  test "expired drain refresh replay completes the source without exhausting it" do
    started_at = Time.utc(2026, 8, 7, 12)
    clock = started_at
    source = lease_response(started_at, lease_id: "lease-1", fence: 4, generation: 9, account: "account-1")
    successor = lease_response(started_at + 31.minutes, lease_id: "lease-2", fence: 5, generation: 1, account: "account-2")
    client = FakeClient.new(source, refresh_response: source, acquire_responses: [ source, successor ])
    service = Autorotate::ParentLeaseService.new(client: client, now: -> { clock })
    service.acquire!(operation_id: "83fb2121-16ef-4eb1-9fb1-1cf819f83f6e")
    pin = Autorotate::ExecutionPinService.new(now: -> { clock }).create!(operation_id: "pin-op", execution_id: "exe-1")
    service.begin_drain_from_pin!(pin_oid: pin.oid, operation_id: "quota-op")
    clock = started_at + 31.minutes

    lease = service.reconcile!(acquire_operation_id: "710ec33e-c377-4b3c-a6e9-4c988bbdaaf1")

    assert_equal "active", lease.state
    assert_equal "lease-2", lease.lease_id
    assert_equal "completed", lease.drain_phase
    refute client.calls.any? { |call| call.first == :exhaust }
  end

  test "performs a fresh authoritative probe after pins drain for more than fifteen minutes" do
    started_at = Time.utc(2026, 8, 7, 12)
    clock = started_at
    source = lease_response(started_at, lease_id: "lease-1", fence: 4, generation: 9, account: "account-1",
                                         lease_ttl: 90.minutes, token_ttl: 60.minutes)
    first_refresh = lease_response(started_at, lease_id: "lease-1", fence: 4, generation: 10, account: "account-1",
                                               lease_ttl: 90.minutes, token_ttl: 60.minutes)
    final_refresh = lease_response(started_at + 16.minutes, lease_id: "lease-1", fence: 4, generation: 11, account: "account-1",
                                                           lease_ttl: 90.minutes, token_ttl: 60.minutes)
    client = FakeClient.new(source, refresh_responses: [ first_refresh, final_refresh ])
    service = Autorotate::ParentLeaseService.new(client: client, now: -> { clock })
    service.acquire!(operation_id: "83fb2121-16ef-4eb1-9fb1-1cf819f83f6e")
    pin = Autorotate::ExecutionPinService.new(now: -> { clock }).create!(operation_id: "pin-op", execution_id: "exe-1")
    service.begin_drain_from_pin!(pin_oid: pin.oid, operation_id: "quota-op")
    service.reconcile!(acquire_operation_id: SecureRandom.uuid)
    # Models the bounded pin-heartbeat extensions that keep a workflow live
    # across the fifteen-minute interval without changing its version.
    pin.update!(expires_at: started_at + 19.minutes)
    clock = started_at + 16.minutes
    pin.release!

    service.reconcile!(acquire_operation_id: SecureRandom.uuid)

    refresh_calls = client.calls.select { |call| call.first == :refresh }
    assert_equal 2, refresh_calls.size
    assert_equal [ 9, 10 ], refresh_calls.map { |call| call[3] }
    refute_equal refresh_calls.first.last, refresh_calls.second.last
    assert_equal [ :exhaust, "lease-1", 4, 11 ], client.calls.find { |call| call.first == :exhaust }
  end

  test "quota reset before the last pin exits clean-releases the refreshed source" do
    now = Time.utc(2026, 8, 7, 12)
    source = lease_response(now, lease_id: "lease-1", fence: 4, generation: 9, account: "account-1")
    first_refresh = lease_response(now, lease_id: "lease-1", fence: 4, generation: 10, account: "account-1")
    final_refresh = lease_response(now, lease_id: "lease-1", fence: 4, generation: 11, account: "account-1")
    successor = lease_response(now, lease_id: "lease-2", fence: 5, generation: 1, account: "account-2")
    client = FakeClient.new(source, refresh_responses: [ first_refresh, final_refresh ],
                            acquire_responses: [ source, successor ])
    client.not_exhausted_once!
    service = Autorotate::ParentLeaseService.new(client: client, now: -> { now })
    service.acquire!(operation_id: "83fb2121-16ef-4eb1-9fb1-1cf819f83f6e")
    pin = Autorotate::ExecutionPinService.new(now: -> { now }).create!(operation_id: "pin-op", execution_id: "exe-1")
    service.begin_drain_from_pin!(pin_oid: pin.oid, operation_id: "quota-op")
    service.reconcile!(acquire_operation_id: SecureRandom.uuid)
    pin.release!

    lease = service.reconcile!(acquire_operation_id: SecureRandom.uuid)

    assert_equal "active", lease.state
    assert_equal "lease-2", lease.lease_id
    assert_equal "completed", lease.drain_phase
    assert_equal [ :release, "lease-1", 4, 11 ], client.calls.find { |call| call.first == :release }
  end

  test "ambiguous final refresh retries its persisted UUID before exhaust" do
    now = Time.utc(2026, 8, 7, 12)
    source = lease_response(now, lease_id: "lease-1", fence: 4, generation: 9, account: "account-1")
    first_refresh = lease_response(now, lease_id: "lease-1", fence: 4, generation: 10, account: "account-1")
    final_refresh = lease_response(now, lease_id: "lease-1", fence: 4, generation: 11, account: "account-1")
    client = FakeClient.new(source, refresh_responses: [ first_refresh, final_refresh ])
    client.fail_refresh_on_call!(2)
    service = Autorotate::ParentLeaseService.new(client: client, now: -> { now })
    service.acquire!(operation_id: "83fb2121-16ef-4eb1-9fb1-1cf819f83f6e")
    pin = Autorotate::ExecutionPinService.new(now: -> { now }).create!(operation_id: "pin-op", execution_id: "exe-1")
    lease = service.begin_drain_from_pin!(pin_oid: pin.oid, operation_id: "quota-op")
    pin.release!

    assert_raises(Timeout::Error) { service.reconcile!(acquire_operation_id: SecureRandom.uuid) }
    lease.reload
    assert_equal "final_refreshing", lease.drain_phase
    final_operation_id = lease.drain_final_refresh_operation_id
    assert_equal final_operation_id, lease.pending_operation_id

    service.reconcile!(acquire_operation_id: SecureRandom.uuid)

    refresh_calls = client.calls.select { |call| call.first == :refresh }
    assert_equal 3, refresh_calls.size
    assert_equal final_operation_id, refresh_calls[1].last
    assert_equal final_operation_id, refresh_calls[2].last
    assert_equal "completed", lease.reload.drain_phase
  end

  test "expired scheduled refresh replay retires the unusable parent and reacquires" do
    started_at = Time.utc(2026, 8, 7, 12)
    clock = started_at
    source = lease_response(started_at, lease_id: "lease-1", fence: 4, generation: 9, account: "account-1")
    successor = lease_response(started_at + 31.minutes, lease_id: "lease-2", fence: 5, generation: 1, account: "account-2")
    client = FakeClient.new(source, refresh_response: source, acquire_responses: [ source, successor ],
                            on_refresh: -> { clock = started_at + 31.minutes })
    service = Autorotate::ParentLeaseService.new(client: client, now: -> { clock })
    service.acquire!(operation_id: "83fb2121-16ef-4eb1-9fb1-1cf819f83f6e")

    lease = service.refresh!

    assert_equal "active", lease.state
    assert_equal "lease-2", lease.lease_id
    acquire_calls = client.calls.select { |call| call.first == :acquire }
    refute_equal acquire_calls.first.last, acquire_calls.second.last
  end

  test "initial drain refresh waits through a 503 and retires only terminal lease_expired replay" do
    now = Time.utc(2026, 8, 7, 12)
    source = lease_response(now, lease_id: "lease-1", fence: 4, generation: 9, account: "account-1")
    successor = lease_response(now, lease_id: "lease-2", fence: 5, generation: 1, account: "account-2")
    client = FakeClient.new(source, acquire_responses: [ source, successor ])
    client.refresh_error_on_call!(1, code: "proxy_parent_refresh_inconclusive", status: 503)
    client.refresh_error_on_call!(2, code: "lease_expired", status: 409)
    service = Autorotate::ParentLeaseService.new(client: client, now: -> { now })
    service.acquire!(operation_id: "83fb2121-16ef-4eb1-9fb1-1cf819f83f6e")
    pin = Autorotate::ExecutionPinService.new(now: -> { now }).create!(operation_id: "pin-op", execution_id: "exe-1")
    lease = service.begin_drain_from_pin!(pin_oid: pin.oid, operation_id: "quota-op")

    assert_raises(AutorotateProxyParentClient::Error) { service.reconcile!(acquire_operation_id: SecureRandom.uuid) }
    lease.reload
    assert_equal "begun", lease.drain_phase
    refresh_operation_id = lease.pending_operation_id

    service.reconcile!(acquire_operation_id: SecureRandom.uuid)
    lease.reload
    assert_equal "lease_expired", lease.drain_phase
    assert_nil lease.pending_operation_id
    assert_equal "release_pending", pin.reload.state

    pin.release!
    lease = service.reconcile!(acquire_operation_id: SecureRandom.uuid)
    assert_equal "active", lease.state
    assert_equal "lease-2", lease.lease_id
    refresh_calls = client.calls.select { |call| call.first == :refresh }
    assert_equal refresh_operation_id, refresh_calls.first.last
    assert_equal refresh_operation_id, refresh_calls.second.last
  end

  test "final drain refresh treats lease_expired as terminal only after its 503 retry" do
    now = Time.utc(2026, 8, 7, 12)
    source = lease_response(now, lease_id: "lease-1", fence: 4, generation: 9, account: "account-1")
    first_refresh = lease_response(now, lease_id: "lease-1", fence: 4, generation: 10, account: "account-1")
    successor = lease_response(now, lease_id: "lease-2", fence: 5, generation: 1, account: "account-2")
    client = FakeClient.new(source, refresh_responses: [ first_refresh ], acquire_responses: [ source, successor ])
    client.refresh_error_on_call!(2, code: "proxy_parent_refresh_inconclusive", status: 503)
    client.refresh_error_on_call!(3, code: "lease_expired", status: 409)
    service = Autorotate::ParentLeaseService.new(client: client, now: -> { now })
    service.acquire!(operation_id: "83fb2121-16ef-4eb1-9fb1-1cf819f83f6e")
    pin = Autorotate::ExecutionPinService.new(now: -> { now }).create!(operation_id: "pin-op", execution_id: "exe-1")
    service.begin_drain_from_pin!(pin_oid: pin.oid, operation_id: "quota-op")
    service.reconcile!(acquire_operation_id: SecureRandom.uuid)
    pin.release!

    assert_raises(AutorotateProxyParentClient::Error) { service.reconcile!(acquire_operation_id: SecureRandom.uuid) }
    lease = AutorotateParentLease.for_bojack!
    assert_equal "final_refreshing", lease.drain_phase
    final_operation_id = lease.pending_operation_id

    lease = service.reconcile!(acquire_operation_id: SecureRandom.uuid)
    assert_equal "active", lease.state
    assert_equal "lease-2", lease.lease_id
    refresh_calls = client.calls.select { |call| call.first == :refresh }
    assert_equal final_operation_id, refresh_calls[1].last
    assert_equal final_operation_id, refresh_calls[2].last
  end

  private

  def lease_response(now, lease_id:, fence:, generation:, account:, lease_ttl: 30.minutes, token_ttl: 20.minutes)
    {
      "lease_id" => lease_id, "fence" => fence, "expires_at" => (now + lease_ttl).iso8601,
      "bundle" => {
        "auth_generation" => generation, "access_token_expires_at" => (now + token_ttl).iso8601,
        "provider_account_id" => account, "access_token" => "runtime-secret-#{generation}"
      }
    }
  end
end
