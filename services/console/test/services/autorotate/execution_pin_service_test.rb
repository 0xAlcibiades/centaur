require "test_helper"

class AutorotateExecutionPinServiceTest < ActiveSupport::TestCase
  def setup
    @lease = AutorotateParentLease.create!(consumer: "bojack", lease_id: "lease-1", fence: 4,
                                           external_generation: 9, state: "active", expires_at: 30.minutes.from_now)
    @version = AutorotateCredentialVersion.create!(parent_lease: @lease, access_token: "runtime-secret",
                                                    broker_lease_id: "lease-1", provider_account_id: "account-1", external_generation: 9,
                                                    expires_at: 20.minutes.from_now)
    @service = Autorotate::ExecutionPinService.new
  end

  test "is idempotent for operation and execution and bounds ttl" do
    first = @service.create!(operation_id: "op-1", execution_id: "exe-1")
    retry_pin = @service.create!(operation_id: "op-1", execution_id: "exe-1")
    execution_retry = @service.create!(operation_id: "op-2", execution_id: "exe-1")

    assert_equal first.id, retry_pin.id
    assert_equal first.id, execution_retry.id
    assert_operator first.expires_at, :<=, @version.expires_at - AutorotateExecutionPin::TOKEN_EXPIRY_MARGIN
  end

  test "rejects reuse of an operation id for another execution" do
    @service.create!(operation_id: "op-1", execution_id: "exe-1")

    assert_raises(Autorotate::ExecutionPinService::InvalidExecution) do
      @service.create!(operation_id: "op-1", execution_id: "exe-2")
    end
  end

  test "quota drain fences new pins immediately" do
    @service.create!(operation_id: "op-1", execution_id: "exe-1")
    @lease.update!(state: "draining")

    assert_raises(Autorotate::ExecutionPinService::Unavailable) do
      @service.create!(operation_id: "op-2", execution_id: "exe-2")
    end
  end
end
