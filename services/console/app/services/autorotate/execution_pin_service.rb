module Autorotate
  class ExecutionPinService
    class Unavailable < StandardError; end
    class InvalidExecution < StandardError; end

    def initialize(now: -> { Time.current })
      @now = now
    end

    def create!(operation_id:, execution_id:)
      execution_id = execution_id.to_s
      raise InvalidExecution, "execution id is required" if execution_id.blank?
      request_hash = Digest::SHA256.hexdigest(JSON.generate("execution_id" => execution_id))

      AutorotateExecutionPin.transaction do
        by_operation = AutorotateExecutionPin.lock.find_by(operation_id: operation_id)
        if by_operation
          raise InvalidExecution, "operation id conflicts with a different request" unless by_operation.request_hash == request_hash
          return by_operation
        end
        existing = AutorotateExecutionPin.lock.find_by(execution_id: execution_id)
        return existing if existing

        lease = AutorotateParentLease.for_bojack!
        lease.lock!
        raise Unavailable, "parent lease is not available" unless lease.usable?(now: now)
        version = lease.current_version
        raise Unavailable, "parent credential is not available" unless version&.usable?(now: now)

        expires_at = [ now + AutorotateExecutionPin::DEFAULT_TTL,
                       lease.expires_at, version.expires_at - AutorotateExecutionPin::TOKEN_EXPIRY_MARGIN ].min
        raise Unavailable, "parent credential is expiring" if expires_at <= now

        AutorotateExecutionPin.create!(parent_lease: lease, credential_version: version,
                                       operation_id: operation_id, execution_id: execution_id,
                                       request_hash: request_hash, lease_id: lease.lease_id, fence: lease.fence,
                                       expires_at: expires_at)
      end
    rescue ActiveRecord::RecordNotUnique
      existing = AutorotateExecutionPin.find_by(operation_id: operation_id) ||
                 AutorotateExecutionPin.find_by(execution_id: execution_id)
      raise unless existing
      raise InvalidExecution, "operation id conflicts with a different request" if existing.operation_id == operation_id && existing.request_hash != request_hash

      existing
    end

    def release!(pin_oid:)
      pin = AutorotateExecutionPin.find_by_oid!(pin_oid)
      pin.release!
      pin
    end

    def heartbeat!(pin_oid:, operation_id:)
      AutorotateExecutionPin.transaction do
        pin = AutorotateExecutionPin.lock.find_by_oid!(pin_oid)
        return pin if pin.last_heartbeat_operation_id == operation_id
        raise Unavailable, "execution pin is not live" unless pin.state.in?(%w[active release_pending]) && pin.expires_at > now

        version_limit = pin.credential_version.expires_at - AutorotateExecutionPin::TOKEN_EXPIRY_MARGIN
        expiry = [ now + AutorotateExecutionPin::DEFAULT_TTL, pin.parent_lease.expires_at, version_limit ].min
        raise Unavailable, "parent credential is expiring" if expiry <= now

        pin.update!(expires_at: expiry, last_heartbeat_operation_id: operation_id)
        pin
      end
    end

    def quota_exhausted!(pin_oid:, operation_id:)
      AutorotateExecutionPin.transaction do
        prior = AutorotateExecutionPin.lock.find_by(quota_exhausted_operation_id: operation_id)
        if prior
          raise InvalidExecution, "quota operation belongs to a different pin" unless prior.oid == pin_oid
          return prior
        end
        pin = AutorotateExecutionPin.lock.find_by_oid!(pin_oid)
        raise Unavailable, "execution pin is not live" unless pin.state.in?(%w[active release_pending]) && pin.expires_at > now

        pin.update!(quota_exhausted_operation_id: operation_id)
        pin
      end
    rescue ActiveRecord::RecordNotUnique
      retry
    end

    private

    def now = @now.call
  end
end
