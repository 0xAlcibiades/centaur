module Autorotate
  class ParentLeaseService
    class Unavailable < StandardError; end
    class StaleLease < StandardError; end
    class ExpiredBrokerResponse < Unavailable; end

    PROXY_PARENT_NOT_EXHAUSTED = "proxy_parent_not_exhausted".freeze

    def initialize(client: nil, now: -> { Time.current })
      @client = client
      @now = now
    end

    def acquire!(operation_id:, recover_expired_replay: true)
      lease = reserve!(operation_id) do |row|
        row.usable?(now: now) && row.current_version&.usable?(now: now)
      end
      return lease if lease.usable?(now: now) && lease.current_version&.usable?(now: now)

      persist_bundle!(lease.id, operation_id, client.acquire(operation_id: operation_id), state: "active")
    rescue ExpiredBrokerResponse => error
      raise error unless recover_expired_replay && retire_expired_acquire_replay!(lease.id, operation_id)

      acquire!(operation_id: SecureRandom.uuid, recover_expired_replay: false)
    end

    def heartbeat!
      lease = live_lease_for!("heartbeat")
      response = client.heartbeat(lease_id: lease.lease_id, fence: lease.fence,
                                  expected_generation: lease.external_generation)
      persist_heartbeat!(lease.id, pending_id("heartbeat", lease.id), response)
    end

    def refresh!
      lease, operation_id = reserve_refresh!
      persist_bundle!(lease.id, operation_id, client.refresh(lease_id: lease.lease_id, fence: lease.fence,
                                                             expected_generation: lease.external_generation, operation_id: operation_id),
                      state: lease.state)
    rescue ExpiredBrokerResponse => error
      raise error unless retire_expired_acquire_replay!(lease.id, operation_id)

      acquire!(operation_id: SecureRandom.uuid)
    end

    # The pin is the trusted quota-report selector. Fence new pin creation
    # before asking the broker to refresh quota, so no work starts against an
    # account whose current quota is being adjudicated.
    def begin_drain_from_pin!(pin_oid:, operation_id:)
      AutorotateParentLease.transaction do
        pin = AutorotateExecutionPin.lock.find_by_oid!(pin_oid)
        prior = AutorotateExecutionPin.lock.find_by(quota_exhausted_operation_id: operation_id)
        if prior
          raise StaleLease, "quota operation belongs to a different pin" unless prior.id == pin.id
          lease = AutorotateParentLease.lock.find(pin.autorotate_parent_lease_id)
          return lease if drain_matches_pin_source?(lease, pin)

          raise StaleLease, "quota operation lost its drain state"
        end

        lease = AutorotateParentLease.lock.find(pin.autorotate_parent_lease_id)
        current = lease.current_version
        unless pin.state.in?(%w[active release_pending]) && pin.expires_at > now &&
               pin.lease_id == lease.lease_id && pin.fence == lease.fence &&
               current && current.provider_account_id == pin.credential_version.provider_account_id
          raise StaleLease, "execution pin is stale"
        end

        if drain_matches_pin_source?(lease, pin)
          pin.update!(quota_exhausted_operation_id: operation_id)
          lease.execution_pins.where(state: "active").update_all(state: "release_pending", updated_at: now)
          return lease
        end

        raise Unavailable, "another parent lease operation is in progress" if lease.pending_operation_id.present?

        pin.update!(quota_exhausted_operation_id: operation_id)
        lease.update!(state: "draining", drain_operation_id: operation_id,
                      drain_lease_id: lease.lease_id, drain_fence: lease.fence,
                      drain_external_generation: lease.external_generation,
                      drain_provider_account_id: current.provider_account_id,
                      drain_refresh_operation_id: SecureRandom.uuid, drain_phase: "begun")
        lease.execution_pins.where(state: "active").update_all(state: "release_pending", updated_at: now)
        lease
      end
    end

    # Phase records are durable before every subsequent request. A retry may
    # continue the source lease but may never reinterpret its pin against a
    # newer acquired lease.
    def resume_pin_quota_drain!(id, operation_id)
      lease = AutorotateParentLease.find(id)
      raise StaleLease unless lease.drain_operation_id == operation_id
      return lease if lease.drain_completed?

      if lease.drain_phase == "begun"
        refresh_source_drain!(lease, operation_id)
        lease = AutorotateParentLease.find(id)
      end
      return lease if lease.drain_completed?
      lease.expire_pins!(now: now)
      lease.reload
      return complete_terminal_drain_and_acquire!(lease, operation_id) if lease.drain_phase == "lease_expired" && lease.pins_drained?(now: now)
      return lease if lease.drain_phase == "lease_expired"
      return lease unless lease.pins_drained?(now: now)

      return release_not_exhausted_source!(lease, operation_id) if lease.drain_phase == "release_pending"

      if lease.drain_phase.in?(%w[refreshed final_refreshing])
        refresh_final_source_drain!(lease, operation_id)
        lease = AutorotateParentLease.find(id)
      end
      return complete_terminal_drain_and_acquire!(lease, operation_id) if lease.drain_phase == "lease_expired" && lease.pins_drained?(now: now)
      return lease if lease.drain_phase == "lease_expired"
      return lease if lease.drain_completed?
      return lease unless lease.drain_phase == "final_refreshed"

      validate_drain_source!(lease)
      begin
        client.exhaust(lease_id: lease.drain_lease_id, fence: lease.drain_fence,
                       expected_generation: lease.external_generation)
      rescue AutorotateProxyParentClient::Error => error
        raise error unless error.upstream_code == PROXY_PARENT_NOT_EXHAUSTED

        return release_not_exhausted_source!(lease, operation_id)
      end
      mark_drain_phase!(id, operation_id, "exhausted")
      lease = AutorotateParentLease.find(id)
      return lease if lease.drain_completed?

      acquire_drain_successor!(lease, operation_id) if lease.lease_id == lease.drain_lease_id
      mark_drain_phase!(id, operation_id, "completed")
    end

    def release!(fence:, generation:)
      lease = mark_draining!(fence: fence, generation: generation, pending: "release")
      return clear_pending!(lease.id) unless lease.pins_drained?

      client.release(lease_id: lease.lease_id, fence: lease.fence, expected_generation: lease.external_generation)
      finalize_release!(lease.id, pending_id("release", lease.id))
    end

    def reconcile!(acquire_operation_id:)
      lease = AutorotateParentLease.for_bojack!
      acquire_operation_id = lease.pending_operation_id if lease.pending_operation_id&.match?(AutorotateProxyParentClient::REQUEST_ID)
      lease.expire_pins!(now: now)
      if lease.draining? && lease.drain_operation_id.present?
        lease = resume_pin_quota_drain!(lease.id, lease.drain_operation_id)
        return lease if lease.drain_phase == "lease_expired"
        return heartbeat! if lease.draining? && !lease.pins_drained?(now: now)

        return lease
      end
      return heartbeat! if lease.draining?
      return acquire!(operation_id: acquire_operation_id) unless lease.usable?(now: now)
      return refresh! unless lease.current_version&.usable?(now: now)

      heartbeat!
    end

    private

    def now = @now.call

    def client = @client ||= AutorotateProxyParentClient.new

    def pending_id(kind, id) = "#{kind}:#{id}"

    # Reserve a local operation under lock, then make the broker call after the
    # transaction closes. The pending id is compared again before persistence.
    def reserve!(pending)
      AutorotateParentLease.transaction do
        lease = AutorotateParentLease.for_bojack!
        lease.lock!
        return lease if yield lease

        if lease.pending_operation_id.present? && lease.pending_operation_id != pending
          raise Unavailable, "another parent lease operation is in progress"
        end
        lease.update!(pending_operation_id: pending)
        lease
      end
    end

    def live_lease_for!(kind)
      lease = AutorotateParentLease.for_bojack!
      reserve!(pending_id(kind, lease.id)) do |row|
        raise Unavailable, "no live parent lease" unless row.state.in?(%w[active draining]) && row.expires_at > now
        false
      end
    end

    def reserve_refresh!
      lease = AutorotateParentLease.for_bojack!
      operation_id = lease.pending_operation_id if lease.pending_operation_id&.match?(AutorotateProxyParentClient::REQUEST_ID)
      operation_id ||= SecureRandom.uuid
      [ reserve!(operation_id) do |row|
          raise Unavailable, "no live parent lease" unless row.state.in?(%w[active draining]) && row.expires_at > now
          false
        end, operation_id ]
    end

    def mark_draining!(fence:, generation:, pending:)
      lease = AutorotateParentLease.for_bojack!
      pending = pending_id(pending, lease.id)
      AutorotateParentLease.transaction do
        lease.lock!
        raise StaleLease unless lease.fence == fence.to_i && lease.external_generation == generation.to_i
        if lease.pending_operation_id.present? && lease.pending_operation_id != pending
          raise Unavailable, "another parent lease operation is in progress"
        end

        lease.update!(state: "draining", pending_operation_id: pending)
        lease.execution_pins.where(state: "active").update_all(state: "release_pending", updated_at: now)
        lease
      end
    end

    def persist_heartbeat!(id, pending, response)
      lease_id = response.fetch("lease_id")
      fence = Integer(response.fetch("fence"))
      expiry = Time.iso8601(response.fetch("expires_at"))
      raise Unavailable, "invalid parent lease response" if expiry <= now

      AutorotateParentLease.transaction do
        lease = AutorotateParentLease.lock.find(id)
        raise StaleLease unless lease.pending_operation_id == pending && lease.lease_id == lease_id && lease.fence == fence

        lease.update!(expires_at: expiry, operation_id: pending, pending_operation_id: nil)
        lease
      end
    rescue KeyError, ArgumentError, TypeError
      raise Unavailable, "invalid parent lease response"
    end

    def persist_bundle!(id, pending, response, state:, drain_phase: nil, drain_expected_phase: nil)
      values = parse_bundle_response(response)
      AutorotateParentLease.transaction do
        lease = AutorotateParentLease.lock.find(id)
        raise StaleLease unless lease.pending_operation_id == pending
        validate_drain_bundle_source!(lease, values, expected_phase: drain_expected_phase) if drain_phase

        if lease.execution_pins.where.not(state: "released").where("expires_at > ?", now).exists? &&
           (lease.lease_id.present? && lease.lease_id != values.fetch(:lease_id) ||
            lease.fence.present? && lease.fence != values.fetch(:fence) ||
            lease.current_version && lease.current_version.provider_account_id != values.fetch(:account_id))
          raise StaleLease, "cannot change account or lease while execution pins remain"
        end

        version = lease.credential_versions.find_or_initialize_by(
          broker_lease_id: values.fetch(:lease_id), external_generation: values.fetch(:generation)
        )
        if version.new_record?
          version.assign_attributes(access_token: values.fetch(:access_token), broker_lease_id: values.fetch(:lease_id), provider_account_id: values.fetch(:account_id),
                                    expires_at: values.fetch(:credential_expiry))
          version.save!
        elsif version.provider_account_id != values.fetch(:account_id) || version.expires_at != values.fetch(:credential_expiry)
          raise StaleLease, "credential generation changed unexpectedly"
        end
        attributes = { lease_id: values.fetch(:lease_id), fence: values.fetch(:fence),
                       external_generation: values.fetch(:generation), expires_at: values.fetch(:lease_expiry),
                       state: state, operation_id: pending, pending_operation_id: nil }
        attributes[:drain_phase] = drain_phase if drain_phase
        lease.update!(attributes)
        lease
      end
    end

    def finalize_release!(id, pending)
      AutorotateParentLease.transaction do
        lease = AutorotateParentLease.lock.find(id)
        return lease unless lease.draining? && lease.pending_operation_id == pending

        lease.update!(state: "released", lease_id: nil, fence: nil, external_generation: nil,
                      expires_at: now, operation_id: pending, pending_operation_id: nil)
        lease
      end
    end

    def refresh_source_drain!(lease, operation_id)
      pending = lease.drain_refresh_operation_id
      retrying = lease.pending_operation_id == pending
      reserved = reserve!(pending) do |row|
        raise StaleLease unless row.drain_operation_id == operation_id && row.drain_phase == "begun" &&
                                 row.lease_id == row.drain_lease_id && row.fence == row.drain_fence &&
                                 row.external_generation == row.drain_external_generation &&
                                 row.current_version&.provider_account_id == row.drain_provider_account_id &&
                                 row.drain_refresh_operation_id&.match?(AutorotateProxyParentClient::REQUEST_ID)
        false
      end
      response = client.refresh(lease_id: reserved.drain_lease_id, fence: reserved.drain_fence,
                                expected_generation: reserved.drain_external_generation, operation_id: pending)
      persist_bundle!(reserved.id, pending, response, state: "draining", drain_phase: "refreshed", drain_expected_phase: "begun")
    rescue ExpiredBrokerResponse => error
      raise error unless retire_expired_drain_refresh!(lease.id, pending, operation_id)

      acquire!(operation_id: SecureRandom.uuid)
    rescue AutorotateProxyParentClient::Error => error
      raise error unless retrying && error.upstream_code == "lease_expired"

      retire_terminal_drain_refresh!(lease.id, pending, operation_id)
    end

    # A long-running pin can outlive the first quota probe. Once the final pin
    # drains, take a second authoritative snapshot before deciding to exhaust.
    def refresh_final_source_drain!(lease, operation_id)
      pending = lease.drain_final_refresh_operation_id || SecureRandom.uuid
      retrying = lease.pending_operation_id == pending
      reserved = reserve!(pending) do |row|
        raise StaleLease unless row.drain_operation_id == operation_id &&
                                 row.drain_phase.in?(%w[refreshed final_refreshing]) &&
                                 row.pins_drained?(now: now)
        validate_drain_source!(row)
        row.update!(drain_phase: "final_refreshing", drain_final_refresh_operation_id: pending) if row.drain_phase == "refreshed"
        false
      end
      response = client.refresh(lease_id: reserved.drain_lease_id, fence: reserved.drain_fence,
                                expected_generation: reserved.external_generation, operation_id: pending)
      persist_bundle!(reserved.id, pending, response, state: "draining", drain_phase: "final_refreshed",
                      drain_expected_phase: "final_refreshing")
    rescue ExpiredBrokerResponse => error
      raise error unless retire_expired_drain_refresh!(lease.id, pending, operation_id)

      acquire!(operation_id: SecureRandom.uuid)
    rescue AutorotateProxyParentClient::Error => error
      raise error unless retrying && error.upstream_code == "lease_expired"

      retire_terminal_drain_refresh!(lease.id, pending, operation_id)
    end

    def release_not_exhausted_source!(lease, operation_id)
      lease = mark_not_exhausted_release_pending!(lease.id, operation_id) unless lease.drain_phase == "release_pending"
      client.release(lease_id: lease.drain_lease_id, fence: lease.drain_fence,
                     expected_generation: lease.external_generation)
      complete_not_exhausted_release!(lease.id, operation_id)
      acquire!(operation_id: SecureRandom.uuid)
    end

    def mark_not_exhausted_release_pending!(id, operation_id)
      AutorotateParentLease.transaction do
        lease = AutorotateParentLease.lock.find(id)
        raise StaleLease unless lease.drain_operation_id == operation_id && lease.drain_phase == "final_refreshed"

        validate_drain_source!(lease)
        lease.update!(drain_phase: "release_pending")
        lease
      end
    end

    def complete_not_exhausted_release!(id, operation_id)
      AutorotateParentLease.transaction do
        lease = AutorotateParentLease.lock.find(id)
        raise StaleLease unless lease.drain_operation_id == operation_id && lease.drain_phase == "release_pending"

        validate_drain_source!(lease)
        lease.update!(state: "released", expires_at: now, pending_operation_id: nil, drain_phase: "completed")
        lease
      end
    end

    # Only an explicit terminal response on a replay retires a source. A first
    # inconclusive response leaves its UUID pending so the broker can replay it.
    def retire_terminal_drain_refresh!(id, pending, operation_id)
      AutorotateParentLease.transaction do
        lease = AutorotateParentLease.lock.find(id)
        raise StaleLease unless lease.pending_operation_id == pending && lease.drain_operation_id == operation_id &&
                                 lease.drain_phase.in?(%w[begun final_refreshing])

        lease.update!(state: "draining", expires_at: now, pending_operation_id: nil, drain_phase: "lease_expired")
        lease.execution_pins.where(state: "active").update_all(state: "release_pending", updated_at: now)
        lease
      end
    end

    def complete_terminal_drain_and_acquire!(lease, operation_id)
      AutorotateParentLease.transaction do
        row = AutorotateParentLease.lock.find(lease.id)
        raise StaleLease unless row.drain_operation_id == operation_id && row.drain_phase == "lease_expired" &&
                                 row.pins_drained?(now: now)

        row.update!(state: "released", expires_at: now, pending_operation_id: nil, drain_phase: "completed")
      end
      acquire!(operation_id: SecureRandom.uuid)
    end

    def mark_drain_phase!(id, operation_id, phase)
      AutorotateParentLease.transaction do
        lease = AutorotateParentLease.lock.find(id)
        raise StaleLease unless lease.drain_operation_id == operation_id
        return lease if lease.drain_phase == phase

        allowed = { "final_refreshed" => "exhausted", "exhausted" => "completed" }
        raise StaleLease, "quota drain phase is stale" unless allowed[lease.drain_phase] == phase
        validate_drain_source!(lease) unless phase == "completed"

        lease.update!(drain_phase: phase)
        lease
      end
    end

    # A source lease is retired before acquiring its replacement.  The acquire
    # request id stays in pending_operation_id, so a crash after the broker
    # accepts it retries the same request instead of allocating another lease.
    def acquire_drain_successor!(lease, operation_id)
      request_id = nil
      AutorotateParentLease.transaction do
        row = AutorotateParentLease.lock.find(lease.id)
        raise StaleLease unless row.drain_operation_id == operation_id && row.drain_phase == "exhausted" &&
                                 row.lease_id == row.drain_lease_id && row.fence == row.drain_fence &&
                                 row.current_version&.provider_account_id == row.drain_provider_account_id

        row.update!(state: "released", expires_at: now) unless row.state == "released"
        request_id = row.pending_operation_id if row.pending_operation_id&.match?(AutorotateProxyParentClient::REQUEST_ID)
        request_id ||= SecureRandom.uuid
      end
      acquire!(operation_id: request_id)
    end

    def validate_drain_source!(lease)
      raise StaleLease unless lease.drain_lease_id.present? && lease.drain_fence.present? &&
                               lease.drain_external_generation.present? && lease.drain_provider_account_id.present? &&
                               lease.lease_id == lease.drain_lease_id && lease.fence == lease.drain_fence &&
                               lease.current_version&.provider_account_id == lease.drain_provider_account_id
    end

    def validate_drain_bundle_source!(lease, values, expected_phase:)
      raise StaleLease unless lease.drain_operation_id.present? && lease.drain_phase == expected_phase &&
                               lease.lease_id == lease.drain_lease_id && lease.fence == lease.drain_fence &&
                               values.fetch(:lease_id) == lease.drain_lease_id && values.fetch(:fence) == lease.drain_fence &&
                               values.fetch(:account_id) == lease.drain_provider_account_id
      raise StaleLease unless expected_phase != "begun" || lease.external_generation == lease.drain_external_generation
    end

    # An exact broker replay can outlive its lease. Retiring is safe only once
    # the local parent is unusable and no still-live child pin could reference
    # it; otherwise retain the pending UUID and wait for the authoritative call.
    def retire_expired_acquire_replay!(id, pending)
      AutorotateParentLease.transaction do
        lease = AutorotateParentLease.lock.find(id)
        return false unless lease.pending_operation_id == pending && !lease.usable?(now: now)

        lease.expire_pins!(now: now)
        return false unless lease.pins_drained?(now: now)

        lease.update!(state: "released", expires_at: now, operation_id: pending, pending_operation_id: nil)
        true
      end
    end

    # Refresh reached the broker, but an expired replay proves its source is no
    # longer usable. Complete the local drain without reporting an exhaustion,
    # then let the normal acquire path create a healthy successor.
    def retire_expired_drain_refresh!(id, pending, operation_id)
      AutorotateParentLease.transaction do
        lease = AutorotateParentLease.lock.find(id)
        return false unless lease.pending_operation_id == pending && lease.drain_operation_id == operation_id &&
                            lease.drain_phase.in?(%w[begun final_refreshing]) && !lease.usable?(now: now) &&
                            lease.lease_id == lease.drain_lease_id && lease.fence == lease.drain_fence

        lease.expire_pins!(now: now)
        return false unless lease.pins_drained?(now: now)

        lease.update!(state: "released", lease_id: nil, fence: nil, external_generation: nil,
                      expires_at: now, operation_id: pending,
                      pending_operation_id: nil, drain_phase: "completed")
        true
      end
    end

    def drain_matches_pin_source?(lease, pin)
      lease.drain_operation_id.present? && lease.drain_lease_id == pin.lease_id &&
        lease.drain_fence == pin.fence &&
        lease.drain_provider_account_id == pin.credential_version.provider_account_id
    end

    def clear_pending!(id)
      AutorotateParentLease.transaction do
        lease = AutorotateParentLease.lock.find(id)
        lease.update!(pending_operation_id: nil)
        lease
      end
    end

    def parse_bundle_response(response)
      bundle = response.fetch("bundle")
      values = {
        lease_id: response.fetch("lease_id"),
        fence: Integer(response.fetch("fence")),
        lease_expiry: Time.iso8601(response.fetch("expires_at")),
        generation: Integer(bundle.fetch("auth_generation")),
        credential_expiry: Time.iso8601(bundle.fetch("access_token_expires_at")),
        account_id: bundle.fetch("provider_account_id"),
        access_token: bundle.fetch("access_token")
      }
      raise ExpiredBrokerResponse, "broker lease replay expired" if values[:lease_expiry] <= now || values[:credential_expiry] <= now
      raise Unavailable, "invalid parent lease response" if values[:access_token].blank? || values[:account_id].blank?

      values
    rescue KeyError, ArgumentError, TypeError
      raise Unavailable, "invalid parent lease response"
    end
  end
end
