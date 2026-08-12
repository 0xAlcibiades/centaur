class SlackDm::SyncLedger < ApplicationRecord
  CLAIM_LEASE = 2.hours
  SUCCESS_INTERVAL = 10.minutes
  BACKOFF_BASE_SECONDS = 5
  BACKOFF_MAX_SECONDS = 1.hour.to_i

  belongs_to :broker_credential

  validates :home_team_id, :conversation_id, :conversation_type, presence: true
  validates :backoff_level, numericality: { only_integer: true, greater_than_or_equal_to: 0 }

  scope :due, ->(now = Time.current) {
    where(active: true)
      .where(next_sync_at: ..now)
      .where("claim_token IS NULL OR claimed_until <= ?", now)
  }

  def self.refresh_inventory!(credential:, home_team_id:, conversations:, now: Time.current)
    transaction do
      where(broker_credential: credential, home_team_id: home_team_id).update_all(
        active: false,
        updated_at: now
      )
      conversations.index_by { |conversation| conversation.fetch(:conversation_id) }.each_value do |conversation|
        ledger = find_or_initialize_by(
          broker_credential: credential,
          home_team_id: home_team_id,
          conversation_id: conversation.fetch(:conversation_id)
        )
        ledger.assign_attributes(
          conversation_type: conversation.fetch(:conversation_type),
          is_archived: conversation.fetch(:is_archived),
          is_ext_shared: conversation.fetch(:is_ext_shared),
          raw_payload: conversation.fetch(:raw_payload),
          active: true,
          last_seen_at: now
        )
        ledger.save!
      end
    end
  end

  def self.claim_due(limit:, broker_credential_ids:, now: Time.current, lease: CLAIM_LEASE)
    transaction do
      due(now)
        .where(broker_credential_id: broker_credential_ids)
        .order(:next_sync_at, :id)
        .lock("FOR UPDATE SKIP LOCKED")
        .limit(limit)
        .map do |ledger|
          ledger.update!(claim_token: SecureRandom.uuid, claimed_until: now + lease)
          ledger
        end
    end
  end

  def complete_claim!(claim_token:, watermark_ts:, now: Time.current)
    with_lock do
      return false unless self.claim_token == claim_token

      update!(
        watermark_ts: watermark_ts,
        next_sync_at: now + SUCCESS_INTERVAL,
        last_error: "",
        backoff_level: 0,
        claim_token: nil,
        claimed_until: nil
      )
    end
    true
  end

  def fail_claim!(claim_token:, error:, now: Time.current)
    with_lock do
      return false unless self.claim_token == claim_token

      delay = [ BACKOFF_BASE_SECONDS * (2**backoff_level), BACKOFF_MAX_SECONDS ].min
      update!(
        next_sync_at: now + delay.seconds,
        last_error: error.to_s,
        backoff_level: [ backoff_level + 1, 10 ].min,
        claim_token: nil,
        claimed_until: nil
      )
    end
    true
  end

  def renew_claim!(claim_token:, now: Time.current, lease: CLAIM_LEASE)
    with_lock do
      return false unless self.claim_token == claim_token

      update!(claimed_until: now + lease)
    end
    true
  end

  def release_claim!(claim_token:)
    with_lock do
      return false unless self.claim_token == claim_token

      update!(claim_token: nil, claimed_until: nil)
    end
    true
  end
end
