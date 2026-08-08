class AutorotateParentLease < ApplicationRecord
  oid_prefix "apl"

  CONSUMER = "bojack".freeze
  STATES = %w[active draining released].freeze
  UUID_PATTERN = /\A[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}\z/i

  has_many :credential_versions, class_name: "AutorotateCredentialVersion", dependent: :restrict_with_exception
  has_many :execution_pins, class_name: "AutorotateExecutionPin", dependent: :restrict_with_exception

  encrypts :drain_provider_account_id

  validates :consumer, inclusion: { in: [ CONSUMER ] }, uniqueness: true
  validates :state, inclusion: { in: STATES }
  validates :lease_id, :fence, :external_generation, :expires_at, presence: true, if: :active_or_draining?
  validates :drain_refresh_operation_id, :drain_final_refresh_operation_id,
            format: { with: UUID_PATTERN }, allow_nil: true

  def self.for_bojack!
    find_or_create_by!(consumer: CONSUMER)
  end

  def active_or_draining? = state.in?(%w[active draining])
  def usable?(now: Time.current) = state == "active" && expires_at.present? && expires_at > now
  def draining? = state == "draining"
  def current_version = credential_versions.order(created_at: :desc, id: :desc).first
  def pins_drained?(now: Time.current) = execution_pins.where.not(state: "released").where("expires_at > ?", now).none?

  def expire_pins!(now: Time.current)
    execution_pins.where.not(state: "released").where("expires_at <= ?", now)
                  .update_all(state: "released", released_at: now, updated_at: now)
  end

  def drain_completed? = drain_phase == "completed"
end
