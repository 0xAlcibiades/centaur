class AutorotateExecutionPin < ApplicationRecord
  oid_prefix "apn"

  TOKEN_EXPIRY_MARGIN = 60.seconds
  DEFAULT_TTL = 5.minutes
  PROXY_PIN_LABEL = "centaur.autorotate_pin_id".freeze
  PROXY_EXECUTION_LABEL = "centaur.execution_id".freeze
  STATES = %w[active release_pending released].freeze

  belongs_to :parent_lease, class_name: "AutorotateParentLease", foreign_key: :autorotate_parent_lease_id
  belongs_to :credential_version, class_name: "AutorotateCredentialVersion", foreign_key: :autorotate_credential_version_id
  belongs_to :proxy, optional: true

  validates :operation_id, :execution_id, :request_hash, :lease_id, :fence, :expires_at, presence: true
  validates :state, inclusion: { in: STATES }
  validates :operation_id, :execution_id, uniqueness: true
  validate :expiry_is_bounded_by_version

  scope :live, -> { where(state: %w[active release_pending]).where("expires_at > ?", Time.current) }

  def usable_for_proxy?(candidate_proxy, now: Time.current)
    proxy_id == candidate_proxy.id && state.in?(%w[active release_pending]) && expires_at > now &&
      candidate_proxy.labels&.fetch(PROXY_PIN_LABEL, nil) == oid &&
      candidate_proxy.labels&.fetch(PROXY_EXECUTION_LABEL, nil) == execution_id &&
      parent_lease.state.in?(%w[active draining]) && parent_lease.expires_at > now &&
      lease_id == parent_lease.lease_id && fence == parent_lease.fence &&
      credential_version.provider_account_id == parent_lease.current_version&.provider_account_id &&
      credential_version.usable?(now: now)
  end

  # The runtime is the only actor that owns proxy labels. Binding on the first
  # observed label lets it mint the pin before proxy creation without trusting
  # a sandbox-supplied proxy id. A pin can never migrate between proxies.
  def bind_proxy!(candidate_proxy)
    with_lock do
      expected_execution = candidate_proxy.labels&.fetch(PROXY_EXECUTION_LABEL, nil)
      raise ActiveRecord::RecordInvalid, self unless expected_execution == execution_id
      return self if proxy_id == candidate_proxy.id
      raise ActiveRecord::RecordInvalid, self if proxy_id.present?

      update!(proxy: candidate_proxy)
    end
  end

  def release!
    return if state == "released"

    update!(state: "released", released_at: Time.current)
  end

  private

  def expiry_is_bounded_by_version
    return unless expires_at && credential_version

    max_expiry = credential_version.expires_at - TOKEN_EXPIRY_MARGIN
    errors.add(:expires_at, "must leave a token expiry margin") if expires_at > max_expiry
  end
end
