class AutorotateCredentialVersion < ApplicationRecord
  oid_prefix "acv"

  belongs_to :parent_lease, class_name: "AutorotateParentLease", foreign_key: :autorotate_parent_lease_id
  has_many :execution_pins, class_name: "AutorotateExecutionPin", dependent: :restrict_with_exception

  encrypts :access_token
  encrypts :provider_account_id

  validates :access_token, :broker_lease_id, :provider_account_id, :external_generation, :expires_at, presence: true
  validates :external_generation, uniqueness: { scope: %i[autorotate_parent_lease_id broker_lease_id] }
  validate :immutable_after_create, on: :update

  def usable?(now: Time.current)
    expires_at > now + AutorotateExecutionPin::TOKEN_EXPIRY_MARGIN
  end

  def as_json(options = nil)
    super(options).except("access_token", "provider_account_id")
  end

  private

  def immutable_after_create
    changed = changes_to_save.keys - [ "updated_at" ]
    errors.add(:base, "credential versions are immutable") if changed.any?
  end
end
