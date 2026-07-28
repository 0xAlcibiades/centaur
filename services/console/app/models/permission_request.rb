class PermissionRequest < ApplicationRecord
  oid_prefix "preq"

  STATUSES = %w[pending approved denied].freeze
  KINDS = %w[text].freeze
  NOTIFICATION_STATUSES = %w[pending sent skipped failed].freeze
  TEXT_KIND = "text".freeze
  TEXT_METADATA_SCHEMA = JSONSchemer.schema({
    "type" => "object",
    "additionalProperties" => false,
    "required" => [ "request" ],
    "properties" => {
      "request" => { "type" => "string", "minLength" => 1 }
    }
  })
  METADATA_SCHEMAS = {
    TEXT_KIND => TEXT_METADATA_SCHEMA
  }.freeze

  belongs_to :requesting_principal, class_name: "Principal"
  belongs_to :decided_by, class_name: "User", optional: true

  before_validation :normalize_request_payload

  validates :status, inclusion: { in: STATUSES }
  validates :kind, inclusion: { in: KINDS }
  validates :requesting_proxy_id, presence: true
  validates :requesting_slack_channel_id, presence: true,
                                          format: { with: Principal::SLACK_CHANNEL_ID_FORMAT,
                                                    message: "is not a valid Slack channel ID" }
  validates :approver_notification_status, :approver_decision_update_status,
            :requester_outcome_notification_status, inclusion: { in: NOTIFICATION_STATUSES }
  validate :request_payload_matches_kind
  validate :requesting_principal_is_slack_channel
  validate :requesting_proxy_matches_principal, on: :create
  validate :decision_metadata_matches_status

  scope :recent_first, -> { order(created_at: :desc, id: :desc) }

  def pending?
    status == "pending"
  end

  def approved?
    status == "approved"
  end

  def denied?
    status == "denied"
  end

  def text?
    kind == TEXT_KIND
  end

  def text_request
    metadata["request"].to_s
  end

  def approve!(by:)
    transition!("approved", by: by)
  end

  def deny!(by:)
    transition!("denied", by: by)
  end

  def decision_label
    status.titleize
  end

  def requesting_proxy=(proxy)
    @requesting_proxy = proxy
    self.requesting_proxy_id = proxy&.id
  end

  def requesting_proxy
    @requesting_proxy ||= Proxy.find_by(id: requesting_proxy_id) if requesting_proxy_id.present?
  end

  private

  def transition!(next_status, by:)
    changed = false
    with_lock do
      return false unless pending?

      yield if block_given?
      self.status = next_status
      self.decided_by = by
      self.decided_at = Time.current
      save!
      changed = true
    end
    changed
  end

  def normalize_request_payload
    self.requesting_slack_channel_id = requesting_slack_channel_id.to_s.strip.upcase
    self.metadata = normalize_metadata
  end

  def normalize_metadata
    return {} if metadata.nil?
    return metadata unless metadata.is_a?(Hash)

    values = metadata.stringify_keys
    case kind
    when TEXT_KIND
      return values unless values.key?("request")

      values.merge("request" => values["request"].to_s.strip)
    else
      values
    end
  end

  def request_payload_matches_kind
    unless metadata.is_a?(Hash)
      errors.add(:metadata, "must be a hash")
      return
    end

    schema = METADATA_SCHEMAS[kind]
    return unless schema

    schema.validate(metadata).each do |err|
      pointer = err["data_pointer"].presence || "(root)"
      errors.add(:metadata, "#{pointer} #{err["error"]}")
    end
  end

  def requesting_principal_is_slack_channel
    return if requesting_principal.nil?
    return if requesting_principal.labels["kind"] == "slack_channel" &&
              requesting_principal.foreign_id == requesting_slack_channel_id

    errors.add(:requesting_principal, "must be a Slack channel principal")
  end

  def requesting_proxy_matches_principal
    return if requesting_principal.nil?
    unless requesting_proxy
      errors.add(:requesting_proxy, "must exist")
      return
    end
    return if requesting_proxy.principal_id == requesting_principal.id

    errors.add(:requesting_proxy, "must be assigned to the requesting principal")
  end

  def decision_metadata_matches_status
    if pending?
      errors.add(:decided_at, "must be blank while pending") if decided_at.present?
      errors.add(:decided_by, "must be blank while pending") if decided_by.present?
    elsif decided_at.blank? || decided_by.blank?
      errors.add(:base, "decision metadata is required once decided")
    end
  end
end
