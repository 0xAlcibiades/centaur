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

  belongs_to :requesting_principal, class_name: "Principal", optional: true
  belongs_to :requesting_proxy, class_name: "Proxy", optional: true
  belongs_to :decided_by, class_name: "User", optional: true

  before_validation :copy_requesting_audit_fields
  before_validation :normalize_request_payload

  validates :status, inclusion: { in: STATUSES }
  validates :kind, inclusion: { in: KINDS }
  validates :requesting_principal_oid, :requesting_proxy_oid, :requesting_proxy_name, presence: true
  validates :requesting_slack_channel_id, presence: true,
                                          format: { with: Principal::SLACK_CHANNEL_ID_FORMAT,
                                                    message: "is not a valid Slack channel ID" }
  validates :approver_notification_status, :approver_decision_update_status,
            :requester_outcome_notification_status, inclusion: { in: NOTIFICATION_STATUSES }
  validate :request_payload_matches_kind
  validate :requesting_principal_is_slack_channel
  validate :requesting_proxy_matches_principal
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

  def decision_notifications_retryable?
    return false if pending?

    approver_decision_update_status.in?(%w[pending failed]) ||
      requester_outcome_notification_status.in?(%w[pending failed])
  end

  def mark_approver_notification_skipped!
    update!(
      approver_notification_status: "skipped",
      approver_notification_attempted_at: Time.current,
      approver_notification_delivered_at: Time.current,
      approver_notification_last_error: nil
    )
  end

  def mark_approver_notification_sent!(channel_id:, message_ts:)
    update!(
      approver_notification_status: "sent",
      approver_notification_channel_id: channel_id,
      approver_notification_message_ts: message_ts,
      approver_notification_attempted_at: Time.current,
      approver_notification_delivered_at: Time.current,
      approver_notification_last_error: nil
    )
  end

  def mark_approver_notification_failed!(error)
    update!(
      approver_notification_status: "failed",
      approver_notification_attempted_at: Time.current,
      approver_notification_last_error: notification_error_message(error)
    )
  end

  def mark_approver_decision_update_skipped!
    update!(
      approver_decision_update_status: "skipped",
      approver_decision_update_attempted_at: Time.current,
      approver_decision_update_delivered_at: Time.current,
      approver_decision_update_last_error: nil
    )
  end

  def mark_approver_decision_update_sent!
    update!(
      approver_decision_update_status: "sent",
      approver_decision_update_attempted_at: Time.current,
      approver_decision_update_delivered_at: Time.current,
      approver_decision_update_last_error: nil
    )
  end

  def mark_approver_decision_update_failed!(error)
    update!(
      approver_decision_update_status: "failed",
      approver_decision_update_attempted_at: Time.current,
      approver_decision_update_last_error: notification_error_message(error)
    )
  end

  def mark_requester_outcome_sent!(message_ts:)
    update!(
      requester_outcome_notification_status: "sent",
      requester_outcome_message_ts: message_ts,
      requester_outcome_notification_attempted_at: Time.current,
      requester_outcome_notification_delivered_at: Time.current,
      requester_outcome_notification_last_error: nil
    )
  end

  def mark_requester_outcome_failed!(error)
    update!(
      requester_outcome_notification_status: "failed",
      requester_outcome_notification_attempted_at: Time.current,
      requester_outcome_notification_last_error: notification_error_message(error)
    )
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

  def copy_requesting_audit_fields
    if requesting_principal
      self.requesting_principal_oid ||= requesting_principal.oid
      self.requesting_principal_name ||= requesting_principal.name.presence || requesting_principal.foreign_id
    end
    return unless requesting_proxy

    self.requesting_proxy_oid ||= requesting_proxy.oid
    self.requesting_proxy_name ||= requesting_proxy.name
  end

  def notification_error_message(error)
    "#{error.class}: #{error.message}".truncate(1000)
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
    return if requesting_proxy.nil? || requesting_principal.nil?
    return if requesting_proxy.principal_id == requesting_principal_id

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
