class PermissionRequest < ApplicationRecord
  oid_prefix "preq"

  STATUSES = %w[pending approved denied].freeze
  KINDS = %w[slack_channels services].freeze
  NOTIFICATION_STATUSES = %w[pending sent skipped failed].freeze
  SLACK_CHANNELS_KIND = "slack_channels".freeze
  SERVICES_KIND = "services".freeze
  SERVICE_ALIASES = {
    "calendar" => "google_calendar",
    "drive" => "google_drive",
    "google" => "google",
    "google_calendar" => "google_calendar",
    "google_drive" => "google_drive"
  }.freeze
  SERVICE_IDENTIFIERS = (Oauth::Providers.keys + %w[gmail google_calendar google_drive]).uniq.sort.freeze

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

  def slack_channels?
    kind == SLACK_CHANNELS_KIND
  end

  def services?
    kind == SERVICES_KIND
  end

  def approve!(by:)
    transition!("approved", by: by) do
      grant_requested_slack_channels! if slack_channels?
    end
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

  def grant_requested_slack_channels!
    unless requesting_principal
      errors.add(:requesting_principal, "is no longer available")
      raise ActiveRecord::RecordInvalid, self
    end

    now = Time.current
    rows = requested_channel_ids.map do |channel_id|
      {
        principal_id: requesting_principal.id,
        channel_id: channel_id,
        upload_enabled: true,
        download_enabled: true,
        history_enabled: true,
        created_at: now,
        updated_at: now
      }
    end
    SlackChannelPermission.insert_all(rows, unique_by: :idx_slack_permissions_unique_principal_channel) if rows.any?
    SlackChannelPermission
      .where(principal_id: requesting_principal.id, channel_id: requested_channel_ids)
      .update_all(upload_enabled: true, download_enabled: true, history_enabled: true, updated_at: now)
    requesting_principal.reset_slack_channel_permissions_cache!
    Principal.bump_sync_config_cache_versions([ requesting_principal.id ])
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
    self.requested_channel_ids = normalize_strings(requested_channel_ids).map(&:upcase).uniq
    self.services = normalize_services(services)
  end

  def normalize_services(values)
    normalize_strings(values).map do |value|
      normalized = value.downcase.tr(" -", "__").squeeze("_")
      SERVICE_ALIASES.fetch(normalized, normalized)
    end.uniq
  end

  def normalize_strings(values)
    Array(values)
      .map { |value| value.to_s.strip }
      .reject(&:blank?)
      .uniq
  end

  def request_payload_matches_kind
    case kind
    when SLACK_CHANNELS_KIND
      errors.add(:requested_channel_ids, "must include at least one channel ID") if requested_channel_ids.blank?
      errors.add(:services, "must be empty for Slack channel requests") if services.present?
      requested_channel_ids.each do |channel_id|
        next if channel_id.match?(Principal::SLACK_CHANNEL_ID_FORMAT)
        errors.add(:requested_channel_ids, "#{channel_id} is not a valid Slack channel ID")
      end
    when SERVICES_KIND
      errors.add(:services, "must include at least one service") if services.blank?
      errors.add(:requested_channel_ids, "must be empty for service requests") if requested_channel_ids.present?
      unknown = services - SERVICE_IDENTIFIERS
      if unknown.any?
        errors.add(:services, "contains unknown service identifiers: #{unknown.join(", ")}")
      end
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
