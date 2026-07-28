class PermissionRequest < ApplicationRecord
  oid_prefix "preq"

  STATUSES = %w[pending approved denied].freeze
  KINDS = %w[slack_channels services].freeze
  SLACK_CHANNELS_KIND = "slack_channels".freeze
  SERVICES_KIND = "services".freeze

  belongs_to :requesting_principal, class_name: "Principal"
  belongs_to :requesting_proxy, class_name: "Proxy"
  belongs_to :decided_by, class_name: "User", optional: true

  before_validation :normalize_request_payload

  validates :status, inclusion: { in: STATUSES }
  validates :kind, inclusion: { in: KINDS }
  validates :requesting_slack_channel_id, presence: true,
                                          format: { with: Principal::SLACK_CHANNEL_ID_FORMAT,
                                                    message: "is not a valid Slack channel ID" }
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
    requested_channel_ids.each do |channel_id|
      permission = requesting_principal.slack_channel_permissions.find_or_initialize_by(channel_id: channel_id)
      permission.assign_attributes(SlackChannelPermission::DEFAULT_ENABLED_ATTRIBUTES)
      permission.save!
    end
  rescue ActiveRecord::RecordNotUnique
    retry
  end

  def normalize_request_payload
    self.requesting_slack_channel_id = requesting_slack_channel_id.to_s.strip.upcase
    self.requested_channel_ids = normalize_strings(requested_channel_ids).map(&:upcase).uniq
    self.services = normalize_strings(services)
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
