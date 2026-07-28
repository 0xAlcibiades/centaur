require "json"

class PermissionRequestSlackNotifier
  Result = Data.define(:channel_id, :message_ts)
  SlackApiError = Class.new(StandardError)

  DEFAULT_API_URL = "https://slack.com/api".freeze
  OPEN_TIMEOUT_SECONDS = 2
  READ_TIMEOUT_SECONDS = 5
  WRITE_TIMEOUT_SECONDS = 2

  def self.permission_requests_enabled?
    approver_channel_id.present? && slack_bot_token.present?
  end

  def self.post_approver_notification(permission_request, review_url)
    channel_id = approver_channel_id
    raise SlackApiError, "permission request approval channel is not configured" if channel_id.blank?

    payload = slack_api(
      "chat.postMessage",
      {
        channel: channel_id,
        text: approver_notification_text(permission_request, review_url),
        unfurl_links: false,
        unfurl_media: false
      }
    )
    Result.new(channel_id: channel_id, message_ts: payload.fetch("ts").to_s)
  end

  def self.update_approver_notification(permission_request)
    return unless permission_request.approver_notification_channel_id.present? &&
                  permission_request.approver_notification_message_ts.present?

    payload = slack_api(
      "chat.update",
      {
        channel: permission_request.approver_notification_channel_id,
        ts: permission_request.approver_notification_message_ts,
        text: decided_approver_notification_text(permission_request),
        unfurl_links: false,
        unfurl_media: false
      }
    )
    Result.new(channel_id: permission_request.approver_notification_channel_id, message_ts: payload.fetch("ts").to_s)
  end

  def self.post_requester_outcome(permission_request)
    body = {
      channel: permission_request.requesting_slack_channel_id,
      text: requester_outcome_text(permission_request),
      unfurl_links: false,
      unfurl_media: false
    }
    body[:thread_ts] = permission_request.requesting_slack_thread_ts if permission_request.requesting_slack_thread_ts.present?
    payload = slack_api("chat.postMessage", body)
    Result.new(channel_id: permission_request.requesting_slack_channel_id, message_ts: payload.fetch("ts").to_s)
  end

  def self.approver_notification_text(permission_request, review_url)
    [
      "*Permission Request*",
      "Requester: #{slack_channel_link(permission_request.requesting_slack_channel_id)}",
      "Request:",
      slack_code_block(permission_request.text_request),
      "Review in Console: <#{review_url}|Open request>"
    ].join("\n")
  end

  def self.decided_approver_notification_text(permission_request)
    [
      "*Permission Request #{permission_request.decision_label}*",
      "Requester: #{slack_channel_link(permission_request.requesting_slack_channel_id)}",
      "Request:",
      slack_code_block(permission_request.text_request),
      "Decision: #{permission_request.decision_label} by #{slack_escape(permission_request.decided_by.email)} at #{permission_request.decided_at.utc.iso8601}"
    ].join("\n")
  end

  def self.requester_outcome_text(permission_request)
    case permission_request.status
    when "approved"
      "Permission request approved:\n#{slack_code_block(permission_request.text_request)}"
    when "denied"
      "Permission request denied:\n#{slack_code_block(permission_request.text_request)}"
    else
      "Permission request is still pending:\n#{slack_code_block(permission_request.text_request)}"
    end
  end

  def self.approver_channel_id
    ENV["CENTAUR_CONSOLE_PERMISSION_REQUEST_APPROVAL_CHANNEL_ID"].to_s.strip
  end

  def self.slack_api(method, body)
    token = slack_bot_token
    raise SlackApiError, "SLACK_BOT_TOKEN is not configured" if token.blank?

    response = HttpClient.new(
      open_timeout: OPEN_TIMEOUT_SECONDS,
      read_timeout: READ_TIMEOUT_SECONDS,
      write_timeout: WRITE_TIMEOUT_SECONDS
    ).post(
      "#{slack_api_url}/#{method}",
      json: body,
      headers: { "Authorization" => "Bearer #{token}" }
    )
    raise SlackApiError, "Slack API returned HTTP #{response.status}" unless response.success?

    payload = response.json
    raise SlackApiError, "Slack API request failed: #{payload.fetch("error", "unknown_error")}" unless payload["ok"] == true

    payload
  rescue JSON::ParserError
    raise SlackApiError, "Slack API response was not JSON"
  rescue StandardError => e
    raise if e.is_a?(SlackApiError)

    raise SlackApiError, "Slack API request failed: #{e.class}"
  end
  private_class_method :slack_api

  def self.slack_api_url
    (ENV["SLACK_API_URL"].presence || DEFAULT_API_URL).to_s.delete_suffix("/")
  end
  private_class_method :slack_api_url

  def self.slack_bot_token
    ENV["CENTAUR_CONSOLE_SLACK_BOT_TOKEN"].presence || ENV["SLACK_BOT_TOKEN"].presence
  end
  private_class_method :slack_bot_token

  def self.slack_escape(value)
    value.to_s
      .gsub("&", "&amp;")
      .gsub("<", "&lt;")
      .gsub(">", "&gt;")
  end
  private_class_method :slack_escape

  def self.slack_code_block(value)
    "```#{slack_escape(value).gsub("```", "` ` `")}```"
  end
  private_class_method :slack_code_block

  def self.slack_channel_link(channel_id)
    value = channel_id.to_s.strip.upcase
    return slack_escape(channel_id) unless value.match?(Principal::SLACK_CHANNEL_ID_FORMAT)

    "<##{value}>"
  end
  private_class_method :slack_channel_link
end
