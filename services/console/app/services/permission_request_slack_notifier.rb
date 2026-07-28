require "json"

class PermissionRequestSlackNotifier
  Result = Data.define(:channel_id, :message_ts)
  SlackApiError = Class.new(StandardError)

  DEFAULT_API_URL = "https://slack.com/api".freeze
  OPEN_TIMEOUT_SECONDS = 2
  READ_TIMEOUT_SECONDS = 5
  WRITE_TIMEOUT_SECONDS = 2

  def self.post_approver_notification(permission_request, review_url)
    channel_id = approver_channel_id
    payload = slack_api(
      "chat.postMessage",
      {
        channel: channel_id,
        text: approver_notification_text(permission_request, review_url),
        unfurl_links: false,
        unfurl_media: false
      }
    )
    Result.new(channel_id: channel_id, message_ts: payload.fetch("ts"))
  end

  def self.update_approver_notification(permission_request)
    return unless permission_request.approver_notification_channel_id.present? &&
                  permission_request.approver_notification_message_ts.present?

    slack_api(
      "chat.update",
      {
        channel: permission_request.approver_notification_channel_id,
        ts: permission_request.approver_notification_message_ts,
        text: decided_approver_notification_text(permission_request),
        unfurl_links: false,
        unfurl_media: false
      }
    )
  end

  def self.post_requester_outcome(permission_request)
    body = {
      channel: permission_request.requesting_slack_channel_id,
      text: requester_outcome_text(permission_request),
      unfurl_links: false,
      unfurl_media: false
    }
    body[:thread_ts] = permission_request.requesting_slack_thread_ts if permission_request.requesting_slack_thread_ts.present?
    slack_api("chat.postMessage", body)
  end

  def self.approver_notification_text(permission_request, review_url)
    [
      "*Permission Request:* #{request_summary(permission_request)}",
      "Requester: #{permission_request.requesting_slack_channel_id}",
      "Review in Console: <#{review_url}|Open request>"
    ].join("\n")
  end

  def self.decided_approver_notification_text(permission_request)
    [
      "*Permission Request #{permission_request.decision_label}:* #{request_summary(permission_request)}",
      "Requester: #{permission_request.requesting_slack_channel_id}",
      "Decision: #{permission_request.decision_label} by #{permission_request.decided_by.email} at #{permission_request.decided_at.utc.iso8601}"
    ].join("\n")
  end

  def self.requester_outcome_text(permission_request)
    case permission_request.status
    when "approved"
      if permission_request.slack_channels?
        "Permission request approved. Channel access has been granted for #{permission_request.requested_channel_ids.join(", ")}."
      else
        "Permission request approved. Service authorization was approved for #{permission_request.services.join(", ")}."
      end
    when "denied"
      "Permission request denied for #{request_summary(permission_request)}."
    else
      "Permission request is still pending for #{request_summary(permission_request)}."
    end
  end

  def self.request_summary(permission_request)
    if permission_request.slack_channels?
      "Slack channels #{permission_request.requested_channel_ids.join(", ")}"
    else
      "services #{permission_request.services.join(", ")}"
    end
  end

  def self.approver_channel_id
    channel_id = ENV["CENTAUR_CONSOLE_PERMISSION_REQUEST_APPROVAL_CHANNEL_ID"].to_s.strip
    raise SlackApiError, "CENTAUR_CONSOLE_PERMISSION_REQUEST_APPROVAL_CHANNEL_ID is not configured" if channel_id.blank?

    channel_id
  end

  def self.slack_api(method, body)
    token = ENV["CENTAUR_CONSOLE_SLACK_BOT_TOKEN"].presence || ENV["SLACK_BOT_TOKEN"].presence
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
  end
  private_class_method :slack_api

  def self.slack_api_url
    (ENV["SLACK_API_URL"].presence || DEFAULT_API_URL).to_s.delete_suffix("/")
  end
  private_class_method :slack_api_url
end
