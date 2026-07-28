class PermissionRequestApproverNotificationJob < ApplicationJob
  queue_as :default

  retry_on PermissionRequestSlackNotifier::SlackApiError, wait: :polynomially_longer, attempts: 8

  def perform(permission_request_id, review_url)
    permission_request = PermissionRequest.find_by(id: permission_request_id)
    return unless permission_request
    return if permission_request.approver_notification_status.in?(%w[sent skipped])

    result = PermissionRequestSlackNotifier.post_approver_notification(permission_request, review_url)
    permission_request.update!(
      approver_notification_status: "sent",
      approver_notification_channel_id: result.channel_id,
      approver_notification_message_ts: result.message_ts
    )
  rescue PermissionRequestSlackNotifier::SlackApiError => e
    permission_request&.update!(approver_notification_status: "failed")
    raise
  end
end
