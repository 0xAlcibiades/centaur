class PermissionRequestRequesterOutcomeNotificationJob < ApplicationJob
  queue_as :default

  retry_on PermissionRequestSlackNotifier::SlackApiError, wait: :polynomially_longer, attempts: 8

  def perform(permission_request_id)
    permission_request = PermissionRequest.find_by(id: permission_request_id)
    return unless permission_request
    return if permission_request.pending? || permission_request.requester_outcome_notification_status == "sent"

    result = PermissionRequestSlackNotifier.post_requester_outcome(permission_request)
    permission_request.update!(
      requester_outcome_notification_status: "sent",
      requester_outcome_message_ts: result.message_ts
    )
  rescue PermissionRequestSlackNotifier::SlackApiError => e
    permission_request&.update!(requester_outcome_notification_status: "failed")
    raise
  end
end
