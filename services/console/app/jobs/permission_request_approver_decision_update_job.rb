class PermissionRequestApproverDecisionUpdateJob < ApplicationJob
  queue_as :default

  retry_on PermissionRequestSlackNotifier::SlackApiError, wait: :polynomially_longer, attempts: 8

  def perform(permission_request_id)
    permission_request = PermissionRequest.find_by(id: permission_request_id)
    return unless permission_request
    return if permission_request.pending? || permission_request.approver_decision_update_status.in?(%w[sent skipped])

    unless permission_request.approver_notification_message_ts.present?
      permission_request.update!(approver_decision_update_status: "skipped")
      return
    end

    PermissionRequestSlackNotifier.update_approver_notification(permission_request)
    permission_request.update!(approver_decision_update_status: "sent")
  rescue PermissionRequestSlackNotifier::SlackApiError => e
    permission_request&.update!(approver_decision_update_status: "failed")
    raise
  end
end
