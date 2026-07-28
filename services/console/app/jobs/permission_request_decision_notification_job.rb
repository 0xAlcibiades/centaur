class PermissionRequestDecisionNotificationJob < ApplicationJob
  queue_as :default

  retry_on PermissionRequestSlackNotifier::SlackApiError, wait: :polynomially_longer, attempts: 8

  def perform(permission_request_id)
    permission_request = PermissionRequest.find_by(id: permission_request_id)
    return unless permission_request
    return if permission_request.pending?

    deliver_approver_update(permission_request)
    deliver_requester_outcome(permission_request)
  end

  private

  def deliver_approver_update(permission_request)
    return if permission_request.approver_decision_update_status.in?(%w[sent skipped])

    unless permission_request.approver_notification_message_ts.present?
      permission_request.mark_approver_decision_update_skipped!
      return
    end

    PermissionRequestSlackNotifier.update_approver_notification(permission_request)
    permission_request.mark_approver_decision_update_sent!
  rescue PermissionRequestSlackNotifier::SlackApiError => e
    permission_request.mark_approver_decision_update_failed!(e)
    raise
  end

  def deliver_requester_outcome(permission_request)
    return if permission_request.requester_outcome_notification_status == "sent"

    result = PermissionRequestSlackNotifier.post_requester_outcome(permission_request)
    permission_request.mark_requester_outcome_sent!(message_ts: result.message_ts)
  rescue PermissionRequestSlackNotifier::SlackApiError => e
    permission_request.mark_requester_outcome_failed!(e)
    raise
  end
end
