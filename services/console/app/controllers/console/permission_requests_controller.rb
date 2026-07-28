module Console
  class PermissionRequestsController < ApplicationController
    layout "auth"

    before_action :require_admin
    before_action :set_permission_request

    def show; end

    def approve
      changed = @permission_request.approve!(by: current_user)
      enqueue_decision_notifications if changed || retryable_decision_notifications?
      redirect_to console_permission_request_path(@permission_request.oid),
                  notice: changed ? "Permission request approved." : "Permission request was already decided."
    rescue ActiveRecord::RecordInvalid => e
      redirect_to console_permission_request_path(@permission_request.oid),
                  alert: e.record.errors.full_messages.to_sentence
    end

    def deny
      changed = @permission_request.deny!(by: current_user)
      enqueue_decision_notifications if changed || retryable_decision_notifications?
      redirect_to console_permission_request_path(@permission_request.oid),
                  notice: changed ? "Permission request denied." : "Permission request was already decided."
    end

    private

    def set_permission_request
      @permission_request = PermissionRequest
        .includes(:requesting_principal, :requesting_proxy, :decided_by)
        .find_by_oid!(params[:id])
    end

    def enqueue_decision_notifications
      request = @permission_request.reload
      if request.approver_decision_update_status.in?(%w[pending failed])
        PermissionRequestApproverDecisionUpdateJob.perform_later(request.id)
      end
      if request.requester_outcome_notification_status.in?(%w[pending failed])
        PermissionRequestRequesterOutcomeNotificationJob.perform_later(request.id)
      end
    end

    def retryable_decision_notifications?
      request = @permission_request.reload
      request.approver_decision_update_status.in?(%w[pending failed]) ||
        request.requester_outcome_notification_status.in?(%w[pending failed])
    end
  end
end
