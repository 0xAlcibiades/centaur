module Console
  class PermissionRequestsController < ApplicationController
    layout "console"

    before_action :require_admin
    before_action :set_permission_request

    def show; end

    def approve
      changed = @permission_request.approve!(by: current_user)
      enqueue_decision_notification if changed || @permission_request.reload.decision_notifications_retryable?
      redirect_to console_permission_request_path(@permission_request.oid),
                  notice: changed ? "Permission request approved." : "Permission request was already decided."
    rescue ActiveRecord::RecordInvalid => e
      redirect_to console_permission_request_path(@permission_request.oid),
                  alert: e.record.errors.full_messages.to_sentence
    end

    def deny
      changed = @permission_request.deny!(by: current_user)
      enqueue_decision_notification if changed || @permission_request.reload.decision_notifications_retryable?
      redirect_to console_permission_request_path(@permission_request.oid),
                  notice: changed ? "Permission request denied." : "Permission request was already decided."
    end

    private

    def set_permission_request
      @permission_request = PermissionRequest
        .includes(:requesting_principal, :requesting_proxy, :decided_by)
        .find_by_oid!(params[:id])
    end

    def enqueue_decision_notification
      PermissionRequestDecisionNotificationJob.perform_later(@permission_request.id)
    end
  end
end
