module Console
  class PermissionRequestsController < ApplicationController
    layout "console"

    before_action :require_admin
    before_action :set_permission_request

    def show; end

    def approve
      changed = @permission_request.approve!(by: current_user)
      notify_decision if changed
      redirect_to console_permission_request_path(@permission_request.oid),
                  notice: changed ? "Permission request approved." : "Permission request was already decided."
    rescue ActiveRecord::RecordInvalid => e
      redirect_to console_permission_request_path(@permission_request.oid),
                  alert: e.record.errors.full_messages.to_sentence
    rescue PermissionRequestSlackNotifier::SlackApiError => e
      redirect_to console_permission_request_path(@permission_request.oid),
                  alert: "Permission request approved, but Slack notification failed: #{e.message}"
    end

    def deny
      changed = @permission_request.deny!(by: current_user)
      notify_decision if changed
      redirect_to console_permission_request_path(@permission_request.oid),
                  notice: changed ? "Permission request denied." : "Permission request was already decided."
    rescue PermissionRequestSlackNotifier::SlackApiError => e
      redirect_to console_permission_request_path(@permission_request.oid),
                  alert: "Permission request denied, but Slack notification failed: #{e.message}"
    end

    private

    def set_permission_request
      @permission_request = PermissionRequest
        .includes(:requesting_principal, :requesting_proxy, :decided_by)
        .find_by_oid!(params[:id])
    end

    def notify_decision
      PermissionRequestSlackNotifier.update_approver_notification(@permission_request)
      PermissionRequestSlackNotifier.post_requester_outcome(@permission_request)
    end
  end
end
