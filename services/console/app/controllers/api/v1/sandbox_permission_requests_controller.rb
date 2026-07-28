module Api
  module V1
    class SandboxPermissionRequestsController < ActionController::API
      include ApiSandboxAuthentication

      rescue_from ActiveRecord::RecordInvalid, with: :render_record_invalid
      before_action :ensure_permission_requests_enabled!

      def create
        principal = current_proxy.principal
        permission_request = PermissionRequest.create!(permission_request_attributes(principal))
        PermissionRequestApproverNotificationJob.perform_later(
          permission_request.id,
          console_permission_request_url(permission_request.oid)
        )

        render status: :created, json: { data: permission_request_payload(permission_request) }
      end

      private

      def permission_request_attributes(principal)
        attrs = data_params.permit(
          :kind,
          :requesting_slack_thread_ts
        )
        {
          kind: attrs.require(:kind),
          requesting_principal: principal,
          requesting_proxy: current_proxy,
          requesting_slack_channel_id: normalized_slack_channel_id(requesting_slack_channel_id(principal)),
          requesting_slack_thread_ts: attrs[:requesting_slack_thread_ts],
          metadata: metadata_params
        }
      end

      def requesting_slack_channel_id(principal)
        principal.labels[Principal::SLACK_CHANNEL_ID_LABEL].presence || principal.foreign_id
      end

      def normalized_slack_channel_id(channel_id)
        channel_id.to_s.strip.upcase
      end

      def ensure_permission_requests_enabled!
        return if PermissionRequestSlackNotifier.permission_requests_enabled?

        render_error(status: :service_unavailable, message: "permission requests are not configured")
      end

      def permission_request_payload(permission_request)
        {
          id: permission_request.oid,
          status: permission_request.status,
          kind: permission_request.kind,
          requesting_principal_id: permission_request.requesting_principal_id,
          requesting_proxy_id: permission_request.requesting_proxy_id,
          requesting_slack_channel_id: permission_request.requesting_slack_channel_id,
          requesting_slack_thread_ts: permission_request.requesting_slack_thread_ts,
          metadata: permission_request.metadata,
          approver_notification_status: permission_request.approver_notification_status,
          created_at: permission_request.created_at,
          updated_at: permission_request.updated_at
        }
      end

      def console_permission_request_url(id)
        URI.join(public_base_url, "/console/permission_requests/#{id}").to_s
      end

      def public_base_url
        ConsoleEnv["PUBLIC_URL"].presence || request.base_url
      end

      def render_record_invalid(error)
        render_error(status: :unprocessable_entity, message: "validation failed",
                     details: error.record.errors.as_json)
      end

      def metadata_params
        metadata = data_params[:metadata]
        return {} if metadata.blank?
        return metadata.to_unsafe_h if metadata.respond_to?(:to_unsafe_h)
        return metadata if metadata.is_a?(Hash)

        metadata
      end
    end
  end
end
