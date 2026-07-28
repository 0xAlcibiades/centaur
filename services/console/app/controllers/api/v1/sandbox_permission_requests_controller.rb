module Api
  module V1
    class SandboxPermissionRequestsController < ActionController::API
      include ApiSandboxAuthentication

      rescue_from ActiveRecord::RecordInvalid, with: :render_record_invalid
      rescue_from PermissionRequestSlackNotifier::SlackApiError, with: :render_slack_error

      def create
        principal = current_proxy.principal
        permission_request = PermissionRequest.create!(permission_request_attributes(principal))
        notification = PermissionRequestSlackNotifier.post_approver_notification(
          permission_request,
          console_permission_request_url(permission_request.oid)
        )
        permission_request.update!(
          approver_notification_channel_id: notification.channel_id,
          approver_notification_message_ts: notification.message_ts
        )

        render status: :created, json: { data: permission_request_payload(permission_request) }
      rescue PermissionRequestSlackNotifier::SlackApiError
        permission_request&.destroy
        raise
      end

      private

      def permission_request_attributes(principal)
        attrs = data_params.permit(
          :kind,
          :requesting_slack_thread_ts,
          requested_channel_ids: [],
          services: []
        )
        {
          kind: attrs.require(:kind),
          requesting_principal: principal,
          requesting_proxy: current_proxy,
          requesting_slack_channel_id: principal.foreign_id,
          requesting_slack_thread_ts: attrs[:requesting_slack_thread_ts],
          requested_channel_ids: attrs[:requested_channel_ids],
          services: attrs[:services]
        }
      end

      def permission_request_payload(permission_request)
        {
          id: permission_request.oid,
          status: permission_request.status,
          kind: permission_request.kind,
          requesting_principal_id: permission_request.requesting_principal.oid,
          requesting_proxy_id: permission_request.requesting_proxy.oid,
          requesting_slack_channel_id: permission_request.requesting_slack_channel_id,
          requesting_slack_thread_ts: permission_request.requesting_slack_thread_ts,
          requested_channel_ids: permission_request.requested_channel_ids,
          services: permission_request.services,
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

      def render_slack_error(error)
        render_error(status: :bad_gateway, message: error.message)
      end
    end
  end
end
