module Api
  module V1
    class SandboxAutorotateController < ActionController::API
      include ApiRequestSupport
      include AssignedSandboxAuthentication

      rescue_from AutorotateClient::Error, with: :render_autorotate_error

      def status
        render json: { data: autorotate_client.status }
      end

      private

      def autorotate_client
        @autorotate_client ||= AutorotateClient.new
      end

      def render_autorotate_error(error)
        status = case error.upstream_status
        when 400 then :bad_request
        when 401, 403 then :bad_gateway
        when 404 then :not_found
        when 409 then :conflict
        when 429 then :too_many_requests
        else :service_unavailable
        end
        details = error.upstream_code ? { code: error.upstream_code } : nil
        render_error(status: status, message: "Autorotate request failed", details: details)
      end
    end
  end
end
