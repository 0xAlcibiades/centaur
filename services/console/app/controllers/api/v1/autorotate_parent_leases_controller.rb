module Api
  module V1
    # Internal control-plane API; it never serializes a runtime credential.
    class AutorotateParentLeasesController < Api::BaseController
      before_action { response.headers["Cache-Control"] = "no-store" }

      rescue_from Autorotate::ParentLeaseService::Unavailable, with: :render_unavailable
      rescue_from Autorotate::ParentLeaseService::StaleLease, with: :render_stale
      rescue_from Autorotate::ExecutionPinService::Unavailable, with: :render_unavailable
      rescue_from Autorotate::ExecutionPinService::InvalidExecution, with: :render_pin_invalid
      rescue_from AutorotateProxyParentClient::Error, with: :render_unavailable

      def create_pin
        pin = pin_service.create!(operation_id: operation_id, execution_id: data_params.require(:execution_id))
        render json: { data: pin_payload(pin) }
      end

      def release_pin
        pin_service.release!(pin_oid: params.require(:id))
        head :no_content
      end

      def heartbeat_pin
        pin = pin_service.heartbeat!(pin_oid: params.require(:id), operation_id: operation_id)
        render json: { data: pin_payload(pin) }
      end

      def quota_exhausted_pin
        service.begin_drain_from_pin!(pin_oid: params.require(:id), operation_id: operation_id)
        head :no_content
      end

      private

      def service = @service ||= Autorotate::ParentLeaseService.new
      def pin_service = @pin_service ||= Autorotate::ExecutionPinService.new
      def operation_id = data_params.require(:operation_id).to_s

      def pin_payload(pin)
        { pin_id: pin.oid, version_id: pin.credential_version.oid, expires_at: pin.expires_at.utc.iso8601 }
      end

      def render_unavailable
        render_error(status: :service_unavailable, message: "Autorotate runtime credential is unavailable")
      end

      def render_stale
        render_error(status: :conflict, message: "Autorotate parent lease is stale")
      end

      def render_pin_invalid
        render_error(status: :conflict, message: "Autorotate execution pin conflicts with prior request")
      end
    end
  end
end
