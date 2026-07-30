module Api
  module V1
    class ProxyMppAuthorizationsController < Api::ProxyBaseController
      def create
        return render json: { retry: false } unless params[:status].to_i == 402

        principal = current_proxy.principal
        return render_error(status: :forbidden, message: "proxy is not assigned") unless principal

        slack_user_id = principal.labels["slack_user_id"]
        slack_team_id = principal.labels["slack_team_id"]
        identity = UserIdentity.find_by(provider: UserIdentity::SLACK_PROVIDER, subject: slack_user_id, team_id: slack_team_id)
        access_key = identity&.mpp_access_key
        return render_error(status: :payment_required, message: "Slack user has no active MPP wallet") unless access_key&.revoked_at.nil? && access_key.expires_at.future?

        challenge = Array(params.require(:response_headers)["Www-Authenticate"] || params[:response_headers]["WWW-Authenticate"]).find do |value|
          value.start_with?("Payment ")
        end
        return render json: { retry: false } unless challenge

        result = MppSignerClient.new.payment_credential(
          key_handle: access_key.key_handle,
          challenge: challenge,
          host: params.require(:host),
          method: params.require(:method),
          path: params.require(:path)
        )
        render json: { retry: true, headers: { "Authorization" => result.fetch("authorization") } }
      rescue ActionController::ParameterMissing => e
        render_error(status: :unprocessable_entity, message: e.message)
      rescue MppSignerClient::Error => e
        render_error(status: :bad_gateway, message: e.message)
      end
    end
  end
end
