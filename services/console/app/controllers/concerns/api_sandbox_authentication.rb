module ApiSandboxAuthentication
  extend ActiveSupport::Concern

  included do
    include ApiRequestSupport

    before_action :authenticate_sandbox_token!
  end

  private

  attr_reader :current_proxy, :sandbox_claims

  def authenticate_sandbox_token!
    token = bearer_token
    if token.blank?
      return render_error(status: :unauthorized, message: "invalid or missing sandbox token")
    end

    # KeyError (signing secret unconfigured) is deliberately not rescued:
    # that is a server fault and should surface as a 500, not a 401.
    claims = SandboxEntitlements::Jwt.decode(token)
    proxy = Proxy.find_by_oid(claims["proxy_id"])
    unless proxy&.assigned? && proxy.principal&.oid == claims["principal_id"] &&
           proxy.name == claims["sandbox_id"]
      return render_error(status: :unauthorized, message: "invalid sandbox token")
    end

    @sandbox_claims = claims
    @current_proxy = proxy
  rescue CentaurJwt::Hs256::VerificationError
    render_error(status: :unauthorized, message: "invalid or missing sandbox token")
  end
end
