# Verifies a sandbox entitlement and its live proxy-to-principal assignment.
# Endpoints that expose principal-scoped state must use this stronger check,
# rather than merely verifying the JWT signature.
module AssignedSandboxAuthentication
  extend ActiveSupport::Concern

  included do
    before_action :authenticate_assigned_sandbox!
  end

  private

  attr_reader :current_proxy, :sandbox_claims

  def current_sandbox_principal
    current_proxy.principal
  end

  def authenticate_assigned_sandbox!
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
