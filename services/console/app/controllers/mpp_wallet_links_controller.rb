class MppWalletLinksController < ActionController::API
  def show
    link = MppWalletLink.find_active(params[:token])
    return render json: { error: "connection link is invalid or expired" }, status: :not_found unless link

    render json: {
      access_key_address: link.access_key_address,
      access_key_public_key: link.access_key_public_key,
      expires_at: link.expires_at
    }
  end

  def update
    link = MppWalletLink.find_active(params[:token])
    return render json: { error: "connection link is invalid or expired" }, status: :not_found unless link

    result = MppSignerClient.new.authorize_access_key(
      key_handle: link.key_handle,
      wallet_address: params.require(:wallet_address),
      key_authorization: params.require(:key_authorization)
    )
    MppAccessKey.transaction do
      link.user_identity.mpp_access_key&.update!(revoked_at: Time.current)
      MppAccessKey.create!(
        user_identity: link.user_identity,
        wallet_address: result.fetch("wallet_address"),
        key_handle: link.key_handle,
        access_key_address: link.access_key_address,
        key_authorization: result.fetch("key_authorization"),
        expires_at: Time.iso8601(result.fetch("expires_at"))
      )
      link.update!(used_at: Time.current)
    end
    render json: { connected: true, wallet_address: result.fetch("wallet_address") }
  rescue ActionController::ParameterMissing => e
    render json: { error: e.message }, status: :unprocessable_entity
  rescue MppSignerClient::Error => e
    render json: { error: e.message }, status: :bad_gateway
  end
end
