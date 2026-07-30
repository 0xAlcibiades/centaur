module Console
  class MppWalletsController < ApplicationController
    def create
      identity = current_user.user_identities.find_by(provider: UserIdentity::SLACK_PROVIDER)
      return render json: { error: "connect Slack SSO before configuring an MPP wallet" }, status: :unprocessable_entity unless identity

      key = MppSignerClient.new.create_access_key
      link = MppWalletLink.create!(
        user_identity: identity,
        key_handle: key.fetch("key_handle"),
        access_key_address: key.fetch("access_key_address"),
        access_key_public_key: key.fetch("access_key_public_key")
      )
      render json: { connect_url: mpp_wallet_link_url(link.token), expires_at: link.expires_at }
    rescue MppSignerClient::Error => e
      render json: { error: e.message }, status: :bad_gateway
    end

    def destroy
      identity = current_user.user_identities.find_by(provider: UserIdentity::SLACK_PROVIDER)
      identity&.mpp_access_key&.update!(revoked_at: Time.current)
      head :no_content
    end
  end
end
