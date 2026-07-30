require "test_helper"

class ProxyMppAuthorizationsControllerTest < ActionDispatch::IntegrationTest
  TOKEN = "iprx_#{'b' * 64}".freeze

  test "selects the access key from authenticated proxy Slack labels" do
    identity = user_identities(:pending_user_slack)
    principal = Principal.create!(
      namespace: "default",
      foreign_id: "slack-user-test",
      name: "Slack user",
      labels: { "slack_user_id" => identity.subject },
      created_by: users(:acme_admin)
    )
    proxy = Proxy.create!(name: "mpp-test", principal: principal, token: TOKEN)
    key = MppAccessKey.create!(
      user_identity: identity,
      wallet_address: "0x1111111111111111111111111111111111111111",
      key_handle: "key-1",
      access_key_address: "0x2222222222222222222222222222222222222222",
      key_authorization: { "authorization" => "signed" },
      expires_at: 1.day.from_now
    )
    signer = Minitest::Mock.new
    signer.expect(
      :payment_credential,
      { "authorization" => "Payment credential" },
      [],
      key_handle: key.key_handle,
      challenge: "Payment challenge",
      host: "api.example",
      method: "POST",
      path: "/paid"
    )

    MppSignerClient.stub(:new, signer) do
      post api_v1_proxy_mpp_authorize_url,
        params: {
          status: 402,
          response_headers: { "Www-Authenticate" => [ "Payment challenge" ] },
          host: "api.example",
          method: "POST",
          path: "/paid"
        }.to_json,
        headers: { "Authorization" => "Bearer #{proxy.token}", "Content-Type" => "application/json" }
    end

    assert_response :ok
    assert_equal(
      { "retry" => true, "headers" => { "Authorization" => "Payment credential" } },
      JSON.parse(response.body)
    )
    signer.verify
  end
end
