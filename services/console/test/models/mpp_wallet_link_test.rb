require "test_helper"

class MppWalletLinkTest < ActiveSupport::TestCase
  test "issues a single-use hashed connection token" do
    link = MppWalletLink.create!(
      user_identity: user_identities(:pending_user_slack),
      key_handle: "key-1",
      access_key_address: "0x1111111111111111111111111111111111111111",
      access_key_public_key: "0x04abc"
    )

    assert link.token.present?
    assert_not_equal link.token, link.token_digest
    assert_equal link, MppWalletLink.find_active(link.token)

    link.update!(used_at: Time.current)
    assert_nil MppWalletLink.find_active(link.token)
  end
end
