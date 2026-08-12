require "test_helper"

module SlackDm
  class SyncCredentialTest < ActiveSupport::TestCase
    test "oauth app slug defaults to slack and honors configuration" do
      with_env("CENTAUR_CONSOLE_SLACK_DM_SYNC_OAUTH_APP_SLUG" => nil) do
        assert_equal "slack", SlackDm::SyncCredential.oauth_app_slug
      end
      with_env("CENTAUR_CONSOLE_SLACK_DM_SYNC_OAUTH_APP_SLUG" => "custom-slack") do
        assert_equal "custom-slack", SlackDm::SyncCredential.oauth_app_slug
      end
    end

    test "supported conversation types follow granted scope pairs" do
      assert_equal [ "im" ], SlackDm::SyncCredential.supported_conversation_types(%w[im:read im:history])
      assert_equal %w[mpim private_channel], SlackDm::SyncCredential.supported_conversation_types(
        %w[mpim:read mpim:history groups:read groups:history]
      )
      refute SlackDm::SyncCredential.required_scopes_granted?(%w[im:read])
    end
  end
end
