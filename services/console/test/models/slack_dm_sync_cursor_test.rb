require "test_helper"

class SlackDmSyncCursorTest < ActiveSupport::TestCase
  test "oauth app slugs identify a single cursor" do
    cursor = SlackDmSyncCursor.create!(oauth_app_slug: "slack-dms")

    assert_empty cursor.conversation_state
    duplicate = SlackDmSyncCursor.new(oauth_app_slug: "slack-dms")
    assert_not duplicate.valid?
    assert_includes duplicate.errors[:oauth_app_slug], "has already been taken"
  end
end
