require "test_helper"

class PermissionRequestSlackNotifierTest < ActiveSupport::TestCase
  test "request summaries escape Slack mrkdwn control characters" do
    permission_request = PermissionRequest.new(
      kind: PermissionRequest::SERVICES_KIND,
      requesting_slack_channel_id: "C0123456789",
      request_text: "Please authorize <!channel> and safe&sound.",
      status: "approved"
    )

    text = PermissionRequestSlackNotifier.requester_outcome_text(permission_request)

    assert_includes text, "&lt;!channel&gt;"
    assert_includes text, "safe&amp;sound"
    refute_includes text, "<!channel>"
  end
end
