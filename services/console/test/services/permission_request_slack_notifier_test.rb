require "test_helper"

class PermissionRequestSlackNotifierTest < ActiveSupport::TestCase
  test "request summaries escape Slack mrkdwn control characters" do
    permission_request = PermissionRequest.new(
      kind: PermissionRequest::TEXT_KIND,
      requesting_slack_channel_id: "C0123456789",
      metadata: { "request" => "Please authorize <!channel> and safe&sound." },
      status: "approved"
    )

    text = PermissionRequestSlackNotifier.requester_outcome_text(permission_request)

    assert_includes text, "```Please authorize &lt;!channel&gt; and safe&amp;sound.```"
    assert_includes text, "safe&amp;sound"
    refute_includes text, "<!channel>"
  end

  test "approver notification renders request as a code block" do
    permission_request = PermissionRequest.new(
      kind: PermissionRequest::TEXT_KIND,
      requesting_slack_channel_id: "C0123456789",
      metadata: { "request" => "Please authorize Gmail." }
    )

    text = PermissionRequestSlackNotifier.approver_notification_text(permission_request, "https://console.test/request")

    assert_includes text, "Requester: <#C0123456789>"
    assert_includes text, "Request:\n```Please authorize Gmail.```"
    assert_includes text, "Review in Console: <https://console.test/request|Open request>"
  end

  test "decided approver notification links requester channel" do
    permission_request = PermissionRequest.new(
      kind: PermissionRequest::TEXT_KIND,
      requesting_slack_channel_id: "C0123456789",
      metadata: { "request" => "Please authorize Gmail." },
      status: "approved",
      decided_by: users(:acme_admin),
      decided_at: Time.utc(2026, 7, 28, 20, 0, 0)
    )

    text = PermissionRequestSlackNotifier.decided_approver_notification_text(permission_request)

    assert_includes text, "Requester: <#C0123456789>"
  end

  test "code block formatting prevents embedded triple backtick breakout" do
    permission_request = PermissionRequest.new(
      kind: PermissionRequest::TEXT_KIND,
      requesting_slack_channel_id: "C0123456789",
      metadata: { "request" => "Need ``` admin access" },
      status: "denied"
    )

    text = PermissionRequestSlackNotifier.requester_outcome_text(permission_request)

    assert_includes text, "```Need ` ` ` admin access```"
    refute_includes text, "Need ``` admin access"
  end
end
