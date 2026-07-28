require "test_helper"

class PermissionRequestNotificationsJobTest < ActiveJob::TestCase
  setup do
    @permission_request = PermissionRequest.create!(
      kind: PermissionRequest::TEXT_KIND,
      requesting_principal: principals(:acme_channel),
      requesting_proxy: proxies(:acme_proxy),
      requesting_slack_channel_id: "C0123456789",
      requesting_slack_thread_ts: "170.123",
      metadata: { "request" => "Please authorize Gmail." }
    )
  end

  test "approver notification records Slack message metadata" do
    result = PermissionRequestSlackNotifier::Result.new(channel_id: "CAPPROVERS", message_ts: "171.1")

    PermissionRequestSlackNotifier.stub(:post_approver_notification, result) do
      PermissionRequestApproverNotificationJob.perform_now(@permission_request.id, "https://console.test/request")
    end

    assert_equal "sent", @permission_request.reload.approver_notification_status
    assert_equal "CAPPROVERS", @permission_request.approver_notification_channel_id
    assert_equal "171.1", @permission_request.approver_notification_message_ts
  end

  test "approver notification failure is persisted before retry" do
    error = PermissionRequestSlackNotifier::SlackApiError.new("timeout")

    PermissionRequestSlackNotifier.stub(:post_approver_notification, ->(_request, _url) { raise error }) do
      assert_enqueued_jobs 1, only: PermissionRequestApproverNotificationJob do
        PermissionRequestApproverNotificationJob.perform_now(@permission_request.id, "https://console.test/request")
      end
    end

    assert_equal "failed", @permission_request.reload.approver_notification_status
  end

  test "approver decision update records Slack update" do
    @permission_request.update!(
      status: "approved",
      decided_by: users(:acme_admin),
      decided_at: Time.current,
      approver_notification_status: "sent",
      approver_notification_channel_id: "CAPPROVERS",
      approver_notification_message_ts: "171.1"
    )
    update_result = PermissionRequestSlackNotifier::Result.new(channel_id: "CAPPROVERS", message_ts: "171.1")

    PermissionRequestSlackNotifier.stub(:update_approver_notification, update_result) do
      PermissionRequestApproverDecisionUpdateJob.perform_now(@permission_request.id)
    end

    assert_equal "sent", @permission_request.reload.approver_decision_update_status
  end

  test "requester outcome failure is persisted before retry" do
    @permission_request.update!(
      status: "approved",
      decided_by: users(:acme_admin),
      decided_at: Time.current
    )
    error = PermissionRequestSlackNotifier::SlackApiError.new("rate_limited")

    PermissionRequestSlackNotifier.stub(:post_requester_outcome, ->(_request) { raise error }) do
      assert_enqueued_jobs 1, only: PermissionRequestRequesterOutcomeNotificationJob do
        PermissionRequestRequesterOutcomeNotificationJob.perform_now(@permission_request.id)
      end
    end

    assert_equal "failed", @permission_request.reload.requester_outcome_notification_status
  end

  test "requester outcome records Slack message metadata" do
    @permission_request.update!(
      status: "approved",
      decided_by: users(:acme_admin),
      decided_at: Time.current
    )
    outcome_result = PermissionRequestSlackNotifier::Result.new(channel_id: "C0123456789", message_ts: "172.1")

    PermissionRequestSlackNotifier.stub(:post_requester_outcome, outcome_result) do
      PermissionRequestRequesterOutcomeNotificationJob.perform_now(@permission_request.id)
    end

    @permission_request.reload
    assert_equal "sent", @permission_request.requester_outcome_notification_status
    assert_equal "172.1", @permission_request.requester_outcome_message_ts
  end
end
