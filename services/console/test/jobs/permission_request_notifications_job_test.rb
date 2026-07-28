require "test_helper"

class PermissionRequestNotificationsJobTest < ActiveJob::TestCase
  setup do
    @permission_request = PermissionRequest.create!(
      kind: PermissionRequest::SLACK_CHANNELS_KIND,
      requesting_principal: principals(:acme_channel),
      requesting_proxy: proxies(:acme_proxy),
      requesting_slack_channel_id: "C0123456789",
      requesting_slack_thread_ts: "170.123",
      requested_channel_ids: [ "C1111111111" ]
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
    assert_nil @permission_request.approver_notification_last_error
  end

  test "approver notification failure is persisted before retry" do
    error = PermissionRequestSlackNotifier::SlackApiError.new("timeout")

    PermissionRequestSlackNotifier.stub(:post_approver_notification, ->(_request, _url) { raise error }) do
      assert_enqueued_jobs 1, only: PermissionRequestApproverNotificationJob do
        PermissionRequestApproverNotificationJob.perform_now(@permission_request.id, "https://console.test/request")
      end
    end

    assert_equal "failed", @permission_request.reload.approver_notification_status
    assert_match "timeout", @permission_request.approver_notification_last_error
  end

  test "decision notification updates approver message and requester outcome once" do
    @permission_request.update!(
      status: "approved",
      decided_by: users(:acme_admin),
      decided_at: Time.current,
      approver_notification_status: "sent",
      approver_notification_channel_id: "CAPPROVERS",
      approver_notification_message_ts: "171.1"
    )
    update_result = PermissionRequestSlackNotifier::Result.new(channel_id: "CAPPROVERS", message_ts: "171.1")
    outcome_result = PermissionRequestSlackNotifier::Result.new(channel_id: "C0123456789", message_ts: "172.1")

    PermissionRequestSlackNotifier.stub(:update_approver_notification, update_result) do
      PermissionRequestSlackNotifier.stub(:post_requester_outcome, outcome_result) do
        PermissionRequestDecisionNotificationJob.perform_now(@permission_request.id)
      end
    end

    @permission_request.reload
    assert_equal "sent", @permission_request.approver_decision_update_status
    assert_equal "sent", @permission_request.requester_outcome_notification_status
    assert_equal "172.1", @permission_request.requester_outcome_message_ts
  end

  test "decision notification skips approver update without approver message but posts requester outcome" do
    @permission_request.update!(
      status: "denied",
      decided_by: users(:acme_admin),
      decided_at: Time.current,
      approver_notification_status: "skipped"
    )
    outcome_result = PermissionRequestSlackNotifier::Result.new(channel_id: "C0123456789", message_ts: "172.1")

    PermissionRequestSlackNotifier.stub(:update_approver_notification, ->(_request) { raise "should not update" }) do
      PermissionRequestSlackNotifier.stub(:post_requester_outcome, outcome_result) do
        PermissionRequestDecisionNotificationJob.perform_now(@permission_request.id)
      end
    end

    @permission_request.reload
    assert_equal "skipped", @permission_request.approver_decision_update_status
    assert_equal "sent", @permission_request.requester_outcome_notification_status
  end

  test "decision notification failure is persisted and retry skips already sent update" do
    @permission_request.update!(
      status: "approved",
      decided_by: users(:acme_admin),
      decided_at: Time.current,
      approver_notification_status: "sent",
      approver_notification_channel_id: "CAPPROVERS",
      approver_notification_message_ts: "171.1"
    )
    update_calls = 0
    error = PermissionRequestSlackNotifier::SlackApiError.new("rate_limited")

    PermissionRequestSlackNotifier.stub(:update_approver_notification, ->(_request) {
      update_calls += 1
      PermissionRequestSlackNotifier::Result.new(channel_id: "CAPPROVERS", message_ts: "171.1")
    }) do
      PermissionRequestSlackNotifier.stub(:post_requester_outcome, ->(_request) { raise error }) do
        assert_enqueued_jobs 1, only: PermissionRequestDecisionNotificationJob do
          PermissionRequestDecisionNotificationJob.perform_now(@permission_request.id)
        end
      end
    end

    @permission_request.reload
    assert_equal 1, update_calls
    assert_equal "sent", @permission_request.approver_decision_update_status
    assert_equal "failed", @permission_request.requester_outcome_notification_status
    assert_match "rate_limited", @permission_request.requester_outcome_notification_last_error

    outcome_result = PermissionRequestSlackNotifier::Result.new(channel_id: "C0123456789", message_ts: "172.1")
    PermissionRequestSlackNotifier.stub(:update_approver_notification, ->(_request) { raise "should not repeat update" }) do
      PermissionRequestSlackNotifier.stub(:post_requester_outcome, outcome_result) do
        PermissionRequestDecisionNotificationJob.perform_now(@permission_request.id)
      end
    end

    assert_equal "sent", @permission_request.reload.requester_outcome_notification_status
  end
end
