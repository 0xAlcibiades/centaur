require "test_helper"

module Console
  class PermissionRequestsControllerTest < ActionDispatch::IntegrationTest
    include ActiveJob::TestHelper

    setup do
      @permission_request = PermissionRequest.create!(
        kind: PermissionRequest::SLACK_KIND,
        requesting_principal: principals(:acme_channel),
        requesting_proxy: proxies(:acme_proxy),
        requesting_slack_channel_id: "C0123456789",
        requesting_slack_thread_ts: "170.123",
        metadata: { "requested_channel_ids" => [ "C1111111111" ] },
        approver_notification_channel_id: "CAPPROVERS",
        approver_notification_message_ts: "171.1"
      )
    end

    teardown do
      clear_enqueued_jobs
      clear_performed_jobs
    end

    test "redirects signed out users to login" do
      get console_permission_request_url(@permission_request.oid)

      assert_redirected_to login_path
    end

    test "redirects non-admin users away" do
      sign_in users(:member_user)

      get console_permission_request_url(@permission_request.oid)

      assert_redirected_to console_threads_path
    end

    test "approve grants Slack permissions and enqueues Slack updates" do
      sign_in users(:acme_admin)

      assert_enqueued_jobs 1, only: PermissionRequestDecisionNotificationJob do
        assert_difference -> { principals(:acme_channel).slack_channel_permissions.count }, 1 do
          post approve_console_permission_request_url(@permission_request.oid)
        end
      end

      assert_redirected_to console_permission_request_path(@permission_request.oid)
      assert_equal "Permission request approved.", flash[:notice]
      assert @permission_request.reload.approved?
      assert_equal users(:acme_admin), @permission_request.decided_by
      permission = principals(:acme_channel).slack_channel_permissions.find_by!(channel_id: "C1111111111")
      assert permission.upload_enabled
      assert permission.download_enabled
      assert permission.history_enabled
    end

    test "denies request and enqueues Slack updates without granting permissions" do
      sign_in users(:acme_admin)

      assert_enqueued_jobs 1, only: PermissionRequestDecisionNotificationJob do
        assert_no_difference -> { principals(:acme_channel).slack_channel_permissions.count } do
          post deny_console_permission_request_url(@permission_request.oid)
        end
      end

      assert_redirected_to console_permission_request_path(@permission_request.oid)
      assert_equal "Permission request denied.", flash[:notice]
      assert @permission_request.reload.denied?
    end

    test "duplicate decision requeues pending Slack updates" do
      sign_in users(:acme_admin)
      @permission_request.approve!(by: users(:acme_admin))

      assert_enqueued_jobs 1, only: PermissionRequestDecisionNotificationJob do
        post approve_console_permission_request_url(@permission_request.oid)
      end

      assert_redirected_to console_permission_request_path(@permission_request.oid)
      assert_equal "Permission request was already decided.", flash[:notice]
    end

    test "duplicate decision does not requeue completed Slack updates" do
      sign_in users(:acme_admin)
      @permission_request.approve!(by: users(:acme_admin))
      @permission_request.update!(
        approver_decision_update_status: "sent",
        requester_outcome_notification_status: "sent"
      )

      assert_no_enqueued_jobs only: PermissionRequestDecisionNotificationJob do
        post approve_console_permission_request_url(@permission_request.oid)
      end
    end

    test "text approval records decision without granting Slack permissions" do
      text_request = PermissionRequest.create!(
        kind: PermissionRequest::TEXT_KIND,
        requesting_principal: principals(:acme_channel),
        requesting_proxy: proxies(:acme_proxy),
        requesting_slack_channel_id: "C0123456789",
        metadata: { "request" => "Please authorize Gmail." },
        approver_notification_channel_id: "CAPPROVERS",
        approver_notification_message_ts: "171.2"
      )
      sign_in users(:acme_admin)

      assert_enqueued_jobs 1, only: PermissionRequestDecisionNotificationJob do
        assert_no_difference -> { principals(:acme_channel).slack_channel_permissions.count } do
          post approve_console_permission_request_url(text_request.oid)
        end
      end

      assert text_request.reload.approved?
    end

    private

    def sign_in(user)
      post login_url, params: { email: user.email, password: "password123456" }
    end
  end
end
