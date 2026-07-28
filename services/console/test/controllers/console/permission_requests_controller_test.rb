require "test_helper"

module Console
  class PermissionRequestsControllerTest < ActionDispatch::IntegrationTest
    setup do
      @permission_request = PermissionRequest.create!(
        kind: PermissionRequest::SLACK_CHANNELS_KIND,
        requesting_principal: principals(:acme_channel),
        requesting_proxy: proxies(:acme_proxy),
        requesting_slack_channel_id: "C0123456789",
        requesting_slack_thread_ts: "170.123",
        requested_channel_ids: [ "C1111111111" ],
        approver_notification_channel_id: "CAPPROVERS",
        approver_notification_message_ts: "171.1"
      )
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

    test "approve grants Slack permissions and sends Slack updates" do
      sign_in users(:acme_admin)
      updated = []
      outcomes = []

      with_singleton_method(PermissionRequestSlackNotifier, :update_approver_notification, ->(request) { updated << request.oid }) do
        with_singleton_method(PermissionRequestSlackNotifier, :post_requester_outcome, ->(request) { outcomes << request.oid }) do
          assert_difference -> { principals(:acme_channel).slack_channel_permissions.count }, 1 do
            post approve_console_permission_request_url(@permission_request.oid)
          end
        end
      end

      assert_redirected_to console_permission_request_path(@permission_request.oid)
      assert_equal "Permission request approved.", flash[:notice]
      assert @permission_request.reload.approved?
      assert_equal users(:acme_admin), @permission_request.decided_by
      assert_equal [ @permission_request.oid ], updated
      assert_equal [ @permission_request.oid ], outcomes
      permission = principals(:acme_channel).slack_channel_permissions.find_by!(channel_id: "C1111111111")
      assert permission.upload_enabled
      assert permission.download_enabled
      assert permission.history_enabled
    end

    test "denies request and sends Slack updates without granting permissions" do
      sign_in users(:acme_admin)
      updated = []
      outcomes = []

      with_singleton_method(PermissionRequestSlackNotifier, :update_approver_notification, ->(request) { updated << request.oid }) do
        with_singleton_method(PermissionRequestSlackNotifier, :post_requester_outcome, ->(request) { outcomes << request.oid }) do
          assert_no_difference -> { principals(:acme_channel).slack_channel_permissions.count } do
            post deny_console_permission_request_url(@permission_request.oid)
          end
        end
      end

      assert_redirected_to console_permission_request_path(@permission_request.oid)
      assert_equal "Permission request denied.", flash[:notice]
      assert @permission_request.reload.denied?
      assert_equal [ @permission_request.oid ], updated
      assert_equal [ @permission_request.oid ], outcomes
    end

    test "duplicate decision does not resend Slack updates" do
      sign_in users(:acme_admin)
      @permission_request.approve!(by: users(:acme_admin))

      with_singleton_method(PermissionRequestSlackNotifier, :update_approver_notification, ->(_request) { raise "should not notify" }) do
        with_singleton_method(PermissionRequestSlackNotifier, :post_requester_outcome, ->(_request) { raise "should not notify" }) do
          post approve_console_permission_request_url(@permission_request.oid)
        end
      end

      assert_redirected_to console_permission_request_path(@permission_request.oid)
      assert_equal "Permission request was already decided.", flash[:notice]
    end

    test "service approval records decision without granting Slack permissions" do
      service_request = PermissionRequest.create!(
        kind: PermissionRequest::SERVICES_KIND,
        requesting_principal: principals(:acme_channel),
        requesting_proxy: proxies(:acme_proxy),
        requesting_slack_channel_id: "C0123456789",
        services: [ "gmail" ],
        approver_notification_channel_id: "CAPPROVERS",
        approver_notification_message_ts: "171.2"
      )
      sign_in users(:acme_admin)

      with_singleton_method(PermissionRequestSlackNotifier, :update_approver_notification, ->(_request) { }) do
        with_singleton_method(PermissionRequestSlackNotifier, :post_requester_outcome, ->(_request) { }) do
          assert_no_difference -> { principals(:acme_channel).slack_channel_permissions.count } do
            post approve_console_permission_request_url(service_request.oid)
          end
        end
      end

      assert service_request.reload.approved?
    end

    test "approval keeps decision when Slack notification fails" do
      sign_in users(:acme_admin)

      with_singleton_method(
        PermissionRequestSlackNotifier,
        :update_approver_notification,
        ->(_request) { raise PermissionRequestSlackNotifier::SlackApiError, "rate_limited" }
      ) do
        post approve_console_permission_request_url(@permission_request.oid)
      end

      assert_redirected_to console_permission_request_path(@permission_request.oid)
      assert @permission_request.reload.approved?
      assert_equal "Permission request approved, but Slack notification failed: rate_limited", flash[:alert]
    end

    private

    def sign_in(user)
      post login_url, params: { email: user.email, password: "password123456" }
    end

    def with_singleton_method(target, method_name, implementation)
      singleton = class << target; self; end
      original = target.method(method_name)
      singleton.define_method(method_name, implementation)
      yield
    ensure
      singleton.define_method(method_name, original)
    end
  end
end
