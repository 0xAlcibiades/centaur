require "test_helper"

class PermissionRequestTest < ActiveSupport::TestCase
  setup do
    @proxy = proxies(:acme_proxy)
    @principal = @proxy.principal
  end

  test "rejects text request with only whitespace" do
    request = build_request(kind: PermissionRequest::TEXT_KIND, metadata: { "request" => "   " })

    assert_not request.valid?
    assert request.errors[:metadata].any? { |message| message.include?("pattern") }
  end

  test "rejects text request without request text" do
    request = build_request(kind: PermissionRequest::TEXT_KIND, metadata: {})

    assert_not request.valid?
    assert request.errors[:metadata].any? { |message| message.include?("missing required properties: request") }
  end

  test "rejects metadata keys that do not belong to the request kind" do
    request = build_request(metadata: { "request" => "Please authorize Gmail.", "requested_channel_ids" => [ "C0123456789" ] })

    assert_not request.valid?
    assert request.errors[:metadata].any? { |message| message.include?("disallowed additional property") }
  end

  test "approve records decision without granting permissions" do
    request = build_request
    request.save!

    assert_no_difference -> { @principal.slack_channel_permissions.count } do
      assert request.approve!(by: users(:acme_admin))
    end

    assert request.reload.approved?
    assert_equal users(:acme_admin), request.decided_by
    assert_not_nil request.decided_at
  end

  test "approve is idempotent once decided" do
    request = build_request
    request.save!

    assert request.approve!(by: users(:acme_admin))
    assert_no_difference -> { @principal.slack_channel_permissions.count } do
      assert_not request.approve!(by: users(:globex_admin))
    end
    assert_equal users(:acme_admin), request.reload.decided_by
  end

  test "stores requester ids without requiring proxy retention" do
    request = build_request
    request.save!

    assert_equal @principal, request.requesting_principal
    assert_equal @proxy.id, request.requesting_proxy_id

    assert_nothing_raised { @proxy.destroy! }
    assert request.reload.approve!(by: users(:acme_admin))
  end

  private

  def build_request(overrides = {})
    PermissionRequest.new({
      kind: PermissionRequest::TEXT_KIND,
      requesting_principal: @principal,
      requesting_proxy: @proxy,
      requesting_slack_channel_id: @principal.labels.fetch(Principal::SLACK_CHANNEL_ID_LABEL),
      metadata: { "request" => "Please authorize Gmail." }
    }.merge(overrides))
  end
end
