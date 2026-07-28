require "test_helper"

class PermissionRequestTest < ActiveSupport::TestCase
  setup do
    @proxy = proxies(:acme_proxy)
    @principal = @proxy.principal
  end

  test "normalizes text request metadata" do
    request = build_request(
      kind: PermissionRequest::TEXT_KIND,
      metadata: { "request" => "  Please authorize Google Drive for quarterly reporting.  " }
    )

    assert request.valid?
    assert_equal "Please authorize Google Drive for quarterly reporting.", request.text_request
    assert_equal({ "request" => "Please authorize Google Drive for quarterly reporting." }, request.metadata)
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

  test "stores copied audit fields and does not block principal or proxy deletion" do
    request = build_request
    request.save!

    assert_equal @principal.oid, request.requesting_principal_oid
    assert_equal @principal.foreign_id, request.requesting_principal_name
    assert_equal @proxy.oid, request.requesting_proxy_oid
    assert_equal @proxy.name, request.requesting_proxy_name

    assert_nothing_raised { @proxy.destroy! }
    assert_nothing_raised { @principal.destroy! }
    assert_equal @principal.oid, request.reload.requesting_principal_oid
    assert_equal @proxy.oid, request.requesting_proxy_oid
  end

  private

  def build_request(overrides = {})
    PermissionRequest.new({
      kind: PermissionRequest::TEXT_KIND,
      requesting_principal: @principal,
      requesting_proxy: @proxy,
      requesting_slack_channel_id: @principal.foreign_id,
      metadata: { "request" => "Please authorize Gmail." }
    }.merge(overrides))
  end
end
