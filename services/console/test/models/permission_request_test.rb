require "test_helper"

class PermissionRequestTest < ActiveSupport::TestCase
  setup do
    @proxy = proxies(:acme_proxy)
    @principal = @proxy.principal
  end

  test "normalizes Slack channel request channel IDs" do
    request = build_request(
      metadata: { "requested_channel_ids" => [ " c0123456789 ", "C0123456789", "g9876543210" ] }
    )

    assert request.valid?
    assert_equal %w[C0123456789 G9876543210], request.requested_channel_ids
    assert_equal({ "requested_channel_ids" => %w[C0123456789 G9876543210] }, request.metadata)
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

  test "rejects Slack channel request without requested channels" do
    request = build_request(metadata: { "requested_channel_ids" => [] })

    assert_not request.valid?
    assert request.errors[:metadata].any? { |message| message.include?("less than: 1") }
  end

  test "rejects text request without request text" do
    request = build_request(kind: PermissionRequest::TEXT_KIND, metadata: {})

    assert_not request.valid?
    assert request.errors[:metadata].any? { |message| message.include?("missing required properties: request") }
  end

  test "rejects metadata keys that do not belong to the request kind" do
    request = build_request(metadata: { "requested_channel_ids" => [ "C0123456789" ], "request" => "extra" })

    assert_not request.valid?
    assert request.errors[:metadata].any? { |message| message.include?("disallowed additional property") }
  end

  test "rejects non Slack channel principal" do
    proxy = proxies(:globex_proxy)
    request = build_request(
      requesting_proxy: proxy,
      requesting_principal: proxy.principal,
      requesting_slack_channel_id: "C0123456789"
    )

    assert_not request.valid?
    assert_includes request.errors[:requesting_principal], "must be a Slack channel principal"
  end

  test "approve grants requested Slack channel permissions" do
    request = build_request(metadata: { "requested_channel_ids" => [ "C1111111111", "G2222222222" ] })
    request.save!

    assert_difference -> { @principal.slack_channel_permissions.count }, 2 do
      assert request.approve!(by: users(:acme_admin))
    end

    assert request.reload.approved?
    assert_equal users(:acme_admin), request.decided_by
    assert_not_nil request.decided_at
    rows = @principal.slack_channel_permissions.order(:channel_id).where(channel_id: %w[C1111111111 G2222222222])
    assert_equal 2, rows.count
    rows.each do |permission|
      assert permission.upload_enabled
      assert permission.download_enabled
      assert permission.history_enabled
    end
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

  test "deny records decision without granting text permissions" do
    request = build_request(
      kind: PermissionRequest::TEXT_KIND,
      metadata: { "request" => "Please authorize Calendar access." }
    )
    request.save!

    assert_no_difference -> { @principal.slack_channel_permissions.count } do
      assert request.deny!(by: users(:acme_admin))
    end
    assert request.reload.denied?
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
      kind: PermissionRequest::SLACK_KIND,
      requesting_principal: @principal,
      requesting_proxy: @proxy,
      requesting_slack_channel_id: @principal.foreign_id,
      metadata: { "requested_channel_ids" => [ "C0123456789" ] }
    }.merge(overrides))
  end
end
