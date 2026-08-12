require "test_helper"

class SlackDm::SyncLedgerTest < ActiveSupport::TestCase
  include ActiveSupport::Testing::TimeHelpers

  def setup
    app = OauthApp.create!(
      provider: "slack",
      slug: "slack-ledger-#{SecureRandom.hex(4)}",
      client_id: "client",
      client_secret: "secret",
      allowed_scopes: SlackDm::SyncCredential::REQUIRED_SCOPES,
      created_by: users(:acme_admin)
    )
    @credential = BrokerCredential.create!(
      oauth_app: app,
      foreign_id: "slack-ledger-#{SecureRandom.hex(4)}",
      token_endpoint: "https://slack.com/api/oauth.v2.access",
      access_token: "xoxp-live",
      scopes: SlackDm::SyncCredential::REQUIRED_SCOPES
    )
  end

  test "claims due rows in next sync order and skips active claims" do
    travel_to Time.zone.at(1_000), with_usec: true
    later = create_ledger("D2", next_sync_at: 2.minutes.ago)
    earlier = create_ledger("D1", next_sync_at: 3.minutes.ago)
    create_ledger("D3", next_sync_at: 4.minutes.ago, claim_token: "busy", claimed_until: 1.hour.from_now)

    claimed = SlackDm::SyncLedger.claim_due(
      limit: 2,
      broker_credential_ids: [ @credential.id ]
    )

    assert_equal [ earlier.id, later.id ], claimed.map(&:id)
    assert claimed.all? { |ledger| ledger.claim_token.present? }
    assert claimed.all? { |ledger| ledger.claimed_until == 2.hours.from_now }
  end

  test "success advances the watermark and clears retry state" do
    travel_to Time.zone.at(1_000), with_usec: true
    ledger = create_ledger(
      "D1",
      claim_token: "mine",
      claimed_until: 1.hour.from_now,
      backoff_level: 3,
      last_error: "old error"
    )

    assert ledger.complete_claim!(claim_token: "mine", watermark_ts: "123.456")

    ledger.reload
    assert_equal "123.456", ledger.watermark_ts
    assert_equal 10.minutes.from_now, ledger.next_sync_at
    assert_equal "", ledger.last_error
    assert_equal 0, ledger.backoff_level
    assert_nil ledger.claim_token
  end

  test "failure applies exponential row backoff" do
    travel_to Time.zone.at(1_000), with_usec: true
    ledger = create_ledger("D1", claim_token: "mine", claimed_until: 1.hour.from_now, backoff_level: 2)

    assert ledger.fail_claim!(claim_token: "mine", error: "rate limited")

    ledger.reload
    assert_equal 20.seconds.from_now, ledger.next_sync_at
    assert_equal "rate limited", ledger.last_error
    assert_equal 3, ledger.backoff_level
    assert_nil ledger.claim_token
  end

  test "a stale owner cannot mutate a newer claim" do
    ledger = create_ledger("D1", claim_token: "new-owner", claimed_until: 1.hour.from_now)

    refute ledger.complete_claim!(claim_token: "old-owner", watermark_ts: "123.456")
    refute ledger.fail_claim!(claim_token: "old-owner", error: "failure")

    assert_equal "new-owner", ledger.reload.claim_token
    assert_nil ledger.watermark_ts
  end

  private

  def create_ledger(conversation_id, **attributes)
    SlackDm::SyncLedger.create!(
      {
        broker_credential: @credential,
        home_team_id: "T123",
        conversation_id: conversation_id,
        conversation_type: "im",
        raw_payload: { "id" => conversation_id, "is_im" => true }
      }.merge(attributes)
    )
  end
end
