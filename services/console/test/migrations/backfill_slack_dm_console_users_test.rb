require "test_helper"
require Rails.root.join("db/migrate/20260811183627_backfill_slack_dm_console_users")

class BackfillSlackDmConsoleUsersTest < ActiveSupport::TestCase
  setup do
    oauth_apps(:acme_slack).update!(client_secret: "slack-secret")
  end

  test "backfills only unambiguous Slack DM ownership evidence" do
    sso = create_principal("U4123456789", "T4123456789")
    create_identity(users(:member_user), sso)

    oauth = create_principal("U4223456789", "T4223456789", namespace: "globex")
    create_credential(users(:member_user), oauth)

    email = create_principal(
      "U4323456789",
      "T4323456789",
      slack_email: "google-member@example.com"
    )
    create_google_identity(users(:member_user), "google-member@example.com")

    combined = create_principal(
      "U4423456789",
      "T4423456789",
      slack_email: "combined@example.com"
    )
    create_identity(users(:member_user), combined)
    create_credential(users(:member_user), combined)
    create_google_identity(users(:member_user), "combined@example.com")

    ambiguous = create_principal(
      "U4523456789",
      "T4523456789",
      slack_email: "ambiguous@example.com"
    )
    create_google_identity(users(:member_user), "ambiguous@example.com")
    create_credential(users(:acme_admin), ambiguous)

    unverified = create_principal(
      "U4623456789",
      "T4623456789",
      slack_email: "unverified-migration@example.com"
    )
    create_google_identity(users(:member_user), "unverified-migration@example.com", email_verified: false)

    unmatched = create_principal("U4723456789", "T4723456789")

    existing = create_principal(
      "U4823456789",
      "T4823456789",
      slack_email: "existing@example.com",
      console_user: users(:acme_admin)
    )
    create_google_identity(users(:member_user), "existing@example.com")

    # Live callbacks repair these records as evidence is created. Clear only the
    # principals intended to simulate rows from before this migration existed.
    [ sso, oauth, email, combined, ambiguous, unverified, unmatched ].each do |principal|
      principal.update_columns(console_user_id: nil, console_user_email: nil)
    end

    BackfillSlackDmConsoleUsers.new.up

    [ sso, oauth, email, combined ].each do |principal|
      assert_equal users(:member_user), principal.reload.console_user
      assert_equal users(:member_user).email, principal.console_user_email
    end
    assert_nil ambiguous.reload.console_user
    assert_nil unverified.reload.console_user
    assert_nil unmatched.reload.console_user
    assert_equal users(:acme_admin), existing.reload.console_user
    assert_equal users(:acme_admin).email, existing.console_user_email
  end

  private

  def create_principal(
    slack_user_id,
    slack_team_id,
    namespace: "acme",
    slack_email: nil,
    console_user: nil
  )
    Principal.create!(
      namespace: namespace,
      foreign_id: "migration-#{namespace}-#{slack_team_id}-#{slack_user_id}",
      kind: "slack_dm",
      slack_user_id: slack_user_id,
      slack_team_id: slack_team_id,
      slack_email: slack_email,
      console_user: console_user,
      console_user_email: console_user&.email,
      created_by: users(:acme_admin)
    )
  end

  def create_identity(user, principal)
    user.user_identities.create!(
      provider: "slack",
      subject: principal.slack_user_id,
      team_id: principal.slack_team_id,
      email: user.email,
      email_verified: true
    )
  end

  def create_credential(user, principal)
    app = oauth_apps(:acme_slack)
    BrokerCredential.create!(
      namespace: app.credential_namespace,
      oauth_app: app,
      provider_subject: principal.slack_user_id,
      labels: { "slack_team_id" => principal.slack_team_id },
      token_endpoint: app.provider_strategy.token_endpoint,
      refresh_token: "refresh-#{principal.slack_user_id}",
      access_token: "access-#{principal.slack_user_id}",
      expires_at: 1.hour.from_now,
      last_refresh: Time.current,
      external_user_key: "user-#{principal.slack_user_id}",
      created_by: user
    )
  end

  def create_google_identity(user, email, email_verified: true)
    user.user_identities.create!(
      provider: "google",
      subject: "google-#{SecureRandom.hex(8)}",
      email: email,
      email_verified: email_verified
    )
  end
end
