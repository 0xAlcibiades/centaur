require "test_helper"

class PrincipalConsoleUserReconciliationTest < ActiveSupport::TestCase
  setup do
    oauth_apps(:acme_slack).update!(client_secret: "slack-secret")
  end

  test "links a new Slack DM principal through an existing Slack SSO identity" do
    user = users(:member_user)
    create_slack_identity(user:, slack_user_id: "U2123456789", slack_team_id: "T2123456789")

    principal = create_slack_dm_principal(
      slack_user_id: "U2123456789",
      slack_team_id: "T2123456789"
    )

    assert_equal user, principal.console_user
    assert_equal user.email, principal.console_user_email
  end

  test "links an existing Slack DM principal when its Slack SSO identity arrives" do
    user = users(:member_user)
    principal = create_slack_dm_principal(
      slack_user_id: "U2223456789",
      slack_team_id: "T2223456789"
    )

    assert_nil principal.console_user
    create_slack_identity(user:, slack_user_id: "U2223456789", slack_team_id: "T2223456789")

    assert_equal user, principal.reload.console_user
    assert_equal user.email, principal.console_user_email
  end

  test "links when an existing Slack SSO identity gains its team" do
    user = users(:member_user)
    principal = create_slack_dm_principal(
      slack_user_id: "U3323456789",
      slack_team_id: "T3323456789"
    )
    identity = create_slack_identity(
      user: user,
      slack_user_id: "U3323456789",
      slack_team_id: nil
    )

    assert_nil principal.reload.console_user
    identity.update!(team_id: "T3323456789")

    assert_equal user, principal.reload.console_user
  end

  test "links a new Slack DM principal through an existing owned Slack OAuth credential" do
    user = users(:member_user)
    create_slack_credential(
      user:,
      slack_user_id: "U2323456789",
      slack_team_id: "T2323456789"
    )

    principal = create_slack_dm_principal(
      slack_user_id: "U2323456789",
      slack_team_id: "T2323456789",
      namespace: "globex"
    )

    assert_equal user, principal.console_user
    assert_equal user.email, principal.console_user_email
  end

  test "links an existing Slack DM principal when an owned Slack OAuth credential arrives" do
    user = users(:member_user)
    principal = create_slack_dm_principal(
      slack_user_id: "U2423456789",
      slack_team_id: "T2423456789"
    )

    assert_nil principal.console_user
    create_slack_credential(
      user:,
      slack_user_id: "U2423456789",
      slack_team_id: "T2423456789"
    )

    assert_equal user, principal.reload.console_user
  end

  test "links when an owned Slack OAuth credential gains its team" do
    user = users(:member_user)
    principal = create_slack_dm_principal(
      slack_user_id: "U3423456789",
      slack_team_id: "T3423456789"
    )
    credential = create_slack_credential(
      user: user,
      slack_user_id: "U3423456789",
      slack_team_id: nil
    )

    assert_nil principal.reload.console_user
    credential.update!(labels: { "slack_team_id" => "T3423456789" })

    assert_equal user, principal.reload.console_user
  end

  test "deduplicates Slack and verified email evidence owned by the same user" do
    user = users(:member_user)
    create_slack_identity(user:, slack_user_id: "U2523456789", slack_team_id: "T2523456789")
    create_slack_credential(user:, slack_user_id: "U2523456789", slack_team_id: "T2523456789")
    create_google_identity(user:, email: "same-user@example.com")

    principal = create_slack_dm_principal(
      slack_user_id: "U2523456789",
      slack_team_id: "T2523456789",
      slack_email: "same-user@example.com"
    )

    assert_equal user, principal.console_user
  end

  test "does not link when authenticated sources resolve to different users" do
    create_google_identity(user: users(:member_user), email: "conflict@example.com")
    create_slack_credential(
      user: users(:acme_admin),
      slack_user_id: "U2623456789",
      slack_team_id: "T2623456789"
    )

    principal = create_slack_dm_principal(
      slack_user_id: "U2623456789",
      slack_team_id: "T2623456789",
      slack_email: "conflict@example.com"
    )

    assert_nil principal.console_user
    assert_nil principal.console_user_email
  end

  test "links through a Slack profile email matching a verified Google identity" do
    user = users(:member_user)
    create_google_identity(user:, email: "Person@Example.com")

    principal = create_slack_dm_principal(
      slack_user_id: "U2723456789",
      slack_team_id: "T2723456789",
      slack_email: "PERSON@EXAMPLE.COM"
    )

    assert_equal user, principal.console_user
    assert_equal user.email, principal.console_user_email
  end

  test "links an existing Slack DM when a verified Google identity arrives" do
    user = users(:member_user)
    principal = create_slack_dm_principal(
      slack_user_id: "U3523456789",
      slack_team_id: "T3523456789",
      slack_email: "late-google@example.com"
    )

    assert_nil principal.console_user
    create_google_identity(user:, email: "late-google@example.com")

    assert_equal user, principal.reload.console_user
  end

  test "waits for an identity email to become verified" do
    user = users(:member_user)
    identity = create_google_identity(user:, email: "unverified@example.com", email_verified: false)

    principal = create_slack_dm_principal(
      slack_user_id: "U3623456789",
      slack_team_id: "T3623456789",
      slack_email: "unverified@example.com"
    )

    assert_nil principal.console_user
    identity.update!(email_verified: true)

    assert_equal user, principal.reload.console_user
  end

  test "does not link when a verified email belongs to multiple users" do
    create_google_identity(user: users(:member_user), email: "shared@example.com")
    create_google_identity(user: users(:acme_admin), email: "shared@example.com")

    principal = create_slack_dm_principal(
      slack_user_id: "U3723456789",
      slack_team_id: "T3723456789",
      slack_email: "shared@example.com"
    )

    assert_nil principal.console_user
  end

  test "ignores incomplete or malformed Slack identities" do
    reconciliation = PrincipalConsoleUserReconciliation.new
    principals = [
      Principal.new(kind: "slack_dm", slack_user_id: "U2823456789"),
      Principal.new(kind: "slack_dm", slack_team_id: "T2823456789"),
      Principal.new(kind: "slack_dm", slack_user_id: "not-a-user", slack_team_id: "T2823456789")
    ]

    principals.each do |principal|
      refute reconciliation.assign_for_principal(principal)
      assert_nil principal.console_user
    end
  end

  test "does not link non-DM principals" do
    user = users(:member_user)
    create_slack_identity(user:, slack_user_id: "U2923456789", slack_team_id: "T2923456789")

    principal = Principal.create!(
      namespace: "acme",
      foreign_id: "ordinary-slack-user",
      kind: "user",
      slack_user_id: "U2923456789",
      slack_team_id: "T2923456789",
      created_by: users(:acme_admin)
    )

    assert_nil principal.console_user
  end

  test "preserves an existing link even when current evidence points elsewhere" do
    existing_user = users(:acme_admin)
    create_slack_identity(
      user: users(:member_user),
      slack_user_id: "U3023456789",
      slack_team_id: "T3023456789"
    )

    principal = create_slack_dm_principal(
      slack_user_id: "U3023456789",
      slack_team_id: "T3023456789",
      console_user: existing_user
    )

    assert_equal existing_user, principal.console_user
    assert_equal existing_user.email, principal.console_user_email
  end

  test "source callback failures do not abort SSO identity or credential writes" do
    failure = proc { raise "boom" }

    PrincipalConsoleUserReconciliation.stub(:new, failure) do
      assert_difference("UserIdentity.count", 1) do
        create_google_identity(user: users(:member_user), email: "failure@example.com")
      end

      assert_difference("BrokerCredential.count", 1) do
        create_slack_credential(
          user: users(:member_user),
          slack_user_id: "U3223456789",
          slack_team_id: "T3223456789"
        )
      end
    end
  end

  private

  def create_slack_identity(user:, slack_user_id:, slack_team_id:)
    user.user_identities.create!(
      provider: "slack",
      subject: slack_user_id,
      team_id: slack_team_id,
      email: user.email,
      email_verified: true
    )
  end

  def create_google_identity(user:, email:, email_verified: true)
    user.user_identities.create!(
      provider: "google",
      subject: "google-#{SecureRandom.hex(8)}",
      email: email,
      email_verified: email_verified
    )
  end

  def create_slack_credential(user:, slack_user_id:, slack_team_id:)
    app = oauth_apps(:acme_slack)
    BrokerCredential.create!(
      namespace: app.credential_namespace,
      oauth_app: app,
      provider_subject: slack_user_id,
      labels: slack_team_id ? { "slack_team_id" => slack_team_id } : {},
      token_endpoint: app.provider_strategy.token_endpoint,
      refresh_token: "refresh-#{slack_user_id}",
      access_token: "access-#{slack_user_id}",
      expires_at: 1.hour.from_now,
      last_refresh: Time.current,
      external_user_key: "user-#{slack_user_id}",
      created_by: user
    )
  end

  def create_slack_dm_principal(
    slack_user_id:,
    slack_team_id:,
    namespace: "acme",
    slack_email: nil,
    console_user: nil
  )
    Principal.create!(
      namespace: namespace,
      foreign_id: "slack-user-#{namespace}-#{slack_team_id}-#{slack_user_id}",
      name: "Slack DM #{slack_user_id}",
      kind: "slack_dm",
      slack_user_id: slack_user_id,
      slack_team_id: slack_team_id,
      slack_email: slack_email,
      console_user: console_user,
      console_user_email: console_user&.email,
      created_by: users(:acme_admin)
    )
  end
end
