# Links a Slack DM principal to the Console user who authenticated the same
# Slack workspace/user pair or whose IdP-verified email matches the email fetched
# from that Slack user's profile. Existing links are sticky and are never
# overwritten here.
class PrincipalConsoleUserReconciliation
  SLACK_PROVIDER = Oauth::Providers::Slack::KEY

  def assign_for_principal(principal)
    return false unless linkable_principal?(principal)

    user = unambiguous_user_for(
      slack_team_id: principal.slack_team_id,
      slack_user_id: principal.slack_user_id,
      slack_email: principal.slack_email
    )
    return false unless user

    principal.console_user = user
    principal.console_user_email = user.email
    true
  end

  def apply_for_principal(principal)
    return false unless assign_for_principal(principal)

    principal.save! if principal.persisted?
    true
  end

  def apply_for_user_identity(identity)
    linked = 0
    if identity.slack?
      linked += apply_for_slack_identity(
        slack_team_id: identity.team_id,
        slack_user_id: identity.subject
      )
    end
    linked += apply_for_verified_email(identity.email) if identity.email_verified?
    linked
  end

  def apply_for_slack_credential(credential)
    return 0 unless credential.created_by_id.present?
    return 0 unless credential.oauth_app&.provider == SLACK_PROVIDER

    apply_for_slack_identity(
      slack_team_id: credential.labels.to_h["slack_team_id"],
      slack_user_id: credential.provider_subject
    )
  end

  private

  def apply_for_slack_identity(slack_team_id:, slack_user_id:)
    return 0 unless valid_identity?(slack_team_id:, slack_user_id:)

    matching_unlinked_principals(slack_team_id:, slack_user_id:).count do |principal|
      apply_for_principal(principal)
    end
  end

  def apply_for_verified_email(email)
    email = normalize_email(email)
    return 0 unless email

    matching_unlinked_principals_by_email(email).count do |principal|
      apply_for_principal(principal)
    end
  end

  def linkable_principal?(principal)
    principal.console_user_id.blank? &&
      principal.kind == "slack_dm" &&
      valid_identity?(
        slack_team_id: principal.slack_team_id,
        slack_user_id: principal.slack_user_id
      )
  end

  def valid_identity?(slack_team_id:, slack_user_id:)
    Principal::SLACK_TEAM_ID_FORMAT.match?(slack_team_id.to_s) &&
      Principal::SLACK_USER_ID_FORMAT.match?(slack_user_id.to_s)
  end

  def matching_unlinked_principals(slack_team_id:, slack_user_id:)
    Principal.where(
      kind: "slack_dm",
      console_user_id: nil,
      slack_team_id: slack_team_id,
      slack_user_id: slack_user_id
    )
  end

  def matching_unlinked_principals_by_email(email)
    Principal.where(kind: "slack_dm", console_user_id: nil)
      .where("lower(trim(slack_email)) = ?", email)
  end

  def unambiguous_user_for(slack_team_id:, slack_user_id:, slack_email:)
    user_ids = sso_user_ids(slack_team_id:, slack_user_id:) |
      oauth_credential_user_ids(slack_team_id:, slack_user_id:) |
      verified_email_user_ids(slack_email)
    return unless user_ids.one?

    User.find_by(id: user_ids.first)
  end

  def sso_user_ids(slack_team_id:, slack_user_id:)
    UserIdentity.slack
      .where(team_id: slack_team_id, subject: slack_user_id)
      .pluck(:user_id)
  end

  def oauth_credential_user_ids(slack_team_id:, slack_user_id:)
    BrokerCredential.joins(:oauth_app)
      .where(oauth_apps: { provider: SLACK_PROVIDER })
      .where(provider_subject: slack_user_id)
      .where.not(created_by_id: nil)
      .where("broker_credentials.labels ->> 'slack_team_id' = ?", slack_team_id)
      .pluck(:created_by_id)
  end

  def verified_email_user_ids(email)
    email = normalize_email(email)
    return [] unless email

    UserIdentity.where(email_verified: true, email: email).pluck(:user_id)
  end

  def normalize_email(value)
    value.to_s.strip.downcase.presence
  end
end
