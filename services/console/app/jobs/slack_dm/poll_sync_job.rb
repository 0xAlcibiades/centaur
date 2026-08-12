module SlackDm
  class PollSyncJob < ApplicationJob
    queue_as :default

    def perform(oauth_app_slug = SlackDm::SyncCredential.oauth_app_slug)
      credentials = BrokerCredential
        .includes(:oauth_app)
        .joins(:oauth_app)
        .where(dead: false)
        .where(oauth_apps: {
          provider: Oauth::Providers::Slack::KEY,
          slug: oauth_app_slug,
          enabled: true
        })

      credentials
        .select { |credential| credential.access_token.present? }
        .select { |credential| SlackDm::SyncCredential.required_scopes_granted?(credential.scopes) }
        .group_by { |credential| SlackDm::SyncCredential.sync_scope_for(credential) }
        .each do |sync_scope, scoped_credentials|
          SlackDm::SyncCredentialJob.perform_later(
            sync_scope,
            scoped_credentials.map(&:id).sort
          )
        end
    end
  end
end
