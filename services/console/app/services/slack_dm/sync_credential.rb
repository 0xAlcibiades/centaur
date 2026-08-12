module SlackDm
  class SyncCredential
    DM_REQUIRED_SCOPES = %w[im:read im:history].freeze
    MPIM_REQUIRED_SCOPES = %w[mpim:read mpim:history].freeze
    PRIVATE_CHANNEL_REQUIRED_SCOPES = %w[groups:read groups:history].freeze
    REQUIRED_SCOPES = (
      DM_REQUIRED_SCOPES + MPIM_REQUIRED_SCOPES + PRIVATE_CHANNEL_REQUIRED_SCOPES
    ).freeze

    class << self
      def oauth_app_slug
        ConsoleEnv["SLACK_DM_SYNC_OAUTH_APP_SLUG"].presence || "slack"
      end

      def required_scopes_granted?(scopes)
        supported_conversation_types(scopes).any?
      end

      def supported_conversation_types(scopes)
        granted = Array(scopes)
        types = []
        types << "im" if (DM_REQUIRED_SCOPES - granted).empty?
        types << "mpim" if (MPIM_REQUIRED_SCOPES - granted).empty?
        types << "private_channel" if (PRIVATE_CHANNEL_REQUIRED_SCOPES - granted).empty?
        types
      end

      def syncable_credentials(oauth_app_slug = self.oauth_app_slug)
        BrokerCredential
          .includes(:oauth_app)
          .joins(:oauth_app)
          .where(dead: false)
          .where(oauth_apps: {
            provider: Oauth::Providers::Slack::KEY,
            slug: oauth_app_slug,
            enabled: true
          })
          .select { |credential| syncable?(credential) }
      end

      def syncable?(credential)
        credential.present? &&
          !credential.dead? &&
          credential.access_token.present? &&
          credential.oauth_app&.enabled? &&
          credential.oauth_app.provider == Oauth::Providers::Slack::KEY &&
          required_scopes_granted?(credential.scopes)
      end
    end
  end
end
