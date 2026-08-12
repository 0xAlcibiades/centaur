module SlackDm
  class SyncCredentialJob < ApplicationJob
    queue_as :default

    limits_concurrency to: 1,
                       key: ->(sync_scope_or_credential_id, credential_ids_or_scope = nil) {
                         scope = if credential_ids_or_scope.is_a?(Array)
                           sync_scope_or_credential_id
                         else
                           credential_ids_or_scope.presence || sync_scope_or_credential_id
                         end
                         "slack_dm_sync_#{scope}"
                       },
                       duration: 30.minutes,
                       on_conflict: :discard

    def perform(sync_scope_or_credential_id, credential_ids_or_scope = nil)
      if credential_ids_or_scope.is_a?(Array)
        perform_scope(sync_scope_or_credential_id, credential_ids_or_scope)
      else
        # Compatibility for jobs enqueued before workspace-scoped polling shipped.
        perform_credential(sync_scope_or_credential_id)
      end
    end

    private

    def perform_scope(sync_scope, credential_ids)
      ids = credential_ids.map(&:to_i).uniq.sort
      return if ids.empty?

      cursor = Rails.cache.read(scope_cursor_key(sync_scope)).to_i % ids.length
      credentials = BrokerCredential.includes(:oauth_app).where(id: ids).index_by(&:id)
      selected_id = ids.rotate(cursor).find do |credential_id|
        syncable?(credentials[credential_id])
      end
      return unless selected_id

      perform_credential(selected_id)
    ensure
      if selected_id
        next_cursor = (ids.index(selected_id) + 1) % ids.length
        Rails.cache.write(scope_cursor_key(sync_scope), next_cursor, expires_in: 30.days)
      end
    end

    def perform_credential(credential_id)
      credential = BrokerCredential.includes(:oauth_app).find_by(id: credential_id)
      return unless syncable?(credential)

      SlackDm::SyncCredential.new(credential).call
    rescue SlackDm::SyncCredential::SlackApiRateLimited,
           SlackDm::SyncCredential::SyncBudgetExhausted => e
      Rails.logger.info(
        event: "slack_dm_sync_deferred",
        message: e.message,
        credential_id: credential&.oid
      )
    end

    def syncable?(credential)
      credential.present? &&
        !credential.dead? &&
        credential.access_token.present? &&
        credential.oauth_app&.provider == Oauth::Providers::Slack::KEY &&
        SlackDm::SyncCredential.required_scopes_granted?(credential.scopes)
    end

    def scope_cursor_key(sync_scope)
      "slack_dm_sync_scope_cursor:#{sync_scope}"
    end
  end
end
