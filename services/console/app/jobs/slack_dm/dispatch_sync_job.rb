module SlackDm
  class DispatchSyncJob < ApplicationJob
    queue_as :default

    def perform(oauth_app_slug = SlackDm::SyncCredential.oauth_app_slug)
      credential_ids = SlackDm::SyncCredential.syncable_credentials(oauth_app_slug).map(&:id)
      SlackDm::SyncLedger.claim_due(
        limit: dispatch_batch_size,
        broker_credential_ids: credential_ids
      ).each do |ledger|
        enqueue(ledger)
      end
    end

    private

    def enqueue(ledger)
      SlackDm::SyncConversationJob.perform_later(ledger.id, ledger.claim_token)
    rescue StandardError => e
      ledger.fail_claim!(claim_token: ledger.claim_token, error: "enqueue failed: #{e.message}")
      raise
    end

    def dispatch_batch_size
      configured = ConsoleEnv["SLACK_DM_SYNC_DISPATCH_BATCH_SIZE"].to_i
      configured.positive? ? configured : 10
    end
  end
end
