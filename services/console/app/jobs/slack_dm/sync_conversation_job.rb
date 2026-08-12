module SlackDm
  class SyncConversationJob < ApplicationJob
    queue_as :default

    def perform(ledger_id, claim_token)
      ledger = SlackDm::SyncLedger.includes(broker_credential: :oauth_app).find_by(id: ledger_id)
      return unless ledger
      unless ledger.active?
        ledger.release_claim!(claim_token: claim_token)
        return
      end
      unless SlackDm::SyncCredential.syncable?(ledger.broker_credential)
        ledger.release_claim!(claim_token: claim_token)
        return
      end

      SlackDm::SyncConversation.new(ledger, claim_token: claim_token).call
    rescue StandardError => e
      ledger&.fail_claim!(claim_token: claim_token, error: "#{e.class}: #{e.message}")
      Rails.logger.warn(
        event: "slack_dm_conversation_sync_failed",
        message: e.message,
        ledger_id: ledger_id,
        credential_id: ledger&.broker_credential&.oid,
        conversation_id: ledger&.conversation_id
      )
    end
  end
end
