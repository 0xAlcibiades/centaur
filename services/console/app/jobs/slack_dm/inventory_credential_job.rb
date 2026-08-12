module SlackDm
  class InventoryCredentialJob < ApplicationJob
    queue_as :default

    limits_concurrency to: 1,
                       key: ->(credential_id) { "slack_dm_inventory_#{credential_id}" },
                       duration: 30.minutes,
                       on_conflict: :discard

    def perform(credential_id)
      credential = BrokerCredential.includes(:oauth_app).find_by(id: credential_id)
      return unless SlackDm::SyncCredential.syncable?(credential)

      SlackDm::InventoryCredential.new(credential).call
    rescue SlackDm::ApiClient::SlackApiError, CentaurApiClient::Error => e
      Rails.logger.warn(
        event: "slack_dm_inventory_failed",
        message: e.message,
        credential_id: credential&.oid
      )
    end
  end
end
