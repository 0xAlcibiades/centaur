module SlackDm
  class PollSyncJob < ApplicationJob
    queue_as :default

    def perform(oauth_app_slug = SlackDm::SyncCredential.oauth_app_slug)
      SlackDm::SyncCredential.syncable_credentials(oauth_app_slug).each do |credential|
        SlackDm::InventoryCredentialJob.perform_later(credential.id)
      end
    end
  end
end
