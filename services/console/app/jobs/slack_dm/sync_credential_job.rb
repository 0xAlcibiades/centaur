module SlackDm
  # Compatibility shim for jobs persisted before inventory and unit sync were split.
  class SyncCredentialJob < ApplicationJob
    queue_as :default

    def perform(sync_scope_or_credential_id, credential_ids_or_scope = nil)
      credential_ids = if credential_ids_or_scope.is_a?(Array)
        credential_ids_or_scope
      else
        [ sync_scope_or_credential_id ]
      end
      credential_ids.map(&:to_i).uniq.each do |credential_id|
        SlackDm::InventoryCredentialJob.perform_later(credential_id)
      end
    end
  end
end
