module Autorotate
  class ReconcileParentLeaseJob < ApplicationJob
    queue_as :default

    def perform
      ParentLeaseService.new.reconcile!(acquire_operation_id: SecureRandom.uuid)
    rescue AutorotateProxyParentClient::Error, ParentLeaseService::Unavailable => error
      Rails.logger.warn(event: "autorotate_parent_lease_reconcile_failed", error_class: error.class.name)
    end
  end
end
