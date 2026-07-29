module Broker
  # The recurring driver of the refresh loop. Level-triggered: every tick
  # re-derives which credentials are due from the database, so a missed tick is
  # caught by the next one. A self-rescheduling job could orphan a credential
  # if its enqueue were ever lost.
  #
  # FOR UPDATE SKIP LOCKED skips credentials whose refresh is already in flight
  # (locked by a RefreshCredentialJob), so we don't enqueue redundant work for
  # them.
  class PollRefreshJob < ApplicationJob
    queue_as :default

    def perform
      ids = BrokerCredential.transaction do
        BrokerCredential.refreshable.lock("FOR UPDATE SKIP LOCKED").pluck(:id)
      end
      ids.each { |id| Broker::RefreshCredentialJob.perform_later(id) }
    end
  end
end
