class AddConversationStateToSlackDmSyncCursors < ActiveRecord::Migration[8.1]
  def change
    add_column :slack_dm_sync_cursors, :conversation_state, :jsonb, null: false, default: {}
  end
end
