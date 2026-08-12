class CreateSlackDmSyncLedgers < ActiveRecord::Migration[8.1]
  def change
    create_table :slack_dm_sync_ledgers do |t|
      t.references :broker_credential, null: false, foreign_key: true
      t.string :home_team_id, null: false
      t.string :conversation_id, null: false
      t.string :conversation_type, null: false
      t.boolean :is_archived, null: false, default: false
      t.boolean :is_ext_shared, null: false, default: false
      t.jsonb :raw_payload, null: false, default: {}
      t.string :watermark_ts
      t.datetime :next_sync_at, null: false, default: -> { "CURRENT_TIMESTAMP" }
      t.text :last_error, null: false, default: ""
      t.integer :backoff_level, null: false, default: 0
      t.string :claim_token
      t.datetime :claimed_until
      t.boolean :active, null: false, default: true
      t.datetime :last_seen_at, null: false, default: -> { "CURRENT_TIMESTAMP" }

      t.timestamps
    end

    add_index :slack_dm_sync_ledgers,
              %i[broker_credential_id home_team_id conversation_id],
              unique: true,
              name: "idx_slack_dm_sync_ledgers_identity"
    add_index :slack_dm_sync_ledgers,
              %i[active next_sync_at claimed_until],
              name: "idx_slack_dm_sync_ledgers_due"
    add_index :slack_dm_sync_ledgers, :claim_token, unique: true, where: "claim_token IS NOT NULL"
  end
end
