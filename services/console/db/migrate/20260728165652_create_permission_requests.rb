class CreatePermissionRequests < ActiveRecord::Migration[8.1]
  def change
    create_table :permission_requests do |t|
      t.string :status, null: false, default: "pending"
      t.string :kind, null: false
      t.references :requesting_principal, null: false, foreign_key: { to_table: :principals }
      t.references :requesting_proxy, null: false, foreign_key: { to_table: :proxies }
      t.string :requesting_slack_channel_id, null: false
      t.string :requesting_slack_thread_ts
      t.jsonb :requested_channel_ids, null: false, default: []
      t.jsonb :services, null: false, default: []
      t.string :approver_notification_channel_id
      t.string :approver_notification_message_ts
      t.references :decided_by, foreign_key: { to_table: :users }
      t.datetime :decided_at

      t.timestamps
    end

    add_index :permission_requests, :status
    add_index :permission_requests, :kind
    add_index :permission_requests, :requesting_slack_channel_id
  end
end
