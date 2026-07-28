class CreatePermissionRequests < ActiveRecord::Migration[8.1]
  def change
    create_table :permission_requests do |t|
      t.string :status, null: false, default: "pending"
      t.string :kind, null: false
      t.references :requesting_principal, null: false, foreign_key: { to_table: :principals }
      t.bigint :requesting_proxy_id, null: false
      t.string :requesting_slack_channel_id, null: false
      t.string :requesting_slack_thread_ts
      t.jsonb :metadata, null: false, default: {}
      t.string :approver_notification_status, null: false, default: "pending"
      t.string :approver_notification_channel_id
      t.string :approver_notification_message_ts
      t.string :approver_decision_update_status, null: false, default: "pending"
      t.string :requester_outcome_notification_status, null: false, default: "pending"
      t.string :requester_outcome_message_ts
      t.references :decided_by, foreign_key: { to_table: :users }
      t.datetime :decided_at

      t.timestamps
    end

    add_index :permission_requests, :status
    add_index :permission_requests, :kind
    add_index :permission_requests, :requesting_proxy_id
    add_index :permission_requests, :requesting_slack_channel_id
    add_index :permission_requests, :approver_notification_status
    add_index :permission_requests, :approver_decision_update_status
    add_index :permission_requests, :requester_outcome_notification_status,
              name: "idx_permission_requests_on_requester_outcome_status"
  end
end
