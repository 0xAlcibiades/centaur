class CreateAutorotateParentLeases < ActiveRecord::Migration[8.1]
  def change
    create_table :autorotate_parent_leases do |t|
      t.string :consumer, null: false
      t.string :lease_id
      t.bigint :fence
      t.bigint :external_generation
      t.string :state, null: false, default: "released"
      t.datetime :expires_at
      t.string :operation_id
      t.string :pending_operation_id
      t.string :drain_operation_id
      t.string :drain_refresh_operation_id
      t.string :drain_final_refresh_operation_id
      t.string :drain_lease_id
      t.bigint :drain_fence
      t.bigint :drain_external_generation
      t.text :drain_provider_account_id
      t.string :drain_phase

      t.timestamps
    end
    add_index :autorotate_parent_leases, :consumer, unique: true
    add_index :autorotate_parent_leases, :drain_operation_id, unique: true,
              where: "drain_operation_id IS NOT NULL"
    add_index :autorotate_parent_leases, :drain_refresh_operation_id, unique: true,
              where: "drain_refresh_operation_id IS NOT NULL"
    add_index :autorotate_parent_leases, :drain_final_refresh_operation_id, unique: true,
              where: "drain_final_refresh_operation_id IS NOT NULL"

    create_table :autorotate_credential_versions do |t|
      t.references :autorotate_parent_lease, null: false, foreign_key: true
      t.text :access_token, null: false
      t.string :provider_account_id, null: false
      t.string :broker_lease_id, null: false
      t.bigint :external_generation, null: false
      t.datetime :expires_at, null: false

      t.timestamps
    end
    add_index :autorotate_credential_versions,
              %i[autorotate_parent_lease_id broker_lease_id external_generation],
              unique: true,
              name: "index_autorotate_versions_on_parent_lease_and_generation"

    create_table :autorotate_execution_pins do |t|
      t.references :autorotate_parent_lease, null: false, foreign_key: true
      t.references :autorotate_credential_version, null: false, foreign_key: true
      t.references :proxy, foreign_key: true
      t.string :lease_id, null: false
      t.bigint :fence, null: false
      t.string :operation_id, null: false
      t.string :execution_id, null: false
      t.string :request_hash, null: false
      t.string :state, null: false, default: "active"
      t.datetime :expires_at, null: false
      t.datetime :released_at
      t.string :last_heartbeat_operation_id
      t.string :quota_exhausted_operation_id

      t.timestamps
    end
    add_index :autorotate_execution_pins, :operation_id, unique: true
    add_index :autorotate_execution_pins, :execution_id, unique: true
    add_index :autorotate_execution_pins, :last_heartbeat_operation_id, unique: true,
              where: "last_heartbeat_operation_id IS NOT NULL",
              name: "index_autorotate_pins_on_heartbeat_operation"
    add_index :autorotate_execution_pins, :quota_exhausted_operation_id, unique: true,
              where: "quota_exhausted_operation_id IS NOT NULL",
              name: "index_autorotate_pins_on_quota_operation"
    add_index :autorotate_execution_pins, %i[autorotate_credential_version_id state],
              name: "index_autorotate_pins_on_version_and_state"
  end
end
