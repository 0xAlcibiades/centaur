class CreateMppWalletConnections < ActiveRecord::Migration[8.0]
  def change
    create_table :mpp_wallet_links do |t|
      t.references :user_identity, null: false, foreign_key: true
      t.string :token_digest, null: false
      t.string :key_handle, null: false
      t.string :access_key_address, null: false
      t.string :access_key_public_key, null: false
      t.datetime :expires_at, null: false
      t.datetime :used_at
      t.timestamps
    end
    add_index :mpp_wallet_links, :token_digest, unique: true

    create_table :mpp_access_keys do |t|
      t.references :user_identity, null: false, foreign_key: true
      t.string :wallet_address, null: false
      t.string :key_handle, null: false
      t.string :access_key_address, null: false
      t.jsonb :key_authorization, null: false, default: {}
      t.datetime :expires_at, null: false
      t.datetime :revoked_at
      t.timestamps
    end
    add_index :mpp_access_keys, :key_handle, unique: true
    add_index :mpp_access_keys, :user_identity_id, unique: true, where: "revoked_at IS NULL"
  end
end
