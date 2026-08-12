class CreateSlackDmRateLimits < ActiveRecord::Migration[8.1]
  def change
    create_table :slack_dm_rate_limits do |t|
      t.references :oauth_app, null: false, foreign_key: true
      t.string :home_team_id, null: false
      t.string :slack_method, null: false
      t.datetime :next_available_at, null: false, default: -> { "CURRENT_TIMESTAMP" }

      t.timestamps
    end

    add_index :slack_dm_rate_limits,
              %i[oauth_app_id home_team_id slack_method],
              unique: true,
              name: "idx_slack_dm_rate_limits_scope"
  end
end
