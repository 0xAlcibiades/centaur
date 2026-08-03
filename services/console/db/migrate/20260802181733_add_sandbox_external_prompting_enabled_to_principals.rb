class AddSandboxExternalPromptingEnabledToPrincipals < ActiveRecord::Migration[8.1]
  def change
    add_column :principals, :sandbox_external_prompting_enabled, :boolean, null: false, default: false
  end
end
