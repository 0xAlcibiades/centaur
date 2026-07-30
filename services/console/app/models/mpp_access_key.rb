class MppAccessKey < ApplicationRecord
  belongs_to :user_identity

  validates :wallet_address, :key_handle, :access_key_address, :key_authorization, :expires_at, presence: true
  validates :key_handle, uniqueness: true

  scope :current, -> { where(revoked_at: nil) }
end
