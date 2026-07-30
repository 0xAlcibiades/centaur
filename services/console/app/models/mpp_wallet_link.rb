class MppWalletLink < ApplicationRecord
  TTL = 10.minutes

  belongs_to :user_identity

  attr_accessor :token

  validates :token_digest, :key_handle, :access_key_address, :access_key_public_key, :expires_at, presence: true

  before_validation :issue_token, on: :create

  def self.find_active(token)
    find_by(token_digest: Digest::SHA256.hexdigest(token.to_s))&.then do |link|
      link if link.used_at.nil? && link.expires_at.future?
    end
  end

  private

  def issue_token
    return if token_digest.present?

    self.token = SecureRandom.urlsafe_base64(32)
    self.token_digest = Digest::SHA256.hexdigest(token)
    self.expires_at ||= TTL.from_now
  end
end
