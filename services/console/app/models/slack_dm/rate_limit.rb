class SlackDm::RateLimit < ApplicationRecord
  belongs_to :oauth_app

  validates :home_team_id, :slack_method, presence: true

  def self.reserve!(oauth_app:, home_team_id:, slack_method:, requests_per_minute:, now: Time.current)
    interval = 60.0 / requests_per_minute
    transaction do
      bucket = create_or_find_by!(
        oauth_app: oauth_app,
        home_team_id: home_team_id,
        slack_method: slack_method
      )
      bucket.lock!
      available_at = [ bucket.next_available_at, now ].max
      wait_seconds = [ available_at - now, 0.0 ].max
      bucket.update!(next_available_at: available_at + interval.seconds)
      wait_seconds
    end
  end

  def self.penalize!(oauth_app:, home_team_id:, slack_method:, retry_after_seconds:, now: Time.current)
    transaction do
      bucket = create_or_find_by!(
        oauth_app: oauth_app,
        home_team_id: home_team_id,
        slack_method: slack_method
      )
      bucket.lock!
      bucket.update!(next_available_at: [ bucket.next_available_at, now + retry_after_seconds ].max)
    end
  end
end
