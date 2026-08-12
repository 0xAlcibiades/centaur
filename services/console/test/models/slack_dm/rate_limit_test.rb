require "test_helper"

class SlackDm::RateLimitTest < ActiveSupport::TestCase
  include ActiveSupport::Testing::TimeHelpers

  test "reservations atomically advance one method scope" do
    travel_to Time.zone.parse("2030-01-01 00:00:00"), with_usec: true
    app = oauth_apps(:acme_slack)

    first_wait = SlackDm::RateLimit.reserve!(
      oauth_app: app,
      home_team_id: "T123",
      slack_method: "conversations.list",
      requests_per_minute: 20
    )
    second_wait = SlackDm::RateLimit.reserve!(
      oauth_app: app,
      home_team_id: "T123",
      slack_method: "conversations.list",
      requests_per_minute: 20
    )

    assert_equal 0.0, first_wait
    assert_in_delta 3.0, second_wait, 0.001
    assert_equal 6.seconds.from_now, SlackDm::RateLimit.last.next_available_at
  end

  test "Retry-After moves the shared method scope forward" do
    travel_to Time.zone.parse("2030-01-01 00:00:00"), with_usec: true
    app = oauth_apps(:acme_slack)
    SlackDm::RateLimit.penalize!(
      oauth_app: app,
      home_team_id: "T123",
      slack_method: "conversations.history",
      retry_after_seconds: 12
    )

    wait = SlackDm::RateLimit.reserve!(
      oauth_app: app,
      home_team_id: "T123",
      slack_method: "conversations.history",
      requests_per_minute: 50
    )

    assert_in_delta 12.0, wait, 0.001
  end
end
