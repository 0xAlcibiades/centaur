module SlackDm
  class ApiClient
    AUTH_TEST_ENDPOINT = "https://slack.com/api/auth.test"
    CONVERSATIONS_LIST_ENDPOINT = "https://slack.com/api/conversations.list"
    CONVERSATIONS_MEMBERS_ENDPOINT = "https://slack.com/api/conversations.members"
    CONVERSATIONS_HISTORY_ENDPOINT = "https://slack.com/api/conversations.history"
    CONVERSATIONS_REPLIES_ENDPOINT = "https://slack.com/api/conversations.replies"

    SlackApiError = Class.new(StandardError)
    class SlackApiRateLimited < SlackApiError
      attr_reader :slack_method, :retry_after_seconds

      def initialize(slack_method:, retry_after_seconds:)
        @slack_method = slack_method
        @retry_after_seconds = retry_after_seconds
        super("Slack API rate limited #{slack_method}; retry after #{retry_after_seconds}s")
      end
    end

    METHODS = {
      AUTH_TEST_ENDPOINT => "auth.test",
      CONVERSATIONS_LIST_ENDPOINT => "conversations.list",
      CONVERSATIONS_MEMBERS_ENDPOINT => "conversations.members",
      CONVERSATIONS_HISTORY_ENDPOINT => "conversations.history",
      CONVERSATIONS_REPLIES_ENDPOINT => "conversations.replies"
    }.freeze
    REQUESTS_PER_MINUTE = {
      "auth.test" => 20,
      "conversations.list" => 20,
      "conversations.members" => 100,
      "conversations.history" => 50,
      "conversations.replies" => 50
    }.freeze
    RETRY_AFTER_BUFFER_SECONDS = 0.25
    MAX_RATE_LIMIT_SLEEP_SECONDS = 30.0

    def initialize(credential, slack_api_http: nil, sleeper: ->(seconds) { sleep(seconds) })
      @credential = credential
      @slack_api_http = slack_api_http
      @sleeper = sleeper
      @home_team_id = credential.labels&.[]("slack_team_id").presence ||
                      credential.oauth_app&.labels&.[]("slack_team_id").presence
    end

    def auth_test
      request(AUTH_TEST_ENDPOINT).tap { |auth| @home_team_id = auth.fetch("team_id") }
    end

    def conversations_list(params)
      request(CONVERSATIONS_LIST_ENDPOINT, params)
    end

    def conversations_members(params)
      request(CONVERSATIONS_MEMBERS_ENDPOINT, params)
    end

    def conversations_history(params)
      request(CONVERSATIONS_HISTORY_ENDPOINT, params)
    end

    def conversations_replies(params)
      request(CONVERSATIONS_REPLIES_ENDPOINT, params)
    end

    private

    def request(endpoint, params = {})
      slack_method = METHODS.fetch(endpoint)
      retry_count = 0
      logged = false

      loop do
        wait_for_reservation(slack_method)
        response = response_for(endpoint, params)
        return response if response.is_a?(Hash)

        if response.status == 429
          retry_after = parse_retry_after(response["retry-after"])
          paced_retry = penalize(slack_method, retry_after)
          unless logged
            log_rate_limit(slack_method, retry_after)
            logged = true
          end
          if retry_count >= max_retries || retry_after > MAX_RATE_LIMIT_SLEEP_SECONDS
            raise SlackApiRateLimited.new(
              slack_method: slack_method,
              retry_after_seconds: retry_after
            )
          end
          retry_count += 1
          @sleeper.call(retry_after + RETRY_AFTER_BUFFER_SECONDS) unless paced_retry
          next
        end

        parsed = response.json
        raise SlackApiError, "Slack API returned HTTP #{response.status}" unless response.success?
        raise SlackApiError, "Slack API returned #{parsed['error']}" unless parsed["ok"] == true

        return parsed
      end
    end

    def wait_for_reservation(slack_method)
      return unless @home_team_id.present?

      wait_seconds = SlackDm::RateLimit.reserve!(
        oauth_app: @credential.oauth_app,
        home_team_id: @home_team_id,
        slack_method: slack_method,
        requests_per_minute: REQUESTS_PER_MINUTE.fetch(slack_method)
      )
      @sleeper.call(wait_seconds) if wait_seconds.positive?
    end

    def penalize(slack_method, retry_after)
      return false unless @home_team_id.present?

      SlackDm::RateLimit.penalize!(
        oauth_app: @credential.oauth_app,
        home_team_id: @home_team_id,
        slack_method: slack_method,
        retry_after_seconds: retry_after + RETRY_AFTER_BUFFER_SECONDS
      )
      true
    end

    def response_for(endpoint, params)
      if @slack_api_http
        return @slack_api_http.call(
          endpoint: endpoint,
          params: params,
          access_token: @credential.access_token
        )
      end

      HttpClient.new(open_timeout: slack_timeout, read_timeout: slack_timeout).get(
        endpoint,
        params: params,
        headers: { "Authorization" => "Bearer #{@credential.access_token}" }
      )
    end

    def parse_retry_after(value)
      parsed = Float(value)
      parsed.finite? && parsed.positive? ? parsed : 1.0
    rescue ArgumentError, TypeError
      5.0
    end

    def log_rate_limit(slack_method, retry_after)
      Rails.logger.warn(
        event: "slack_dm_sync_rate_limited",
        message: "Slack DM sync paused after Slack API rate limit",
        slack_method: slack_method,
        slack_team_id: @home_team_id,
        oauth_app_id: @credential.oauth_app_id,
        credential_id: @credential.oid,
        retry_after_seconds: retry_after
      )
    end

    def slack_timeout = positive_env("SLACK_DM_SYNC_TIMEOUT_SECONDS", 20)
    def max_retries = positive_env("SLACK_DM_SYNC_RATE_LIMIT_MAX_RETRIES", 3)

    def positive_env(name, default)
      value = ConsoleEnv[name].to_i
      value.positive? ? value : default
    end
  end
end
