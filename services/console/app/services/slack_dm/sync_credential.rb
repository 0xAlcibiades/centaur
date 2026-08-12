module SlackDm
  class SyncCredential
    DM_REQUIRED_SCOPES = %w[im:read im:history].freeze
    MPIM_REQUIRED_SCOPES = %w[mpim:read mpim:history].freeze
    PRIVATE_CHANNEL_REQUIRED_SCOPES = %w[groups:read groups:history].freeze
    REQUIRED_SCOPES = (
      DM_REQUIRED_SCOPES + MPIM_REQUIRED_SCOPES + PRIVATE_CHANNEL_REQUIRED_SCOPES
    ).freeze

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
    SyncBudgetExhausted = Class.new(SlackApiError)

    SLACK_METHODS = {
      AUTH_TEST_ENDPOINT => "auth.test",
      CONVERSATIONS_LIST_ENDPOINT => "conversations.list",
      CONVERSATIONS_MEMBERS_ENDPOINT => "conversations.members",
      CONVERSATIONS_HISTORY_ENDPOINT => "conversations.history",
      CONVERSATIONS_REPLIES_ENDPOINT => "conversations.replies"
    }.freeze
    # Slack rate limits are shared per method, app, and workspace. Stay at the
    # documented floor for each tier instead of relying on burst tolerance.
    REQUESTS_PER_MINUTE = {
      "auth.test" => 20,
      "conversations.list" => 20,
      "conversations.members" => 100,
      "conversations.history" => 50,
      "conversations.replies" => 50
    }.freeze
    RATE_LIMIT_CACHE_TTL = 10.minutes
    CONVERSATION_CURSOR_CACHE_TTL = 30.days
    RETRY_AFTER_BUFFER_SECONDS = 0.25
    MAX_RETRY_AFTER_SECONDS = 30.0
    SLACK_TEAM_LABEL = "slack_team_id"

    class << self
      attr_accessor :slack_api_http

      def oauth_app_slug
        ConsoleEnv["SLACK_DM_SYNC_OAUTH_APP_SLUG"].presence || "slack"
      end

      def required_scopes_granted?(scopes)
        supported_conversation_types(scopes).any?
      end

      def supported_conversation_types(scopes)
        granted = Array(scopes)
        types = []
        types << "im" if (DM_REQUIRED_SCOPES - granted).empty?
        types << "mpim" if (MPIM_REQUIRED_SCOPES - granted).empty?
        types << "private_channel" if (PRIVATE_CHANNEL_REQUIRED_SCOPES - granted).empty?
        types
      end

      def sync_scope_for(credential)
        app_scope = credential.oauth_app_id || credential.oauth_app&.slug || "unknown-app"
        team_scope = credential.labels&.[](SlackDm::SyncCredential::SLACK_TEAM_LABEL).presence ||
                     credential.oauth_app&.labels&.[](SlackDm::SyncCredential::SLACK_TEAM_LABEL).presence
        team_scope ||= "credential:#{credential.id || credential.oid}"
        "#{app_scope}:#{team_scope}"
      end
    end

    def initialize(
      credential,
      api_client: CentaurApiClient.new,
      slack_api_http: nil,
      rate_limit_store: Rails.cache,
      sleeper: ->(seconds) { sleep(seconds) }
    )
      @credential = credential
      @api_client = api_client
      @slack_api_http = slack_api_http || self.class.slack_api_http
      @rate_limit_store = rate_limit_store
      @sleeper = sleeper
      @last_request_started_at = {}
      @slack_api_calls = 0
      @run_id = "sdms_#{SecureRandom.hex(16)}"
      @messages_fetched = 0
      @replies_fetched = 0
    end

    def call
      @run_started_at = Time.current.to_f
      auth = slack_api(AUTH_TEST_ENDPOINT)
      home_team_id = auth.fetch("team_id")
      @home_team_id = home_team_id
      source_user_id = auth["user_id"].presence || @credential.provider_subject.to_s
      checkpoints = load_checkpoints(home_team_id)
      batch = empty_batch(home_team_id, source_user_id)

      conversations = rotate_conversations(list_conversations, home_team_id)
      batch[:run][:conversations_requested] = conversations.length
      conversations_handled = 0
      terminal_error = nil

      begin
        conversations.each do |conversation|
          normalize_conversation(conversation, home_team_id, batch)
          normalize_members(conversation, home_team_id, batch)
          sync_history(conversation, home_team_id, checkpoints[conversation.fetch("id")], batch)
          batch[:run][:conversations_synced] += 1
          conversations_handled += 1
        rescue SlackApiRateLimited, SyncBudgetExhausted
          raise
        rescue StandardError => e
          raise if Rails.env.test?

          batch[:run][:conversations_failed] += 1
          conversations_handled += 1
          Rails.logger.warn do
            "slack DM sync failed for conversation #{conversation['id']}: #{e.class}: #{e.message}"
          end
        end
      rescue SlackApiRateLimited, SyncBudgetExhausted => e
        terminal_error = e
      end

      batch[:run][:status] = if terminal_error || batch[:run][:conversations_failed].positive?
        "partial"
      else
        "completed"
      end
      batch[:run][:error_text] = terminal_error&.message.to_s
      batch[:run][:messages_fetched] = @messages_fetched
      batch[:run][:messages_upserted] = batch[:messages].length
      batch[:run][:replies_fetched] = @replies_fetched
      batch[:run][:replies_upserted] = batch[:messages].count { |message| message[:parent_message_ts].present? }
      batch[:run][:finished] = true
      result = @api_client.ingest_slack_dm_sync_batch(sanitize_for_postgres(batch))
      advance_conversation_cursor(home_team_id, conversations.length, conversations_handled)
      result
    end

    private

    def load_checkpoints(home_team_id)
      response = @api_client.list_slack_dm_sync_checkpoints(
        broker_credential_id: @credential.oid,
        home_team_id: home_team_id
      )
      Array(response["checkpoints"]).to_h do |checkpoint|
        [ checkpoint.fetch("conversation_id"), checkpoint["watermark_ts"] ]
      end
    end

    def empty_batch(home_team_id, source_user_id)
      {
        run: {
          run_id: @run_id,
          mode: "incremental",
          status: "running",
          broker_credential_id: @credential.oid,
          source_user_id: source_user_id,
          home_team_id: home_team_id,
          conversations_requested: 0,
          conversations_synced: 0,
          conversations_failed: 0,
          messages_fetched: 0,
          messages_upserted: 0,
          replies_fetched: 0,
          replies_upserted: 0,
          metadata: {
            oauth_app_slug: @credential.oauth_app&.slug,
            credential_id: @credential.oid
          }
        },
        replace_memberships: true,
        conversations: [],
        members: [],
        messages: [],
        attachments: [],
        checkpoints: []
      }
    end

    def list_conversations
      types = self.class.supported_conversation_types(@credential.scopes)
      raise SlackApiError, "Slack credential has no supported conversation scopes" if types.empty?

      each_page(
        CONVERSATIONS_LIST_ENDPOINT,
        { "types" => types.join(","), "exclude_archived" => "false", "limit" => list_page_size },
        max_pages: list_max_pages
      ).flat_map { |page| Array(page["channels"]) }
    end

    def normalize_conversation(conversation, home_team_id, batch)
      batch[:conversations] << {
        home_team_id: home_team_id,
        conversation_id: conversation.fetch("id"),
        conversation_type: conversation_type(conversation),
        is_archived: conversation["is_archived"] == true,
        is_ext_shared: conversation["is_ext_shared"] == true,
        raw_payload: conversation
      }
    end

    def normalize_members(conversation, home_team_id, batch)
      conversation_id = conversation.fetch("id")
      members = conversation_members(conversation)
      members.each do |member_id|
        batch[:members] << {
          home_team_id: home_team_id,
          conversation_id: conversation_id,
          user_id: member_id,
          is_external: false,
          is_current_member: true,
          raw_payload: { source: "conversations.members" }
        }
      end
    end

    def conversation_members(conversation)
      if conversation["is_im"] && conversation["user"].present?
        members = [ conversation["user"] ]
        members << @credential.provider_subject if @credential.provider_subject.present?
        return members.uniq
      end

      complete = true
      pages = each_page(
        CONVERSATIONS_MEMBERS_ENDPOINT,
        { "channel" => conversation.fetch("id"), "limit" => members_page_size },
        max_pages: members_max_pages
      ) do |_page, truncated|
        complete = false if truncated
      end
      unless complete
        raise SlackApiError,
              "Slack membership pagination truncated for #{conversation.fetch('id')}"
      end

      members = pages.flat_map { |page| Array(page["members"]) }.compact
      members << @credential.provider_subject if @credential.provider_subject.present?
      members.uniq
    end

    def conversation_type(conversation)
      return "mpim" if conversation["is_mpim"]
      return "im" if conversation["is_im"]
      return "private_channel" if conversation["is_private"]

      raise SlackApiError, "Unsupported Slack conversation #{conversation['id']}"
    end

    def sync_history(conversation, home_team_id, checkpoint, batch)
      conversation_id = conversation.fetch("id")
      max_message_ts = checkpoint
      completed = true
      pages = each_page(
        CONVERSATIONS_HISTORY_ENDPOINT,
        history_params(conversation_id, checkpoint),
        max_pages: history_max_pages
      ) do |_page, truncated|
        completed = false if truncated
      end

      pages.each do |page|
        Array(page["messages"]).each do |message|
          @messages_fetched += 1
          max_message_ts = max_slack_ts(max_message_ts, message["ts"])
          normalize_message(message, home_team_id, conversation_id, nil, batch)
          normalize_files(message, home_team_id, conversation_id, batch)
          sync_replies(message, home_team_id, conversation_id, batch) if message["reply_count"].to_i.positive?
        end
      end

      return unless completed

      batch[:checkpoints] << {
        broker_credential_id: @credential.oid,
        home_team_id: home_team_id,
        conversation_id: conversation_id,
        watermark_ts: max_message_ts,
        last_run_id: @run_id
      }
    end

    def sync_replies(root_message, home_team_id, conversation_id, batch)
      thread_ts = root_message["thread_ts"].presence || root_message["ts"]
      pages = each_page(
        CONVERSATIONS_REPLIES_ENDPOINT,
        { "channel" => conversation_id, "ts" => thread_ts, "limit" => replies_page_size },
        max_pages: replies_max_pages
      )

      pages.each do |page|
        Array(page["messages"]).each do |reply|
          next if reply["ts"] == root_message["ts"]

          @replies_fetched += 1
          normalize_message(reply, home_team_id, conversation_id, root_message["ts"], batch)
          normalize_files(reply, home_team_id, conversation_id, batch)
        end
      end
    end

    def normalize_message(message, home_team_id, conversation_id, parent_message_ts, batch)
      ts = message.fetch("ts")
      thread_ts = message["thread_ts"].presence || parent_message_ts
      batch[:messages] << {
        home_team_id: home_team_id,
        conversation_id: conversation_id,
        message_ts: ts,
        thread_ts: thread_ts,
        parent_message_ts: parent_message_ts,
        is_thread_root: thread_ts.present? && thread_ts == ts,
        user_id: message["user"].to_s,
        user_team_id: message["user_team"],
        bot_id: message["bot_id"].to_s,
        message_type: message["type"].presence || "message",
        message_subtype: message["subtype"],
        text: message["text"].to_s,
        permalink: message["permalink"].to_s,
        reply_count: message["reply_count"].to_i,
        reply_users: Array(message["reply_users"]),
        latest_reply_ts: message["latest_reply"],
        thread_refreshed: parent_message_ts.present?,
        raw_payload: message,
        source_run_id: @run_id
      }
    end

    def normalize_files(message, home_team_id, conversation_id, batch)
      Array(message["files"]).each do |file|
        next if file["id"].blank?

        batch[:attachments] << {
          home_team_id: home_team_id,
          conversation_id: conversation_id,
          message_ts: message.fetch("ts"),
          slack_file_id: file.fetch("id"),
          name: file["name"].to_s,
          title: file["title"].to_s,
          mimetype: file["mimetype"].to_s,
          filetype: file["filetype"].to_s,
          size_bytes: file["size"].to_i,
          url_private: file["url_private"].to_s,
          permalink: file["permalink"].to_s,
          raw_payload: file,
          source_run_id: @run_id
        }
      end
    end

    def history_params(conversation_id, checkpoint)
      params = { "channel" => conversation_id, "limit" => history_page_size }
      params["oldest"] = checkpoint if checkpoint.present?
      params["inclusive"] = "false" if checkpoint.present?
      params
    end

    def each_page(endpoint, params, max_pages:)
      pages = []
      cursor = nil
      max_pages.times do |index|
        page = slack_api(endpoint, params.merge("cursor" => cursor).compact)
        pages << page
        cursor = page.dig("response_metadata", "next_cursor").presence
        has_more = cursor.present?
        truncated = has_more && index == max_pages - 1
        yield page, truncated if block_given?
        break unless has_more
        break if truncated
      end
      pages
    end

    def slack_api(endpoint, params = {})
      slack_method = SLACK_METHODS.fetch(endpoint)
      retry_count = 0
      rate_limit_logged = false

      loop do
        enforce_sync_budget!
        pace_slack_method(slack_method)
        enforce_sync_budget!
        @slack_api_calls += 1
        response = slack_api_response(endpoint, params)
        return response if response.is_a?(Hash)

        if response.status == 429
          retry_after_seconds = parse_retry_after(response["retry-after"])
          unless rate_limit_logged
            log_rate_limit(slack_method, retry_after_seconds)
            rate_limit_logged = true
          end
          if retry_count >= rate_limit_max_retries
            raise SlackApiRateLimited.new(
              slack_method: slack_method,
              retry_after_seconds: retry_after_seconds
            )
          end

          retry_count += 1
          @sleeper.call(retry_after_seconds + RETRY_AFTER_BUFFER_SECONDS)
          next
        end

        parsed = response.json
        raise SlackApiError, "Slack API returned HTTP #{response.status}" unless response.success?
        raise SlackApiError, "Slack API returned #{parsed['error']}" unless parsed["ok"] == true

        return parsed
      end
    end

    def slack_api_response(endpoint, params)
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

    def pace_slack_method(slack_method)
      interval = 60.0 / REQUESTS_PER_MINUTE.fetch(slack_method)
      cache_key = rate_limit_cache_key(slack_method)
      last_started_at = [
        @last_request_started_at[cache_key],
        @rate_limit_store.read(cache_key)&.to_f
      ].compact.max
      now = Time.current.to_f
      elapsed = [ now - last_started_at, 0.0 ].max if last_started_at
      wait_seconds = interval - elapsed if elapsed
      @sleeper.call(wait_seconds) if wait_seconds&.positive?

      started_at = Time.current.to_f
      @last_request_started_at[cache_key] = started_at
      @rate_limit_store.write(cache_key, started_at, expires_in: RATE_LIMIT_CACHE_TTL)
    end

    def rate_limit_cache_key(slack_method)
      app_scope = @credential.oauth_app_id || @credential.oauth_app&.slug || "unknown-app"
      team_scope = @home_team_id.presence ||
                   @credential.labels&.[](SLACK_TEAM_LABEL).presence ||
                   @credential.oauth_app&.labels&.[](SLACK_TEAM_LABEL).presence ||
                   "unknown-team"
      "slack_dm_sync_rate_limit:#{app_scope}:#{team_scope}:#{slack_method}"
    end

    def parse_retry_after(value)
      parsed = Float(value)
      seconds = parsed.finite? && parsed.positive? ? parsed : 1.0
      [ seconds, MAX_RETRY_AFTER_SECONDS ].min
    rescue ArgumentError, TypeError
      5.0
    end

    def log_rate_limit(slack_method, retry_after_seconds)
      Rails.logger.warn(
        event: "slack_dm_sync_rate_limited",
        message: "Slack DM sync paused after Slack API rate limit",
        slack_method: slack_method,
        slack_team_id: @home_team_id.presence ||
          @credential.labels&.[](SLACK_TEAM_LABEL).presence ||
          @credential.oauth_app&.labels&.[](SLACK_TEAM_LABEL).presence,
        oauth_app_id: @credential.oauth_app_id,
        credential_id: @credential.oid,
        retry_after_seconds: retry_after_seconds
      )
    end

    def rotate_conversations(conversations, home_team_id)
      return conversations if conversations.empty?

      @conversation_cursor_key = [
        "slack_dm_sync_conversation_cursor",
        @credential.oid,
        home_team_id
      ].join(":")
      @conversation_cursor = @rate_limit_store.read(@conversation_cursor_key).to_i % conversations.length
      conversations.rotate(@conversation_cursor)
    end

    def advance_conversation_cursor(home_team_id, conversation_count, conversations_handled)
      return if conversation_count.zero? || conversations_handled.zero?

      key = @conversation_cursor_key || [
        "slack_dm_sync_conversation_cursor",
        @credential.oid,
        home_team_id
      ].join(":")
      next_cursor = (@conversation_cursor.to_i + conversations_handled) % conversation_count
      @rate_limit_store.write(key, next_cursor, expires_in: CONVERSATION_CURSOR_CACHE_TTL)
    end

    def enforce_sync_budget!
      if @slack_api_calls >= max_api_calls_per_run
        raise SyncBudgetExhausted, "Slack DM sync request budget exhausted"
      end
      if Time.current.to_f - @run_started_at >= max_run_seconds
        raise SyncBudgetExhausted, "Slack DM sync time budget exhausted"
      end
    end

    def max_slack_ts(left, right)
      return right if left.blank?
      return left if right.blank?

      (slack_ts_sort_key(right) <=> slack_ts_sort_key(left)).positive? ? right : left
    end

    def slack_ts_sort_key(value)
      seconds, micros = value.to_s.split(".", 2)
      [ seconds.to_i, micros.to_s.ljust(6, "0")[0, 6].to_i ]
    end

    def sanitize_for_postgres(value)
      case value
      when Hash
        value.to_h do |key, nested_value|
          [ sanitize_for_postgres(key), sanitize_for_postgres(nested_value) ]
        end
      when Array
        value.map { |nested_value| sanitize_for_postgres(nested_value) }
      when String
        value.delete("\u0000")
      else
        value
      end
    end

    def slack_timeout = positive_env("SLACK_DM_SYNC_TIMEOUT_SECONDS", 20)
    def rate_limit_max_retries = positive_env("SLACK_DM_SYNC_RATE_LIMIT_MAX_RETRIES", 3)
    def max_api_calls_per_run = positive_env("SLACK_DM_SYNC_MAX_API_CALLS_PER_RUN", 250)
    def max_run_seconds = positive_env("SLACK_DM_SYNC_MAX_RUN_SECONDS", 15.minutes.to_i)
    def list_page_size = positive_env("SLACK_DM_SYNC_LIST_PAGE_SIZE", 200)
    def list_max_pages = positive_env("SLACK_DM_SYNC_LIST_MAX_PAGES", 10)
    def members_page_size = positive_env("SLACK_DM_SYNC_MEMBERS_PAGE_SIZE", 200)
    def members_max_pages = positive_env("SLACK_DM_SYNC_MEMBERS_MAX_PAGES", 10)
    def history_page_size = positive_env("SLACK_DM_SYNC_HISTORY_PAGE_SIZE", 200)
    def history_max_pages = positive_env("SLACK_DM_SYNC_HISTORY_MAX_PAGES", 5)
    def replies_page_size = positive_env("SLACK_DM_SYNC_REPLIES_PAGE_SIZE", 200)
    def replies_max_pages = positive_env("SLACK_DM_SYNC_REPLIES_MAX_PAGES", 5)

    def positive_env(name, default)
      value = ConsoleEnv[name].to_i
      value.positive? ? value : default
    end
  end
end
