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
    API_READ_TIMEOUT_SECONDS = 120
    SKIPPABLE_INGEST_STATUSES = [ 400, 413, 422 ].freeze
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
    end

    def initialize(credential, api_client: nil, slack_api_http: nil, http_client: nil)
      @credential = credential
      @api_client = api_client || CentaurApiClient.new(read_timeout: API_READ_TIMEOUT_SECONDS)
      @slack_api_http = slack_api_http || self.class.slack_api_http
      @http_client = http_client
      @run_id = "sdms_#{SecureRandom.hex(16)}"
      @messages_fetched = 0
      @replies_fetched = 0
      @messages_upserted = 0
      @replies_upserted = 0
    end

    def call(starting_conversation_id: nil, conversation_state: {}, deadline: nil)
      auth = slack_api(AUTH_TEST_ENDPOINT)
      home_team_id = auth.fetch("team_id")
      source_user_id = auth["user_id"].presence || @credential.provider_subject.to_s
      checkpoints = load_checkpoints(home_team_id)
      conversations = list_conversations.sort_by { |conversation| conversation.fetch("id") }
      conversations = remaining_conversations(conversations, starting_conversation_id)
      run = empty_batch(home_team_id, source_user_id).fetch(:run)
      run[:conversations_requested] = conversations.length
      checkpointed_conversation_id = starting_conversation_id
      active_conversation_state = resumable_conversation_state(
        conversation_state,
        starting_conversation_id
      )

      conversations.each_with_index do |conversation, index|
        if deadline && Time.current >= deadline
          finish_run(run, status: "partial")
          return false
        end

        conversation_id = conversation.fetch("id")
        if conversation_id != checkpointed_conversation_id
          active_conversation_state = {}
          yield conversation_id, active_conversation_state if block_given?
          checkpointed_conversation_id = conversation_id
        end

        begin
          completed = sync_conversation(
            conversation,
            home_team_id,
            source_user_id,
            checkpoints[conversation_id],
            active_conversation_state,
            run,
            deadline
          ) do |new_state|
            active_conversation_state = new_state
            yield conversation_id, new_state if block_given?
          end
          unless completed
            finish_run(run, status: "partial")
            return false
          end
        rescue CentaurApiClient::Error => e
          raise unless SKIPPABLE_INGEST_STATUSES.include?(e.status)

          run[:conversations_failed] += 1
          Rails.logger.warn do
            "Slack DM ingest rejected conversation #{conversation_id}: " \
              "status=#{e.status} error=#{e.message}"
          end
        rescue StandardError => e
          raise if e.is_a?(SlackApi::RetryableError)
          raise if Rails.env.test?

          run[:conversations_failed] += 1
          Rails.logger.warn do
            "slack DM sync failed for conversation #{conversation_id}: #{e.class}: #{e.message}"
          end
        end

        next_conversation_id = conversations[index + 1]&.fetch("id")
        if next_conversation_id
          active_conversation_state = {}
          yield next_conversation_id, active_conversation_state if block_given?
          checkpointed_conversation_id = next_conversation_id
        end
      end

      status = run[:conversations_failed].positive? ? "partial" : "completed"
      finish_run(run, status: status)
      true
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

    def empty_batch(home_team_id, source_user_id, replace_memberships: true)
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
        replace_memberships: replace_memberships,
        conversations: [],
        members: [],
        messages: [],
        attachments: [],
        checkpoints: []
      }
    end

    def sync_conversation(conversation, home_team_id, source_user_id, checkpoint,
                          conversation_state, run, deadline)
      state = normalized_conversation_state(conversation_state, checkpoint)
      yield state if conversation_state.blank?
      pages_processed = Hash.new(0)

      loop do
        return false if deadline && Time.current >= deadline

        case state.fetch("phase")
        when "members"
          return false if pages_processed["members"] >= members_max_pages

          state = sync_members_page(conversation, state)
          pages_processed["members"] += 1 unless conversation["is_im"]
          yield state
        when "members_ingest"
          batch = empty_batch(home_team_id, source_user_id)
          normalize_conversation(conversation, home_team_id, batch)
          normalize_members(conversation, home_team_id, state.fetch("member_ids"), batch)
          ingest_sync_batch(batch, run)
          state = {
            "phase" => "history",
            "history_cursor" => nil,
            "oldest_ts" => state["oldest_ts"],
            "max_message_ts" => state["max_message_ts"]
          }
          yield state
        when "history"
          return false if pages_processed["history"] >= history_max_pages

          state = sync_history_page(
            conversation,
            home_team_id,
            source_user_id,
            state,
            run
          )
          pages_processed["history"] += 1
          yield state
        when "replies"
          return false if pages_processed["replies"] >= replies_max_pages

          state = sync_replies_page(
            conversation,
            home_team_id,
            source_user_id,
            state,
            run
          )
          pages_processed["replies"] += 1
          yield state
        when "complete"
          finish_conversation(conversation, home_team_id, source_user_id, state, run)
          return true
        else
          raise SlackApi::Error,
                "Invalid Slack conversation sync phase #{state['phase'].inspect}"
        end
      end
    end

    def normalized_conversation_state(conversation_state, checkpoint)
      state = conversation_state.to_h.deep_stringify_keys
      return state unless state.empty?

      {
        "phase" => "members",
        "members_cursor" => nil,
        "member_ids" => [],
        "oldest_ts" => checkpoint,
        "max_message_ts" => checkpoint
      }
    end

    def resumable_conversation_state(conversation_state, conversation_id)
      state = conversation_state.to_h.deep_stringify_keys
      broker_credential_id = state.delete("_broker_credential_id")
      state_conversation_id = state.delete("_conversation_id")
      has_identity = broker_credential_id.present? || state_conversation_id.present?
      return state unless has_identity
      return {} unless broker_credential_id == @credential.oid
      return {} unless state_conversation_id == conversation_id

      state
    end

    def sync_members_page(conversation, state)
      if conversation["is_im"] && conversation["user"].present?
        member_ids = [ conversation["user"], @credential.provider_subject ].compact_blank.uniq
        return state.merge("phase" => "members_ingest", "member_ids" => member_ids)
      end

      page = slack_api(
        CONVERSATIONS_MEMBERS_ENDPOINT,
        {
          "channel" => conversation.fetch("id"),
          "limit" => members_page_size,
          "cursor" => state["members_cursor"]
        }.compact
      )
      member_ids = (Array(state["member_ids"]) + Array(page["members"])).compact
      cursor = page.dig("response_metadata", "next_cursor").presence
      if cursor
        state.merge("members_cursor" => cursor, "member_ids" => member_ids.uniq)
      else
        member_ids << @credential.provider_subject if @credential.provider_subject.present?
        state.merge(
          "phase" => "members_ingest",
          "members_cursor" => nil,
          "member_ids" => member_ids.uniq
        )
      end
    end

    def sync_history_page(conversation, home_team_id, source_user_id, state, run)
      conversation_id = conversation.fetch("id")
      params = history_params(conversation_id, state["oldest_ts"])
      params["cursor"] = state["history_cursor"] if state["history_cursor"].present?
      page = slack_api(CONVERSATIONS_HISTORY_ENDPOINT, params)
      batch = empty_batch(home_team_id, source_user_id, replace_memberships: false)
      max_message_ts = state["max_message_ts"]
      pending_threads = []

      Array(page["messages"]).each do |message|
        @messages_fetched += 1
        max_message_ts = max_slack_ts(max_message_ts, message["ts"])
        normalize_message(message, home_team_id, conversation_id, nil, batch)
        normalize_files(message, home_team_id, conversation_id, batch)
        if message["reply_count"].to_i.positive?
          pending_threads << {
            "thread_ts" => message["thread_ts"].presence || message.fetch("ts"),
            "root_message_ts" => message.fetch("ts")
          }
        end
      end

      ingest_sync_batch(batch, run)
      history_cursor = page.dig("response_metadata", "next_cursor").presence
      if pending_threads.any?
        {
          "phase" => "replies",
          "history_cursor" => history_cursor,
          "oldest_ts" => state["oldest_ts"],
          "max_message_ts" => max_message_ts,
          "pending_threads" => pending_threads,
          "replies_cursor" => nil
        }
      elsif history_cursor
        {
          "phase" => "history",
          "history_cursor" => history_cursor,
          "oldest_ts" => state["oldest_ts"],
          "max_message_ts" => max_message_ts
        }
      else
        {
          "phase" => "complete",
          "oldest_ts" => state["oldest_ts"],
          "max_message_ts" => max_message_ts
        }
      end
    end

    def sync_replies_page(conversation, home_team_id, source_user_id, state, run)
      conversation_id = conversation.fetch("id")
      pending_threads = Array(state["pending_threads"])
      thread = pending_threads.first || raise(
        SlackApi::Error,
        "Slack replies state has no pending thread for #{conversation_id}"
      )
      params = {
        "channel" => conversation_id,
        "ts" => thread.fetch("thread_ts"),
        "limit" => replies_page_size,
        "cursor" => state["replies_cursor"]
      }.compact
      page = slack_api(CONVERSATIONS_REPLIES_ENDPOINT, params)
      batch = empty_batch(home_team_id, source_user_id, replace_memberships: false)

      Array(page["messages"]).each do |reply|
        next if reply["ts"] == thread.fetch("root_message_ts")

        @replies_fetched += 1
        normalize_message(
          reply,
          home_team_id,
          conversation_id,
          thread.fetch("root_message_ts"),
          batch
        )
        normalize_files(reply, home_team_id, conversation_id, batch)
      end

      ingest_sync_batch(batch, run)
      replies_cursor = page.dig("response_metadata", "next_cursor").presence
      return state.merge("replies_cursor" => replies_cursor) if replies_cursor

      pending_threads = pending_threads.drop(1)
      if pending_threads.any?
        state.merge("pending_threads" => pending_threads, "replies_cursor" => nil)
      elsif state["history_cursor"].present?
        {
          "phase" => "history",
          "history_cursor" => state["history_cursor"],
          "oldest_ts" => state["oldest_ts"],
          "max_message_ts" => state["max_message_ts"]
        }
      else
        {
          "phase" => "complete",
          "oldest_ts" => state["oldest_ts"],
          "max_message_ts" => state["max_message_ts"]
        }
      end
    end

    def finish_conversation(conversation, home_team_id, source_user_id, state, run)
      batch = empty_batch(home_team_id, source_user_id, replace_memberships: false)
      batch[:checkpoints] << {
        broker_credential_id: @credential.oid,
        home_team_id: home_team_id,
        conversation_id: conversation.fetch("id"),
        watermark_ts: state["max_message_ts"],
        last_run_id: @run_id
      }
      ingest_sync_batch(batch, run, conversation_completed: true)
    end

    def remaining_conversations(conversations, starting_conversation_id)
      return conversations if starting_conversation_id.blank?

      conversations.drop_while do |conversation|
        conversation.fetch("id") < starting_conversation_id
      end
    end

    def ingest_sync_batch(batch, run, conversation_completed: false)
      batch_replies_upserted = batch[:messages].count do |message|
        message[:parent_message_ts].present?
      end
      messages_upserted = @messages_upserted + batch[:messages].length
      replies_upserted = @replies_upserted + batch_replies_upserted
      conversations_synced = run[:conversations_synced] + (conversation_completed ? 1 : 0)
      batch[:run] = run.merge(
        conversations_synced: conversations_synced,
        messages_fetched: @messages_fetched,
        messages_upserted: messages_upserted,
        replies_fetched: @replies_fetched,
        replies_upserted: replies_upserted,
        finished: false
      )
      @api_client.ingest_slack_dm_sync_batch(sanitize_for_postgres(batch))
      run[:conversations_synced] = conversations_synced
      @messages_upserted = messages_upserted
      @replies_upserted = replies_upserted
    end

    def finish_run(run, status:)
      update_run_counts(run)
      batch = {
        run: run.merge(status: status, finished: true),
        replace_memberships: false,
        conversations: [],
        members: [],
        messages: [],
        attachments: [],
        checkpoints: []
      }
      @api_client.ingest_slack_dm_sync_batch(sanitize_for_postgres(batch))
    end

    def update_run_counts(run)
      run[:messages_fetched] = @messages_fetched
      run[:messages_upserted] = @messages_upserted
      run[:replies_fetched] = @replies_fetched
      run[:replies_upserted] = @replies_upserted
    end

    def list_conversations
      types = self.class.supported_conversation_types(@credential.scopes)
      raise SlackApi::Error, "Slack credential has no supported conversation scopes" if types.empty?

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

    def normalize_members(conversation, home_team_id, member_ids, batch)
      conversation_id = conversation.fetch("id")
      member_ids.each do |member_id|
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

    def conversation_type(conversation)
      return "mpim" if conversation["is_mpim"]
      return "im" if conversation["is_im"]
      return "private_channel" if conversation["is_private"]

      raise SlackApi::Error, "Unsupported Slack conversation #{conversation['id']}"
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
      if @slack_api_http
        return @slack_api_http.call(
          endpoint: endpoint,
          params: params,
          access_token: @credential.access_token
        )
      end

      http_client = @http_client || HttpClient.new(open_timeout: slack_timeout, read_timeout: slack_timeout)
      response = http_client.get(
        endpoint,
        params: params,
        headers: { "Authorization" => "Bearer #{@credential.access_token}" }
      )
      SlackApi.parse_response!(response, max_rate_limit_wait: nil)
    rescue Socket::ResolutionError => e
      raise SlackApi::TransientError.new(
        "Slack API hostname resolution failed: #{e.message}",
        retry_after: SlackApi::DEFAULT_TRANSIENT_RETRY_AFTER_SECONDS,
        code: "hostname_resolution_failed"
      )
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
