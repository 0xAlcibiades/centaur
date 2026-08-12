module SlackDm
  class SyncConversation
    LostClaim = Class.new(StandardError)

    def initialize(ledger, claim_token:, api_client: CentaurApiClient.new, slack_client: nil)
      @ledger = ledger
      @claim_token = claim_token
      @credential = ledger.broker_credential
      @api_client = api_client
      @slack_client = slack_client || SlackDm::ApiClient.new(@credential)
      @run_id = "sdms_#{SecureRandom.hex(16)}"
      @messages_fetched = 0
      @replies_fetched = 0
    end

    def call
      raise LostClaim, "Slack conversation is no longer active" unless @ledger.active?

      batch = empty_batch
      normalize_conversation(batch)
      normalize_members(batch)
      watermark_ts = sync_history(batch)
      finish_batch(batch)
      @api_client.ingest_slack_dm_sync_batch(sanitize_for_postgres(batch))
      unless @ledger.complete_claim!(claim_token: @claim_token, watermark_ts: watermark_ts)
        raise LostClaim, "Slack conversation sync claim expired before completion"
      end
    end

    private

    def empty_batch
      {
        run: {
          run_id: @run_id,
          mode: "incremental",
          status: "running",
          broker_credential_id: @credential.oid,
          source_user_id: @credential.provider_subject.to_s,
          home_team_id: @ledger.home_team_id,
          conversations_requested: 1,
          conversations_synced: 0,
          conversations_failed: 0,
          messages_fetched: 0,
          messages_upserted: 0,
          replies_fetched: 0,
          replies_upserted: 0,
          metadata: {
            oauth_app_slug: @credential.oauth_app&.slug,
            credential_id: @credential.oid,
            ledger_id: @ledger.id
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

    def normalize_conversation(batch)
      batch[:conversations] << {
        home_team_id: @ledger.home_team_id,
        conversation_id: @ledger.conversation_id,
        conversation_type: @ledger.conversation_type,
        is_archived: @ledger.is_archived,
        is_ext_shared: @ledger.is_ext_shared,
        raw_payload: @ledger.raw_payload
      }
    end

    def normalize_members(batch)
      conversation_members.each do |member_id|
        batch[:members] << {
          home_team_id: @ledger.home_team_id,
          conversation_id: @ledger.conversation_id,
          user_id: member_id,
          is_external: false,
          is_current_member: true,
          raw_payload: { source: "conversations.members" }
        }
      end
    end

    def conversation_members
      if @ledger.conversation_type == "im" && @ledger.raw_payload["user"].present?
        return [ @ledger.raw_payload["user"], @credential.provider_subject ].compact_blank.uniq
      end

      pages = each_page do |cursor|
        @slack_client.conversations_members(
          { "channel" => @ledger.conversation_id, "limit" => members_page_size, "cursor" => cursor }.compact
        )
      end
      (pages.flat_map { |page| Array(page["members"]) } + [ @credential.provider_subject ]).compact_blank.uniq
    end

    def sync_history(batch)
      max_message_ts = @ledger.watermark_ts
      pages = each_page do |cursor|
        params = {
          "channel" => @ledger.conversation_id,
          "limit" => history_page_size,
          "cursor" => cursor
        }.compact
        if @ledger.watermark_ts.present?
          params["oldest"] = @ledger.watermark_ts
          params["inclusive"] = "false"
        end
        @slack_client.conversations_history(params)
      end

      pages.each do |page|
        Array(page["messages"]).each do |message|
          @messages_fetched += 1
          max_message_ts = max_slack_ts(max_message_ts, message["ts"])
          normalize_message(message, nil, batch)
          normalize_files(message, batch)
          sync_replies(message, batch) if message["reply_count"].to_i.positive?
        end
      end

      batch[:checkpoints] << {
        broker_credential_id: @credential.oid,
        home_team_id: @ledger.home_team_id,
        conversation_id: @ledger.conversation_id,
        watermark_ts: max_message_ts,
        last_run_id: @run_id
      }
      max_message_ts
    end

    def sync_replies(root_message, batch)
      thread_ts = root_message["thread_ts"].presence || root_message["ts"]
      pages = each_page do |cursor|
        @slack_client.conversations_replies(
          {
            "channel" => @ledger.conversation_id,
            "ts" => thread_ts,
            "limit" => replies_page_size,
            "cursor" => cursor
          }.compact
        )
      end
      pages.each do |page|
        Array(page["messages"]).each do |reply|
          next if reply["ts"] == root_message["ts"]

          @replies_fetched += 1
          normalize_message(reply, root_message["ts"], batch)
          normalize_files(reply, batch)
        end
      end
    end

    def each_page
      pages = []
      cursor = nil
      loop do
        renew_claim!
        page = yield cursor
        pages << page
        cursor = page.dig("response_metadata", "next_cursor").presence
        break unless cursor
      end
      pages
    end

    def renew_claim!
      return if @ledger.renew_claim!(claim_token: @claim_token)

      raise LostClaim, "Slack conversation sync claim is no longer owned"
    end

    def normalize_message(message, parent_message_ts, batch)
      ts = message.fetch("ts")
      thread_ts = message["thread_ts"].presence || parent_message_ts
      batch[:messages] << {
        home_team_id: @ledger.home_team_id,
        conversation_id: @ledger.conversation_id,
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

    def normalize_files(message, batch)
      Array(message["files"]).each do |file|
        next if file["id"].blank?

        batch[:attachments] << {
          home_team_id: @ledger.home_team_id,
          conversation_id: @ledger.conversation_id,
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

    def finish_batch(batch)
      batch[:run].merge!(
        status: "completed",
        conversations_synced: 1,
        messages_fetched: @messages_fetched,
        messages_upserted: batch[:messages].length,
        replies_fetched: @replies_fetched,
        replies_upserted: batch[:messages].count { |message| message[:parent_message_ts].present? },
        finished: true
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
        value.to_h { |key, nested| [ sanitize_for_postgres(key), sanitize_for_postgres(nested) ] }
      when Array
        value.map { |nested| sanitize_for_postgres(nested) }
      when String
        value.delete("\u0000")
      else
        value
      end
    end

    def members_page_size = positive_env("SLACK_DM_SYNC_MEMBERS_PAGE_SIZE", 200)
    def history_page_size = positive_env("SLACK_DM_SYNC_HISTORY_PAGE_SIZE", 200)
    def replies_page_size = positive_env("SLACK_DM_SYNC_REPLIES_PAGE_SIZE", 200)

    def positive_env(name, default)
      value = ConsoleEnv[name].to_i
      value.positive? ? value : default
    end
  end
end
