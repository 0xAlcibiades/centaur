module SlackDm
  class InventoryCredential
    def initialize(credential, api_client: CentaurApiClient.new, slack_client: nil)
      @credential = credential
      @api_client = api_client
      @slack_client = slack_client || SlackDm::ApiClient.new(credential)
    end

    def call
      auth = @slack_client.auth_test
      home_team_id = auth.fetch("team_id")
      persist_auth_identity(auth)
      conversations = list_conversations
      normalized_conversations = conversations.map do |conversation|
        normalize_conversation(conversation, home_team_id)
      end

      @api_client.ingest_slack_dm_sync_batch(
        run: nil,
        replace_memberships: false,
        conversations: normalized_conversations,
        members: [],
        messages: [],
        attachments: [],
        checkpoints: []
      )
      SlackDm::SyncLedger.refresh_inventory!(
        credential: @credential,
        home_team_id: home_team_id,
        conversations: normalized_conversations
      )
      conversations.length
    end

    private

    def list_conversations
      types = SlackDm::SyncCredential.supported_conversation_types(@credential.scopes)
      raise SlackDm::ApiClient::SlackApiError, "Slack credential has no supported conversation scopes" if types.empty?

      pages = each_page(
        { "types" => types.join(","), "exclude_archived" => "false", "limit" => page_size },
        max_pages: max_pages
      )
      pages.flat_map { |page| Array(page["channels"]) }
    end

    def each_page(params, max_pages:)
      pages = []
      cursor = nil
      max_pages.times do |index|
        page = @slack_client.conversations_list(params.merge("cursor" => cursor).compact)
        pages << page
        cursor = page.dig("response_metadata", "next_cursor").presence
        break unless cursor
        if index == max_pages - 1
          raise SlackDm::ApiClient::SlackApiError, "Slack conversation inventory pagination truncated"
        end
      end
      pages
    end

    def normalize_conversation(conversation, home_team_id)
      {
        home_team_id: home_team_id,
        conversation_id: conversation.fetch("id"),
        conversation_type: conversation_type(conversation),
        is_archived: conversation["is_archived"] == true,
        is_ext_shared: conversation["is_ext_shared"] == true,
        raw_payload: conversation
      }
    end

    def conversation_type(conversation)
      return "mpim" if conversation["is_mpim"]
      return "im" if conversation["is_im"]
      return "private_channel" if conversation["is_private"]

      raise SlackDm::ApiClient::SlackApiError, "Unsupported Slack conversation #{conversation['id']}"
    end

    def persist_auth_identity(auth)
      attributes = {}
      home_team_id = auth.fetch("team_id")
      if @credential.labels&.[]("slack_team_id") != home_team_id
        attributes[:labels] = (@credential.labels || {}).merge("slack_team_id" => home_team_id)
      end
      if @credential.provider_subject.blank? && auth["user_id"].present?
        attributes[:provider_subject] = auth["user_id"]
      end
      @credential.update!(attributes) if attributes.any?
    end

    def page_size = positive_env("SLACK_DM_SYNC_LIST_PAGE_SIZE", 200)
    def max_pages = positive_env("SLACK_DM_SYNC_LIST_MAX_PAGES", 10)

    def positive_env(name, default)
      value = ConsoleEnv[name].to_i
      value.positive? ? value : default
    end
  end
end
