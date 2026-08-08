require "uri"

# Separate from observer/operator/control credentials. This client can only
# operate the broker's narrow proxy-parent lease projection.
class AutorotateProxyParentClient
  class Error < StandardError
    attr_reader :upstream_status, :upstream_code

    def initialize(message, upstream_status: nil, upstream_code: nil)
      super(message)
      @upstream_status = upstream_status
      @upstream_code = upstream_code
    end
  end

  MAX_RESPONSE_BYTES = 16 * 1024
  DEFAULT_TIMEOUT_SECONDS = 135
  DEFAULT_TTL_SECONDS = 300
  REQUEST_ID = /\A[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}\z/i

  def initialize(base_url: nil, proxy_parent_token: nil, http: nil, timeout: DEFAULT_TIMEOUT_SECONDS,
                 ttl_seconds: DEFAULT_TTL_SECONDS)
    @base_url = normalized_base_url(base_url || ConsoleEnv["AUTOROTATE_RUNTIME_URL"])
    @proxy_parent_token = (proxy_parent_token || ConsoleEnv["AUTOROTATE_RUNTIME_TOKEN"]).to_s.strip
    raise Error, "Autorotate proxy-parent token is not configured" if @proxy_parent_token.blank?

    @ttl_seconds = Integer(ttl_seconds)
    raise Error, "Autorotate proxy-parent TTL is not configured correctly" unless @ttl_seconds.positive?

    @api = HttpClient.new(http: http, open_timeout: timeout, read_timeout: timeout,
                          write_timeout: timeout, max_body_bytes: MAX_RESPONSE_BYTES)
  end

  def acquire(operation_id:)
    request_id = operation_id.to_s
    raise Error, "Autorotate acquire operation id must be a UUID" unless request_id.match?(REQUEST_ID)

    request_json(:post, "/v1/proxy-parent-leases/acquire", {
      parent_key: AutorotateParentLease::CONSUMER, client_id: "centaur-console", ttl_seconds: @ttl_seconds
    }, request_id: request_id)
  end

  def heartbeat(lease_id:, fence:, expected_generation:)
    request_json(:post, lease_path(lease_id, "heartbeat"), {
      fence: fence, expected_generation: expected_generation, ttl_seconds: @ttl_seconds
    })
  end

  def refresh(lease_id:, fence:, expected_generation:, operation_id:)
    request_id = operation_id.to_s
    raise Error, "Autorotate refresh operation id must be a UUID" unless request_id.match?(REQUEST_ID)

    request_json(:post, lease_path(lease_id, "refresh"), { fence: fence, expected_generation: expected_generation }, request_id: request_id)
  end

  def exhaust(lease_id:, fence:, expected_generation:)
    request_no_content(:post, lease_path(lease_id, "exhausted"), { fence: fence, expected_generation: expected_generation })
  end

  def release(lease_id:, fence:, expected_generation:)
    request_no_content(:delete, lease_path(lease_id), { fence: fence, expected_generation: expected_generation })
  end

  private

  def normalized_base_url(value)
    uri = URI.parse(value.to_s.strip)
    unless uri.is_a?(URI::HTTP) && uri.host.present? && uri.userinfo.nil? && uri.query.nil? && uri.fragment.nil?
      raise Error, "Autorotate URL is not configured correctly"
    end
    raise Error, "Autorotate URL must use HTTPS" if uri.scheme != "https" && !%w[localhost 127.0.0.1 ::1].include?(uri.host)

    uri.path = uri.path.to_s.delete_suffix("/")
    uri.to_s.delete_suffix("/")
  rescue URI::InvalidURIError
    raise Error, "Autorotate URL is not configured correctly"
  end

  def lease_path(lease_id, action = nil)
    suffix = action ? "/#{action}" : ""
    "/v1/proxy-parent-leases/#{URI.encode_uri_component(lease_id.to_s)}#{suffix}"
  end

  def request_json(method, path, payload, request_id: nil)
    response = request(method, path, payload, request_id: request_id)
    body = HttpClient.decode_json_body(response.body)
    raise Error, "Autorotate returned an invalid response" unless body.is_a?(Hash)
    return body if response.success?

    raise_error(response, body)
  rescue JSON::ParserError
    raise Error, "Autorotate returned an invalid response"
  end

  def request_no_content(method, path, payload)
    response = request(method, path, payload)
    return nil if response.success?

    body = HttpClient.decode_json_body(response.body)
    raise_error(response, body)
  rescue JSON::ParserError
    raise Error, "Autorotate request failed"
  end

  def request(method, path, payload, request_id: nil)
    headers = { "Authorization" => "Bearer #{@proxy_parent_token}", "Cache-Control" => "no-store" }
    headers["X-Request-Id"] = request_id if request_id
    url = "#{@base_url}#{path}"
    method == :delete ? @api.delete(url, json: payload, headers: headers) : @api.post(url, json: payload, headers: headers)
  end

  def raise_error(response, body)
    code = body["code"] if body.is_a?(Hash)
    raise Error.new("Autorotate returned HTTP #{response.status}", upstream_status: response.status,
                    upstream_code: code.to_s.presence)
  end
end
