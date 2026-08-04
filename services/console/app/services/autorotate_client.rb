require "uri"

class AutorotateClient
  class Error < StandardError
    attr_reader :upstream_status, :upstream_code

    def initialize(message, upstream_status: nil, upstream_code: nil)
      super(message)
      @upstream_status = upstream_status
      @upstream_code = upstream_code
    end
  end

  STATUS_FIELDS = %w[
    generated_at total healthy available limited login_required disabled leased
    removed next_available_at pending_enrollments
  ].freeze
  MAX_RESPONSE_BYTES = 64 * 1024
  DEFAULT_TIMEOUT_SECONDS = 10

  def initialize(base_url: nil, observer_token: nil, http: nil, timeout: DEFAULT_TIMEOUT_SECONDS)
    @base_url = normalized_base_url(base_url || ConsoleEnv["AUTOROTATE_URL"])
    @observer_token = (observer_token || ConsoleEnv["AUTOROTATE_OBSERVER_TOKEN"]).to_s.strip
    raise Error, "Autorotate observer token is not configured" if @observer_token.blank?

    @api = HttpClient.new(
      http: http,
      open_timeout: timeout,
      read_timeout: timeout,
      write_timeout: timeout,
      max_body_bytes: MAX_RESPONSE_BYTES
    )
  end

  def status
    filter_payload(request(:get, "/v1/status"), STATUS_FIELDS)
  end

  private

  def normalized_base_url(value)
    uri = URI.parse(value.to_s.strip)
    unless uri.is_a?(URI::HTTP) && uri.host.present? && uri.userinfo.nil? &&
           uri.query.nil? && uri.fragment.nil?
      raise Error, "Autorotate URL is not configured correctly"
    end
    if uri.scheme != "https" && !local_host?(uri.host)
      raise Error, "Autorotate URL must use HTTPS"
    end

    uri.path = uri.path.to_s.delete_suffix("/")
    uri.to_s.delete_suffix("/")
  rescue URI::InvalidURIError
    raise Error, "Autorotate URL is not configured correctly"
  end

  def local_host?(host)
    host == "localhost" || host == "127.0.0.1" || host == "::1"
  end

  def request(method, path, payload = nil)
    response = @api.request(
      method: method,
      url: "#{@base_url}#{path}",
      json: payload,
      headers: {
        "Accept" => "application/json",
        "Authorization" => "Bearer #{@observer_token}"
      }
    )
    body = decode_object(response.body)
    return body if response.success?

    code = body.dig("error", "code") if body["error"].is_a?(Hash)
    raise Error.new(
      "Autorotate returned HTTP #{response.status}",
      upstream_status: response.status,
      upstream_code: code.to_s.presence
    )
  rescue Error
    raise
  rescue StandardError
    raise Error, "Autorotate request failed"
  end

  def decode_object(body)
    parsed = HttpClient.decode_json_body(body)
    raise Error, "Autorotate returned an invalid response" unless parsed.is_a?(Hash)

    parsed
  rescue JSON::ParserError
    raise Error, "Autorotate returned an invalid response"
  end

  def filter_payload(payload, allowed_fields)
    payload.slice(*allowed_fields)
  end
end
