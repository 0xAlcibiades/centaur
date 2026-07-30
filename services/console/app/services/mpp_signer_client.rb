require "net/http"

class MppSignerClient
  class Error < StandardError; end

  def initialize(url: ENV["MPP_SIGNER_URL"], token: ENV["MPP_SIGNER_TOKEN"])
    @base_url = URI.parse(url.to_s)
    @token = token.to_s
    validate_configuration!
  end

  def create_access_key
    post("/v1/access_keys", {})
  end

  def authorize_access_key(key_handle:, wallet_address:, key_authorization:)
    post("/v1/access_keys/#{CGI.escapeURIComponent(key_handle)}/authorize", {
      wallet_address: wallet_address,
      key_authorization: key_authorization
    })
  end

  def payment_credential(key_handle:, challenge:, host:, method:, path:)
    post("/v1/credentials", {
      key_handle: key_handle,
      challenge: challenge,
      host: host,
      method: method,
      path: path
    })
  end

  private

  def post(path, payload)
    uri = @base_url + path
    request = Net::HTTP::Post.new(uri)
    request["Authorization"] = "Bearer #{@token}"
    request["Content-Type"] = "application/json"
    request.body = JSON.generate(payload)
    response = Net::HTTP.start(uri.hostname, uri.port, use_ssl: uri.scheme == "https", open_timeout: 5, read_timeout: 30) do |http|
      http.request(request)
    end
    raise Error, "MPP signer rejected request with status #{response.code}" unless response.is_a?(Net::HTTPSuccess)

    JSON.parse(response.body)
  rescue JSON::ParserError, SocketError, SystemCallError, Timeout::Error => e
    raise Error, "MPP signer request failed: #{e.class}"
  end

  def validate_configuration!
    raise Error, "MPP signer is not configured" if @base_url.host.blank? || @token.blank?
    return if @base_url.scheme == "https"
    return if @base_url.scheme == "http" && [ "localhost", "127.0.0.1", "::1" ].include?(@base_url.hostname)

    raise Error, "MPP signer URL must use HTTPS unless it is loopback"
  end
end
