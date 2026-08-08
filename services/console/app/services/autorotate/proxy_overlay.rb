module Autorotate
  class ProxyOverlay
    def self.apply(proxy, config, now: Time.current)
      pin = pin_for(proxy)
      return config unless pin&.usable_for_proxy?(proxy, now: now)

      version = pin.credential_version
      config.deep_dup.tap do |copy|
        copy["secrets"] = Array(copy["secrets"]) + [
          {
            "source" => { "type" => "control_plane", "value" => version.access_token },
            "inject" => { "header" => "Authorization", "formatter" => "Bearer {{ .Value }}" },
            "rules" => chatgpt_rules
          },
          {
            "source" => { "type" => "control_plane", "value" => version.provider_account_id },
            "inject" => { "header" => "chatgpt-account-id", "formatter" => "{{ .Value }}" },
            "rules" => chatgpt_rules
          }
        ]
      end
    end

    def self.pin_for(proxy)
      oid = proxy.labels&.fetch(AutorotateExecutionPin::PROXY_PIN_LABEL, nil)
      return if oid.blank?

      pin = AutorotateExecutionPin.includes(:parent_lease, :credential_version).find_by_oid(oid)
      return unless pin
      pin.bind_proxy!(proxy) if pin.proxy_id.nil?
      return unless pin.proxy_id == proxy.id

      pin
    rescue ActiveRecord::RecordInvalid
      nil
    end

    def self.chatgpt_rules
      [ { "host" => "chatgpt.com" } ]
    end
    private_class_method :chatgpt_rules
  end
end
