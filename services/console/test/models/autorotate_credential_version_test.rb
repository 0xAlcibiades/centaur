require "test_helper"

class AutorotateCredentialVersionTest < ActiveSupport::TestCase
  def create_version(access_token: "access-secret")
    lease = AutorotateParentLease.create!(consumer: "bojack", lease_id: "lease-1", fence: 7,
                                          external_generation: 3, state: "active", expires_at: 1.hour.from_now)
    AutorotateCredentialVersion.create!(parent_lease: lease, access_token: access_token,
                                        broker_lease_id: "lease-1", provider_account_id: "account-1", external_generation: 3,
                                        expires_at: 30.minutes.from_now)
  end

  test "encrypts the token at rest and never serializes it" do
    version = create_version

    raw = AutorotateCredentialVersion.connection.select_value(
      "SELECT access_token FROM autorotate_credential_versions WHERE id = #{version.id}"
    )
    refute_includes raw, "access-secret"
    raw_account = AutorotateCredentialVersion.connection.select_value(
      "SELECT provider_account_id FROM autorotate_credential_versions WHERE id = #{version.id}"
    )
    refute_includes raw_account, "account-1"
    refute_includes version.as_json.to_json, "access-secret"
    refute_includes version.as_json.to_json, "account-1"
  end

  test "credential versions cannot be changed after creation" do
    version = create_version
    version.provider_account_id = "another-account"

    refute version.save
    assert_includes version.errors.full_messages.to_sentence, "immutable"
  end
end
