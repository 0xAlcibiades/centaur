#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
CHART="$REPO_ROOT/contrib/chart"
test_dir="$(mktemp -d)"
trap 'rm -rf "$test_dir"' EXIT

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

require_env_value() {
  local file="$1"
  local name="$2"
  local value="$3"
  awk -v name="$name" -v value="value: \"$value\"" '
    $0 ~ "^[[:space:]]*- name: " name "$" { waiting = 1; next }
    waiting && index($0, value) { found = 1; exit }
    waiting && $0 ~ "^[[:space:]]*- name:" { waiting = 0 }
    END { exit(found ? 0 : 1) }
  ' "$file" || fail "$(basename "$file") does not bind $name to $value"
}

require_env_secret_key_count() {
  local file="$1"
  local name="$2"
  local key="$3"
  local expected="$4"
  local count
  count="$(awk -v name="$name" -v key="$key" '
    $0 ~ "^[[:space:]]*- name: " name "$" { waiting = 1; next }
    waiting && $1 == "key:" {
      rendered_key = $2
      gsub(/"/, "", rendered_key)
      if (rendered_key == key) count += 1
      waiting = 0
      next
    }
    waiting && $0 ~ "^[[:space:]]*- name:" { waiting = 0 }
    END { print count + 0 }
  ' "$file")"
  [[ "$count" == "$expected" ]] || fail "$(basename "$file") binds $name to $key $count times, expected $expected"
}

require_deployment_env_secret_key() {
  local file="$1"
  local deployment="$2"
  local name="$3"
  local key="$4"
  local count
  count="$(awk -v deployment="$deployment" -v name="$name" -v key="$key" '
    $0 == "kind: Deployment" {
      if (in_deployment && deployment_name == deployment) exit
      in_deployment = 1; deployment_name = ""; waiting = 0; next
    }
    in_deployment && deployment_name == "" && $1 == "name:" { deployment_name = $2; next }
    in_deployment && deployment_name == deployment && $0 ~ "^[[:space:]]*- name: " name "$" { waiting = 1; next }
    waiting && $1 == "key:" {
      rendered_key = $2
      gsub(/"/, "", rendered_key)
      if (rendered_key == key) count += 1
      waiting = 0
      next
    }
    waiting && $0 ~ "^[[:space:]]*- name:" { waiting = 0 }
    END { print count + 0 }
  ' "$file")"
  [[ "$count" == "1" ]] || fail "$(basename "$file") does not bind $name to $key exactly once on $deployment"
}

require_resource_verb() {
  local file="$1"
  local resource="$2"
  local verb="$3"
  awk -v resource="resources: [\"$resource\"]" -v verb="\"$verb\"" '
    index($0, resource) { waiting = 1; next }
    waiting && $1 == "verbs:" { found = index($0, verb) > 0; exit }
    END { exit(found ? 0 : 1) }
  ' "$file" || fail "$(basename "$file") does not grant $verb on $resource"
}

deployment_annotation() {
  local file="$1"
  local deployment="$2"
  local annotation="$3"
  awk -v deployment="$deployment" -v annotation="$annotation" '
    $0 == "kind: Deployment" { in_deployment = 1; name = ""; next }
    in_deployment && name == "" && $1 == "name:" { name = $2; next }
    in_deployment && name == deployment && $1 == annotation ":" { print $2; exit }
  ' "$file"
}

trace_checksum_deployments() {
  local file="$1"
  awk '
    $0 == "kind: Deployment" { in_deployment = 1; name = ""; next }
    in_deployment && name == "" && $1 == "name:" { name = $2; next }
    in_deployment && $1 == "checksum/trace-consent-secret:" { print name }
  ' "$file"
}

render() {
  local name="$1"
  shift
  helm template "$name" "$CHART" "$@" > "$test_dir/$name.yaml"
}

# The release workflow builds dependencies before this test. Keep direct local
# use just as convenient without committing downloaded chart artifacts.
if ! find "$CHART/charts" -maxdepth 1 -name 'connect-*.tgz' -print -quit | grep -q .; then
  helm_repo_dir="$test_dir/helm-repository"
  mkdir -p "$helm_repo_dir"
  helm repo add onepassword https://1password.github.io/connect-helm-charts \
    --repository-config "$helm_repo_dir/repositories.yaml" \
    --repository-cache "$helm_repo_dir/cache" >/dev/null
  helm dependency build "$CHART" \
    --repository-config "$helm_repo_dir/repositories.yaml" \
    --repository-cache "$helm_repo_dir/cache" >/dev/null
fi

render onepassword \
  --set ironProxy.secretSource=onepassword \
  --set-string ironProxy.sourceAuth.existingSecretName=proxy-source-auth \
  --set-string ironProxy.sourceAuth.serviceAccountTokenKey=service-account-key
require_env_value "$test_dir/onepassword.yaml" KUBERNETES_IRON_PROXY_SOURCE_AUTH_SECRET_NAME proxy-source-auth
require_env_value "$test_dir/onepassword.yaml" KUBERNETES_IRON_PROXY_SOURCE_AUTH_SECRET_KEY service-account-key

render onepassword-connect \
  --set ironProxy.secretSource=onepassword-connect \
  --set-string ironProxy.sourceAuth.existingSecretName=proxy-source-auth \
  --set-string ironProxy.sourceAuth.connectTokenKey=connect-token-key
require_env_value "$test_dir/onepassword-connect.yaml" KUBERNETES_IRON_PROXY_SOURCE_AUTH_SECRET_NAME proxy-source-auth
require_env_value "$test_dir/onepassword-connect.yaml" KUBERNETES_IRON_PROXY_SOURCE_AUTH_SECRET_KEY connect-token-key

render environment --set ironProxy.secretSource=env
require_resource_verb "$test_dir/environment.yaml" persistentvolumeclaims patch
if grep -Fq 'KUBERNETES_IRON_PROXY_SOURCE_AUTH_SECRET_' "$test_dir/environment.yaml"; then
  fail "environment source rendered a non-env source-auth reference"
fi

# Autorotate runtime credentials belong only to Console. api-rs receives the
# mode and registers a headerless fragment; a sandbox never sees this token.
render autorotate-runtime \
  --set sandbox.codexAuthMode=autorotate \
  --set console.enabled=true \
  --set-string slackbotv2.autorotate.url=https://autorotate.example.test \
  --set-string slackbotv2.autorotate.credentialsSecretName=centaur-autorotate
require_env_value "$test_dir/autorotate-runtime.yaml" CODEX_AUTH_MODE autorotate
require_deployment_env_secret_key \
  "$test_dir/autorotate-runtime.yaml" \
  autorotate-runtime-centaur-console \
  CENTAUR_CONSOLE_AUTOROTATE_RUNTIME_TOKEN \
  AUTOROTATE_PROXY_PARENT_TOKEN
require_deployment_env_secret_key \
  "$test_dir/autorotate-runtime.yaml" \
  autorotate-runtime-centaur-console-worker \
  CENTAUR_CONSOLE_AUTOROTATE_RUNTIME_TOKEN \
  AUTOROTATE_PROXY_PARENT_TOKEN
require_env_secret_key_count \
  "$test_dir/autorotate-runtime.yaml" \
  CENTAUR_CONSOLE_AUTOROTATE_RUNTIME_TOKEN \
  AUTOROTATE_PROXY_PARENT_TOKEN \
  2
if grep -Fq 'CENTAUR_CONSOLE_AUTOROTATE_OBSERVER_TOKEN' "$test_dir/autorotate-runtime.yaml"; then
  fail "autorotate runtime rendering unexpectedly mounted the observer token"
fi
if grep -Fq 'AUTOROTATE_API_TOKEN' "$test_dir/autorotate-runtime.yaml"; then
  fail "autorotate runtime rendering used the general runner API token"
fi

if helm template autorotate-runtime-missing-secret "$CHART" \
  --set sandbox.codexAuthMode=autorotate \
  --set console.enabled=true \
  --set-string slackbotv2.autorotate.url=https://autorotate.example.test \
  > "$test_dir/autorotate-runtime-missing-secret.yaml" \
  2> "$test_dir/autorotate-runtime-missing-secret.err"; then
  fail "autorotate mode rendered without a dedicated runtime Secret"
fi

# The trace-consent bearer must restart only the two workloads that consume it.
# A separate annotation keeps unrelated workloads insulated from its rotations.
render trace-consent-a \
  --set ironProxy.secretSource=env \
  --set console.enabled=true \
  --set-string secretManager.envPrefix=LEAN_ \
  --set-string slackbotv2.traceConsent.apiKeySecretName=trace-consent-a
render trace-consent-b \
  --set ironProxy.secretSource=env \
  --set console.enabled=true \
  --set-string secretManager.envPrefix=LEAN_ \
  --set-string slackbotv2.traceConsent.apiKeySecretName=trace-consent-b

require_env_secret_key_count "$test_dir/trace-consent-a.yaml" SLACKBOT_API_KEY LEAN_SLACKBOT_API_KEY 2

expected_trace_deployments=$'trace-consent-a-centaur-api-rs\ntrace-consent-a-centaur-slackbotv2'
if [[ "$(trace_checksum_deployments "$test_dir/trace-consent-a.yaml")" != "$expected_trace_deployments" ]]; then
  fail "trace-consent checksum did not render exclusively on api-rs and slackbotv2"
fi

for deployment in trace-consent-a-centaur-api-rs trace-consent-a-centaur-slackbotv2; do
  annotation_a="$(deployment_annotation "$test_dir/trace-consent-a.yaml" "$deployment" checksum/trace-consent-secret)"
  deployment_b="${deployment/trace-consent-a/trace-consent-b}"
  annotation_b="$(deployment_annotation "$test_dir/trace-consent-b.yaml" "$deployment_b" checksum/trace-consent-secret)"
  [[ -n "$annotation_a" && -n "$annotation_b" && "$annotation_a" != "$annotation_b" ]] || fail "$deployment trace-consent checksum did not change with the Secret identity"
done

console_checksum_a="$(deployment_annotation "$test_dir/trace-consent-a.yaml" trace-consent-a-centaur-console checksum/infra-secrets)"
console_checksum_b="$(deployment_annotation "$test_dir/trace-consent-b.yaml" trace-consent-b-centaur-console checksum/infra-secrets)"
[[ -n "$console_checksum_a" && "$console_checksum_a" == "$console_checksum_b" ]] || fail "trace-consent Secret unexpectedly changes console's infra checksum"

if helm template collision "$CHART" \
  --set ironProxy.secretSource=onepassword \
  --set-string secretManager.existingSecretName=proxy-source-auth \
  --set-string ironProxy.sourceAuth.existingSecretName=proxy-source-auth \
  > "$test_dir/collision.yaml" 2> "$test_dir/collision.err"; then
  fail "source-auth/static Secret collision unexpectedly rendered"
fi

if helm template bootstrap-collision "$CHART" \
  --set ironProxy.secretSource=onepassword \
  --set-string secrets.bootstrapSecretName=proxy-source-auth \
  --set-string ironProxy.sourceAuth.existingSecretName=proxy-source-auth \
  > "$test_dir/bootstrap-collision.yaml" 2> "$test_dir/bootstrap-collision.err"; then
  fail "source-auth/bootstrap Secret collision unexpectedly rendered"
fi

for ca_value in firewall.existingCaSecretName firewall.existingCaKeySecretName; do
  if helm template ca-collision "$CHART" \
    --set ironProxy.secretSource=onepassword \
    --set-string ironProxy.sourceAuth.existingSecretName=proxy-source-auth \
    --set-string "${ca_value}=proxy-source-auth" \
    > "$test_dir/ca-collision.yaml" 2> "$test_dir/ca-collision.err"; then
    fail "source-auth/$ca_value collision unexpectedly rendered"
  fi
done

for managed_env in \
  KUBERNETES_SANDBOX_IRON_PROXY_MODE \
  KUBERNETES_IRON_PROXY_IMAGE \
  KUBERNETES_IRON_PROXY_IMAGE_PULL_POLICY \
  KUBERNETES_IRON_PROXY_UPSTREAM_DENY_CIDRS \
  KUBERNETES_FIREWALL_CA_SECRET_NAME \
  KUBERNETES_FIREWALL_CA_KEY_SECRET_NAME \
  KUBERNETES_SECRET_ENV_NAME \
  KUBERNETES_BOOTSTRAP_SECRET_NAME \
  KUBERNETES_API_POD_LABEL_SELECTOR \
  FIREWALL_MANAGER_SECRET_SOURCE \
  FIREWALL_MANAGER_SECRET_TTL \
  KUBERNETES_IRON_PROXY_SOURCE_AUTH_SECRET_NAME \
  KUBERNETES_IRON_PROXY_SOURCE_AUTH_SECRET_KEY \
  OP_VAULT \
  KUBERNETES_OP_CONNECT_HOST \
  CODEX_AUTH_MODE \
  CLAUDE_CODE_AUTH_MODE \
  KUBERNETES_IRON_PROXY_HARNESS_ENGINE \
  KUBERNETES_IRON_PROXY_HARNESS_AUTH_MODE; do
  if helm template "managed-env-override" "$CHART" \
    --set-string "apiRs.extraEnv.${managed_env}=override" \
    > "$test_dir/managed-env-override.yaml" \
    2> "$test_dir/managed-env-override.err"; then
    fail "apiRs.extraEnv unexpectedly overrode chart-managed $managed_env"
  fi
done

if helm template sandbox-auth-mode-override "$CHART" \
  --set-string sandbox.extraEnv.CODEX_AUTH_MODE=access_token \
  > "$test_dir/sandbox-auth-mode-override.yaml" \
  2> "$test_dir/sandbox-auth-mode-override.err"; then
  fail "sandbox.extraEnv unexpectedly overrode chart-managed CODEX_AUTH_MODE"
fi

if helm template autorotate-without-infra-sync "$CHART" \
  --set sandbox.codexAuthMode=autorotate \
  --set apiRs.syncInfraSecrets=false \
  > "$test_dir/autorotate-without-infra-sync.yaml" \
  2> "$test_dir/autorotate-without-infra-sync.err"; then
  fail "autorotate mode rendered without infra-role reconciliation enabled"
fi

echo "iron-proxy source-auth chart render tests passed"
