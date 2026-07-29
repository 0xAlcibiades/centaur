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
if grep -Fq 'KUBERNETES_IRON_PROXY_SOURCE_AUTH_SECRET_' "$test_dir/environment.yaml"; then
  fail "environment source rendered a non-env source-auth reference"
fi

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
  KUBERNETES_IRON_PROXY_HARNESS_ENGINE; do
  if helm template "managed-env-override" "$CHART" \
    --set-string "apiRs.extraEnv.${managed_env}=override" \
    > "$test_dir/managed-env-override.yaml" \
    2> "$test_dir/managed-env-override.err"; then
    fail "apiRs.extraEnv unexpectedly overrode chart-managed $managed_env"
  fi
done

echo "iron-proxy source-auth chart render tests passed"
