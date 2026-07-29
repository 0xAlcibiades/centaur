#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BOOTSTRAP_SCRIPT="$REPO_ROOT/contrib/scripts/bootstrap-k8s-secrets.sh"
test_dir="$(mktemp -d)"
trap 'rm -rf "$test_dir"' EXIT

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

# The bootstrap script invokes kubectl in a child Bash process. Exporting this
# function lets the test exercise its real create and patch branches without a
# cluster, while recording only the credential key name—not a fixture value.
kubectl() {
  local arg joined source_auth_key
  joined="$*"
  if [[ "$joined" != *'"op":"remove"'* && "$joined" != *" get secret "* ]]; then
    for arg in "$@"; do
      if [[ "$arg" == *GITHUB_TOKEN* ]]; then
        printf '%s\n' GITHUB_TOKEN >> "$FAKE_KUBECTL_LOG"
      fi
    done
  fi
  if [[ "$joined" == *" centaur-iron-proxy-source-auth "* ]]; then
    if [[ "$joined" == *"--from-file=OP_SERVICE_ACCOUNT_TOKEN="* ]]; then
      printf '%s\n' SOURCE_AUTH_SERVICE_ACCOUNT >> "$FAKE_KUBECTL_LOG"
    fi
    if [[ "$joined" == *"--from-file=OP_CONNECT_TOKEN="* ]]; then
      printf '%s\n' SOURCE_AUTH_CONNECT >> "$FAKE_KUBECTL_LOG"
    fi
  fi
  if [[ "$joined" == *" centaur-infra-env "* ]]; then
    if [[ "$joined" != *'"op":"remove"'* && "$joined" != *" get secret "* && "$joined" == *OP_SERVICE_ACCOUNT_TOKEN* ]]; then
      printf '%s\n' STATIC_SERVICE_ACCOUNT >> "$FAKE_KUBECTL_LOG"
    fi
    if [[ "$joined" != *'"op":"remove"'* && "$joined" != *" get secret "* && "$joined" == *OP_CONNECT_TOKEN* ]]; then
      printf '%s\n' STATIC_CONNECT >> "$FAKE_KUBECTL_LOG"
    fi
    if [[ "$joined" == *'"op":"remove"'* && "$joined" == *'/data/GITHUB_TOKEN'* ]]; then
      printf '%s\n' REMOVE_STATIC_GITHUB >> "$FAKE_KUBECTL_LOG"
    fi
    if [[ "$joined" == *'"op":"remove"'* && "$joined" == *'/data/OP_SERVICE_ACCOUNT_TOKEN'* ]]; then
      printf '%s\n' REMOVE_STATIC_SERVICE_ACCOUNT >> "$FAKE_KUBECTL_LOG"
    fi
    if [[ "$joined" == *'"op":"remove"'* && "$joined" == *'/data/OP_CONNECT_TOKEN'* ]]; then
      printf '%s\n' REMOVE_STATIC_CONNECT >> "$FAKE_KUBECTL_LOG"
    fi
  fi
  if [[ "$joined" == *" centaur-iron-proxy-source-auth "* &&
    "$joined" == *"--dry-run=client -o yaml"* ]]; then
    if [[ "$joined" == *"--from-file=OP_SERVICE_ACCOUNT_TOKEN="* ]]; then
      source_auth_key="OP_SERVICE_ACCOUNT_TOKEN"
    else
      source_auth_key="OP_CONNECT_TOKEN"
    fi
    printf 'apiVersion: v1\ndata:\n  %s: Zml4dHVyZQ==\nkind: Secret\n' "$source_auth_key"
    return 0
  fi
  if [[ "$joined" == *" patch secret centaur-iron-proxy-source-auth "* &&
    "$joined" == *"--patch-file /dev/stdin"* ]]; then
    cat > "$FAKE_KUBECTL_PATCH"
    return 0
  fi

  if [[ "$1" == "-n" ]]; then
    shift 2
  fi

  if [[ "$1" == "get" && "$2" == "secret" ]]; then
    if [[ "$3" == "centaur-infra-env" ]]; then
      if [[ "$FAKE_KUBECTL_MODE" == "existing" ]]; then
        if [[ " $* " == *" -o "* ]]; then
          printf '%s' present
        fi
        return 0
      fi
      return 1
    fi
    if [[ "$3" == "centaur-iron-proxy-source-auth" ]]; then
      if [[ "$FAKE_KUBECTL_MODE" == "existing" ]]; then
        return 0
      fi
      return 1
    fi
    # Treat the firewall Secrets as pre-existing so this focused test never
    # needs to create certificates.
    return 0
  fi

  return 0
}
export -f kubectl

run_case() {
  local mode="$1"
  local source="$2"
  local github_expected="$3"
  local source_auth_expected="$4"
  local log_file="$test_dir/${mode}-${source:-default}.log"
  local output_file="$test_dir/${mode}-${source:-default}.out"
  local patch_file="$test_dir/${mode}-${source:-default}.patch"
  local -a args=(--namespace test)

  if [[ -n "$source" ]]; then
    args+=(--secret-source "$source")
  fi
  : > "$log_file"
  : > "$patch_file"

  if ! FAKE_KUBECTL_MODE="$mode" \
    FAKE_KUBECTL_LOG="$log_file" \
    FAKE_KUBECTL_PATCH="$patch_file" \
    OP_SERVICE_ACCOUNT_TOKEN=fixture-service-account \
    OP_VAULT=fixture-vault \
    OP_CONNECT_TOKEN=fixture-connect-token \
    SLACK_BOT_TOKEN=fixture-slack-token \
    SLACK_SIGNING_SECRET=fixture-slack-signing-secret \
    SLACKBOT_API_KEY=fixture-slackbot-api-key \
    GITHUB_TOKEN=fixture-github-token \
    "$BOOTSTRAP_SCRIPT" "${args[@]}" > "$output_file" 2>&1; then
    cat "$output_file" >&2
    fail "$mode ${source:-default} bootstrap unexpectedly failed"
  fi

  case "$github_expected" in
    present)
      grep -Fxq GITHUB_TOKEN "$log_file" || fail "$mode ${source:-default} did not write GITHUB_TOKEN"
      ;;
    absent)
      ! grep -Fxq GITHUB_TOKEN "$log_file" || fail "$mode $source wrote GITHUB_TOKEN"
      ;;
    *) fail "unknown GitHub expectation: $github_expected" ;;
  esac

  case "$source_auth_expected" in
    none)
      ! grep -q '^SOURCE_AUTH_' "$log_file" || fail "$mode ${source:-default} unexpectedly wrote a source-auth Secret"
      ;;
    service)
      grep -Fxq SOURCE_AUTH_SERVICE_ACCOUNT "$log_file" || fail "$mode $source did not write the service-account source-auth key"
      ! grep -Fxq SOURCE_AUTH_CONNECT "$log_file" || fail "$mode $source wrote the Connect source-auth key"
      ;;
    connect)
      grep -Fxq SOURCE_AUTH_CONNECT "$log_file" || fail "$mode $source did not write the Connect source-auth key"
      ! grep -Fxq SOURCE_AUTH_SERVICE_ACCOUNT "$log_file" || fail "$mode $source wrote the service-account source-auth key"
      ;;
    *) fail "unknown source-auth expectation: $source_auth_expected" ;;
  esac

  if [[ "$source" == "onepassword" || "$source" == "onepassword-connect" ]]; then
    ! grep -q '^STATIC_\(SERVICE_ACCOUNT\|CONNECT\)$' "$log_file" || \
      fail "$mode $source wrote a source credential to centaur-infra-env"
    if [[ "$mode" == "existing" ]]; then
      grep -Fxq REMOVE_STATIC_GITHUB "$log_file" || fail "$mode $source did not scrub GITHUB_TOKEN"
      grep -Fxq REMOVE_STATIC_SERVICE_ACCOUNT "$log_file" || fail "$mode $source did not scrub OP_SERVICE_ACCOUNT_TOKEN"
      grep -Fxq REMOVE_STATIC_CONNECT "$log_file" || fail "$mode $source did not scrub OP_CONNECT_TOKEN"
    fi
  fi

  # Existing dedicated Secrets are merge-patched with only their selected key,
  # preserving unrelated operator-managed keys rather than replacing the Secret.
  if [[ "$mode" == "existing" && "$source" == "onepassword" ]]; then
    grep -Fxq 'data:' "$patch_file" || fail "existing onepassword patch omitted data map"
    grep -Fxq '  OP_SERVICE_ACCOUNT_TOKEN: Zml4dHVyZQ==' "$patch_file" || \
      fail "existing onepassword patch used the wrong source-auth key"
    [[ "$(wc -l < "$patch_file")" -eq 2 ]] || \
      fail "existing onepassword patch included fields outside the selected data key"
  fi
  if [[ "$mode" == "existing" && "$source" == "onepassword-connect" ]]; then
    grep -Fxq 'data:' "$patch_file" || fail "existing Connect patch omitted data map"
    grep -Fxq '  OP_CONNECT_TOKEN: Zml4dHVyZQ==' "$patch_file" || \
      fail "existing Connect patch used the wrong source-auth key"
    [[ "$(wc -l < "$patch_file")" -eq 2 ]] || \
      fail "existing Connect patch included fields outside the selected data key"
  fi
}

# The standalone script's default stays environment-backed for existing callers.
run_case create "" present none
run_case existing "" present none
run_case create env present none
run_case existing env present none
run_case create onepassword absent service
run_case existing onepassword absent service
run_case create onepassword-connect absent connect
run_case existing onepassword-connect absent connect

missing_token_log="$test_dir/existing-onepassword-missing-token.log"
: > "$missing_token_log"
if FAKE_KUBECTL_MODE=existing \
  FAKE_KUBECTL_LOG="$missing_token_log" \
  FAKE_KUBECTL_PATCH="$test_dir/existing-onepassword-missing-token.patch" \
  OP_VAULT=fixture-vault \
  SLACK_BOT_TOKEN=fixture-slack-token \
  SLACK_SIGNING_SECRET=fixture-slack-signing-secret \
  SLACKBOT_API_KEY=fixture-slackbot-api-key \
  "$BOOTSTRAP_SCRIPT" --secret-source onepassword \
  > "$test_dir/existing-onepassword-missing-token.out" 2>&1; then
  fail "existing onepassword bootstrap unexpectedly succeeded without its selected token"
fi
if grep -q '^REMOVE_STATIC_' "$missing_token_log"; then
  fail "missing source token removed a legacy static credential before dedicated auth was ready"
fi

bootstrap_recipe="$(
  awk '
    /^bootstrap-secrets \*args:$/ { capture = 1; next }
    capture && /^[^[:space:]]/ { exit }
    capture { print }
  ' "$REPO_ROOT/Justfile"
)"
if [[ "$bootstrap_recipe" != *'contrib/scripts/bootstrap-k8s-secrets.sh {{args}} "${bootstrap_args[@]}"'* ]]; then
  fail "Justfile must pass canonical source/auth settings after passthrough bootstrap flags"
fi

if FAKE_KUBECTL_MODE=existing \
  FAKE_KUBECTL_LOG="$test_dir/invalid.log" \
  "$BOOTSTRAP_SCRIPT" --secret-source invalid > "$test_dir/invalid.out" 2>&1; then
  fail "invalid secret source unexpectedly succeeded"
fi

if FAKE_KUBECTL_MODE=existing \
  FAKE_KUBECTL_LOG="$test_dir/collision.log" \
  "$BOOTSTRAP_SCRIPT" \
    --secret-source onepassword \
    --source-auth-secret-name centaur-infra-env \
    > "$test_dir/collision.out" 2>&1; then
  fail "source-auth/static Secret collision unexpectedly succeeded"
fi

for collision_name in \
  centaur-firewall-ca \
  centaur-firewall-ca-key \
  centaur-onepassword-connect-credentials; do
  if FAKE_KUBECTL_MODE=existing \
    FAKE_KUBECTL_LOG="$test_dir/collision.log" \
    "$BOOTSTRAP_SCRIPT" \
      --secret-source onepassword \
      --source-auth-secret-name "$collision_name" \
      > "$test_dir/collision.out" 2>&1; then
    fail "source-auth/$collision_name collision unexpectedly succeeded"
  fi
done

echo "bootstrap secret-source tests passed"
