#!/bin/bash
set -e

HOME_DIR="$(eval echo ~)"
FIREWALL_HOSTNAME="${FIREWALL_HOST:-firewall}"
STATE_DIR="${CENTAUR_STATE_DIR:-$HOME_DIR/state}"

if [ -d "$STATE_DIR" ] && [ -w "$STATE_DIR" ]; then
    mkdir -p "$STATE_DIR/workspace" "$STATE_DIR/uploads" "$STATE_DIR/branches" "$STATE_DIR/codex" "$STATE_DIR/claude"
    rm -rf "$HOME_DIR/.codex" "$HOME_DIR/.claude" "$HOME_DIR/uploads" "$HOME_DIR/branches"
    ln -s "$STATE_DIR/codex" "$HOME_DIR/.codex"
    ln -s "$STATE_DIR/claude" "$HOME_DIR/.claude"
    ln -s "$STATE_DIR/uploads" "$HOME_DIR/uploads"
    ln -s "$STATE_DIR/branches" "$HOME_DIR/branches"
    export CENTAUR_PERSISTENT_STATE=1
fi

mkdir -p "$HOME_DIR/.config/amp"

if [ "${SCCACHE_ENABLE:-}" = "1" ] && command -v sccache >/dev/null 2>&1; then
    export SCCACHE_DIR="${SCCACHE_DIR:-$HOME_DIR/.cache/sccache}"
    mkdir -p "$SCCACHE_DIR"
    export RUSTC_WRAPPER="${RUSTC_WRAPPER:-sccache}"
fi

# ── Write harness configs (no MCP — adds ~10s startup overhead) ───────────────
cat > "$HOME_DIR/.config/amp/settings.json" <<EOF
{
  "amp.experimental.compaction": 95,
  "amp.proxy": "http://${FIREWALL_HOSTNAME}:8080",
  "amp.git.commit.coauthor.enabled": false
}
EOF

# ── Mock Google ADC for sandbox-only SDK initialization ─────────────────────
# Some Google client libraries refuse to initialize without ADC, even when the
# per-sandbox proxy is responsible for attaching the real auth headers.
if [ -z "${GOOGLE_APPLICATION_CREDENTIALS:-}" ]; then
    GOOGLE_APPLICATION_CREDENTIALS="$HOME_DIR/.config/gcloud/application_default_credentials.json"
    export GOOGLE_APPLICATION_CREDENTIALS
    mkdir -p "$(dirname "$GOOGLE_APPLICATION_CREDENTIALS")"
    if [ ! -f "$GOOGLE_APPLICATION_CREDENTIALS" ]; then
        # Some SDKs parse ADC into service-account credentials locally before any
        # outbound request reaches the proxy, so the stub must look real enough
        # to pass key loading.
        _mock_gcp_private_key="$(openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 2>/dev/null)"
        MOCK_GCP_PRIVATE_KEY="$_mock_gcp_private_key" python3 - "$GOOGLE_APPLICATION_CREDENTIALS" <<'PYEOF'
import json
import os
import sys

path = sys.argv[1]
client_email = "mock@creds.com"

with open(path, "w") as f:
    json.dump(
        {
            "type": "service_account",
            "project_id": "centaur-sandbox",
            "private_key_id": "0000000000000000000000000000000000000000",
            "private_key": os.environ["MOCK_GCP_PRIVATE_KEY"].rstrip("\n") + "\n",
            "client_email": client_email,
            "client_id": "100000000000000000000",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": f"https://www.googleapis.com/robot/v1/metadata/x509/{client_email.replace('@', '%40')}",
            "universe_domain": "googleapis.com",
        },
        f,
        indent=2,
    )
    f.write("\n")
PYEOF
        unset _mock_gcp_private_key
    fi
fi

# ── Codex settings ──────────────────────────────────────────────────────────
# CODEX_AUTH_MODE selects how codex authenticates with the upstream:
#   - api_key (default): codex uses an OPENAI_API_KEY against api.openai.com.
#     The entrypoint runs `codex login --with-api-key` below, which overwrites
#     auth.json.
#   - access_token: codex uses a ChatGPT-style access token against
#     chatgpt.com. The default auth.json (auth_mode: chatgpt) is always
#     installed and the api-key login step is skipped so iron-proxy can
#     inject the brokered Bearer + chatgpt-account-id headers.
CODEX_AUTH_MODE="${CODEX_AUTH_MODE:-api_key}"
mkdir -p "$HOME_DIR/.codex"
if [ "$CODEX_AUTH_MODE" = "access_token" ] && [ -f /etc/centaur/codex-auth.default.json ]; then
    cp /etc/centaur/codex-auth.default.json "$HOME_DIR/.codex/auth.json"
    chmod 600 "$HOME_DIR/.codex/auth.json"
elif [ ! -f "$HOME_DIR/.codex/auth.json" ] && [ -f /etc/centaur/codex-auth.default.json ]; then
    cp /etc/centaur/codex-auth.default.json "$HOME_DIR/.codex/auth.json"
    chmod 600 "$HOME_DIR/.codex/auth.json"
fi
if [ -n "${CENTAUR_TRACE_ID:-}" ]; then
    printf '%s' "$CENTAUR_TRACE_ID" > "$HOME_DIR/.trace_id"
fi

toml_escape() {
    printf '%s' "$1" | sed 's/\/\\/g; s/"/\"/g'
}

is_placeholder_secret() {
    case "${1:-}" in
        "" | "GITHUB_TOKEN" | "GH_TOKEN" | "GITHUB_PAT")
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}

truthy_env() {
    local value="${1:-}"
    value="${value#"${value%%[![:space:]]*}"}"
    value="${value%"${value##*[![:space:]]}"}"
    case "${value,,}" in
        1|true|yes|on) return 0 ;;
        *) return 1 ;;
    esac
}

HARNESS_CONFIG_DIR="${CENTAUR_HARNESS_CONFIG_DIR:-$HOME_DIR/harness}"
if [ -f "$HARNESS_CONFIG_DIR/codex/config.toml" ]; then
    cp "$HARNESS_CONFIG_DIR/codex/config.toml" "$HOME_DIR/.codex/config.toml"
else
    echo "missing Codex harness config: $HARNESS_CONFIG_DIR/codex/config.toml" >&2
    exit 1
fi

# ── Claude Code settings ────────────────────────────────────────────────────
mkdir -p "$HOME_DIR/.claude"
if [ -f "$HARNESS_CONFIG_DIR/claude/settings.json" ]; then
    cp "$HARNESS_CONFIG_DIR/claude/settings.json" "$HOME_DIR/.claude/settings.json"
fi

# ── Pi-mono settings ─────────────────────────────────────────────────────────
mkdir -p "$HOME_DIR/.pi/agent/extensions"
cat > "$HOME_DIR/.pi/agent/settings.json" <<EOF
{
  "provider": "anthropic",
  "model": "claude-sonnet-4-20250514",
  "thinkingLevel": "medium",
  "autoCompaction": true
}
EOF

# ── Per-session workspace clone (no shared worktree metadata) ────────────────
if [ "${CENTAUR_PERSISTENT_STATE:-0}" = "1" ]; then
    WORKSPACE_DIR="$STATE_DIR/workspace"
else
    WORKSPACE_DIR="$HOME_DIR/workspace"
fi
if [ -n "${AGENT_REPO:-}" ]; then
    REPO_PATH="$HOME_DIR/github/$AGENT_REPO"
    if ! git -C "$REPO_PATH" rev-parse --git-dir >/dev/null 2>&1; then
        echo "AGENT_REPO is not a valid git repository: $REPO_PATH" >&2
        exit 1
    fi

    if ! git -C "$WORKSPACE_DIR" rev-parse --git-dir >/dev/null 2>&1; then
        rm -rf "$WORKSPACE_DIR"
        if ! git clone --quiet --shared "$REPO_PATH" "$WORKSPACE_DIR"; then
            echo "shared clone failed for $REPO_PATH; retrying with regular clone" >&2
            rm -rf "$WORKSPACE_DIR"
            git clone --quiet "$REPO_PATH" "$WORKSPACE_DIR"
        fi

    fi

    case "$AGENT_REPO" in
        */*) git -C "$WORKSPACE_DIR" remote set-url origin "https://github.com/${AGENT_REPO}.git" ;;
    esac

    BRANCH="agent-$(date +%s)-${RANDOM}-${RANDOM}"
    git -C "$WORKSPACE_DIR" checkout -q -b "$BRANCH" || true
else
    mkdir -p "$WORKSPACE_DIR"
fi

if [ -n "${WORKSPACE_ENV_LOCAL_B64:-}" ]; then
    printf '%s' "$WORKSPACE_ENV_LOCAL_B64" | base64 -d > "$WORKSPACE_DIR/.env.local"
    chmod 600 "$WORKSPACE_DIR/.env.local"
fi

# ── Ensure uploads directory exists ──────────────────────────────────────────
mkdir -p "$HOME_DIR/uploads"

# ── Copy project skills into workspace (so `skill` tool discovers them) ──────
BAKED_IN_CENTAUR_SKILLS="$HOME_DIR/.agents/skills"
MOUNTED_CENTAUR_SKILLS="$HOME_DIR/centaur-skills"
MOUNTED_ORG_SKILLS="$HOME_DIR/centaur-overlay-skills"
OVERLAY_TREE_SKILLS=""
if [ -n "${CENTAUR_OVERLAY_DIR:-}" ] && [ -d "${CENTAUR_OVERLAY_DIR}/.agents/skills" ]; then
    OVERLAY_TREE_SKILLS="${CENTAUR_OVERLAY_DIR}/.agents/skills"
fi
CENTAUR_SKILLS=""
if [ -d "$HOME_DIR/github" ]; then
    CENTAUR_SKILLS="$(find "$HOME_DIR/github" -path '*/centaur/.agents/skills' -type d -print -quit 2>/dev/null || true)"
fi
WS_SKILLS="$WORKSPACE_DIR/.agents/skills"
copy_skills_into_workspace() {
    local skills_src="$1"
    local entry
    local name
    local target

    mkdir -p "$WS_SKILLS"
    for entry in "$skills_src"/* "$skills_src"/.[!.]* "$skills_src"/..?*; do
        if [ ! -e "$entry" ] && [ ! -L "$entry" ]; then
            continue
        fi
        name="$(basename "$entry")"
        target="$WS_SKILLS/$name"
        if [ -L "$target" ]; then
            rm -f "$target"
        elif [ -d "$entry" ] && [ -e "$target" ] && [ ! -d "$target" ]; then
            rm -f "$target"
        elif [ ! -d "$entry" ] && [ -d "$target" ]; then
            rm -rf "$target"
        fi
    done
    cp -r "$skills_src"/. "$WS_SKILLS"/
}

import_workspace_claude_skills() {
    local claude_skills="$WORKSPACE_DIR/.claude/skills"
    local entry
    local name
    local target

    if [ ! -d "$claude_skills" ] || [ "$claude_skills" -ef "$WS_SKILLS" ]; then
        return
    fi

    mkdir -p "$WS_SKILLS"
    for entry in "$claude_skills"/* "$claude_skills"/.[!.]* "$claude_skills"/..?*; do
        if [ ! -e "$entry" ] && [ ! -L "$entry" ]; then
            continue
        fi
        name="$(basename "$entry")"
        target="$WS_SKILLS/$name"
        if [ -L "$target" ]; then
            rm -f "$target"
        elif [ -e "$target" ]; then
            rm -rf "$target"
        fi
        cp -r "$entry" "$target"
    done
}

import_workspace_claude_skills

for SKILLS_SRC in "$BAKED_IN_CENTAUR_SKILLS" "$MOUNTED_CENTAUR_SKILLS" "$CENTAUR_SKILLS" "$MOUNTED_ORG_SKILLS" "$OVERLAY_TREE_SKILLS"; do
    if [ -d "$SKILLS_SRC" ]; then
        copy_skills_into_workspace "$SKILLS_SRC"
    fi
done

if [ -d "$WS_SKILLS" ]; then
    mkdir -p "$WORKSPACE_DIR/.claude"
    rm -rf "$WORKSPACE_DIR/.claude/skills"
    ln -sf "$WS_SKILLS" "$WORKSPACE_DIR/.claude/skills"
fi

# ── Assemble system prompt from bind mounts ──────────────────────────────────
# Base prompt: mounted as AGENTS_BASE.md when present, fallback to baked-in AGENTS.md.
# Org/persona overlays are mounted alongside the base prompt when present.
TARGET_PROMPT="$WORKSPACE_DIR/AGENTS.md"
if [ -f "$HOME_DIR/AGENTS_BASE.md" ]; then
    cp "$HOME_DIR/AGENTS_BASE.md" "$TARGET_PROMPT"
elif [ -f "$HOME_DIR/AGENTS.md" ]; then
    cp "$HOME_DIR/AGENTS.md" "$TARGET_PROMPT"
fi

if [ -f "$HOME_DIR/AGENTS_OVERLAY.md" ] && [ -f "$TARGET_PROMPT" ]; then
    printf '\n\n---\n\n' >> "$TARGET_PROMPT"
    cat "$HOME_DIR/AGENTS_OVERLAY.md" >> "$TARGET_PROMPT"
fi

# Persona prompt injection is done by the API when it writes AGENTS_BASE.md.

# Switch to workspace so the harness reads workspace/AGENTS.md (with persona overlay)
cd "$WORKSPACE_DIR"

HARNESS_ADAPTER="${CENTAUR_HARNESS_ADAPTER:-/usr/local/bin/harness-adapter}"
if [ -x "$HARNESS_ADAPTER" ]; then
    "$HARNESS_ADAPTER" "${1:-}" "$TARGET_PROMPT"
fi

CODEX_LOCAL_AUTH_LOADED=0
if truthy_env "${CODEX_USE_LOCAL_AUTH:-}"; then
    mkdir -p "$HOME_DIR/.codex"
    cat > "$HOME_DIR/.codex/auth.json" <<'JSON'
{"auth_mode":"chatgpt","last_refresh":"1970-01-01T00:00:00Z","tokens":{"access_token":"eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0.eyJleHAiOjQxMDI0NDQ4MDAsImVtYWlsIjoiaXJvbi1wcm94eS1jb2RleC1zdHViQGV4YW1wbGUuaW52YWxpZCIsImh0dHBzOi8vYXBpLm9wZW5haS5jb20vcHJvZmlsZSI6eyJlbWFpbCI6Imlyb24tcHJveHktY29kZXgtc3R1YkBleGFtcGxlLmludmFsaWQifSwiaHR0cHM6Ly9hcGkub3BlbmFpLmNvbS9hdXRoIjp7InVzZXJfaWQiOiJpcm9uLXByb3h5LWNvZGV4LXN0dWItdXNlciIsImNoYXRncHRfdXNlcl9pZCI6Imlyb24tcHJveHktY29kZXgtc3R1Yi11c2VyIiwiY2hhdGdwdF9hY2NvdW50X2lkIjoiaXJvbi1wcm94eS1jb2RleC1zdHViLWFjY291bnQiLCJjaGF0Z3B0X2FjY291bnRfaXNfZmVkcmFtcCI6ZmFsc2V9fQ.stub-signature","refresh_token":"iron-proxy-codex-stub-refresh-token","id_token":"eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0.eyJleHAiOjQxMDI0NDQ4MDAsImVtYWlsIjoiaXJvbi1wcm94eS1jb2RleC1zdHViQGV4YW1wbGUuaW52YWxpZCIsImh0dHBzOi8vYXBpLm9wZW5haS5jb20vcHJvZmlsZSI6eyJlbWFpbCI6Imlyb24tcHJveHktY29kZXgtc3R1YkBleGFtcGxlLmludmFsaWQifSwiaHR0cHM6Ly9hcGkub3BlbmFpLmNvbS9hdXRoIjp7InVzZXJfaWQiOiJpcm9uLXByb3h5LWNvZGV4LXN0dWItdXNlciIsImNoYXRncHRfdXNlcl9pZCI6Imlyb24tcHJveHktY29kZXgtc3R1Yi11c2VyIiwiY2hhdGdwdF9hY2NvdW50X2lkIjoiaXJvbi1wcm94eS1jb2RleC1zdHViLWFjY291bnQiLCJjaGF0Z3B0X2FjY291bnRfaXNfZmVkcmFtcCI6ZmFsc2V9fQ.stub-signature","account_id":"iron-proxy-codex-stub-account"}}
JSON
    chmod 600 "$HOME_DIR/.codex/auth.json"
    CODEX_LOCAL_AUTH_LOADED=1
    unset CODEX_API_KEY OPENAI_API_KEY
fi
unset CODEX_AUTH_JSON CODEX_AUTH_JSON_FILE CODEX_ACCESS_TOKEN CODEX_PROXY_AUTH

# Codex reads its auth file when the app server starts. Complete this before
# signaling readiness, otherwise warm pods can be claimed with no auth loaded.
# Skipped under access_token mode — that path relies on the chatgpt auth.json
# installed above plus iron-proxy injecting the real Bearer at request time.
if [ "$CODEX_LOCAL_AUTH_LOADED" != "1" ] && [ "$CODEX_AUTH_MODE" != "access_token" ]; then
    CODEX_KEY="${CODEX_API_KEY:-${OPENAI_API_KEY:-}}"
    if ! is_placeholder_secret "$CODEX_KEY"; then
        echo "$CODEX_KEY" | codex login --with-api-key 2>/dev/null || true
    fi
fi

unset CLAUDE_CREDENTIALS_JSON CLAUDE_CREDENTIALS_JSON_FILE

# Signal readiness
touch "$HOME_DIR/.ready"

# ── Background: slow auth tasks ─────────────────────────────────────────────
{
    GIT_AUTH_TOKEN="${GH_TOKEN:-${GITHUB_TOKEN:-}}"
    if ! is_placeholder_secret "$GIT_AUTH_TOKEN"; then
        git config --global credential.helper store
        printf 'https://x-access-token:%s@github.com\n' "$GIT_AUTH_TOKEN" > "$HOME_DIR/.git-credentials"
        printf '%s\n' "$GIT_AUTH_TOKEN" | gh auth login --with-token 2>/dev/null || true
        gh auth setup-git 2>/dev/null || true
    fi
} &

exec "$@"
