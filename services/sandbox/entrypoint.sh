#!/bin/bash
set -e

HOME_DIR="$(eval echo ~)"
FIREWALL_HOSTNAME="${FIREWALL_HOST:-firewall}"
STATE_DIR="${CENTAUR_STATE_DIR:-$HOME_DIR/state}"

append_tool_dirs() {
    if [ -z "${1:-}" ]; then
        return
    fi
    if [ -n "${TOOL_DIRS:-}" ]; then
        TOOL_DIRS="${TOOL_DIRS}:$1"
    else
        TOOL_DIRS="$1"
    fi
}

append_tool_dirs "${TOOLS_PATH:-}"
append_tool_dirs "${TOOLS_OVERLAY_PATH:-}"
if [ -n "${TOOL_DIRS:-}" ]; then
    export TOOL_DIRS
fi

_add_pythonpath_entry() {
    local entry="$1"
    [ -d "$entry" ] || return 0
    case ":${PYTHONPATH:-}:" in
        *":$entry:"*) ;;
        *) export PYTHONPATH="$entry${PYTHONPATH:+:$PYTHONPATH}" ;;
    esac
}

_add_pythonpath_entry "/opt/centaur"
if [ -n "${TOOL_DIRS:-}" ]; then
    IFS=':' read -ra _centaur_tool_dirs <<< "$TOOL_DIRS"
    for _centaur_tool_dir in "${_centaur_tool_dirs[@]}"; do
        if [ -d "$_centaur_tool_dir" ]; then
            _centaur_tool_root="$(cd "$_centaur_tool_dir/.." && pwd -P)"
            _add_pythonpath_entry "$_centaur_tool_root"
        fi
    done
    unset _centaur_tool_dir _centaur_tool_dirs _centaur_tool_root
fi
export CENTAUR_TOOL_PYTHONPATH="${PYTHONPATH:-}"
unset -f _add_pythonpath_entry

if [ -n "${TOOL_DIRS:-}" ]; then
    install-tool-shims || echo "warning: failed to install Centaur tool CLI shims" >&2
fi

if [ -d "$STATE_DIR" ] && [ -w "$STATE_DIR" ]; then
    mkdir -p "$STATE_DIR/workspace" "$STATE_DIR/uploads" "$STATE_DIR/branches" "$STATE_DIR/codex" "$STATE_DIR/claude"
    rm -rf "$HOME_DIR/.codex" "$HOME_DIR/.claude" "$HOME_DIR/uploads" "$HOME_DIR/branches"
    ln -s "$STATE_DIR/codex" "$HOME_DIR/.codex"
    ln -s "$STATE_DIR/claude" "$HOME_DIR/.claude"
    ln -s "$STATE_DIR/uploads" "$HOME_DIR/uploads"
    ln -s "$STATE_DIR/branches" "$HOME_DIR/branches"
    export CENTAUR_PERSISTENT_STATE=1
fi

PORTABLE_SOURCE_DIR="${CENTAUR_PORTABLE_SOURCE_DIR:-$STATE_DIR/portable-sources}"
if [ -L "$PORTABLE_SOURCE_DIR" ]; then
    echo "CENTAUR_PORTABLE_SOURCE_DIR must not be a symlink" >&2
    exit 1
fi
mkdir -p "$PORTABLE_SOURCE_DIR"
export CENTAUR_PORTABLE_SOURCE_DIR="$PORTABLE_SOURCE_DIR"

validate_config_dir() {
    local config_dir="$1"
    local state_dir="$2"
    [ -L "$config_dir" ] || return 0
    if [ "${CENTAUR_PERSISTENT_STATE:-0}" != "1" ] \
        || [ "$(/usr/bin/readlink -f "$config_dir")" != "$(/usr/bin/readlink -f "$state_dir")" ]; then
        echo "runtime config directories must not use untrusted symlinks" >&2
        exit 1
    fi
}

validate_config_dir "$HOME_DIR/.codex" "$STATE_DIR/codex"
validate_config_dir "$HOME_DIR/.claude" "$STATE_DIR/claude"
unset -f validate_config_dir

mkdir -p "$HOME_DIR/.config/amp"

if [ -e "$HOME_DIR/.codex/AGENTS.override.md" ] || [ -L "$HOME_DIR/.codex/AGENTS.override.md" ]; then
    echo "persisted Codex AGENTS.override.md is not permitted" >&2
    exit 1
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
CODEX_CONFIG_DIR="$(cd -P "$HOME_DIR/.codex" && pwd)"
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

HARNESS_CONFIG_DIR="${CENTAUR_HARNESS_CONFIG_DIR:-$HOME_DIR/harness}"
if [ -f "$HARNESS_CONFIG_DIR/codex/config.toml" ]; then
    cp "$HARNESS_CONFIG_DIR/codex/config.toml" "$CODEX_CONFIG_DIR/config.toml"
    CODEX_CONFIG_PATH="$CODEX_CONFIG_DIR/config.toml" python3 - <<'PYEOF'
from pathlib import Path
import os
import subprocess
import sys

path = Path(os.environ["CODEX_CONFIG_PATH"])
lines = path.read_text().splitlines()

# CODEX_MODEL_REASONING_SUMMARY overrides model_reasoning_summary so deployments
# can re-enable reasoning summaries (Codex >= 0.139 no longer emits them by
# default) without rebuilding the sandbox image.
summary = os.environ.get("CODEX_MODEL_REASONING_SUMMARY", "").strip()
if summary:
    if summary not in {"auto", "concise", "detailed", "none"}:
        print(
            f"ignoring invalid CODEX_MODEL_REASONING_SUMMARY: {summary!r} "
            "(expected auto, concise, detailed, or none)",
            file=sys.stderr,
        )
    else:
        first_section = next(
            (i for i, line in enumerate(lines) if line.lstrip().startswith("[")),
            len(lines),
        )
        override = f'model_reasoning_summary = "{summary}"'
        for i in range(first_section):
            if lines[i].split("=", 1)[0].strip() == "model_reasoning_summary":
                lines[i] = override
                break
        else:
            lines.insert(first_section, override)

features_start = next((i for i, line in enumerate(lines) if line.strip() == "[features]"), None)
if features_start is None:
    lines.extend(["", "[features]", "multi_agent = false", "multi_agent_v2 = false"])
else:
    features_end = next(
        (i for i in range(features_start + 1, len(lines)) if lines[i].lstrip().startswith("[")),
        len(lines),
    )
    feature_names = {"multi_agent", "multi_agent_v2"}
    seen = set()
    rewritten = []
    for line in lines[features_start + 1 : features_end]:
        stripped = line.strip()
        name = stripped.split("=", 1)[0].strip() if "=" in stripped else None
        if name in feature_names:
            rewritten.append(f"{name} = false")
            seen.add(name)
        else:
            rewritten.append(line)
    for name in sorted(feature_names - seen):
        rewritten.append(f"{name} = false")
    lines = lines[: features_start + 1] + rewritten + lines[features_end:]

text = "\n".join(lines).rstrip() + "\n"

# CODEX_BEDROCK_REGION: when codex's built-in `amazon-bedrock` provider is enabled
# (the api-rs sandbox env injects this), pin its AWS region from the SAME env var
# that scopes iron-proxy's SigV4 re-signing, so the in-sandbox client signs/sends
# for the region the proxy is bound to. One source of truth instead of a
# hand-written CODEX_CONFIG_OVERLAY that can silently disagree and fail signing.
# Applied before the overlay below, so an operator can still override it. tomli_w
# quotes the value (no TOML injection); a parse failure just skips the patch.
bedrock_region = (os.environ.get("CODEX_BEDROCK_REGION") or "").strip()
if bedrock_region:
    import tomllib
    import tomli_w

    try:
        config = tomllib.loads(text)
    except tomllib.TOMLDecodeError as exc:
        print(f"ignoring CODEX_BEDROCK_REGION patch: {exc}", file=sys.stderr)
    else:
        config.setdefault("model_providers", {}).setdefault(
            "amazon-bedrock", {}
        ).setdefault("aws", {})["region"] = bedrock_region
        text = tomli_w.dumps(config)

# CENTAUR_CODEX_PROFILE_PATH points at a repository-mounted profile. Apply it
# after platform-owned deployment patches and before the operator overlay: the
# profile can carry portable agent defaults, but cannot change credentials,
# providers, sandbox policy, or other platform authority. Unlike the optional
# operator overlay, a configured portable profile fails closed so a deployment
# cannot silently run with unexpected agent behavior.
profile_path = (os.environ.get("CENTAUR_CODEX_PROFILE_PATH") or "").strip()
if profile_path:
    profile_sha256 = (os.environ.get("CENTAUR_CODEX_PROFILE_SHA256") or "").strip()
    if not profile_sha256:
        print("CENTAUR_CODEX_PROFILE_SHA256 is required with CENTAUR_CODEX_PROFILE_PATH", file=sys.stderr)
        raise SystemExit(1)
    profile_snapshot = str(
        Path(os.environ["CENTAUR_PORTABLE_SOURCE_DIR"]) / "codex-profile.toml"
    )
    snapshot = subprocess.run(
        [
            "/usr/local/bin/snapshot-portable-source",
            "--source",
            profile_path,
            "--destination",
            profile_snapshot,
            "--expected-sha256",
            profile_sha256,
            "--label",
            "portable Codex profile",
        ],
        check=False,
    )
    if snapshot.returncode:
        raise SystemExit(snapshot.returncode)
    profile_path = profile_snapshot
    path.write_text(text)
    result = subprocess.run(
        [
            "/usr/local/bin/codex-profile-merge",
            "--config",
            str(path),
            "--profile",
            profile_path,
            "--merge-only",
        ],
        check=False,
    )
    if result.returncode:
        raise SystemExit(result.returncode)
    text = path.read_text()

# CODEX_MODEL_REASONING_EFFORT is a deployment default, not repository policy.
# It deliberately follows the portable profile and precedes the unrestricted
# operator overlay, which owns the final explicit deployment override.
effort = (os.environ.get("CODEX_MODEL_REASONING_EFFORT") or "").strip().lower()
if effort:
    valid = {"none", "minimal", "low", "medium", "high", "xhigh", "max"}
    if effort not in valid:
        print(
            f"ignoring invalid CODEX_MODEL_REASONING_EFFORT={effort!r}; "
            f"expected one of {sorted(valid)}",
            file=sys.stderr,
        )
    else:
        import tomllib
        import tomli_w

        config = tomllib.loads(text)
        config["model_reasoning_effort"] = effort
        text = tomli_w.dumps(config)

# CODEX_CONFIG_OVERLAY: deep-merge an operator-supplied TOML fragment over the
# baked config so a deployment can configure codex -- e.g. point it at a custom
# model provider via a [model_providers.*] block -- through sandbox.extraEnv,
# without forking config.toml. Unset is a no-op; invalid TOML is ignored (the
# baked config stands) rather than written.
overlay_raw = (os.environ.get("CODEX_CONFIG_OVERLAY") or "").strip()
if overlay_raw:
    import tomllib
    import tomli_w

    def _deep_merge(base, overlay):
        for key, value in overlay.items():
            if isinstance(value, dict) and isinstance(base.get(key), dict):
                _deep_merge(base[key], value)
            else:
                base[key] = value
        return base

    try:
        merged = _deep_merge(tomllib.loads(text), tomllib.loads(overlay_raw))
    except tomllib.TOMLDecodeError as exc:
        print(f"ignoring invalid CODEX_CONFIG_OVERLAY: {exc}", file=sys.stderr)
    else:
        text = tomli_w.dumps(merged)

path.write_text(text)
if profile_path:
    result = subprocess.run(
        [
            "/usr/local/bin/codex-profile-merge",
            "--config",
            str(path),
            "--profile",
            profile_path,
            "--attestation-only",
        ],
        check=False,
    )
    if result.returncode:
        raise SystemExit(result.returncode)
PYEOF
else
    echo "missing Codex harness config: $HARNESS_CONFIG_DIR/codex/config.toml" >&2
    exit 1
fi

# A consented metadata-trace sidecar writes its loopback-only OTLP capability
# here. The agent mount is read-only and this bounded wait deliberately keeps
# Codex available when the sidecar is absent or unhealthy.
configure_codex_metadata_trace() {
    local address_file="${CENTAUR_CODEX_METADATA_TRACE_ADDRESS_FILE:-}"
    [ "${1:-}" = "harness-server" ] && [ "${2:-}" = "codex" ] || return 0

    # The image may be reused after an earlier deployment injected OTEL state.
    # Disable every inherited exporter before considering the narrow loopback
    # capability, so an unavailable trace agent cannot fall back to a broad
    # collector or export prompts through stale configuration.
    while IFS='=' read -r name _; do
        case "$name" in OTEL_*) unset "$name" ;; esac
    done < <(env)
    CODEX_CONFIG_PATH="$CODEX_CONFIG_DIR/config.toml" python3 - <<'PYEOF'
from pathlib import Path
import os
import tomllib

try:
    import tomli_w
except ModuleNotFoundError:
    # The production image installs tomli-w. Keep local/minimal images usable
    # with the TOML parser as the authority and a conservative writer fallback.
    import json

    class tomli_w:
        @staticmethod
        def dumps(value):
            def scalar(item):
                if isinstance(item, bool):
                    return str(item).lower()
                if isinstance(item, str):
                    return json.dumps(item)
                if isinstance(item, list):
                    return "[" + ", ".join(scalar(entry) for entry in item) + "]"
                return str(item)

            lines = []
            def write_table(table, path=()):
                for key, item in table.items():
                    if not isinstance(item, dict):
                        lines.append(f"{key} = {scalar(item)}")
                for key, item in table.items():
                    if isinstance(item, dict):
                        heading = ".".join((*path, key))
                        lines.append(f"[{heading}]")
                        write_table(item, (*path, key))
            write_table(value)
            return "\n".join(lines) + ("\n" if lines else "")

path = Path(os.environ["CODEX_CONFIG_PATH"])
config = tomllib.loads(path.read_text())
config.pop("otel", None)
config["otel"] = {
    "exporter": "none",
    "log_user_prompt": False,
    "metrics_exporter": "none",
    "trace_exporter": "none",
}
path.write_text(tomli_w.dumps(config))
PYEOF
    # Metadata-trace mode is the only supported Codex tracing path. This must
    # remain disabled even when this execution has no consent, otherwise a
    # baked or inherited exporter can observe prompts during a config rollout.
    [ "${CENTAUR_SANDBOX_METADATA_TRACE_ENABLED:-false}" = "true" ] || return 0
    [ -n "$address_file" ] || return 0

    local wait_seconds="${CENTAUR_CODEX_METADATA_TRACE_WAIT_SECONDS:-5}"
    local deadline=$(( $(date +%s) + wait_seconds ))
    local endpoint=""
    while [ "$(date +%s)" -lt "$deadline" ]; do
        if [ -r "$address_file" ]; then
            endpoint="$(tr -d '\r\n' < "$address_file")"
            case "$endpoint" in
                http://127.0.0.1:*|http://[::1]:*) break ;;
                *) endpoint="" ;;
            esac
        fi
        sleep 0.2
    done
    if [ -z "$endpoint" ]; then
        echo "metadata trace sidecar unavailable; continuing without trace export" >&2
        return 0
    fi
    CODEX_CONFIG_PATH="$CODEX_CONFIG_DIR/config.toml" CODEX_TRACE_ENDPOINT="$endpoint" python3 - <<'PYEOF'
import os
from pathlib import Path
import tomllib

try:
    import tomli_w
except ModuleNotFoundError:
    import json

    class tomli_w:
        @staticmethod
        def dumps(value):
            def scalar(item):
                if isinstance(item, bool):
                    return str(item).lower()
                if isinstance(item, str):
                    return json.dumps(item)
                if isinstance(item, list):
                    return "[" + ", ".join(scalar(entry) for entry in item) + "]"
                return str(item)

            lines = []
            def write_table(table, path=()):
                for key, item in table.items():
                    if not isinstance(item, dict):
                        lines.append(f"{key} = {scalar(item)}")
                for key, item in table.items():
                    if isinstance(item, dict):
                        heading = ".".join((*path, key))
                        lines.append(f"[{heading}]")
                        write_table(item, (*path, key))
            write_table(value)
            return "\n".join(lines) + ("\n" if lines else "")

path = Path(os.environ["CODEX_CONFIG_PATH"])
# Codex 0.146 configures OTLP/HTTP at the signal endpoint, whereas the
# capability is deliberately published as its base URL. Normalize both current
# and already signal-scoped capabilities without a duplicate path.
endpoint = os.environ["CODEX_TRACE_ENDPOINT"].rstrip("/")
if not endpoint.endswith("/v1/traces"):
    endpoint = f"{endpoint}/v1/traces"
config = tomllib.loads(path.read_text())
config.pop("otel", None)
config["otel"] = {
    "exporter": "none",
    "log_user_prompt": False,
    "metrics_exporter": "none",
    "trace_exporter": {"otlp-http": {"endpoint": endpoint, "protocol": "binary"}},
}
path.write_text(tomli_w.dumps(config))
PYEOF
}

# ── Claude Code settings ────────────────────────────────────────────────────
mkdir -p "$HOME_DIR/.claude"
if [ -f "$HARNESS_CONFIG_DIR/claude/settings.json" ]; then
    cp "$HARNESS_CONFIG_DIR/claude/settings.json" "$HOME_DIR/.claude/settings.json"
fi

# CLAUDE_SETTINGS_OVERLAY: deep-merge an operator-supplied JSON fragment over the
# baked settings.json (symmetric to CODEX_CONFIG_OVERLAY), so a deployment can
# configure Claude Code via sandbox.extraEnv without forking the image. Unset is
# a no-op; invalid JSON is ignored.
if [ -n "${CLAUDE_SETTINGS_OVERLAY:-}" ]; then
    CLAUDE_SETTINGS_PATH="$HOME_DIR/.claude/settings.json" python3 - <<'PYEOF'
import json
import os
import sys
from pathlib import Path

path = Path(os.environ["CLAUDE_SETTINGS_PATH"])
try:
    overlay = json.loads(os.environ["CLAUDE_SETTINGS_OVERLAY"])
except json.JSONDecodeError as exc:
    print(f"ignoring invalid CLAUDE_SETTINGS_OVERLAY: {exc}", file=sys.stderr)
    sys.exit(0)
existing = path.read_text() if path.exists() else ""
base = json.loads(existing) if existing.strip() else {}

def _deep_merge(b, o):
    for key, value in o.items():
        if isinstance(value, dict) and isinstance(b.get(key), dict):
            _deep_merge(b[key], value)
        else:
            b[key] = value
    return b

path.parent.mkdir(parents=True, exist_ok=True)
path.write_text(json.dumps(_deep_merge(base, overlay), indent=2) + "\n")
PYEOF
fi

# CLAUDE_CODE_AUTH_MODE selects how Claude Code authenticates with the upstream
# (mirrors CODEX_AUTH_MODE):
#   - api_key (default): Claude Code uses ANTHROPIC_API_KEY against
#     api.anthropic.com. The harness stub key is left in the env; iron-proxy's
#     ANTHROPIC_API_KEY HttpSecret rewrites the X-Api-Key header on the wire.
#   - access_token: Claude Code runs as a Claude.ai Pro or Max subscription
#     user. We install a dummy ~/.claude/.credentials.json so the CLI emits
#     OAuth-shaped requests, unset the API-key stub so it does not fall back
#     to X-Api-Key, and let iron-proxy inject the current Bearer from the
#     Console-managed anthropic-claude token_broker secret at request time.
CLAUDE_CODE_AUTH_MODE="${CLAUDE_CODE_AUTH_MODE:-api_key}"
case "$CLAUDE_CODE_AUTH_MODE" in
    api_key)
        :
        ;;
    access_token)
        unset ANTHROPIC_API_KEY
        if [ -f /etc/centaur/claude-credentials.default.json ]; then
            cp /etc/centaur/claude-credentials.default.json "$HOME_DIR/.claude/.credentials.json"
            chmod 600 "$HOME_DIR/.claude/.credentials.json"
        fi
        ;;
    *)
        echo "unknown CLAUDE_CODE_AUTH_MODE: $CLAUDE_CODE_AUTH_MODE (expected api_key or access_token)" >&2
        exit 1
        ;;
esac

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
repair_workspace_origin() {
    local repo_path="$1"
    local workspace_dir="$2"
    local upstream_url
    local workspace_origin

    upstream_url="$(git -C "$repo_path" config --get remote.origin.url 2>/dev/null || true)"
    if [ -z "$upstream_url" ]; then
        echo "AGENT_REPO cache checkout has no origin: $repo_path" >&2
        return 1
    fi
    workspace_origin="$(git -C "$workspace_dir" config --get remote.origin.url 2>/dev/null || true)"

    if [ -z "$workspace_origin" ]; then
        git -C "$workspace_dir" remote add origin "$upstream_url"
    else
        git -C "$workspace_dir" remote set-url origin "$upstream_url"
    fi
}

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

        repair_workspace_origin "$REPO_PATH" "$WORKSPACE_DIR"

        BRANCH="agent-$(date +%s)-${RANDOM}-${RANDOM}"
        git -C "$WORKSPACE_DIR" checkout -q -b "$BRANCH" || true
    else
        repair_workspace_origin "$REPO_PATH" "$WORKSPACE_DIR"
    fi
else
    mkdir -p "$WORKSPACE_DIR"
fi

# ── Ensure uploads directory exists ──────────────────────────────────────────
mkdir -p "$HOME_DIR/uploads"

# ── Copy project skills into workspace (so `skill` tool discovers them) ──────
WORKSPACE_DIR="$WORKSPACE_DIR" install-tool-shims --refresh-skills \
    || echo "warning: failed to reload Centaur skills" >&2

# ── Background: refresh repo-cache-backed tools/skills in running sandboxes ───
case "${CENTAUR_TOOLS_AUTO_RELOAD:-true}" in
    0|false|False|FALSE|no|No|NO|off|Off|OFF) _centaur_tools_auto_reload=0 ;;
    *) _centaur_tools_auto_reload=1 ;;
esac
if [ "$_centaur_tools_auto_reload" = "1" ] \
    && [ "${CENTAUR_SANDBOX_REPO_CACHE_ENABLED:-true}" != "false" ] \
    && [ -n "${TOOL_DIRS:-}" ]; then
    (
        WORKSPACE_DIR="$WORKSPACE_DIR" repo-cache-watch \
            || echo "warning: Centaur tool auto-reload watcher stopped" >&2
    ) &
fi
unset _centaur_tools_auto_reload

# ── Assemble system prompt from bind mounts ──────────────────────────────────
# Codex and Claude discover repository instructions themselves. Compose only
# platform instructions in their user-level config directories so startup never
# overwrites a target repository's policy, including uncommitted guidance.
CODEX_PROMPT_DIR="$CODEX_CONFIG_DIR"
CLAUDE_PROMPT_DIR="$(cd -P "$HOME_DIR/.claude" && pwd)"
TARGET_PROMPT="$CODEX_PROMPT_DIR/AGENTS.md"
CLAUDE_PROMPT="$CLAUDE_PROMPT_DIR/CLAUDE.md"
COMPOSE_PROMPT_ARGS=(--home-dir "$HOME_DIR" --target-prompt "$TARGET_PROMPT")
if [ -n "${CENTAUR_AGENT_INSTRUCTIONS_PATH:-}" ]; then
    INSTRUCTIONS_SNAPSHOT="$CENTAUR_PORTABLE_SOURCE_DIR/agent-instructions.md"
    /usr/local/bin/snapshot-portable-source \
        --source "$CENTAUR_AGENT_INSTRUCTIONS_PATH" \
        --destination "$INSTRUCTIONS_SNAPSHOT" \
        --expected-sha256 "${CENTAUR_AGENT_INSTRUCTIONS_SHA256:-}" \
        --label "portable agent instructions"
    COMPOSE_PROMPT_ARGS+=(--agent-instructions-path "$INSTRUCTIONS_SNAPSHOT")
fi
if [ "${CENTAUR_SANDBOX_OBSERVABILITY_ENABLED:-true}" = "false" ]; then
    COMPOSE_PROMPT_ARGS+=(--without-observability)
fi
if [ "${CENTAUR_SANDBOX_API_SERVER_ENABLED:-true}" = "false" ]; then
    COMPOSE_PROMPT_ARGS+=(--without-api-server)
fi
/usr/local/bin/compose-system-prompt "${COMPOSE_PROMPT_ARGS[@]}"
COMPOSE_PROMPT_ARGS[3]="$CLAUDE_PROMPT"
/usr/local/bin/compose-system-prompt "${COMPOSE_PROMPT_ARGS[@]}"
unset COMPOSE_PROMPT_ARGS

if [ -n "${CENTAUR_AGENT_INSTRUCTIONS_PATH:-}" ]; then
    INSTRUCTIONS_SHA256="$(/usr/bin/sha256sum "$INSTRUCTIONS_SNAPSHOT" | /usr/bin/cut -d ' ' -f 1)"
    printf 'CENTAUR_AGENT_INSTRUCTIONS_APPLIED {"instructions_sha256":"%s"}\n' "$INSTRUCTIONS_SHA256"
    unset INSTRUCTIONS_SHA256 INSTRUCTIONS_SNAPSHOT
fi

# Persona prompt injection is done by the API when it writes AGENTS_BASE.md.

# Switch to workspace so Codex discovers target-repository AGENTS.md naturally.
cd "$WORKSPACE_DIR"

configure_codex_metadata_trace "$@"

if [ "${1:-}" = "harness-server" ] && [ "${2:-}" = "amp" ] && [ -f "$TARGET_PROMPT" ]; then
    rm -f "$WORKSPACE_DIR/AGENT.md"
    ln -s "$TARGET_PROMPT" "$WORKSPACE_DIR/AGENT.md"
fi

# Codex reads its auth file when the app server starts. Complete this before
# signaling readiness, otherwise warm pods can be claimed with no auth loaded.
# Skipped under access_token mode — that path relies on the chatgpt auth.json
# installed above plus iron-proxy injecting the real Bearer at request time.
if [ "$CODEX_AUTH_MODE" != "access_token" ]; then
    CODEX_KEY="${CODEX_API_KEY:-${OPENAI_API_KEY:-}}"
    if [ -n "$CODEX_KEY" ]; then
        echo "$CODEX_KEY" | codex login --with-api-key 2>/dev/null || true
    fi
fi

# Wait for the tool-server sidecar before signalling readiness, so the harness
# doesn't issue its first tool call before the server is listening.
if [ -n "${CENTAUR_TOOLS_URL:-}" ]; then
    _tools_deadline=$(( $(date +%s) + ${CENTAUR_TOOLS_WAIT_SECONDS:-10} ))
    until curl -fsS --noproxy '*' --max-time 2 "${CENTAUR_TOOLS_URL}/healthz" >/dev/null 2>&1; do
        if [ "$(date +%s)" -ge "$_tools_deadline" ]; then
            echo "tool-server /healthz not ready after ${CENTAUR_TOOLS_WAIT_SECONDS:-10}s; continuing" >&2
            break
        fi
        sleep 0.5
    done
fi

# Signal readiness
touch "$HOME_DIR/.ready"

# ── Background: slow auth tasks ─────────────────────────────────────────────
{
    if [ -n "${GITHUB_TOKEN:-}" ]; then
        git config --global credential.helper store
        printf 'https://oauth2:%s@github.com\n' "$GITHUB_TOKEN" > "$HOME_DIR/.git-credentials"
        echo "${GITHUB_TOKEN}" | gh auth login --with-token 2>/dev/null || true
        gh auth setup-git 2>/dev/null || true
    fi
} &

exec "$@"
