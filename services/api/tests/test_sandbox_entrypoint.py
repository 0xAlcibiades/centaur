from __future__ import annotations

import base64
import json
import os
import subprocess
import time
import tomllib
from pathlib import Path


ENTRYPOINT_SH = Path(__file__).resolve().parents[2] / "sandbox" / "entrypoint.sh"
CODEX_STUB_ACCOUNT_ID = "iron-proxy-codex-stub-account"


def _write_codex_harness_config(home: Path) -> Path:
    harness_dir = home / "harness"
    codex_dir = harness_dir / "codex"
    codex_dir.mkdir(parents=True)
    (codex_dir / "config.toml").write_text(
        "\n".join(
            [
                'model = "gpt-5.5"',
                'model_reasoning_effort = "low"',
                'plan_mode_reasoning_effort = "high"',
                'approval_policy = "on-request"',
                'approvals_reviewer = "user"',
                'web_search = "live"',
                'personality = "pragmatic"',
                'sandbox_mode = "workspace-write"',
                "check_for_update_on_startup = true",
                "suppress_unstable_features_warning = true",
                'service_tier = "fast"',
                "",
                "[tools]",
                "view_image = true",
                "",
                "[features]",
                "goals = true",
                "memories = true",
                "code_mode = true",
                "hooks = true",
                "browser_use = true",
                "computer_use = true",
                "enable_fanout = true",
                "runtime_metrics = true",
                "",
                "[features.multi_agent_v2]",
                "enabled = true",
                "max_concurrent_threads_per_session = 6",
                "",
                "[agents]",
                "max_depth = 2",
                "job_max_runtime_seconds = 1800",
                "",
            ]
        )
    )
    return harness_dir


def _assert_codex_stub_jwt(token: str) -> None:
    parts = token.split(".")
    assert len(parts) == 3
    assert all(parts)
    payload = json.loads(base64.urlsafe_b64decode(parts[1] + "=="))
    assert payload["exp"] > int(time.time()) + 86400
    assert (
        payload["https://api.openai.com/auth"]["chatgpt_account_id"]
        == CODEX_STUB_ACCOUNT_ID
    )


def test_sandbox_entrypoint_bootstraps_mock_google_adc(tmp_path: Path) -> None:
    home = tmp_path / "home"
    (home / ".config" / "amp").mkdir(parents=True)
    harness_dir = _write_codex_harness_config(home)

    result = subprocess.run(
        [
            "bash",
            str(ENTRYPOINT_SH),
            "sh",
            "-lc",
            'printf \'%s\n\' "$GOOGLE_APPLICATION_CREDENTIALS" && cat "$GOOGLE_APPLICATION_CREDENTIALS"',
        ],
        check=False,
        capture_output=True,
        text=True,
        env={
            "HOME": str(home),
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "CENTAUR_HARNESS_CONFIG_DIR": str(harness_dir),
        },
    )

    assert result.returncode == 0, result.stderr or result.stdout
    adc_path, adc_json = result.stdout.split("\n", 1)
    assert adc_path == str(
        home / ".config" / "gcloud" / "application_default_credentials.json"
    )
    assert Path(adc_path).is_file()
    adc = json.loads(adc_json)
    assert adc == {
        "type": "service_account",
        "project_id": "centaur-sandbox",
        "private_key_id": "0000000000000000000000000000000000000000",
        "private_key": adc["private_key"],
        "client_email": "mock@creds.com",
        "client_id": "100000000000000000000",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/mock%40creds.com",
        "universe_domain": "googleapis.com",
    }
    assert adc["private_key"].startswith("-----BEGIN PRIVATE KEY-----\n")
    assert adc["private_key"].endswith("-----END PRIVATE KEY-----\n")

    codex_config = (home / ".codex" / "config.toml").read_text()
    assert 'model = "gpt-5.5"' in codex_config
    assert 'model_reasoning_effort = "low"' in codex_config
    assert 'plan_mode_reasoning_effort = "high"' in codex_config
    assert 'approval_policy = "on-request"' in codex_config
    assert 'sandbox_mode = "workspace-write"' in codex_config
    assert 'service_tier = "fast"' in codex_config
    assert "max_concurrent_threads_per_session = 6" in codex_config


def test_sandbox_entrypoint_installs_codex_harness_config(tmp_path: Path) -> None:
    home = tmp_path / "home"
    harness_dir = _write_codex_harness_config(home)

    result = subprocess.run(
        [
            "bash",
            str(ENTRYPOINT_SH),
            "sh",
            "-lc",
            'cat "$HOME/.codex/config.toml"',
        ],
        check=False,
        capture_output=True,
        text=True,
        env={
            "HOME": str(home),
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "CENTAUR_HARNESS_CONFIG_DIR": str(harness_dir),
        },
    )

    assert result.returncode == 0, result.stderr or result.stdout
    assert result.stdout == (harness_dir / "codex" / "config.toml").read_text()


def test_sandbox_entrypoint_writes_codex_proxy_auth_stub(tmp_path: Path) -> None:
    home = tmp_path / "home"
    harness_dir = _write_codex_harness_config(home)

    result = subprocess.run(
        [
            "bash",
            str(ENTRYPOINT_SH),
            "sh",
            "-lc",
            'printf "%s/%s\\n" "${OPENAI_API_KEY-unset}" '
            '"${CODEX_API_KEY-unset}"',
        ],
        check=False,
        capture_output=True,
        text=True,
        env={
            "HOME": str(home),
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "CENTAUR_HARNESS_CONFIG_DIR": str(harness_dir),
            "CODEX_USE_LOCAL_AUTH": "true",
            "OPENAI_API_KEY": "OPENAI_API_KEY",
            "CODEX_API_KEY": "CODEX_API_KEY",
        },
    )

    assert result.returncode == 0, result.stderr or result.stdout
    auth = json.loads((home / ".codex" / "auth.json").read_text())
    assert auth["auth_mode"] == "chatgpt"
    _assert_codex_stub_jwt(auth["tokens"]["access_token"])
    _assert_codex_stub_jwt(auth["tokens"]["id_token"])
    assert auth["tokens"]["refresh_token"] == "iron-proxy-codex-stub-refresh-token"
    assert auth["tokens"]["account_id"] == CODEX_STUB_ACCOUNT_ID
    assert result.stdout.splitlines()[-1] == "unset/unset"


def test_sandbox_entrypoint_keeps_claude_api_key_without_credentials(
    tmp_path: Path,
) -> None:
    home = tmp_path / "home"
    harness_dir = _write_codex_harness_config(home)

    result = subprocess.run(
        [
            "bash",
            str(ENTRYPOINT_SH),
            "sh",
            "-lc",
            'printf "%s\\n" "$ANTHROPIC_API_KEY"',
        ],
        check=False,
        capture_output=True,
        text=True,
        env={
            "HOME": str(home),
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "CENTAUR_HARNESS_CONFIG_DIR": str(harness_dir),
            "CLAUDE_USE_LOCAL_AUTH": "true",
            "ANTHROPIC_API_KEY": "ANTHROPIC_API_KEY",
        },
    )

    assert result.returncode == 0, result.stderr or result.stdout
    assert result.stdout.strip() == "ANTHROPIC_API_KEY"


def test_sandbox_entrypoint_codex_local_auth_does_not_fallback_to_api_keys(
    tmp_path: Path,
) -> None:
    home = tmp_path / "home"
    harness_dir = _write_codex_harness_config(home)

    result = subprocess.run(
        [
            "bash",
            str(ENTRYPOINT_SH),
            "sh",
            "-lc",
            'printf "%s/%s/%s\\n" "${OPENAI_API_KEY-unset}" '
            '"${CODEX_API_KEY-unset}" "$ANTHROPIC_API_KEY"',
        ],
        check=False,
        capture_output=True,
        text=True,
        env={
            "HOME": str(home),
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "CENTAUR_HARNESS_CONFIG_DIR": str(harness_dir),
            "CODEX_USE_LOCAL_AUTH": "true",
            "OPENAI_API_KEY": "OPENAI_API_KEY",
            "CODEX_API_KEY": "CODEX_API_KEY",
            "ANTHROPIC_API_KEY": "ANTHROPIC_API_KEY",
        },
    )

    assert result.returncode == 0, result.stderr or result.stdout
    assert result.stdout.strip() == "unset/unset/ANTHROPIC_API_KEY"


def test_sandbox_entrypoint_keeps_claude_proxy_local_auth_placeholder(
    tmp_path: Path,
) -> None:
    home = tmp_path / "home"
    harness_dir = _write_codex_harness_config(home)

    result = subprocess.run(
        [
            "bash",
            str(ENTRYPOINT_SH),
            "sh",
            "-lc",
            'printf "%s/%s\\n" "$ANTHROPIC_AUTH_TOKEN" "${ANTHROPIC_API_KEY-unset}"',
        ],
        check=False,
        capture_output=True,
        text=True,
        env={
            "HOME": str(home),
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "CENTAUR_HARNESS_CONFIG_DIR": str(harness_dir),
            "CLAUDE_USE_LOCAL_AUTH": "true",
            "ANTHROPIC_AUTH_TOKEN": "ANTHROPIC_AUTH_TOKEN",
            "ANTHROPIC_API_KEY": "",
        },
    )

    assert result.returncode == 0, result.stderr or result.stdout
    assert result.stdout.strip() == "ANTHROPIC_AUTH_TOKEN/"


def test_sandbox_entrypoint_appends_codex_laminar_otel_config(tmp_path: Path) -> None:
    home = tmp_path / "home"
    harness_dir = _write_codex_harness_config(home)

    result = subprocess.run(
        [
            "bash",
            str(ENTRYPOINT_SH),
            "sh",
            "-lc",
            'cat "$HOME/.codex/config.toml"',
        ],
        check=False,
        capture_output=True,
        text=True,
        env={
            "HOME": str(home),
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "CENTAUR_HARNESS_CONFIG_DIR": str(harness_dir),
            "CENTAUR_THREAD_KEY": "slack:C123:1700000000.000100",
            "CENTAUR_TRACE_ID": "00000000-0000-0000-0000-000000000123",
            "CODEX_OTEL_ENVIRONMENT": "staging",
            "LMNR_BASE_URL": "http://stg-laminar-app-server.stg-laminar.svc.cluster.local:8000",
            "LMNR_PROJECT_API_KEY": "lmnr-key",
        },
    )

    assert result.returncode == 0, result.stderr or result.stdout
    assert result.stdout.startswith((harness_dir / "codex" / "config.toml").read_text())
    parsed = tomllib.loads(result.stdout)
    assert "exporter" not in parsed["otel"]
    assert (
        parsed["otel"]["trace_exporter"]["otlp-http"]["endpoint"]
        == "http://stg-laminar-app-server.stg-laminar.svc.cluster.local:8000/v1/traces"
    )
    assert "\nexporter = { otlp-http = {" not in result.stdout
    assert "trace_exporter = { otlp-http = {" in result.stdout
    assert (
        'endpoint = "http://stg-laminar-app-server.stg-laminar.svc.cluster.local:8000/v1/traces"'
        in result.stdout
    )
    assert '"x-trace-id" = "00000000-0000-0000-0000-000000000123"' in result.stdout
    assert '"x-centaur-thread-key" = "slack:C123:1700000000.000100"' in result.stdout
    assert '"authorization" = "Bearer lmnr-key"' in result.stdout
    assert 'environment = "staging"' in result.stdout


def test_sandbox_entrypoint_ignores_github_placeholder_token(tmp_path: Path) -> None:
    home = tmp_path / "home"
    harness_dir = _write_codex_harness_config(home)

    result = subprocess.run(
        [
            "bash",
            str(ENTRYPOINT_SH),
            "sh",
            "-lc",
            'sleep 0.2; test ! -f "$HOME/.git-credentials"',
        ],
        check=False,
        capture_output=True,
        text=True,
        env={
            "HOME": str(home),
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "CENTAUR_HARNESS_CONFIG_DIR": str(harness_dir),
            "GITHUB_TOKEN": "GITHUB_TOKEN",
        },
    )

    assert result.returncode == 0, result.stderr or result.stdout


def test_sandbox_entrypoint_writes_real_github_token(tmp_path: Path) -> None:
    home = tmp_path / "home"
    harness_dir = _write_codex_harness_config(home)

    result = subprocess.run(
        [
            "bash",
            str(ENTRYPOINT_SH),
            "sh",
            "-lc",
            'sleep 0.2; cat "$HOME/.git-credentials"',
        ],
        check=False,
        capture_output=True,
        text=True,
        env={
            "HOME": str(home),
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "CENTAUR_HARNESS_CONFIG_DIR": str(harness_dir),
            "GH_TOKEN": "github_pat_fake",
        },
    )

    assert result.returncode == 0, result.stderr or result.stdout
    assert result.stdout == "https://x-access-token:github_pat_fake@github.com\n"


def test_sandbox_entrypoint_sets_github_origin_for_cached_repo(tmp_path: Path) -> None:
    home = tmp_path / "home"
    harness_dir = _write_codex_harness_config(home)
    repo = home / "github" / "leanxyz" / "livermore"
    repo.mkdir(parents=True)
    subprocess.run(["git", "init"], cwd=repo, check=True, capture_output=True, text=True)
    (repo / "README.md").write_text("cached repo\n")
    subprocess.run(["git", "add", "README.md"], cwd=repo, check=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.com",
            "-c",
            "commit.gpgsign=false",
            "commit",
            "-m",
            "initial",
        ],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    )

    result = subprocess.run(
        [
            "bash",
            str(ENTRYPOINT_SH),
            "sh",
            "-lc",
            'git remote get-url origin && git rev-parse --abbrev-ref HEAD',
        ],
        check=False,
        capture_output=True,
        text=True,
        env={
            "HOME": str(home),
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "CENTAUR_HARNESS_CONFIG_DIR": str(harness_dir),
            "AGENT_REPO": "leanxyz/livermore",
        },
    )

    assert result.returncode == 0, result.stderr or result.stdout
    remote_url, branch = result.stdout.strip().split("\n")
    assert remote_url == "https://github.com/leanxyz/livermore.git"
    assert branch.startswith("agent-")


def test_sandbox_entrypoint_imports_claude_skills_before_linking(tmp_path: Path) -> None:
    home = tmp_path / "home"
    harness_dir = _write_codex_harness_config(home)
    repo = home / "github" / "leanxyz" / "livermore"
    repo.mkdir(parents=True)
    subprocess.run(["git", "init"], cwd=repo, check=True, capture_output=True, text=True)

    claude_skill = repo / ".claude" / "skills" / "verify-pr-review"
    agents_skills = repo / ".agents" / "skills"
    claude_skill.mkdir(parents=True)
    agents_skills.mkdir(parents=True)
    (claude_skill / "SKILL.md").write_text("---\nname: verify-pr-review\n---\n")
    (agents_skills / "verify-pr-review").symlink_to(
        "../../.claude/skills/verify-pr-review"
    )
    subprocess.run(["git", "add", "-f", ".claude", ".agents"], cwd=repo, check=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.com",
            "-c",
            "commit.gpgsign=false",
            "commit",
            "-m",
            "initial",
        ],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    )

    result = subprocess.run(
        [
            "bash",
            str(ENTRYPOINT_SH),
            "sh",
            "-lc",
            "test -f .agents/skills/verify-pr-review/SKILL.md && "
            "test -f .claude/skills/verify-pr-review/SKILL.md && "
            "! test -L .agents/skills/verify-pr-review",
        ],
        check=False,
        capture_output=True,
        text=True,
        env={
            "HOME": str(home),
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "CENTAUR_HARNESS_CONFIG_DIR": str(harness_dir),
            "AGENT_REPO": "leanxyz/livermore",
        },
    )

    assert result.returncode == 0, result.stderr or result.stdout
