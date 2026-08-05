#!/usr/bin/env python3
"""Apply a portable, repository-owned Codex profile to a runtime config.

The runtime owns credentials, providers, permissions, and sandbox policy.  A
mounted profile may tune only portable agent behavior, so a repository can
carry its preferred model, planning, memories, and subagent defaults without
gaining a deployment-configuration escape hatch.
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
import re
import sys
import tomllib
from collections.abc import Mapping
from pathlib import Path
from typing import Any


class ProfileError(ValueError):
    """The mounted profile is absent, malformed, or exceeds its authority."""


_TOP_LEVEL_SCALARS = {
    "model",
    "review_model",
    "model_reasoning_effort",
    "plan_mode_reasoning_effort",
    "personality",
}
_TOP_LEVEL_LISTS = {"project_doc_fallback_filenames"}
_FEATURE_BOOLEAN_KEYS = {
    "memories",
    "prevent_idle_sleep",
    "goals",
    "js_repl",
}
_MULTI_AGENT_V2_KEYS = {
    "enabled",
    "hide_spawn_agent_metadata",
    "max_concurrent_threads_per_session",
    "min_wait_timeout_ms",
    "default_wait_timeout_ms",
    "max_wait_timeout_ms",
    "tool_namespace",
}
_MEMORY_KEYS = {
    "generate_memories",
    "use_memories",
    "disable_on_external_context",
}
_AGENT_KEYS = {
    "default_subagent_model",
    "default_subagent_reasoning_effort",
    "max_depth",
    "job_max_runtime_seconds",
}
_ALLOWED_TOP_LEVEL = _TOP_LEVEL_SCALARS | _TOP_LEVEL_LISTS | {"features", "memories", "agents"}
_REASONING_EFFORTS = {"none", "minimal", "low", "medium", "high", "xhigh", "max"}
_SHA256 = re.compile(r"[0-9a-fA-F]{64}")


def _deep_merge(base: dict[str, Any], overlay: Mapping[str, Any]) -> dict[str, Any]:
    for key, value in overlay.items():
        if isinstance(value, Mapping) and isinstance(base.get(key), dict):
            _deep_merge(base[key], value)
        else:
            base[key] = copy.deepcopy(value)
    return base


def _require_table(value: Any, path: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ProfileError(f"{path} must be a TOML table")
    return value


def _require_nonempty_string(value: Any, path: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ProfileError(f"{path} must be a nonempty string")


def _validate_profile(profile: Mapping[str, Any]) -> dict[str, Any]:
    unknown = sorted(set(profile) - _ALLOWED_TOP_LEVEL)
    if unknown:
        raise ProfileError(f"forbidden portable profile key(s): {', '.join(unknown)}")

    for key in _TOP_LEVEL_SCALARS & set(profile):
        _require_nonempty_string(profile[key], key)
    for key in {"model_reasoning_effort", "plan_mode_reasoning_effort"} & set(profile):
        if profile[key] not in _REASONING_EFFORTS:
            raise ProfileError(f"{key} must be one of {sorted(_REASONING_EFFORTS)}")

    if "project_doc_fallback_filenames" in profile:
        filenames = profile["project_doc_fallback_filenames"]
        if not isinstance(filenames, list) or not all(isinstance(item, str) for item in filenames):
            raise ProfileError("project_doc_fallback_filenames must be an array of strings")
        for filename in filenames:
            if (
                not filename
                or filename.strip() != filename
                or filename in {".", ".."}
                or "/" in filename
                or "\\" in filename
            ):
                raise ProfileError("project_doc_fallback_filenames must contain basename-only filenames")

    if "features" in profile:
        features = _require_table(profile["features"], "features")
        unknown_features = sorted(set(features) - _FEATURE_BOOLEAN_KEYS - {"multi_agent_v2"})
        if unknown_features:
            raise ProfileError(
                "forbidden portable profile feature(s): " + ", ".join(unknown_features)
            )
        for key, value in features.items():
            if key in _FEATURE_BOOLEAN_KEYS and not isinstance(value, bool):
                raise ProfileError(f"features.{key} must be a boolean")
        if features.get("js_repl") is True:
            raise ProfileError("features.js_repl may only be false in a portable profile")
        if "multi_agent_v2" in features:
            multi_agent_v2 = features["multi_agent_v2"]
            multi_agent_v2 = _require_table(multi_agent_v2, "features.multi_agent_v2")
            unknown_multi_agent = sorted(set(multi_agent_v2) - _MULTI_AGENT_V2_KEYS)
            if unknown_multi_agent:
                raise ProfileError(
                    "forbidden portable multi-agent setting(s): " + ", ".join(unknown_multi_agent)
                )
            required_multi_agent = {
                "enabled",
                "max_concurrent_threads_per_session",
                "tool_namespace",
            }
            if missing_multi_agent := sorted(required_multi_agent - set(multi_agent_v2)):
                raise ProfileError(
                    "features.multi_agent_v2 requires " + ", ".join(missing_multi_agent)
                )
            for key in {"enabled", "hide_spawn_agent_metadata"} & set(multi_agent_v2):
                if not isinstance(multi_agent_v2[key], bool):
                    raise ProfileError(f"features.multi_agent_v2.{key} must be a boolean")
            for key in {
                "max_concurrent_threads_per_session",
                "min_wait_timeout_ms",
                "default_wait_timeout_ms",
                "max_wait_timeout_ms",
            } & set(multi_agent_v2):
                if not isinstance(multi_agent_v2[key], int) or isinstance(multi_agent_v2[key], bool):
                    raise ProfileError(f"features.multi_agent_v2.{key} must be an integer")
            threads = multi_agent_v2["max_concurrent_threads_per_session"]
            if not 1 <= threads <= 16:
                raise ProfileError(
                    "features.multi_agent_v2.max_concurrent_threads_per_session must be 1..16"
                )
            wait_keys = {
                "min_wait_timeout_ms",
                "default_wait_timeout_ms",
                "max_wait_timeout_ms",
            }
            if wait_keys & set(multi_agent_v2):
                if not wait_keys <= set(multi_agent_v2):
                    raise ProfileError(
                        "features.multi_agent_v2 wait limits must set min, default, and max together"
                    )
                minimum = multi_agent_v2["min_wait_timeout_ms"]
                default = multi_agent_v2["default_wait_timeout_ms"]
                maximum = multi_agent_v2["max_wait_timeout_ms"]
                if minimum < 60_000 or maximum > 3_600_000 or not minimum <= default <= maximum:
                    raise ProfileError(
                        "features.multi_agent_v2 wait limits must be 60000..3600000 and ordered"
                    )
            if multi_agent_v2.get("tool_namespace") != "agents":
                raise ProfileError("features.multi_agent_v2.tool_namespace must be exactly 'agents'")

    if "agents" in profile:
        agents = _require_table(profile["agents"], "agents")
        unknown_agents = sorted(set(agents) - _AGENT_KEYS)
        if unknown_agents:
            raise ProfileError(
                "forbidden portable profile agent setting(s): " + ", ".join(unknown_agents)
            )
        for key in {"default_subagent_model", "default_subagent_reasoning_effort"} & set(agents):
            _require_nonempty_string(agents[key], f"agents.{key}")
        if (
            "default_subagent_reasoning_effort" in agents
            and agents["default_subagent_reasoning_effort"] not in _REASONING_EFFORTS
        ):
            raise ProfileError(
                "agents.default_subagent_reasoning_effort must be one of "
                f"{sorted(_REASONING_EFFORTS)}"
            )
        for key in {"max_depth", "job_max_runtime_seconds"} & set(agents):
            if not isinstance(agents[key], int) or isinstance(agents[key], bool):
                raise ProfileError(f"agents.{key} must be an integer")
        if "max_depth" in agents and not 1 <= agents["max_depth"] <= 3:
            raise ProfileError("agents.max_depth must be 1..3")
        if "job_max_runtime_seconds" in agents and not 60 <= agents["job_max_runtime_seconds"] <= 1800:
            raise ProfileError("agents.job_max_runtime_seconds must be 60..1800")

    if "memories" in profile:
        memories = _require_table(profile["memories"], "memories")
        unknown_memories = sorted(set(memories) - _MEMORY_KEYS)
        if unknown_memories:
            raise ProfileError(
                "forbidden portable memory setting(s): " + ", ".join(unknown_memories)
            )
        for key, value in memories.items():
            if not isinstance(value, bool):
                raise ProfileError(f"memories.{key} must be a boolean")

    return copy.deepcopy(dict(profile))


def _verify_expected_digest(actual: str, expected: str | None, label: str) -> None:
    if expected is None or _SHA256.fullmatch(expected) is None:
        raise ProfileError(f"{label} expected digest must be a 64-character hexadecimal SHA-256")
    if actual != expected.lower():
        raise ProfileError(f"{label} digest does not match the configured expected SHA-256")


def load_profile_with_digest(
    path: Path, *, expected_sha256: str | None = None
) -> tuple[dict[str, Any], str]:
    if not path.is_file():
        raise ProfileError(f"portable Codex profile is not a readable file: {path}")
    try:
        raw = path.read_bytes()
        profile = tomllib.loads(raw.decode())
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise ProfileError(f"invalid portable Codex profile {path}: {exc}") from exc
    digest = hashlib.sha256(raw).hexdigest()
    if expected_sha256 is not None:
        _verify_expected_digest(digest, expected_sha256, "portable Codex profile")
    return _validate_profile(profile), digest


def load_profile(path: Path) -> dict[str, Any]:
    return load_profile_with_digest(path)[0]


def merge_profile(config: Mapping[str, Any], profile: Mapping[str, Any]) -> dict[str, Any]:
    """Return a copy of config with the validated portable profile applied."""
    if not isinstance(config, Mapping):
        raise ProfileError("generated Codex config must be a TOML table")
    return _deep_merge(copy.deepcopy(dict(config)), _validate_profile(profile))


def attestation(profile_sha256: str, config: Mapping[str, Any]) -> str:
    features = config.get("features") if isinstance(config.get("features"), Mapping) else {}
    multi_agent = features.get("multi_agent_v2")
    if isinstance(multi_agent, Mapping):
        multi_agent_enabled = multi_agent.get("enabled")
        multi_agent_threads = multi_agent.get("max_concurrent_threads_per_session")
    else:
        multi_agent_enabled = multi_agent
        multi_agent_threads = None
    agents = config.get("agents") if isinstance(config.get("agents"), Mapping) else {}
    memories = config.get("memories") if isinstance(config.get("memories"), Mapping) else {}
    return json.dumps(
        {
            "default_subagent_model": agents.get("default_subagent_model"),
            "default_subagent_reasoning_effort": agents.get("default_subagent_reasoning_effort"),
            "features_goals": features.get("goals"),
            "features_js_repl": features.get("js_repl"),
            "features_memories": features.get("memories"),
            "features_prevent_idle_sleep": features.get("prevent_idle_sleep"),
            "job_max_runtime_seconds": agents.get("job_max_runtime_seconds"),
            "max_concurrent_threads_per_session": multi_agent_threads,
            "max_depth": agents.get("max_depth"),
            "memories_disable_on_external_context": memories.get("disable_on_external_context"),
            "memories_generate_memories": memories.get("generate_memories"),
            "memories_use_memories": memories.get("use_memories"),
            "model": config.get("model"),
            "model_reasoning_effort": config.get("model_reasoning_effort"),
            "multi_agent_enabled": multi_agent_enabled,
            "multi_agent_default_wait_timeout_ms": (
                multi_agent.get("default_wait_timeout_ms")
                if isinstance(multi_agent, Mapping)
                else None
            ),
            "multi_agent_hide_spawn_agent_metadata": (
                multi_agent.get("hide_spawn_agent_metadata")
                if isinstance(multi_agent, Mapping)
                else None
            ),
            "multi_agent_max_wait_timeout_ms": (
                multi_agent.get("max_wait_timeout_ms") if isinstance(multi_agent, Mapping) else None
            ),
            "multi_agent_min_wait_timeout_ms": (
                multi_agent.get("min_wait_timeout_ms") if isinstance(multi_agent, Mapping) else None
            ),
            "multi_agent_tool_namespace": (
                multi_agent.get("tool_namespace") if isinstance(multi_agent, Mapping) else None
            ),
            "personality": config.get("personality"),
            "plan_mode_reasoning_effort": config.get("plan_mode_reasoning_effort"),
            "profile_sha256": profile_sha256,
            "project_doc_fallback_filenames": config.get("project_doc_fallback_filenames"),
            "review_model": config.get("review_model"),
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--profile", type=Path, required=True)
    parser.add_argument("--expected-sha256")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--merge-only", action="store_true")
    mode.add_argument("--attestation-only", action="store_true")
    args = parser.parse_args()

    try:
        config = tomllib.loads(args.config.read_text())
        profile, profile_sha256 = load_profile_with_digest(
            args.profile, expected_sha256=args.expected_sha256
        )
        if args.attestation_only:
            merged = config
        else:
            merged = merge_profile(config, profile)
            import tomli_w

            args.config.write_text(tomli_w.dumps(merged))
    except (OSError, ProfileError, tomllib.TOMLDecodeError) as exc:
        print(f"portable Codex profile rejected: {exc}", file=sys.stderr)
        return 1
    if not args.merge_only:
        print(f"CENTAUR_CODEX_PROFILE_APPLIED {attestation(profile_sha256, merged)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
