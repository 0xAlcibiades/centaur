from __future__ import annotations

import contextlib
import hashlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path

import codex_profile


ATTESTATION_KEYS = {
    "profile_sha256",
    "model",
    "review_model",
    "model_reasoning_effort",
    "plan_mode_reasoning_effort",
    "personality",
    "project_doc_fallback_filenames",
    "features_memories",
    "features_prevent_idle_sleep",
    "features_goals",
    "features_js_repl",
    "multi_agent_enabled",
    "multi_agent_hide_spawn_agent_metadata",
    "max_concurrent_threads_per_session",
    "multi_agent_min_wait_timeout_ms",
    "multi_agent_default_wait_timeout_ms",
    "multi_agent_max_wait_timeout_ms",
    "multi_agent_tool_namespace",
    "memories_generate_memories",
    "memories_use_memories",
    "memories_disable_on_external_context",
    "default_subagent_model",
    "default_subagent_reasoning_effort",
    "max_depth",
    "job_max_runtime_seconds",
}


class CodexProfileTests(unittest.TestCase):
    def test_entrypoint_uses_the_immutable_profile_helper(self) -> None:
        entrypoint = (Path(__file__).parent / "entrypoint.sh").read_text()

        self.assertIn('"/usr/local/bin/codex-profile-merge"', entrypoint)
        self.assertNotIn('["codex-profile-merge",', entrypoint)
        self.assertIn('"--merge-only"', entrypoint)
        self.assertIn('"--attestation-only"', entrypoint)

    def test_entrypoint_orders_profile_before_deploy_reasoning_and_operator_overlay(self) -> None:
        entrypoint = (Path(__file__).parent / "entrypoint.sh").read_text()

        self.assertLess(
            entrypoint.index("CENTAUR_CODEX_PROFILE_PATH"),
            entrypoint.index("CODEX_MODEL_REASONING_EFFORT is a deployment default"),
        )
        self.assertLess(
            entrypoint.index("CODEX_MODEL_REASONING_EFFORT is a deployment default"),
            entrypoint.index("# CODEX_CONFIG_OVERLAY:"),
        )
        self.assertIn('TARGET_PROMPT="$CODEX_PROMPT_DIR/AGENTS.md"', entrypoint)
        self.assertIn('CLAUDE_PROMPT="$CLAUDE_PROMPT_DIR/CLAUDE.md"', entrypoint)
        self.assertNotIn('TARGET_PROMPT="$WORKSPACE_DIR/AGENTS.md"', entrypoint)
        self.assertIn("CENTAUR_AGENT_INSTRUCTIONS_APPLIED", entrypoint)
        self.assertIn('snapshot-portable-source', entrypoint)
        self.assertIn('"--attestation-only"', entrypoint)
        self.assertIn("CENTAUR_CODEX_PROFILE_SHA256", entrypoint)
        self.assertIn("CENTAUR_AGENT_INSTRUCTIONS_SHA256", entrypoint)
        self.assertIn("AGENTS.override.md is not permitted", entrypoint)
        self.assertIn("must not use untrusted symlinks", entrypoint)
        self.assertIn("/usr/local/bin/compose-system-prompt", entrypoint)
        self.assertIn("/usr/bin/sha256sum", entrypoint)

    def test_merges_portable_agent_defaults_without_touching_runtime_policy(self) -> None:
        config = {
            "model": "baked-model",
            "sandbox_mode": "danger-full-access",
            "features": {"goals": False, "multi_agent_v2": False, "hooks": True},
            "agents": {"max_depth": 2, "job_max_runtime_seconds": 900},
        }
        profile = {
            "model": "gpt-5.6-sol",
            "model_reasoning_effort": "max",
            "features": {
                "goals": True,
                "multi_agent_v2": {
                    "enabled": True,
                    "max_concurrent_threads_per_session": 10,
                    "tool_namespace": "agents",
                },
            },
            "agents": {"max_depth": 3, "default_subagent_model": "gpt-5.6-sol"},
            "memories": {"use_memories": True},
        }

        merged = codex_profile.merge_profile(config, profile)

        self.assertEqual(merged["model"], "gpt-5.6-sol")
        self.assertEqual(merged["sandbox_mode"], "danger-full-access")
        self.assertEqual(
            merged["features"],
            {
                "goals": True,
                "multi_agent_v2": {
                    "enabled": True,
                    "max_concurrent_threads_per_session": 10,
                    "tool_namespace": "agents",
                },
                "hooks": True,
            },
        )
        self.assertEqual(merged["agents"]["max_depth"], 3)
        self.assertEqual(merged["agents"]["job_max_runtime_seconds"], 900)
        self.assertEqual(merged["agents"]["default_subagent_model"], "gpt-5.6-sol")
        self.assertEqual(merged["memories"], {"use_memories": True})

    def test_rejects_forbidden_runtime_and_provider_configuration(self) -> None:
        for profile in (
            {"service_tier": "priority"},
            {"sandbox_mode": "danger-full-access"},
            {"model_providers": {"custom": {}}},
            {"projects": {"/": {"trust_level": "trusted"}}},
            {"mcp_servers": {"database": {}}},
            {"features": {"hooks": True}},
        ):
            with self.subTest(profile=profile):
                with self.assertRaisesRegex(codex_profile.ProfileError, "forbidden"):
                    codex_profile.merge_profile({}, profile)

    def test_rejects_wrong_value_types(self) -> None:
        for profile, message in (
            ({"features": {"goals": "true"}}, "boolean"),
            ({"agents": {"max_depth": True}}, "integer"),
            ({"project_doc_fallback_filenames": ["AGENTS.md", 7]}, "array of strings"),
            ({"memories": True}, "TOML table"),
        ):
            with self.subTest(profile=profile):
                with self.assertRaisesRegex(codex_profile.ProfileError, message):
                    codex_profile.merge_profile({}, profile)

    def test_load_profile_fails_closed_for_missing_or_invalid_toml(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self.assertRaisesRegex(codex_profile.ProfileError, "not a readable file"):
                codex_profile.load_profile(root / "missing.toml")

            invalid = root / "invalid.toml"
            invalid.write_text("model = [\n")
            with self.assertRaisesRegex(codex_profile.ProfileError, "invalid portable Codex profile"):
                codex_profile.load_profile(invalid)

    def test_expected_profile_digest_fails_closed_for_missing_or_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            profile = Path(tmp) / "profile.toml"
            raw = b'model = "gpt-5.6-sol"\n'
            profile.write_bytes(raw)
            expected = hashlib.sha256(raw).hexdigest()

            self.assertEqual(
                codex_profile.load_profile_with_digest(profile, expected_sha256=expected)[1], expected
            )
            for invalid in ("", "a" * 64, "not-a-digest"):
                with self.subTest(expected=invalid):
                    with self.assertRaisesRegex(codex_profile.ProfileError, "digest"):
                        codex_profile.load_profile_with_digest(
                            profile, expected_sha256=invalid
                        )

    def test_loads_nested_portable_profile_shape(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            profile = Path(tmp) / "config.toml"
            profile.write_text(
                '''model = "gpt-5.6-sol"
review_model = "gpt-5.6-sol"
model_reasoning_effort = "max"
plan_mode_reasoning_effort = "high"
personality = "pragmatic"
project_doc_fallback_filenames = ["AGENTS.md", "CONTRIBUTING.md"]

[features]
memories = true
prevent_idle_sleep = true
goals = true
js_repl = false

[features.multi_agent_v2]
enabled = true
hide_spawn_agent_metadata = false
max_concurrent_threads_per_session = 10
min_wait_timeout_ms = 60000
default_wait_timeout_ms = 600000
max_wait_timeout_ms = 3600000
tool_namespace = "agents"

[memories]
generate_memories = true
use_memories = true
disable_on_external_context = true

[agents]
default_subagent_model = "gpt-5.6-terra"
default_subagent_reasoning_effort = "high"
max_depth = 3
job_max_runtime_seconds = 1800
'''
            )

            loaded = codex_profile.load_profile(profile)
            merged = codex_profile.merge_profile(
                {"features": {"multi_agent_v2": False}}, loaded
            )

            self.assertTrue(loaded["features"]["multi_agent_v2"]["enabled"])
            self.assertEqual(
                loaded["features"]["multi_agent_v2"]["max_concurrent_threads_per_session"],
                10,
            )
            self.assertTrue(loaded["memories"]["use_memories"])
            self.assertEqual(
                merged["features"]["multi_agent_v2"]["tool_namespace"], "agents"
            )

    def test_rejects_unknown_nested_agent_and_memory_settings(self) -> None:
        for profile, message in (
            ({"features": {"multi_agent_v2": {"arbitrary": True}}}, "forbidden"),
            (
                {"features": {"multi_agent_v2": {"enabled": True, "tool_namespace": "agents"}}},
                "requires",
            ),
            ({"memories": {"export_to_remote": True}}, "forbidden"),
        ):
            with self.subTest(profile=profile):
                with self.assertRaisesRegex(codex_profile.ProfileError, message):
                    codex_profile.merge_profile({}, profile)

    def test_rejects_out_of_bounds_or_nonportable_agent_settings(self) -> None:
        valid_multi_agent = {
            "enabled": True,
            "hide_spawn_agent_metadata": False,
            "max_concurrent_threads_per_session": 10,
            "min_wait_timeout_ms": 60_000,
            "default_wait_timeout_ms": 600_000,
            "max_wait_timeout_ms": 3_600_000,
            "tool_namespace": "agents",
        }
        for profile, message in (
            ({"model": ""}, "nonempty"),
            ({"personality": ""}, "nonempty"),
            ({"model_reasoning_effort": "ultra"}, "one of"),
            ({"features": {"js_repl": True}}, "only be false"),
            ({"features": {"multi_agent_v2": {**valid_multi_agent, "tool_namespace": "other"}}}, "exactly"),
            ({"features": {"multi_agent_v2": {**valid_multi_agent, "max_concurrent_threads_per_session": 17}}}, "1..16"),
            ({"features": {"multi_agent_v2": {**valid_multi_agent, "min_wait_timeout_ms": 59_999}}}, "60000"),
            ({"agents": {"max_depth": 4}}, "1..3"),
            ({"agents": {"job_max_runtime_seconds": 1_801}}, "60..1800"),
            ({"project_doc_fallback_filenames": ["docs/AGENTS.md"]}, "basename-only"),
        ):
            with self.subTest(profile=profile):
                with self.assertRaisesRegex(codex_profile.ProfileError, message):
                    codex_profile.merge_profile({}, profile)

    def test_attestation_is_sanitized_and_deterministic(self) -> None:
        encoded = codex_profile.attestation(
            "a" * 64,
            {
                "model": "gpt-5.6-sol",
                "model_reasoning_effort": "max",
                "features": {
                    "multi_agent_v2": {
                        "enabled": True,
                        "max_concurrent_threads_per_session": 10,
                    }
                },
                "agents": {
                    "default_subagent_model": "gpt-5.6-terra",
                    "default_subagent_reasoning_effort": "high",
                    "max_depth": 3,
                    "job_max_runtime_seconds": 1_800,
                },
                "mcp_servers": {"must_not": "appear"},
            },
        )

        self.assertEqual(
            json.loads(encoded),
            {
                "profile_sha256": "a" * 64,
                "model": "gpt-5.6-sol",
                "review_model": None,
                "model_reasoning_effort": "max",
                "plan_mode_reasoning_effort": None,
                "personality": None,
                "project_doc_fallback_filenames": None,
                "features_memories": None,
                "features_prevent_idle_sleep": None,
                "features_goals": None,
                "features_js_repl": None,
                "multi_agent_enabled": True,
                "multi_agent_hide_spawn_agent_metadata": None,
                "max_concurrent_threads_per_session": 10,
                "multi_agent_min_wait_timeout_ms": None,
                "multi_agent_default_wait_timeout_ms": None,
                "multi_agent_max_wait_timeout_ms": None,
                "multi_agent_tool_namespace": None,
                "memories_generate_memories": None,
                "memories_use_memories": None,
                "memories_disable_on_external_context": None,
                "default_subagent_model": "gpt-5.6-terra",
                "default_subagent_reasoning_effort": "high",
                "max_depth": 3,
                "job_max_runtime_seconds": 1_800,
            },
        )

    def test_cli_emits_one_sanitized_profile_attestation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = root / "config.toml"
            profile = root / "profile.toml"
            config.write_text('model = "baked"\nmodel_reasoning_effort = "low"\n')
            profile.write_text('model = "gpt-5.6-sol"\n')

            class FakeTomliWriter:
                @staticmethod
                def dumps(_: object) -> str:
                    return 'model = "gpt-5.6-sol"\n'

            stdout = io.StringIO()
            old_argv = sys.argv
            old_module = sys.modules.get("tomli_w")
            try:
                sys.argv = ["codex-profile-merge", "--config", str(config), "--profile", str(profile)]
                sys.modules["tomli_w"] = FakeTomliWriter
                with contextlib.redirect_stdout(stdout):
                    self.assertEqual(codex_profile.main(), 0)
            finally:
                sys.argv = old_argv
                if old_module is None:
                    del sys.modules["tomli_w"]
                else:
                    sys.modules["tomli_w"] = old_module

            lines = stdout.getvalue().splitlines()
            self.assertEqual(len(lines), 1)
            prefix, encoded = lines[0].split(" ", 1)
            self.assertEqual(prefix, "CENTAUR_CODEX_PROFILE_APPLIED")
            self.assertEqual(set(json.loads(encoded)), ATTESTATION_KEYS)

    def test_cli_can_merge_then_attest_the_effective_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = root / "config.toml"
            profile = root / "profile.toml"
            config.write_text('model = "baked"\nmodel_reasoning_effort = "low"\n')
            profile.write_text('model = "gpt-5.6-sol"\n')

            class FakeTomliWriter:
                @staticmethod
                def dumps(value: dict[str, object]) -> str:
                    return "\n".join(f'{key} = "{item}"' for key, item in value.items()) + "\n"

            old_argv = sys.argv
            old_module = sys.modules.get("tomli_w")
            try:
                sys.modules["tomli_w"] = FakeTomliWriter
                sys.argv = [
                    "codex-profile-merge",
                    "--config",
                    str(config),
                    "--profile",
                    str(profile),
                    "--merge-only",
                ]
                with contextlib.redirect_stdout(io.StringIO()) as merge_stdout:
                    self.assertEqual(codex_profile.main(), 0)
                self.assertEqual(merge_stdout.getvalue(), "")

                config.write_text(
                    'model = "operator-model"\nmodel_reasoning_effort = "max"\n'
                )
                sys.argv = [
                    "codex-profile-merge",
                    "--config",
                    str(config),
                    "--profile",
                    str(profile),
                    "--attestation-only",
                ]
                with contextlib.redirect_stdout(io.StringIO()) as attest_stdout:
                    self.assertEqual(codex_profile.main(), 0)
                _, encoded = attest_stdout.getvalue().split(" ", 1)
                payload = json.loads(encoded)
                self.assertEqual(payload["model"], "operator-model")
                self.assertEqual(payload["model_reasoning_effort"], "max")
            finally:
                sys.argv = old_argv
                if old_module is None:
                    del sys.modules["tomli_w"]
                else:
                    sys.modules["tomli_w"] = old_module


if __name__ == "__main__":
    unittest.main()
