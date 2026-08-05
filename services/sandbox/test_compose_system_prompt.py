from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import compose_system_prompt


class ComposeSystemPromptTest(unittest.TestCase):
    def test_appends_multiple_overlay_prompts_in_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            home = root / "home"
            workspace = root / "workspace"
            home.mkdir()
            workspace.mkdir()
            (home / "AGENTS.md").write_text("base\n")

            repo_mount = home / "github"
            first = repo_mount / "acme" / "first" / "services" / "sandbox" / "SYSTEM_PROMPT.md"
            second = repo_mount / "acme" / "second" / "services" / "sandbox" / "SYSTEM_PROMPT.md"
            first.parent.mkdir(parents=True)
            second.parent.mkdir(parents=True)
            first.write_text("first overlay\n")
            second.write_text("second overlay\n")

            target = workspace / "AGENTS.md"
            compose_system_prompt.compose_system_prompt(
                home_dir=home,
                target_prompt=target,
                repo_mount=repo_mount,
            )

            self.assertEqual(
                target.read_text(),
                "base\n\n\n---\n\nfirst overlay\n\n\n---\n\nsecond overlay\n",
            )

    def test_uses_agents_base_and_appends_home_overlay_before_repo_overlays(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            home = root / "home"
            workspace = root / "workspace"
            home.mkdir()
            workspace.mkdir()
            (home / "AGENTS.md").write_text("baked\n")
            (home / "AGENTS_BASE.md").write_text("persona base\n")
            (home / "AGENTS_OVERLAY.md").write_text("home overlay\n")

            repo_mount = home / "github"
            prompt = repo_mount / "acme" / "overlay" / "services" / "sandbox" / "SYSTEM_PROMPT.md"
            prompt.parent.mkdir(parents=True)
            prompt.write_text("repo overlay\n")

            target = workspace / "AGENTS.md"
            compose_system_prompt.compose_system_prompt(
                home_dir=home,
                target_prompt=target,
                repo_mount=repo_mount,
            )

            self.assertEqual(
                target.read_text(),
                "persona base\n\n\n---\n\nhome overlay\n\n\n---\n\nrepo overlay\n",
            )

    def test_skips_mounted_copy_of_baked_root_prompt(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            home = root / "home"
            workspace = root / "workspace"
            home.mkdir()
            workspace.mkdir()
            (home / "AGENTS.md").write_text("base\n")

            repo_mount = home / "github"
            root_prompt = (
                repo_mount
                / "paradigmxyz"
                / "centaur"
                / "services"
                / "sandbox"
                / "SYSTEM_PROMPT.md"
            )
            overlay_prompt = (
                repo_mount
                / "acme"
                / "overlay"
                / "services"
                / "sandbox"
                / "SYSTEM_PROMPT.md"
            )
            root_prompt.parent.mkdir(parents=True)
            overlay_prompt.parent.mkdir(parents=True)
            root_prompt.write_text("base\n")
            overlay_prompt.write_text("overlay\n")

            target = workspace / "AGENTS.md"
            compose_system_prompt.compose_system_prompt(
                home_dir=home,
                target_prompt=target,
                repo_mount=repo_mount,
            )

            self.assertEqual(
                target.read_text(),
                "base\n\n\n---\n\noverlay\n",
            )

    def test_composes_global_prompt_without_touching_dirty_repository_agents(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            home = root / "home"
            workspace = root / "workspace"
            home.mkdir()
            workspace.mkdir()
            (home / ".codex").mkdir()
            (home / "AGENTS.md").write_text("platform\n")
            (home / "AGENTS_OVERLAY.md").write_text("home overlay\n")
            portable = root / "portable-agent-instructions.md"
            portable.write_text("portable instructions\n")
            repository_agents = workspace / "AGENTS.md"
            repository_agents.write_text("dirty target repository policy\n")

            repo_mount = home / "github"
            prompt = repo_mount / "acme" / "overlay" / "services" / "sandbox" / "SYSTEM_PROMPT.md"
            prompt.parent.mkdir(parents=True)
            prompt.write_text("mounted system prompt\n")

            target = home / ".codex" / "AGENTS.md"
            for _ in range(2):
                compose_system_prompt.compose_system_prompt(
                    home_dir=home,
                    target_prompt=target,
                    repo_mount=repo_mount,
                    agent_instructions_path=portable,
                )

            self.assertEqual(
                target.read_text(),
                "platform\n\n\n---\n\nhome overlay\n\n\n---\n\n"
                "portable instructions\n\n\n---\n\nmounted system prompt\n",
            )
            self.assertEqual(repository_agents.read_text(), "dirty target repository policy\n")

    def test_missing_portable_instructions_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            home = root / "home"
            workspace = root / "workspace"
            home.mkdir()
            workspace.mkdir()
            (home / "AGENTS.md").write_text("platform\n")

            with self.assertRaisesRegex(FileNotFoundError, "portable agent instructions"):
                compose_system_prompt.compose_system_prompt(
                    home_dir=home,
                    target_prompt=workspace / "AGENTS.md",
                    repo_mount=home / "github",
                    agent_instructions_path=root / "missing.md",
                )

    def test_rerun_without_repository_policy_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            home = root / "home"
            workspace = root / "workspace"
            home.mkdir()
            workspace.mkdir()
            (home / "AGENTS.md").write_text("platform\n")
            target = workspace / "AGENTS.md"

            for _ in range(2):
                compose_system_prompt.compose_system_prompt(
                    home_dir=home,
                    target_prompt=target,
                    repo_mount=home / "github",
                )

            self.assertEqual(target.read_text(), "platform\n")

    def test_writes_atomically_without_following_a_prompt_symlink(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            home = root / "home"
            workspace = root / "workspace"
            home.mkdir()
            workspace.mkdir()
            (home / "AGENTS.md").write_text("platform\n")
            destination = workspace / "AGENTS.md"
            destination.symlink_to(home / "AGENTS.md")

            with self.assertRaisesRegex(RuntimeError, "must not use a symlink"):
                compose_system_prompt.compose_system_prompt(
                    home_dir=home,
                    target_prompt=destination,
                    repo_mount=home / "github",
                )
            self.assertEqual((home / "AGENTS.md").read_text(), "platform\n")

    def test_includes_access_boundaries_in_the_atomic_composition(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            home = root / "home"
            workspace = root / "workspace"
            home.mkdir()
            workspace.mkdir()
            (home / "AGENTS.md").write_text("platform\n")
            target = workspace / "AGENTS.md"

            compose_system_prompt.compose_system_prompt(
                home_dir=home,
                target_prompt=target,
                repo_mount=home / "github",
                observability_enabled=False,
                api_server_enabled=False,
            )

            self.assertIn("[Observability access]", target.read_text())
            self.assertIn("[API server access]", target.read_text())


if __name__ == "__main__":
    unittest.main()
