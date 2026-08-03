from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
from pathlib import Path


SANDBOX_DIR = Path(__file__).parent
SETUP_GIT_AUTH = SANDBOX_DIR / "setup-git-auth.sh"


class SetupGitAuthTest(unittest.TestCase):
    def _run(self, home: Path, extra_env: dict[str, str]) -> None:
        subprocess.run(
            [str(SETUP_GIT_AUTH)],
            check=True,
            env={
                **os.environ,
                "HOME": str(home),
                "GIT_CONFIG_NOSYSTEM": "1",
                "GIT_CONFIG_GLOBAL": str(home / ".gitconfig"),
                **extra_env,
            },
        )

    def test_uses_mounted_token_file_without_persisting_token(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            home = Path(tmp) / "home"
            home.mkdir()
            token_file = Path(tmp) / "token"
            token_file.write_text("test-token\n")

            self._run(home, {"CENTAUR_TOOLS_GITHUB_TOKEN_FILE": str(token_file)})

            askpass = home / ".git-askpass"
            self.assertEqual(
                subprocess.run(
                    [str(askpass), "Username for https://github.com"],
                    check=True,
                    capture_output=True,
                    text=True,
                ).stdout,
                "x-access-token\n",
            )
            self.assertEqual(
                subprocess.run(
                    [str(askpass), "Password for https://github.com"],
                    check=True,
                    capture_output=True,
                    text=True,
                ).stdout,
                "test-token\n",
            )
            self.assertNotIn("test-token", (home / ".gitconfig").read_text())
            self.assertFalse((home / ".git-credentials").exists())

    def test_placeholder_only_sandbox_has_no_git_credentials(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            home = Path(tmp) / "home"
            home.mkdir()

            self._run(home, {"GITHUB_TOKEN": "GITHUB_TOKEN"})

            self.assertFalse((home / ".git-askpass").exists())
            self.assertFalse((home / ".git-credentials").exists())
            gitconfig = home / ".gitconfig"
            self.assertFalse(gitconfig.exists())
