from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
from pathlib import Path


ENTRYPOINT = Path(__file__).parent / "entrypoint.sh"


class EntrypointWorkspaceOriginTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        self.upstream = self.root / "upstream.git"
        self.seed = self.root / "seed"
        self.cache = self.root / "home" / "github" / "acme" / "centaur"
        self.workspace = self.root / "workspace"
        self._git("init", "--bare", "--initial-branch=main", str(self.upstream))
        self._git("clone", str(self.upstream), str(self.seed))
        self._git("-C", str(self.seed), "config", "user.name", "Seed User")
        self._git("-C", str(self.seed), "config", "user.email", "seed@example.com")
        (self.seed / "README.md").write_text("seed\n")
        self._git("-C", str(self.seed), "add", "README.md")
        self._git("-C", str(self.seed), "commit", "-m", "chore: seed repository")
        self._git("-C", str(self.seed), "push", "origin", "main")
        self._git("clone", str(self.upstream), str(self.cache))

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _git(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["git", *args],
            check=True,
            capture_output=True,
            text=True,
            env={**os.environ, "GIT_CONFIG_NOSYSTEM": "1"},
        )

    def _repair_workspace_origin(self, *, check: bool = True) -> subprocess.CompletedProcess[str]:
        script = ENTRYPOINT.read_text()
        start = script.index("repair_workspace_origin()")
        end = script.index('if [ "${CENTAUR_PERSISTENT_STATE:-0}" = "1" ]', start)
        functions = script[start:end]
        return subprocess.run(
            [
                "bash",
                "-eu",
                "-c",
                f'{functions}\nrepair_workspace_origin "$1" "$2"',
                "--",
                str(self.cache),
                str(self.workspace),
            ],
            check=check,
            capture_output=True,
            text=True,
            env={**os.environ, "GIT_CONFIG_NOSYSTEM": "1"},
        )

    def _advance_upstream(self) -> str:
        (self.seed / "README.md").write_text("updated\n")
        self._git("-C", str(self.seed), "commit", "-am", "chore: advance upstream")
        self._git("-C", str(self.seed), "push", "origin", "main")
        return self._git("-C", str(self.seed), "rev-parse", "HEAD").stdout.strip()

    def test_shared_workspace_uses_upstream_and_fetches_without_mutating_cache(self) -> None:
        self._git("clone", "--shared", str(self.cache), str(self.workspace))
        self.assertEqual(
            self._git("-C", str(self.workspace), "remote", "get-url", "origin").stdout.strip(),
            str(self.cache),
        )
        expected_head = self._advance_upstream()
        cache_tracking_head = self._git(
            "-C", str(self.cache), "rev-parse", "refs/remotes/origin/main"
        ).stdout.strip()

        self._repair_workspace_origin()
        self._repair_workspace_origin()

        self.assertEqual(
            self._git("-C", str(self.workspace), "remote", "get-url", "origin").stdout.strip(),
            str(self.upstream),
        )
        self._git("-C", str(self.workspace), "fetch", "--quiet", "origin")
        self.assertEqual(
            self._git(
                "-C", str(self.workspace), "rev-parse", "refs/remotes/origin/main"
            ).stdout.strip(),
            expected_head,
        )
        self.assertEqual(
            self._git(
                "-C", str(self.cache), "rev-parse", "refs/remotes/origin/main"
            ).stdout.strip(),
            cache_tracking_head,
        )

    def test_existing_origin_is_replaced_with_cache_upstream(self) -> None:
        self._git("clone", "--shared", str(self.cache), str(self.workspace))
        intentional_origin = "https://example.invalid/intentional.git"
        self._git(
            "-C", str(self.workspace), "remote", "set-url", "origin", intentional_origin
        )

        self._repair_workspace_origin()

        self.assertEqual(
            self._git("-C", str(self.workspace), "remote", "get-url", "origin").stdout.strip(),
            str(self.upstream),
        )

    def test_missing_cache_origin_fails(self) -> None:
        self._git("clone", "--shared", str(self.cache), str(self.workspace))
        self._git("-C", str(self.cache), "remote", "remove", "origin")

        result = self._repair_workspace_origin(check=False)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("cache checkout has no origin", result.stderr)

    def test_regular_workspace_clone_uses_upstream(self) -> None:
        self._git("clone", str(self.cache), str(self.workspace))

        self._repair_workspace_origin()

        self.assertEqual(
            self._git("-C", str(self.workspace), "remote", "get-url", "origin").stdout.strip(),
            str(self.upstream),
        )


if __name__ == "__main__":
    unittest.main()
