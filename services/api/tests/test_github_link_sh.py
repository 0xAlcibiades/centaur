"""Regression tests for the sandbox `github-link` helper."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path


GITHUB_LINK_SH = Path(__file__).resolve().parents[2] / "sandbox" / "github-link.sh"


def _run(args: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["PATH"] = f"/opt/homebrew/bin:{env.get('PATH', '')}"
    return subprocess.run(
        args,
        check=False,
        capture_output=True,
        text=True,
        cwd=cwd,
        env=env,
    )


def _git(repo: Path, *args: str) -> None:
    result = _run(["git", *args], repo)
    assert result.returncode == 0, result.stderr or result.stdout


def _init_repo(tmp_path: Path, remote_url: str) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "config", "user.email", "bojack@lean.xyz")
    _git(repo, "config", "user.name", "bojack")
    (repo / "src").mkdir()
    (repo / "src" / "file.ts").write_text("export const value = 1;\n")
    _git(repo, "add", "src/file.ts")
    _git(repo, "commit", "-q", "-m", "init")
    _git(repo, "remote", "add", "origin", remote_url)
    _git(repo, "update-ref", "refs/remotes/origin/main", "HEAD")
    _git(repo, "symbolic-ref", "refs/remotes/origin/HEAD", "refs/remotes/origin/main")
    return repo


def test_github_link_builds_line_url_from_ssh_remote(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path, "git@github.com:leanxyz/livermore.git")

    result = _run(["bash", str(GITHUB_LINK_SH), "src/file.ts:28", "--ref", "main"], repo)

    assert result.returncode == 0, result.stderr or result.stdout
    assert result.stdout.strip() == "https://github.com/leanxyz/livermore/blob/main/src/file.ts#L28"


def test_github_link_encodes_path_and_line_range_from_https_remote(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path, "https://github.com/leanxyz/livermore.git")
    nested = repo / "src" / "file with space.ts"
    nested.write_text("one\ntwo\nthree\n")
    _git(repo, "add", "src/file with space.ts")

    result = _run(
        ["bash", str(GITHUB_LINK_SH), "src/file with space.ts:3-7", "--ref", "main"],
        repo,
    )

    assert result.returncode == 0, result.stderr or result.stdout
    assert result.stdout.strip() == (
        "https://github.com/leanxyz/livermore/blob/main/src/file%20with%20space.ts#L3-L7"
    )


def test_github_link_prefers_current_branch_when_origin_ref_exists(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path, "git@github.com:leanxyz/livermore.git")
    _git(repo, "checkout", "-q", "-b", "fix/file-links")
    _git(repo, "update-ref", "refs/remotes/origin/fix/file-links", "HEAD")

    result = _run(["bash", str(GITHUB_LINK_SH), "src/file.ts:1"], repo)

    assert result.returncode == 0, result.stderr or result.stdout
    assert result.stdout.strip() == "https://github.com/leanxyz/livermore/blob/fix/file-links/src/file.ts#L1"
