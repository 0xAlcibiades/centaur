#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import tempfile
from pathlib import Path


SEPARATOR = "\n\n---\n\n"
OVERLAY_PROMPT = Path("services/sandbox/SYSTEM_PROMPT.md")
OBSERVABILITY_DISABLED_PROMPT = """[Observability access]
This sandbox does not have Centaur observability access. Do not use vlogs, vmetrics, Grafana, or related internal logs/metrics tools.
"""
API_SERVER_DISABLED_PROMPT = """[API server access]
This sandbox does not have Centaur API server access. Do not use workflows or tool options that call the api-rs control plane, such as dispatching background agent sessions or downloading Centaur attachment handles.
"""


def _prompt_text(source: Path) -> str | None:
    if not source.is_file():
        return None
    return source.read_text()


def _write_prompt_atomically(target: Path, text: str) -> None:
    if target.is_symlink() or target.parent.is_symlink():
        raise RuntimeError("runtime prompt target must not use a symlink")
    target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile("w", dir=target.parent, delete=False) as temporary:
            temporary.write(text)
            temporary.flush()
            os.fsync(temporary.fileno())
            temporary_path = Path(temporary.name)
        os.replace(temporary_path, target)
    except BaseException:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
        raise


def _append_prompt(parts: list[str], source: Path) -> bool:
    text = _prompt_text(source)
    if text is None:
        return False
    parts.append(text)
    return True


def _mounted_overlay_prompts(repo_mount: Path, baked_prompt: Path) -> list[Path]:
    if not repo_mount.is_dir():
        return []
    prompts = sorted(repo_mount.glob(f"*/*/{OVERLAY_PROMPT}"))
    if not baked_prompt.is_file():
        return prompts

    root_text = baked_prompt.read_text()
    return [prompt for prompt in prompts if prompt.read_text() != root_text]


def compose_system_prompt(
    *,
    home_dir: Path,
    target_prompt: Path,
    repo_mount: Path,
    agent_instructions_path: Path | None = None,
    observability_enabled: bool = True,
    api_server_enabled: bool = True,
) -> None:
    if agent_instructions_path is not None and not agent_instructions_path.is_file():
        raise FileNotFoundError(
            f"portable agent instructions are not a readable file: {agent_instructions_path}"
        )

    base_prompt = home_dir / "AGENTS_BASE.md"
    baked_prompt = home_dir / "AGENTS.md"
    if base_prompt.is_file():
        parts = [base_prompt.read_text()]
    elif baked_prompt.is_file():
        parts = [baked_prompt.read_text()]
    else:
        return

    appended: set[Path] = set()

    home_overlay = home_dir / "AGENTS_OVERLAY.md"
    if _append_prompt(parts, home_overlay):
        appended.add(home_overlay.resolve())

    if agent_instructions_path is not None:
        if _append_prompt(parts, agent_instructions_path):
            appended.add(agent_instructions_path.resolve())

    for prompt_path in _mounted_overlay_prompts(repo_mount, baked_prompt):
        if not prompt_path.is_file():
            continue
        resolved = prompt_path.resolve()
        if resolved in appended:
            continue
        if _append_prompt(parts, prompt_path):
            appended.add(resolved)

    if not observability_enabled:
        parts.append(OBSERVABILITY_DISABLED_PROMPT)
    if not api_server_enabled:
        parts.append(API_SERVER_DISABLED_PROMPT)

    _write_prompt_atomically(target_prompt, SEPARATOR.join(parts))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--home-dir", default=os.path.expanduser("~"))
    parser.add_argument("--repo-mount")
    parser.add_argument("--agent-instructions-path")
    parser.add_argument("--without-observability", action="store_true")
    parser.add_argument("--without-api-server", action="store_true")
    parser.add_argument("--target-prompt", required=True)
    args = parser.parse_args()

    home_dir = Path(args.home_dir)
    compose_system_prompt(
        home_dir=home_dir,
        target_prompt=Path(args.target_prompt),
        repo_mount=Path(args.repo_mount) if args.repo_mount else home_dir / "github",
        agent_instructions_path=(
            Path(args.agent_instructions_path) if args.agent_instructions_path else None
        ),
        observability_enabled=not args.without_observability,
        api_server_enabled=not args.without_api_server,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
