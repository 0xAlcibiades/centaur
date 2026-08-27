#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import binascii
import hashlib
import os
from pathlib import Path


SEPARATOR = "\n\n---\n\n"
OVERLAY_PROMPT = Path("services/sandbox/SYSTEM_PROMPT.md")
OBSERVABILITY_DISABLED_PROMPT = """[Observability access]
This sandbox does not have Centaur observability access. Do not use vlogs, vmetrics, Grafana, or related internal logs/metrics tools.
"""


def _sha256(contents: str) -> str:
    return f"sha256:{hashlib.sha256(contents.encode()).hexdigest()}"


def _append_file_fragment(fragments: list[str], source: Path) -> bool:
    if not source.is_file():
        return False
    fragments.append(source.read_text())
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
    persona_id: str | None = None,
    persona_prompt: str | None = None,
    persona_prompt_hash: str | None = None,
    observability_enabled: bool = True,
) -> None:
    base_prompt = home_dir / "AGENTS_BASE.md"
    baked_prompt = home_dir / "AGENTS.md"
    selected_base = base_prompt if base_prompt.is_file() else baked_prompt
    if not selected_base.is_file():
        return

    persona_fields = (persona_id, persona_prompt, persona_prompt_hash)
    if any(value is not None for value in persona_fields) and not all(
        value is not None for value in persona_fields
    ):
        raise ValueError(
            "persona id, prompt, and prompt hash must either all be set or all be absent"
        )
    if persona_id == "":
        raise ValueError("persona id must not be empty")
    if persona_prompt is not None:
        actual_hash = _sha256(persona_prompt)
        if actual_hash != persona_prompt_hash:
            raise ValueError(
                f"persona prompt hash mismatch: expected {persona_prompt_hash}, got {actual_hash}"
            )

    fragments = [selected_base.read_text()]
    appended: set[Path] = set()

    home_overlay = home_dir / "AGENTS_OVERLAY.md"
    if _append_file_fragment(fragments, home_overlay):
        appended.add(home_overlay.resolve())

    for prompt_path in _mounted_overlay_prompts(repo_mount, baked_prompt):
        if not prompt_path.is_file():
            continue
        resolved = prompt_path.resolve()
        if resolved in appended:
            continue
        if _append_file_fragment(fragments, prompt_path):
            appended.add(resolved)

    if persona_prompt is not None:
        fragments.append(persona_prompt)

    if not observability_enabled:
        fragments.append(OBSERVABILITY_DISABLED_PROMPT)

    effective_prompt = SEPARATOR.join(fragments)
    target_prompt.write_text(effective_prompt)


def _persona_prompt_from_environment() -> str | None:
    encoded = os.environ.get("CENTAUR_PERSONA_PROMPT_BASE64")
    if encoded is None:
        return None
    try:
        return base64.b64decode(encoded, validate=True).decode("utf-8")
    except (binascii.Error, UnicodeDecodeError) as error:
        raise ValueError("CENTAUR_PERSONA_PROMPT_BASE64 is not valid base64 UTF-8") from error


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--home-dir", default=os.path.expanduser("~"))
    parser.add_argument("--repo-mount")
    parser.add_argument("--target-prompt", required=True)
    args = parser.parse_args()

    home_dir = Path(args.home_dir)
    compose_system_prompt(
        home_dir=home_dir,
        target_prompt=Path(args.target_prompt),
        repo_mount=Path(args.repo_mount) if args.repo_mount else home_dir / "github",
        persona_id=os.environ.get("CENTAUR_PERSONA_ID"),
        persona_prompt=_persona_prompt_from_environment(),
        persona_prompt_hash=os.environ.get("CENTAUR_PERSONA_PROMPT_HASH"),
        observability_enabled=os.environ.get(
            "CENTAUR_SANDBOX_OBSERVABILITY_ENABLED", "true"
        ).lower()
        != "false",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
