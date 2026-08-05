#!/usr/bin/env python3
"""Verify and atomically snapshot a mounted portable source for runtime use."""
from __future__ import annotations

import argparse
import hashlib
import os
import re
import sys
import tempfile
from pathlib import Path


SHA256 = re.compile(r"[0-9a-fA-F]{64}")


def snapshot(*, source: Path, destination: Path, expected_sha256: str, label: str) -> str:
    if SHA256.fullmatch(expected_sha256) is None:
        raise ValueError(f"{label} expected digest must be a 64-character hexadecimal SHA-256")
    if not source.is_file():
        raise ValueError(f"{label} source is not a readable file")
    if destination.is_symlink() or destination.parent.is_symlink():
        raise ValueError(f"{label} runtime destination must not use a symlink")
    try:
        raw = source.read_bytes()
    except OSError as exc:
        raise ValueError(f"{label} source cannot be read") from exc
    digest = hashlib.sha256(raw).hexdigest()
    if digest != expected_sha256.lower():
        raise ValueError(f"{label} digest does not match the configured expected SHA-256")

    destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{destination.name}.", dir=destination.parent)
    try:
        with os.fdopen(descriptor, "wb") as target:
            target.write(raw)
            target.flush()
            os.fsync(target.fileno())
        os.chmod(temporary, 0o400)
        os.replace(temporary, destination)
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise
    return digest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--destination", type=Path, required=True)
    parser.add_argument("--expected-sha256", required=True)
    parser.add_argument("--label", required=True)
    args = parser.parse_args()
    try:
        snapshot(
            source=args.source,
            destination=args.destination,
            expected_sha256=args.expected_sha256,
            label=args.label,
        )
    except (OSError, ValueError) as exc:
        print(f"portable source rejected: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
