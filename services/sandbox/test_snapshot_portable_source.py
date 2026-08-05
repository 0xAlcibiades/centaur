from __future__ import annotations

import hashlib
import tempfile
import unittest
from pathlib import Path

import snapshot_portable_source


class SnapshotPortableSourceTests(unittest.TestCase):
    def test_snapshots_verified_bytes_and_isolated_from_source_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "mounted-profile.toml"
            destination = root / "runtime" / "profile.toml"
            source.write_bytes(b'model = "gpt-5.6-sol"\n')
            expected = hashlib.sha256(source.read_bytes()).hexdigest()

            actual = snapshot_portable_source.snapshot(
                source=source,
                destination=destination,
                expected_sha256=expected,
                label="profile",
            )
            source.write_text('model = "changed"\n')

            self.assertEqual(actual, expected)
            self.assertEqual(destination.read_bytes(), b'model = "gpt-5.6-sol"\n')
            self.assertFalse(destination.is_symlink())

    def test_rejects_missing_or_mismatched_expected_digest_before_write(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "source.md"
            source.write_text("instructions\n")
            destination = root / "runtime" / "instructions.md"
            for expected in ("", "a" * 64, "not-a-digest"):
                with self.subTest(expected=expected):
                    with self.assertRaisesRegex(ValueError, "digest"):
                        snapshot_portable_source.snapshot(
                            source=source,
                            destination=destination,
                            expected_sha256=expected,
                            label="instructions",
                        )
            self.assertFalse(destination.exists())

    def test_rejects_symlinked_runtime_destination(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "source.md"
            source.write_text("instructions\n")
            destination = root / "runtime" / "instructions.md"
            destination.parent.mkdir()
            destination.symlink_to(source)
            with self.assertRaisesRegex(ValueError, "symlink"):
                snapshot_portable_source.snapshot(
                    source=source,
                    destination=destination,
                    expected_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
                    label="instructions",
                )
