"""Identity of the frozen Python baseline vendored into this branch."""

from __future__ import annotations

import hashlib
from pathlib import Path

BASELINE_COMMIT = "38acb1d0ee1ed7c5343024aead45d3a6a3f70a85"
BASELINE_SCOPE = (
    "src",
    "tests/fixtures/binance",
    "tests/golden",
    "pyproject.toml",
    "requirements",
)
BASELINE_SCHEMA_VERSION = "sbmdr-python-baseline.v1"


def tree_digest(root: Path) -> str:
    """Hash the vendored files deterministically, ignoring caches and markers."""
    root = root.resolve()
    digest = hashlib.sha256()
    files: list[Path] = []
    for relative in BASELINE_SCOPE:
        path = root / relative
        if path.is_file():
            files.append(path)
        elif path.is_dir():
            files.extend(
                candidate
                for candidate in path.rglob("*")
                if candidate.is_file()
                and "__pycache__" not in candidate.parts
                and candidate.suffix != ".pyc"
            )
        else:
            raise FileNotFoundError(f"missing vendored baseline path: {path}")
    for path in sorted(files, key=lambda item: item.relative_to(root).as_posix()):
        relative = path.relative_to(root).as_posix()
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(hashlib.sha256(path.read_bytes()).digest())
    return digest.hexdigest()
