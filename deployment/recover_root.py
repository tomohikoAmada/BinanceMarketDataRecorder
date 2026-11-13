"""Recover the active recorder root, including future R3 partition containers."""

from __future__ import annotations

import argparse
import fcntl
import json
import subprocess
import sys
from pathlib import Path


def partition_roots(root: Path) -> list[Path]:
    """Return the roots that must be recovered before starting the recorder.

    Legacy roots have no ``partitions/`` directory and are returned unchanged.
    Future R3 containers are discovered from ``partitions/*/partition.json``;
    the current partition and every partition not explicitly closed are
    recovered.  Published closed partitions are left alone.
    """

    partitions = root / "partitions"
    if not partitions.is_dir():
        return [root]
    current = ""
    current_path = root / "CURRENT"
    if current_path.is_file():
        current = current_path.read_text(encoding="utf-8").strip()
    selected: list[Path] = []
    for candidate in sorted(partitions.iterdir()):
        if not candidate.is_dir():
            continue
        state = ""
        metadata = candidate / "partition.json"
        try:
            value = json.loads(metadata.read_text(encoding="utf-8"))
            if isinstance(value, dict):
                state = str(value.get("state", "")).lower()
        except (OSError, json.JSONDecodeError):
            state = ""
        if candidate.name == current or state not in {"closed", "archived"}:
            selected.append(candidate)
    return selected


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()
    recover = Path(__file__).resolve().parent.parent / "prototype/recover_raw.py"
    partitioned = (args.root / "partitions").is_dir()
    if partitioned:
        lock_path = args.root / "state/c1.lock"
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        with lock_path.open("a") as lock:
            try:
                fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise RuntimeError(
                    f"recorder container is active: {args.root}"
                ) from error
            for root in partition_roots(args.root):
                command = [
                    sys.executable,
                    str(recover),
                    str(root),
                    "--no-lock",
                ]
                if args.quiet:
                    command.append("--quiet")
                subprocess.run(command, check=True)
        return 0
    for root in partition_roots(args.root):
        command = [sys.executable, str(recover), str(root)]
        if args.quiet:
            command.append("--quiet")
        subprocess.run(command, check=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
