#!/usr/bin/env python3
"""Refresh vendor/python-baseline from a pinned main-branch checkout.

This script is a developer tool, not a runtime dependency. It never follows a
moving branch: the destination is always verified against BASELINE_COMMIT.
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "prototype"))

from baseline import (  # noqa: E402
    BASELINE_COMMIT,
    BASELINE_SCHEMA_VERSION,
    BASELINE_SCOPE,
    tree_digest,
)

README = """# Frozen Python baseline

This directory is a vendored copy of the Python Recorder baseline from `main`
at commit `{commit}`. It exists so `S-BMDR` can build, test, and package its
Python-backed verification/recovery/archive tools without pointing at another
branch or checkout.

Included paths:

{scope}

Do not edit these files by hand. Refresh them from a clean `main` checkout with:

```bash
python3 scripts/vendor_python_baseline.py --source /path/to/main-checkout
```

`BASELINE.json` records the commit, included scope, and a deterministic tree
SHA-256. `prototype/verify.py` refuses to run if the vendored tree changes.
"""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--commit", default=BASELINE_COMMIT)
    parser.add_argument(
        "--destination",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "vendor/python-baseline",
    )
    args = parser.parse_args()
    source = args.source.resolve()

    actual = subprocess.check_output(
        ["git", "-C", str(source), "rev-parse", "HEAD"], text=True
    ).strip()
    if actual != args.commit:
        raise SystemExit(f"source HEAD is {actual}, expected {args.commit}")
    dirty = subprocess.check_output(
        ["git", "-C", str(source), "status", "--porcelain", "--", *BASELINE_SCOPE],
        text=True,
    )
    if dirty.strip():
        raise SystemExit("source has uncommitted changes in the vendored scope")

    destination = args.destination.resolve()
    if destination.exists():
        shutil.rmtree(destination)
    destination.mkdir(parents=True)
    with tempfile.TemporaryDirectory(prefix="sbmdr-baseline-") as temporary:
        archive = Path(temporary) / "baseline.tar"
        subprocess.run(
            [
                "git",
                "-C",
                str(source),
                "archive",
                "--format=tar",
                "--output",
                str(archive),
                args.commit,
                *BASELINE_SCOPE,
            ],
            check=True,
        )
        with tarfile.open(archive, "r:") as bundle:
            bundle.extractall(destination, filter="data")

    marker = {
        "schema_version": BASELINE_SCHEMA_VERSION,
        "commit": args.commit,
        "scope": list(BASELINE_SCOPE),
        "tree_sha256": tree_digest(destination),
    }
    (destination / "BASELINE.json").write_text(
        json.dumps(marker, indent=2, sort_keys=True) + "\n"
    )
    (destination / "README.md").write_text(
        README.format(commit=args.commit, scope="\n".join(f"- `{p}`" for p in BASELINE_SCOPE))
    )
    print(json.dumps(marker, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
