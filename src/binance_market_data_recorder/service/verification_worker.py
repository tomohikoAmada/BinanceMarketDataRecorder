"""One bounded verification child, restarted by the service without stopping capture."""

from __future__ import annotations

import json
import sys
from pathlib import Path

from ..storage.catalog import Catalog
from ..storage.layout import StorageLayout
from ..storage.macos import PlatformVolumeError, StorageRegistry
from ..storage.platform import volume_adapter
from .verification import verify_new


def main() -> int:
    layout = StorageLayout.from_root(Path(sys.argv[1]))
    try:
        with Catalog(layout.catalog) as catalog:
            roots: dict[str, Path] = {}
            try:
                for target in StorageRegistry(catalog=catalog, volumes=volume_adapter()).statuses():
                    if target["state"] in {"READY", "LOW_SPACE"}:
                        roots[str(target["storage_id"])] = Path(str(target["resolved_path"]))
            except (OSError, PlatformVolumeError):
                pass
            result = verify_new(
                layout,
                catalog,
                external_roots=roots,
                max_bytes=int(sys.argv[2]) if len(sys.argv) > 2 else 512 * 1024 * 1024,
            )
        print(json.dumps(result), flush=True)
        return 2 if result.get("failed_files") else 0
    except Exception as exc:
        print(json.dumps({"status": "FAILED", "error_type": type(exc).__name__}), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
