"""Verify interruption/corruption preserve a copied C++ fixture before archive deletion."""

import argparse
import errno
import hashlib
import json
import shutil
import tempfile
from dataclasses import asdict
from pathlib import Path

from binance_market_data_recorder.archive.manager import (
    ArchiveError,
    ArchiveManager,
    ArchiveTarget,
)
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import StorageLayout
from binance_market_data_recorder.storage.macos import StorageRegistry
from binance_market_data_recorder.storage.platform import volume_adapter


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("source", type=Path)
    a = p.parse_args()
    reports = []
    for point in ("copy_progress", "before_verify"):
        root = Path(
            tempfile.mkdtemp(
                prefix="archive-fault-",
                dir="/var/lib/binance-market-data-recorder/S-BMDR",
            )
        )
        shutil.copytree(a.source, root, dirs_exist_ok=True)
        target = Path("/srv/recorder-data/S-BMDR") / root.name
        target.mkdir(parents=True)
        layout = StorageLayout.from_root(root)
        original = {
            path: hashlib.sha256(path.read_bytes()).hexdigest()
            for path in layout.sealed.glob("*.zst")
        }
        assert original

        def fault(where, path, point=point):
            if where == point:
                if point == "copy_progress":
                    raise OSError(errno.EIO, "injected interrupted copy")
                with path.open("r+b") as data:
                    data.write(b"broken")

        with Catalog(layout.catalog) as catalog:
            registered = StorageRegistry(
                catalog=catalog, volumes=volume_adapter()
            ).register(target)
            row = catalog.storage_targets()[0]
            archive_target = ArchiveTarget(
                storage_id=registered["storage_id"],
                volume_uuid=row["volume_uuid"],
                registered_relative_path=row["relative_path"],
                marker_nonce=row["marker_nonce"],
                root=target,
            )
            manager = ArchiveManager(
                layout=layout, catalog=catalog, target=archive_target, fault_hook=fault
            )
            try:
                result = asdict(manager.run_once())
            except ArchiveError as error:
                result = {"expected_error": str(error)}
            assert all(
                path.is_file()
                and hashlib.sha256(path.read_bytes()).hexdigest() == value
                for path, value in original.items()
            )
            retry = ArchiveManager(
                layout=layout, catalog=catalog, target=archive_target
            )
            for _ in range(len(original) + 1):
                retry.run_once()
            assert not list(layout.sealed.glob("*.zst"))
            reports.append(
                {
                    "fault": point,
                    "source_preserved_after_failure": True,
                    "retry_completed": True,
                    "failure": result,
                    "root": str(root),
                    "target": str(target),
                }
            )
    print(json.dumps(reports, indent=2))


if __name__ == "__main__":
    main()
