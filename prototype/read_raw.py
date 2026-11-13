"""Read C1 sealed Raw as NDJSON using the frozen Python Recorder package."""

import argparse
import base64
import json
from pathlib import Path

from binance_market_data_recorder.normalize.raw import (
    iter_source_records,
    load_source_chunks,
)
from binance_market_data_recorder.storage.layout import StorageLayout

from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.macos import StorageRegistry
from binance_market_data_recorder.storage.platform import volume_adapter

from partition_roots import reader_lock, resolve_roots


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_root", type=Path)
    parser.add_argument("--cold-root", type=Path)
    args = parser.parse_args()
    seen: set[str] = set()
    with reader_lock(args.data_root):
        for root in resolve_roots(args.data_root, cold_root=args.cold_root):
            layout = StorageLayout.from_root(root)
            with Catalog(layout.catalog, read_only=True) as catalog:
                external = {}
                if catalog.storage_targets():
                    registry = StorageRegistry(catalog=catalog, volumes=volume_adapter())
                    external = {
                        s["storage_id"]: Path(s["resolved_path"])
                        for s in registry.observe_statuses()
                        if s.get("state") in {"READY", "LOW_SPACE"}
                        and s.get("resolved_path")
                    }
                chunks = load_source_chunks(
                    layout=layout, catalog=catalog, external_roots=external
                )
            for chunk in sorted(
                chunks, key=lambda item: item.manifest["created_at_utc_ns"]
            ):
                if chunk.chunk_id in seen:
                    continue
                seen.add(chunk.chunk_id)
                count = 0
                for record in iter_source_records(chunk):
                    fields = record.envelope.canonical_mapping()
                    del fields["raw_payload"]
                    print(
                        json.dumps(
                            {
                                "chunk_id": chunk.chunk_id,
                                "ordinal": record.ordinal,
                                **fields,
                                "raw_payload_base64": base64.b64encode(
                                    record.envelope.raw_payload
                                ).decode(),
                            },
                            separators=(",", ":"),
                        )
                    )
                    count += 1
                if count != chunk.manifest["record_count"]:
                    raise ValueError(
                        f"record count mismatch for chunk {chunk.chunk_id}"
                    )


if __name__ == "__main__":
    main()
