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


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_root", type=Path)
    layout = StorageLayout.from_root(parser.parse_args().data_root)
    with Catalog(layout.catalog, read_only=True) as catalog:
        chunks = load_source_chunks(layout=layout, catalog=catalog)
        for chunk in sorted(
            chunks, key=lambda item: item.manifest["created_at_utc_ns"]
        ):
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
                raise ValueError(f"record count mismatch for chunk {chunk.chunk_id}")


if __name__ == "__main__":
    main()
