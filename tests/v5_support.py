"""Offline synthetic immutable history; bulk fixture generation is not production code."""

from __future__ import annotations

import hashlib
import io
import json
from dataclasses import replace
from pathlib import Path
from typing import Any
from uuid import UUID

import zstandard

from binance_market_data_recorder.spool.format import decode_chunk_header, encode_chunk_header
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tests.unit.test_historical_reconnect_audit import seal_chunk, usdm_envelope


def synthetic_history(root: Path, count: int) -> None:
    layout = ensure_storage_layout(root)
    with Catalog(layout.catalog) as catalog:
        catalog.migrate_acceptance_sequence()
        manifest = seal_chunk(layout, catalog, [usdm_envelope("one", 1)])
        original = catalog.chunk(manifest["chunk_id"])
        assert original is not None
        transitions = [
            dict(row)
            for row in catalog._connection.execute(
                "SELECT * FROM chunk_transitions ORDER BY transition_id"
            )
        ]
        decoded = zstandard.ZstdDecompressor().decompress(
            (root / manifest["relative_path"]).read_bytes()
        )
        header, header_bytes = decode_chunk_header(io.BytesIO(decoded))
        payload = decoded[len(header_bytes) :]
        connection = catalog._connection
        connection.execute("BEGIN")
        try:
            for ordinal in range(1, count):
                chunk_id = str(UUID(int=ordinal))
                cloned = dict(manifest)
                raw = encode_chunk_header(replace(header, chunk_id=UUID(chunk_id))) + payload
                stored = zstandard.ZstdCompressor().compress(raw)

                def renamed(value: str, chunk_id: str = chunk_id) -> str:
                    return value.replace(manifest["chunk_id"], chunk_id).replace(
                        manifest["chunk_id"].replace("-", ""), chunk_id.replace("-", "")
                    )

                cloned.update(
                    chunk_id=chunk_id,
                    relative_path=renamed(manifest["relative_path"]),
                    stored_bytes=len(stored),
                    uncompressed_bytes=len(raw),
                    stored_sha256=hashlib.sha256(stored).hexdigest(),
                    uncompressed_sha256=hashlib.sha256(raw).hexdigest(),
                )
                row: dict[str, Any] = dict(original)
                row.update(
                    {
                        key: cloned[key]
                        for key in (
                            "chunk_id",
                            "stored_bytes",
                            "uncompressed_bytes",
                            "stored_sha256",
                            "uncompressed_sha256",
                        )
                    }
                )
                for key in ("sealed_path", "partial_path", "manifest_path"):
                    row[key] = renamed(row[key]) if row[key] else None
                raw_path = root / row["sealed_path"]
                raw_path.parent.mkdir(parents=True, exist_ok=True)
                raw_path.write_bytes(stored)
                (root / row["manifest_path"]).write_bytes(
                    (json.dumps(cloned, sort_keys=True, separators=(",", ":")) + "\n").encode()
                )
                columns = list(row)
                connection.execute(
                    f"INSERT INTO chunks({','.join(columns)}) "
                    f"VALUES({','.join('?' for _ in columns)})",
                    tuple(row.values()),
                )
                for old in transitions:
                    new = {key: value for key, value in old.items() if key != "transition_id"}
                    new["chunk_id"] = chunk_id
                    new["idempotency_key"] = renamed(new["idempotency_key"])
                    new["evidence_json"] = (
                        renamed(new["evidence_json"])
                        .replace(manifest["stored_sha256"], cloned["stored_sha256"])
                        .replace(manifest["uncompressed_sha256"], cloned["uncompressed_sha256"])
                    )
                    columns = list(new)
                    connection.execute(
                        f"INSERT INTO chunk_transitions({','.join(columns)}) "
                        f"VALUES({','.join('?' for _ in columns)})",
                        tuple(new.values()),
                    )
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
