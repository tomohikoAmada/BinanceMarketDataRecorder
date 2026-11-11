"""Use frozen Python archive transactions and independent Raw reader for C++ roots."""

import argparse
import hashlib
import json
import sqlite3
from collections import Counter
from pathlib import Path

from binance_market_data_recorder.archive.drain import archive_drain
from binance_market_data_recorder.archive.manager import _validate_external_manifest
from binance_market_data_recorder.normalize.raw import (
    SourceChunk,
    iter_source_records,
)
from binance_market_data_recorder.spool.seal import (
    MANIFEST_SCHEMA_VERSION,
    validate_sealed_artifact,
)
from binance_market_data_recorder.storage.catalog import ArchiveState, Catalog
from binance_market_data_recorder.storage.layout import (
    StorageLayout,
    ensure_storage_layout,
)
from binance_market_data_recorder.storage.macos import StorageRegistry
from binance_market_data_recorder.storage.platform import volume_adapter
from binance_market_data_recorder.storage.source_location import (
    archived_artifact,
    safe_relative,
)


def _load_json(path: Path) -> tuple[bytes, dict[str, object]]:
    body = path.read_bytes()
    document = json.loads(body)
    if not isinstance(document, dict):
        raise ValueError(f"manifest is not an object: {path.name}")
    return body, document


def _load_manifest(path: Path) -> tuple[bytes, dict[str, object]]:
    body, document = _load_json(path)
    if document.get("manifest_schema_version") != MANIFEST_SCHEMA_VERSION:
        raise ValueError(f"unsupported Raw manifest {path.name}")
    if not isinstance(document.get("chunk_id"), str):
        raise ValueError(f"invalid Raw chunk ID in {path.name}")
    for key in ("uncompressed_sha256", "stored_sha256"):
        value = document.get(key)
        if not isinstance(value, str) or len(value) != 64:
            raise ValueError(f"invalid {key} in {path.name}")
    return body, document


def _int(document: dict[str, object], key: str) -> int:
    value = document.get(key)
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f"manifest lacks valid {key}")
    return value


def _catalog_chunks(
    layout: StorageLayout,
    external_roots: dict[str, Path],
    transactions: dict[str, dict[str, object]],
    targets: dict[str, dict[str, object]],
) -> list[tuple[SourceChunk, bool]]:
    """Load manifests, trusting already archived artifacts without re-decompressing them."""
    chunks: list[tuple[SourceChunk, bool]] = []
    for manifest_path in sorted(layout.manifests.glob("*.manifest.json")):
        manifest_bytes, manifest = _load_manifest(manifest_path)
        chunk_id = str(manifest["chunk_id"])
        local = safe_relative(layout.root, manifest.get("relative_path"))
        transaction = transactions.get(chunk_id)
        trusted = False
        if local.is_file():
            artifact = local
        else:
            if transaction is None:
                raise ValueError(f"Raw chunk {chunk_id} is unavailable internally")
            state = ArchiveState(str(transaction["state"]))
            if state not in {
                ArchiveState.VERIFIED,
                ArchiveState.LOCAL_DELETE_PENDING,
                ArchiveState.LOCAL_DELETED,
            }:
                raise ValueError(
                    f"Raw chunk {chunk_id} has incomplete archive state {state.value}"
                )
            root = external_roots.get(str(transaction["storage_id"]))
            if root is None:
                raise ValueError(
                    f"Raw chunk {chunk_id} archive target is not READY"
                )
            artifact = safe_relative(root, transaction["target_relative_path"])
            if not artifact.is_file():
                raise ValueError(f"archived Raw chunk {chunk_id} is missing")
            if artifact.stat().st_size != _int(manifest, "stored_bytes"):
                raise ValueError(f"archived Raw chunk {chunk_id} size mismatch")
            target = targets.get(str(transaction["storage_id"]))
            if target is None:
                raise ValueError(f"archive target row missing for {chunk_id}")
            manifest_digest = hashlib.sha256(manifest_bytes).hexdigest()
            if manifest_digest != transaction["source_manifest_sha256"]:
                raise ValueError(f"Raw chunk {chunk_id} manifest changed after archiving")
            if (
                _int(manifest, "stored_bytes") != transaction["stored_bytes"]
                or manifest["stored_sha256"] != transaction["stored_sha256"]
            ):
                raise ValueError(f"Raw chunk {chunk_id} archive identity mismatch")
            external_manifest = safe_relative(
                root, transaction["external_manifest_relative_path"]
            )
            _, document = _load_json(external_manifest)
            _validate_external_manifest(
                document,
                {
                    "archive_manifest_schema_version": "external-archive-manifest.v1",
                    "transaction_id": transaction["transaction_id"],
                    "chunk_id": chunk_id,
                    "storage_id": transaction["storage_id"],
                    "volume_uuid": target["volume_uuid"],
                    "registered_relative_path": target["relative_path"],
                    "artifact_relative_path": transaction["target_relative_path"],
                    "stored_bytes": transaction["stored_bytes"],
                    "stored_sha256": transaction["stored_sha256"],
                    "source_manifest_sha256": transaction["source_manifest_sha256"],
                },
            )
            trusted = True
        if not trusted:
            validate_sealed_artifact(artifact, manifest)
        chunks.append(
            (
                SourceChunk(
                    chunk_id=chunk_id,
                    artifact_path=artifact,
                    manifest_path=manifest_path,
                    manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
                    manifest=manifest,
                ),
                trusted,
            )
        )
    return sorted(
        chunks,
        key=lambda item: (
            item[0].uncompressed_sha256,
            item[0].chunk_id,
        ),
    )


def reconcile_chunk_sets(
    chunks: list[SourceChunk], catalog_chunks: dict[str, dict[str, object]]
) -> dict[str, object]:
    """Every Catalog chunk and manifest must exist exactly once and be terminal."""
    manifest_ids = {chunk.chunk_id for chunk in chunks}
    catalog_ids = set(catalog_chunks)
    if catalog_ids - manifest_ids:
        raise ValueError(
            f"Catalog chunks without manifests: {sorted(catalog_ids - manifest_ids)}"
        )
    if manifest_ids - catalog_ids:
        raise ValueError(
            f"manifests without Catalog chunks: {sorted(manifest_ids - catalog_ids)}"
        )
    non_terminal = sorted(
        chunk_id
        for chunk_id, row in catalog_chunks.items()
        if row["state"] not in TERMINAL_CHUNK_STATES
    )
    if non_terminal:
        raise ValueError(f"non-terminal Catalog chunks: {non_terminal}")
    for chunk in chunks:
        row = catalog_chunks[chunk.chunk_id]
        if row["record_count"] != _int(chunk.manifest, "record_count"):
            raise ValueError(
                f"Catalog record_count mismatch for {chunk.chunk_id}: "
                f"{row['record_count']} != {chunk.manifest['record_count']}"
            )
        for key in ("uncompressed_bytes", "stored_bytes"):
            if row[key] is not None and row[key] != _int(chunk.manifest, key):
                raise ValueError(
                    f"Catalog {key} mismatch for {chunk.chunk_id}"
                )
        for key in ("uncompressed_sha256", "stored_sha256"):
            if row[key] is not None and row[key] != chunk.manifest[key]:
                raise ValueError(f"Catalog {key} mismatch for {chunk.chunk_id}")
    return {
        "catalog_chunk_count": len(catalog_chunks),
        "manifest_chunk_count": len(manifest_ids),
        "chunk_states": dict(
            Counter(row["state"] for row in catalog_chunks.values())
        ),
    }


TERMINAL_CHUNK_STATES = {
    "SEALED",
    "ARCHIVED_VERIFIED",
    "LOCAL_DELETE_PENDING",
    "LOCAL_DELETED",
}


def _verify(
    layout: StorageLayout,
    catalog: Catalog,
    external_roots: dict[str, Path],
    tables: set[str],
) -> dict[str, object]:
    transactions = (
        {
            str(row["chunk_id"]): row
            for row in catalog.archive_transactions()
        }
        if "archive_transactions" in tables
        else {}
    )
    targets = (
        {
            str(row["storage_id"]): row
            for row in catalog.storage_targets()
        }
        if "storage_targets" in tables
        else {}
    )
    loaded = _catalog_chunks(layout, external_roots, transactions, targets)
    chunks = [chunk for chunk, _ in loaded]
    trusted = {chunk.chunk_id for chunk, is_trusted in loaded if is_trusted}

    with sqlite3.connect(f"file:{layout.catalog}?mode=ro", uri=True) as db:
        catalog_chunks = {
            str(row[0]): {
                "state": str(row[1]),
                "record_count": row[2],
                "uncompressed_bytes": row[3],
                "stored_bytes": row[4],
                "uncompressed_sha256": row[5],
                "stored_sha256": row[6],
            }
            for row in db.execute(
                "SELECT chunk_id,state,record_count,uncompressed_bytes,stored_bytes,"
                "uncompressed_sha256,stored_sha256 FROM chunks"
            )
        }
        archive_states = (
            dict(
                db.execute(
                    "SELECT state,count(*) FROM archive_transactions GROUP BY state"
                )
            )
            if "archive_transactions" in tables
            else {}
        )
        drop_rows = [
            json.loads(row[0])
            for row in db.execute(
                "SELECT evidence_json FROM operational_events "
                "WHERE event_type='INGRESS_DROP'"
            )
        ]
    ingress_drops = {
        "events": len(drop_rows),
        "tail_events": sum(1 for row in drop_rows if row.get("tail")),
        "dropped_ws": sum(
            int(row["dropped"]) for row in drop_rows if not row.get("rest")
        ),
        "dropped_rest": sum(
            int(row["dropped"]) for row in drop_rows if row.get("rest")
        ),
        "first_utc_ns": min(
            (int(row["first_drop_utc_ns"]) for row in drop_rows), default=None
        ),
        "last_utc_ns": max(
            (int(row["last_drop_utc_ns"]) for row in drop_rows), default=None
        ),
    }
    ingress_drops["dropped_total"] = (
        ingress_drops["dropped_ws"] + ingress_drops["dropped_rest"]
    )
    chunk_reconciliation = reconcile_chunk_sets(chunks, catalog_chunks)

    decode: set[str] = set()
    groups: dict[str, list[SourceChunk]] = {}
    for chunk in chunks:
        key = f"{chunk.manifest['market']}/{chunk.manifest['symbol']}/{chunk.manifest['stream']}"
        groups.setdefault(key, []).append(chunk)
        connection_ids = chunk.manifest.get("connection_ids", [])
        chunk_flags = chunk.manifest.get("capture_flags", [])
        if (
            chunk.chunk_id not in trusted
            or chunk.manifest["stream"] == "rest_observations"
            or not isinstance(connection_ids, list)
            or len(connection_ids) != 1
            or "ingress_drop" in chunk_flags
            or "sequence_gap" in chunk_flags
        ):
            decode.add(chunk.chunk_id)
    for group in groups.values():
        ordered = sorted(
            group,
            key=lambda chunk: (
                chunk.manifest.get("receive_time_utc_range_ns", {}).get("min", 0),
                chunk.chunk_id,
            ),
        )
        decode.add(ordered[0].chunk_id)
        decode.add(ordered[-1].chunk_id)

    counts = Counter()
    flags = Counter()
    statuses = Counter()
    total = 0
    ws_records = 0
    rest_records = 0
    ws_payload_bytes = 0
    rest_body_bytes = 0
    source_payload_bytes = Counter()
    connections = {}
    bridge_connections = {}
    connection_ranges = {}
    decoded = 0
    ws_ranges = [
        chunk.manifest["receive_monotonic_range_ns"]
        for chunk in chunks
        if chunk.manifest["stream"] != "rest_observations"
    ]
    ws_span = (
        (max(r["max"] for r in ws_ranges) - min(r["min"] for r in ws_ranges)) / 1e9
        if ws_ranges
        else 0
    )
    for chunk, is_trusted in loaded:
        manifest = chunk.manifest
        key = f"{manifest['market']}/{manifest['symbol']}/{manifest['stream']}"
        if chunk.chunk_id not in decode:
            count = _int(manifest, "record_count")
            total += count
            counts[key] += count
            if manifest["stream"] == "rest_observations":
                rest_records += count
            else:
                ws_records += count
            chunk_connections = manifest.get("connection_ids", [])
            if not isinstance(chunk_connections, list):
                raise ValueError(f"chunk {chunk.chunk_id} has invalid connection_ids")
            connections.setdefault(key, set()).update(str(item) for item in chunk_connections)
            for connection in chunk_connections:
                span = connection_ranges.setdefault(
                    str(connection),
                    [manifest["receive_monotonic_range_ns"]["min"],
                     manifest["receive_monotonic_range_ns"]["max"]],
                )
                span[0] = min(span[0], manifest["receive_monotonic_range_ns"]["min"])
                span[1] = max(span[1], manifest["receive_monotonic_range_ns"]["max"])
            chunk_flags = manifest.get("capture_flags", [])
            if not isinstance(chunk_flags, list):
                raise ValueError(f"chunk {chunk.chunk_id} has invalid capture_flags")
            for flag in chunk_flags:
                flags[f"{key}/{flag}"] += 1
            if "depth_bridge" in chunk_flags:
                bridge_connections.setdefault(key, set()).update(
                    str(item) for item in chunk_connections
                )
            continue
        decoded += 1
        seen = 0
        for record in iter_source_records(
            chunk, canonical=False, validate=False, lightweight=True
        ):
            envelope = record.envelope
            seen += 1
            total += 1
            counts[key] += 1
            source_payload_bytes[key] += len(envelope.raw_payload)
            connections.setdefault(key, set()).add(envelope.connection_id)
            if envelope.module != "binance.public.rest.v1":
                span = connection_ranges.setdefault(
                    envelope.connection_id,
                    [envelope.receive_monotonic_ns, envelope.receive_monotonic_ns],
                )
                span[0] = min(span[0], envelope.receive_monotonic_ns)
                span[1] = max(span[1], envelope.receive_monotonic_ns)
            if "depth_bridge" in envelope.capture_flags:
                bridge_connections.setdefault(key, set()).add(envelope.connection_id)
            for flag in envelope.capture_flags:
                flags[f"{key}/{flag}"] += 1
            if envelope.module == "binance.public.rest.v1":
                rest_records += 1
                provenance = json.loads(envelope.raw_payload)
                encoded = provenance["response"]["raw_body_base64"]
                rest_body_bytes += len(encoded) // 4 * 3 - (
                    2
                    if encoded.endswith("==")
                    else 1
                    if encoded.endswith("=")
                    else 0
                )
                statuses[
                    f"{envelope.market}/{envelope.symbol}/{envelope.source_sequence.get('sourceStream')}/{provenance['response']['status']}/{provenance['response']['valid']}"
                ] += 1
            else:
                ws_records += 1
                ws_payload_bytes += len(envelope.raw_payload)
        if seen != _int(manifest, "record_count"):
            raise ValueError(
                f"Raw chunk {chunk.chunk_id} record count mismatch: {seen}"
            )
    return {
        "status": "PASS",
        "chunks": len(chunks),
        "records": total,
        "ws_records": ws_records,
        "rest_records": rest_records,
        **chunk_reconciliation,
        "ingress_drops": ingress_drops,
        "stored_bytes": sum(_int(c.manifest, "stored_bytes") for c in chunks),
        "uncompressed_bytes": sum(
            _int(c.manifest, "uncompressed_bytes") for c in chunks
        ),
        "ws_payload_bytes": ws_payload_bytes,
        "rest_body_bytes": rest_body_bytes,
        "source_payload_bytes": dict(source_payload_bytes),
        "ws_receive_span_seconds": ws_span,
        "archive_states": archive_states,
        "connections": {k: sorted(v) for k, v in connections.items()},
        "connection_monotonic_ranges_ns": connection_ranges,
        "bridge_connections": {
            k: sorted(v) for k, v in bridge_connections.items()
        },
        "source_counts": dict(counts),
        "capture_flags": dict(flags),
        "rest_statuses": dict(statuses),
        "archive_roots": {k: str(v) for k, v in external_roots.items()},
        "fast_path": {
            "decoded_chunks": decoded,
            "trusted_chunks": len(chunks) - decoded,
            "payload_bytes_complete": decoded == len(chunks),
            "capture_flag_counts_exact": decoded == len(chunks),
        },
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["register", "archive", "verify"])
    parser.add_argument("root", type=Path)
    parser.add_argument("--target", type=Path)
    args = parser.parse_args()
    if args.action == "register":
        layout = ensure_storage_layout(args.root)
        if args.target is None:
            parser.error("--target required")
        args.target.mkdir(parents=True, exist_ok=True)
    else:
        layout = StorageLayout.from_root(args.root)
    with Catalog(layout.catalog, read_only=args.action == "verify") as catalog:
        registry = StorageRegistry(catalog=catalog, volumes=volume_adapter())
        if args.action == "register":
            print(json.dumps(registry.register(args.target), indent=2))
            return
        if args.action == "archive":
            targets = catalog.storage_targets()
            if len(targets) != 1:
                raise ValueError("exactly one archive target required")
            report = archive_drain(
                layout=layout,
                catalog=catalog,
                storage_id=targets[0]["storage_id"],
                max_runtime_seconds=120,
                max_files=1000,
            )
            print(json.dumps(report, indent=2))
            if report.get("failed_transactions", 0):
                raise SystemExit(1)
            return
        with sqlite3.connect(
            f"file:{layout.catalog}?mode=ro", uri=True
        ) as db:
            tables = {
                row[0]
                for row in db.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
        external = {
            s["storage_id"]: Path(s["resolved_path"])
            for s in (registry.observe_statuses() if "storage_targets" in tables else [])
            if s.get("state") in {"READY", "LOW_SPACE"} and s.get("resolved_path")
        }
        print(json.dumps(_verify(layout, catalog, external, tables), indent=2))


if __name__ == "__main__":
    main()
