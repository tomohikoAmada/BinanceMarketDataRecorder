"""M5-A read-only dataset catalog & SQLite online-backup publisher for S-BMDR."""

from __future__ import annotations

import argparse
import base64
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import sqlite3
import stat
import subprocess
import sys
import time
from typing import Any

REPO = Path(__file__).resolve().parent.parent
for _extra_sys_path in (
    REPO / "vendor/python-baseline/src",
    REPO / "python/src",
    REPO / "prototype",
):
    if str(_extra_sys_path) not in sys.path:
        sys.path.insert(0, str(_extra_sys_path))

from binance_market_data_recorder.storage.catalog import (  # noqa: E402
    ArchiveState,
    Catalog,
    CatalogStateError,
    ChunkState,
)
from binance_market_data_recorder.storage.layout import StorageLayout  # noqa: E402
from binance_market_data_recorder.storage.macos import StorageRegistry  # noqa: E402
from binance_market_data_recorder.storage.platform import volume_adapter  # noqa: E402
from partition_roots import (  # noqa: E402
    partition_metadata,
    partition_roots,
    reader_lock,
)

EXPORT_SCHEMA_VERSION = "mcp-export.v1"


class CatalogBackupTimeoutError(CatalogStateError):
    """Raised when bounded SQLite online backup exceeds its time budget."""


def bounded_catalog_backup(
    live_catalog: Catalog,
    destination: sqlite3.Connection,
    *,
    pages: int = -1,
    progress: Callable[[int, int, int], object] | None = None,
    sleep: float = 0.250,
    timeout_seconds: float | None = None,
) -> None:
    """Run SQLite online backup via Catalog.open_live_read_only with page/time budget."""

    if not live_catalog.live_read_only:
        raise CatalogStateError("Catalog backup source must be live read-only")
    if not isinstance(destination, sqlite3.Connection):
        raise TypeError("backup destination must be sqlite3.Connection")
    if timeout_seconds is None and progress is None and pages < 0:
        live_catalog.backup_to(destination)
        return
    if timeout_seconds is not None and timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive when set")

    effective_pages = pages
    deadline = (
        None if timeout_seconds is None else time.monotonic() + float(timeout_seconds)
    )
    effective_sleep = sleep
    if deadline is not None:
        if effective_pages <= 0:
            effective_pages = 64
        effective_sleep = min(sleep, max(0.01, float(timeout_seconds) / 10.0))

    def _progress(status: int, remaining: int, total: int) -> None:
        if progress is not None:
            progress(status, remaining, total)
        if deadline is not None and remaining > 0 and time.monotonic() >= deadline:
            raise CatalogBackupTimeoutError(
                f"catalog online backup exceeded {timeout_seconds}s budget "
                f"({remaining}/{total} pages remaining)"
            )

    effective_progress = (
        _progress if (progress is not None or deadline is not None) else None
    )
    with live_catalog._lock:
        live_catalog._connection.backup(
            destination,
            pages=effective_pages,
            progress=effective_progress,
            sleep=effective_sleep,
        )
READY_ARCHIVE_STATES = frozenset(
    {
        ArchiveState.VERIFIED.value,
        ArchiveState.LOCAL_DELETE_PENDING.value,
        ArchiveState.LOCAL_DELETED.value,
    }
)
READY_CHUNK_STATES = frozenset(
    {
        ChunkState.SEALED.value,
        ChunkState.ARCHIVED_VERIFIED.value,
        ChunkState.LOCAL_DELETE_PENDING.value,
        ChunkState.LOCAL_DELETED.value,
    }
)


@dataclass(frozen=True)
class MarketDiscoveryConfig:
    """Discovered storage roots and archive directories for one market."""

    market: str
    active_roots: tuple[Path, ...]
    cold_roots: tuple[Path, ...]
    configured_archive_targets: tuple[Path, ...]


@dataclass(frozen=True)
class DiscoveredPartition:
    """One active or cold partition directory belonging to a market."""

    market: str
    root_container: Path
    partition_root: Path
    partition_id: str
    is_cold: bool
    lock_root: Path


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    with temp_path.open("wb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temp_path, path)


def _atomic_write_json(path: Path, payload: object) -> None:
    text = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    _atomic_write_bytes(path, text.encode("utf-8"))


def _read_json_mapping(path: Path, *, required: bool = False) -> dict[str, Any]:
    if not path.exists():
        if required:
            raise FileNotFoundError(f"missing config file: {path}")
        return {}
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"expected JSON object in {path}")
    return data


def _coerce_paths(value: object) -> list[Path]:
    if value is None:
        return []
    if isinstance(value, str):
        return [Path(value)]
    if isinstance(value, list):
        paths: list[Path] = []
        for item in value:
            if isinstance(item, str) and item:
                paths.append(Path(item))
            elif isinstance(item, dict) and isinstance(item.get("root"), str):
                paths.append(Path(str(item["root"])))
        return paths
    if isinstance(value, dict) and isinstance(value.get("root"), str):
        return [Path(str(value["root"]))]
    raise ValueError(f"unsupported root entry format: {value!r}")


def discover_market_configs(
    *,
    roots_config: Path = Path("/etc/sbmdr/roots.json"),
    metadata_roots_config: Path = Path("/etc/sbmdr/metadata-roots.json"),
    archive_targets_config: Path = Path("/etc/sbmdr/archive-targets.json"),
    legacy_roots_config: Path | None = Path("/etc/sbmdr/legacy-roots.json"),
    extra_roots: dict[str, list[Path]] | None = None,
) -> dict[str, MarketDiscoveryConfig]:
    """Load market roots dynamically from config files without hardcoding timestamps."""

    active_raw = _read_json_mapping(roots_config, required=True)
    cold_raw = _read_json_mapping(metadata_roots_config, required=False)
    archive_raw = _read_json_mapping(archive_targets_config, required=False)
    legacy_raw = (
        _read_json_mapping(legacy_roots_config, required=False)
        if legacy_roots_config is not None
        else {}
    )

    markets = sorted(
        set(active_raw)
        | set(cold_raw)
        | set(archive_raw)
        | set(legacy_raw)
        | set(extra_roots or {})
    )
    configs: dict[str, MarketDiscoveryConfig] = {}
    for market in markets:
        seen_active: list[Path] = []
        for candidate in (
            _coerce_paths(active_raw.get(market))
            + _coerce_paths(legacy_raw.get(market))
            + list((extra_roots or {}).get(market, []))
        ):
            resolved = candidate.resolve()
            if resolved not in seen_active:
                seen_active.append(resolved)
        cold_paths = tuple(p.resolve() for p in _coerce_paths(cold_raw.get(market)))
        archive_paths = tuple(
            p.resolve() for p in _coerce_paths(archive_raw.get(market))
        )
        configs[market] = MarketDiscoveryConfig(
            market=market,
            active_roots=tuple(seen_active),
            cold_roots=cold_paths,
            configured_archive_targets=archive_paths,
        )
    return configs


def discover_partitions(config: MarketDiscoveryConfig) -> list[DiscoveredPartition]:
    """Enumerate unique active + cold partitions for a market via partition_roots."""

    discovered: dict[Path, DiscoveredPartition] = {}
    containers: list[tuple[Path, Path | None]] = []
    if config.active_roots:
        primary_cold = config.cold_roots[0] if config.cold_roots else None
        for idx, active_root in enumerate(config.active_roots):
            cold_root = (
                config.cold_roots[idx]
                if idx < len(config.cold_roots)
                else primary_cold
            )
            containers.append((active_root, cold_root))
    else:
        for cold_root in config.cold_roots:
            containers.append((cold_root, None))

    for container, cold_root in containers:
        if not container.exists() and (cold_root is None or not cold_root.exists()):
            continue
        for root_path in partition_roots(container, cold_root=cold_root):
            resolved = root_path.resolve()
            if resolved in discovered:
                continue
            meta = partition_metadata(resolved)
            partition_id = str(meta.get("partition_id") or resolved.name)
            in_cold = any(
                resolved == cold or cold in resolved.parents
                for cold in config.cold_roots
            )
            lock_root = (
                container
                if (container / "partitions").exists() or (container / "CURRENT").exists()
                else resolved
            )
            discovered[resolved] = DiscoveredPartition(
                market=config.market,
                root_container=container,
                partition_root=resolved,
                partition_id=partition_id,
                is_cold=in_cold,
                lock_root=lock_root,
            )

    def _sort_key(item: DiscoveredPartition) -> tuple[int, str, str]:
        meta = partition_metadata(item.partition_root)
        return (
            int(meta.get("start_utc_ns") or 0),
            item.partition_id,
            str(item.partition_root),
        )

    return sorted(discovered.values(), key=_sort_key)


def _partition_fingerprint(
    partition: DiscoveredPartition,
    layout: StorageLayout,
    configured_archive_targets: tuple[Path, ...],
) -> tuple[str, int, int, bool]:
    """Compute metadata fingerprint and catalog/WAL byte sizes for snapshot reuse."""

    hasher = hashlib.sha256()
    catalog_stat = layout.catalog.stat()
    wal_path = Path(f"{layout.catalog}-wal")
    wal_bytes = wal_path.stat().st_size if wal_path.exists() else 0
    partition_json = partition.partition_root / "partition.json"
    pjson_sha = _sha256_file(partition_json) if partition_json.is_file() else ""
    if layout.manifests.is_dir():
        manifests_dir_mtime_ns = layout.manifests.stat().st_mtime_ns
        manifest_count = sum(
            1
            for entry in os.scandir(layout.manifests)
            if entry.name.endswith(".manifest.json") and entry.is_file(follow_symlinks=False)
        )
    else:
        manifests_dir_mtime_ns = 0
        manifest_count = 0

    payload = {
        "schema": EXPORT_SCHEMA_VERSION,
        "market": partition.market,
        "partition_id": partition.partition_id,
        "is_cold": partition.is_cold,
        "partition_json_sha256": pjson_sha,
        "catalog_size": catalog_stat.st_size,
        "catalog_mtime_ns": catalog_stat.st_mtime_ns,
        "wal_bytes": wal_bytes,
        "manifest_count": manifest_count,
        "manifests_dir_mtime_ns": manifests_dir_mtime_ns,
        "configured_archive_targets": [str(p) for p in configured_archive_targets],
    }
    hasher.update(json.dumps(payload, sort_keys=True).encode("utf-8"))
    return hasher.hexdigest(), catalog_stat.st_size, wal_bytes, wal_bytes == 0


def _resolve_external_roots(
    catalog: Catalog,
    connection: sqlite3.Connection,
    configured_targets: tuple[Path, ...],
) -> dict[str, Path]:
    """Resolve storage_id -> archive target directory via StorageRegistry + config fallback."""

    external: dict[str, Path] = {}
    try:
        registry = StorageRegistry(catalog=catalog, volumes=volume_adapter())
        for status in registry.observe_statuses():
            if status.get("state") in {"READY", "LOW_SPACE"} and status.get(
                "resolved_path"
            ):
                external[str(status["storage_id"])] = Path(
                    str(status["resolved_path"])
                ).resolve()
    except Exception:
        pass

    rows = connection.execute(
        "SELECT storage_id, relative_path FROM storage_targets"
    ).fetchall()
    for row in rows:
        storage_id = str(row["storage_id"])
        if storage_id in external:
            continue
        rel = str(row["relative_path"]).strip("/")
        for target in configured_targets:
            target_str = str(target).rstrip("/")
            if rel and (target_str.endswith("/" + rel) or target.name == rel):
                external[storage_id] = target.resolve()
                break
            if len(configured_targets) == 1 and target.is_dir():
                external[storage_id] = target.resolve()
                break
    return external


def _verify_sqlite_integrity(db_path: Path) -> None:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro&immutable=1", uri=True)
    try:
        rows = conn.execute("PRAGMA integrity_check").fetchall()
        if [tuple(r) for r in rows] != [("ok",)]:
            raise RuntimeError(f"backed up catalog failed integrity_check: {rows!r}")
    finally:
        conn.close()


def _safe_rel_posix(value: object) -> PurePosixPath | None:
    if not isinstance(value, str) or not value or "\x00" in value:
        return None
    normalized = value.replace("\\", "/")
    posix = PurePosixPath(normalized)
    if posix.is_absolute() or ".." in posix.parts:
        return None
    return posix


def _scan_archive_target(target_root: Path) -> dict[str, int]:
    """Scan raw/ and manifests/ once via os.scandir to map rel_path -> byte size."""

    index: dict[str, int] = {}
    for subdir in ("raw", "manifests"):
        subdir_path = target_root / subdir
        if not subdir_path.is_dir():
            continue
        with os.scandir(subdir_path) as entries:
            for entry in entries:
                if entry.is_file(follow_symlinks=False):
                    index[f"{subdir}/{entry.name}"] = entry.stat(follow_symlinks=False).st_size
    return index


CHUNK_METADATA_VERSION = "m6a.v1"


def _extract_raw_manifest_fields(manifest_path: Path) -> dict[str, Any]:
    """Read raw-manifest.v1 metadata from local manifest or archive-manifest without touching .bmdr.zst."""

    try:
        doc = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(doc, dict):
        return {}
    raw: dict[str, Any] | None = None
    if isinstance(doc.get("raw_manifest"), dict):
        raw = doc["raw_manifest"]
    elif isinstance(doc.get("raw_manifest_bytes_base64"), str) and doc["raw_manifest_bytes_base64"]:
        try:
            decoded = json.loads(
                base64.b64decode(doc["raw_manifest_bytes_base64"], validate=True).decode("utf-8")
            )
            if isinstance(decoded, dict):
                raw = decoded
        except (ValueError, json.JSONDecodeError):
            raw = None
    elif doc.get("schema_version") == "raw-manifest.v1" or "receive_time_utc_range_ns" in doc:
        raw = doc
    if not raw:
        return {}
    out: dict[str, Any] = {}
    for key in (
        "symbol",
        "stream",
        "receive_time_utc_range_ns",
        "exchange_time_ranges",
        "sequence_ranges",
        "connection_ids",
    ):
        val = raw.get(key)
        if val is not None:
            out[key] = val
    return out


def _load_previous_chunks_map(export_root: Path, prev_partition: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    """Index previously published chunks.jsonl rows by chunk_id for O(new) manifest hydration."""

    if not isinstance(prev_partition, dict):
        return {}
    files = prev_partition.get("files")
    if not isinstance(files, dict):
        return {}
    meta = files.get("chunks_jsonl")
    if not isinstance(meta, dict) or not isinstance(meta.get("url_path"), str):
        return {}
    chunks_path = export_root / meta["url_path"].removeprefix("export/")
    if not chunks_path.is_file():
        return {}
    by_id: dict[str, dict[str, Any]] = {}
    try:
        with chunks_path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                cid = obj.get("chunk_id")
                if isinstance(cid, str) and cid:
                    by_id[cid] = obj
    except (OSError, json.JSONDecodeError):
        return {}
    return by_id


def _inspect_snapshot_catalog(
    *,
    snapshot_db_path: Path,
    partition: DiscoveredPartition,
    external_roots: dict[str, Path],
    previous_chunks_by_id: dict[str, dict[str, Any]] | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any], set[Path]]:
    """Build deterministic chunk, readiness, quarantine, and gap payloads from backup DB."""

    conn = sqlite3.connect(f"file:{snapshot_db_path}?mode=ro&immutable=1", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        chunks_rows = conn.execute(
            """
            SELECT chunk_id, state, partial_path, sealed_path, manifest_path,
                   record_count, uncompressed_bytes, stored_bytes,
                   uncompressed_sha256, stored_sha256,
                   created_at_utc_ns, updated_at_utc_ns
            FROM chunks
            ORDER BY created_at_utc_ns ASC, chunk_id ASC
            """
        ).fetchall()
        tx_rows = conn.execute(
            """
            SELECT transaction_id, chunk_id, storage_id, state, market, stream,
                   source_relative_path, source_manifest_relative_path,
                   target_relative_path, external_manifest_relative_path,
                   stored_bytes, stored_sha256, verified_at_utc_ns, local_deleted_at_utc_ns
            FROM archive_transactions
            """
        ).fetchall()
        tx_by_chunk = {str(r["chunk_id"]): dict(r) for r in tx_rows}

        quarantine_rows = conn.execute(
            """
            SELECT artifact_id, relative_path, reason, sha256, quarantined_at_utc_ns
            FROM quarantined_artifacts
            ORDER BY quarantined_at_utc_ns ASC, artifact_id ASC
            """
        ).fetchall()
        availability_rows = conn.execute(
            "SELECT source_id, state_json FROM source_availability ORDER BY source_id ASC"
        ).fetchall()
        gap_rows = conn.execute(
            """
            SELECT gap_id, source_id, started_at_utc_ns, ended_at_utc_ns, evidence_json
            FROM source_availability_gaps
            ORDER BY started_at_utc_ns ASC, gap_id ASC
            """
        ).fetchall()
        op_rows = conn.execute(
            """
            SELECT event_id, event_type, occurred_at_utc_ns, market, symbol, stream, gap_id
            FROM operational_events
            WHERE event_type IN ('STREAM_DISCONTINUITY_STARTED', 'STREAM_DISCONTINUITY_COMPLETED')
            ORDER BY occurred_at_utc_ns ASC, event_id ASC
            """
        ).fetchall()
        target_rows = conn.execute(
            "SELECT storage_id, volume_uuid, relative_path FROM storage_targets ORDER BY storage_id ASC"
        ).fetchall()
    finally:
        conn.close()

    chunk_entries: list[dict[str, Any]] = []
    ready_files_to_grant: set[Path] = set()
    recorded_symbols: set[str] = set()
    recorded_streams: set[str] = set()
    target_indexes = {
        sid: _scan_archive_target(target_root)
        for sid, target_root in external_roots.items()
    }
    prev_chunks = previous_chunks_by_id or {}
    counts = {
        "total": 0,
        "ready": 0,
        "pending": 0,
        "quarantined": len(quarantine_rows),
        "gaps": len(gap_rows),
    }

    for row in chunks_rows:
        counts["total"] += 1
        chunk_id = str(row["chunk_id"])
        chunk_state = str(row["state"])
        tx = tx_by_chunk.get(chunk_id)

        ready = False
        pending_reason: str | None = None
        storage_id: str | None = None
        target_rel: str | None = None
        ext_manifest_rel: str | None = None
        archive_url_path: str | None = None
        manifest_url_path: str | None = None
        verified_bytes = row["stored_bytes"]
        verified_sha256 = row["stored_sha256"]
        resolved_manifest_file: Path | None = None

        if tx is None:
            pending_reason = (
                "active_chunk"
                if chunk_state == ChunkState.ACTIVE.value
                else f"unarchived_{chunk_state.lower()}"
            )
            raw_mp = row["manifest_path"]
            if isinstance(raw_mp, str) and raw_mp:
                cand_mp = Path(raw_mp)
                if not cand_mp.is_absolute():
                    cand_mp = partition.partition_root / raw_mp
                if cand_mp.is_file():
                    resolved_manifest_file = cand_mp
        else:
            tx_state = str(tx["state"])
            storage_id = str(tx["storage_id"])
            target_rel = str(tx["target_relative_path"])
            ext_manifest_rel = str(tx["external_manifest_relative_path"])
            verified_bytes = tx["stored_bytes"]
            verified_sha256 = tx["stored_sha256"]
            if chunk_state not in READY_CHUNK_STATES:
                pending_reason = f"chunk_state_{chunk_state.lower()}"
            elif tx_state not in READY_ARCHIVE_STATES:
                pending_reason = f"archive_state_{tx_state.lower()}"
            elif storage_id not in external_roots:
                pending_reason = "archive_target_unregistered"
            else:
                target_root = external_roots[storage_id]
                target_posix = _safe_rel_posix(target_rel)
                manifest_posix = _safe_rel_posix(ext_manifest_rel)
                if target_posix is None or manifest_posix is None:
                    pending_reason = "unsafe_archive_relative_path"
                else:
                    target_norm = target_posix.as_posix()
                    manifest_norm = manifest_posix.as_posix()
                    file_sizes = target_indexes[storage_id]
                    actual_size = file_sizes.get(target_norm)
                    artifact_path = target_root / target_norm
                    if actual_size is None and artifact_path.is_file():
                        actual_size = artifact_path.stat().st_size
                    if actual_size is None:
                        pending_reason = "archive_artifact_missing"
                    elif (
                        verified_bytes is not None
                        and verified_bytes > 0
                        and actual_size != int(verified_bytes)
                    ):
                        pending_reason = "archive_artifact_size_mismatch"
                    else:
                        ready = True
                        archive_url_path = f"archive/{storage_id}/{target_norm}"
                        ready_files_to_grant.add(artifact_path)
                        manifest_path = target_root / manifest_norm
                        if manifest_norm in file_sizes or manifest_path.is_file():
                            manifest_url_path = f"archive/{storage_id}/{manifest_norm}"
                            ready_files_to_grant.add(manifest_path)
                            resolved_manifest_file = manifest_path

        if ready:
            counts["ready"] += 1
        else:
            counts["pending"] += 1

        tx_archive_state = tx["state"] if tx else None
        prev_entry = prev_chunks.get(chunk_id)
        can_reuse_meta = (
            prev_entry is not None
            and prev_entry.get("state") == chunk_state
            and prev_entry.get("archive_state") == tx_archive_state
            and prev_entry.get("stored_sha256") == verified_sha256
            and prev_entry.get("receive_time_utc_range_ns") is not None
        )
        if can_reuse_meta:
            raw_meta = {
                key: prev_entry[key]
                for key in (
                    "symbol",
                    "stream",
                    "receive_time_utc_range_ns",
                    "exchange_time_ranges",
                    "sequence_ranges",
                    "connection_ids",
                )
                if prev_entry.get(key) is not None
            }
        elif resolved_manifest_file is not None:
            raw_meta = _extract_raw_manifest_fields(resolved_manifest_file)
        else:
            raw_meta = {}

        stream_val = tx["stream"] if tx else raw_meta.get("stream")
        symbol_val = raw_meta.get("symbol")
        if isinstance(symbol_val, str) and symbol_val and symbol_val != "__GLOBAL__":
            recorded_symbols.add(symbol_val)
        if isinstance(stream_val, str) and stream_val:
            recorded_streams.add(stream_val)

        entry: dict[str, Any] = {
            "chunk_id": chunk_id,
            "market": tx["market"] if tx else partition.market,
            "symbol": symbol_val,
            "stream": stream_val,
            "state": chunk_state,
            "ready": ready,
            "pending_reason": pending_reason,
            "archive_state": tx_archive_state,
            "storage_id": storage_id,
            "target_relative_path": target_rel,
            "external_manifest_relative_path": ext_manifest_rel,
            "archive_url_path": archive_url_path,
            "manifest_url_path": manifest_url_path,
            "record_count": int(row["record_count"] or 0),
            "uncompressed_bytes": row["uncompressed_bytes"],
            "stored_bytes": verified_bytes,
            "stored_sha256": verified_sha256,
            "uncompressed_sha256": row["uncompressed_sha256"],
            "created_at_utc_ns": int(row["created_at_utc_ns"]),
            "updated_at_utc_ns": int(row["updated_at_utc_ns"]),
        }
        for opt_key in (
            "receive_time_utc_range_ns",
            "exchange_time_ranges",
            "sequence_ranges",
            "connection_ids",
        ):
            if opt_key in raw_meta:
                entry[opt_key] = raw_meta[opt_key]
        chunk_entries.append(entry)

    availability_payload = {
        "chunk_metadata_version": CHUNK_METADATA_VERSION,
        "partition_id": partition.partition_id,
        "market": partition.market,
        "recorded_symbols": sorted(recorded_symbols),
        "recorded_streams": sorted(recorded_streams),
        "sources": [
            {
                "source_id": str(r["source_id"]),
                "state": json.loads(str(r["state_json"])),
            }
            for r in availability_rows
        ],
        "gaps": [
            {
                "gap_id": str(r["gap_id"]),
                "source_id": str(r["source_id"]),
                "started_at_utc_ns": int(r["started_at_utc_ns"]),
                "ended_at_utc_ns": r["ended_at_utc_ns"],
                "evidence": json.loads(str(r["evidence_json"])),
            }
            for r in gap_rows
        ],
        "discontinuity_events": [dict(r) for r in op_rows],
        "quarantined_artifacts": [dict(r) for r in quarantine_rows],
        "storage_targets": [dict(r) for r in target_rows],
    }

    return counts, chunk_entries, availability_payload, ready_files_to_grant


def _load_previous_manifest(export_root: Path) -> dict[str, Any]:
    latest_path = export_root / "latest.json"
    if not latest_path.is_file():
        return {}
    try:
        latest = json.loads(latest_path.read_text(encoding="utf-8"))
        manifest_rel = latest.get("manifest_rel_path") or (
            str(latest["manifest_url_path"]).removeprefix("export/")
            if isinstance(latest.get("manifest_url_path"), str)
            else None
        )
        if not isinstance(manifest_rel, str):
            return {}
        manifest_path = export_root / manifest_rel
        if not manifest_path.is_file():
            return {}
        data = json.loads(manifest_path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def _snapshot_is_valid(export_root: Path, entry: dict[str, Any]) -> bool:
    files = entry.get("files")
    if not isinstance(files, dict):
        return False
    for key in ("catalog_sqlite", "chunks_jsonl", "availability_json", "partition_json"):
        meta = files.get(key)
        if not isinstance(meta, dict):
            return False
        rel = meta.get("url_path")
        expected_sha = meta.get("sha256")
        if not isinstance(rel, str) or not isinstance(expected_sha, str):
            return False
        rel_in_export = rel.removeprefix("export/")
        path = export_root / rel_in_export
        if not path.is_file() or _sha256_file(path) != expected_sha:
            return False
    return True


def _grant_read_acls(
    user: str,
    *,
    export_root: Path,
    archive_roots: dict[str, Path],
    files: set[Path],
) -> None:
    """Grant minimal POSIX read ACLs for static serving without changing live DB permissions."""

    setfacl_bin = shutil.which("setfacl")
    if not setfacl_bin:
        raise RuntimeError(f"setfacl not found; cannot grant read ACL to {user!r}")

    dirs_to_traverse: set[Path] = set()
    for root in [export_root, *archive_roots.values()]:
        resolved = root.resolve()
        dirs_to_traverse.add(resolved)
        for parent in resolved.parents:
            if parent != parent.parent:
                dirs_to_traverse.add(parent)
        for subdir in ("raw", "manifests"):
            candidate = resolved / subdir
            if candidate.is_dir():
                dirs_to_traverse.add(candidate)

    for directory in sorted(dirs_to_traverse):
        if directory == Path("/"):
            continue
        if (directory.stat().st_mode & stat.S_IXOTH) == 0:
            subprocess.run(
                [setfacl_bin, "-m", f"u:{user}:x", str(directory)],
                check=True,
            )
    if export_root.exists():
        subprocess.run(
            [setfacl_bin, "-R", "-m", f"u:{user}:rX", str(export_root)],
            check=True,
        )

    batch = [str(p) for p in sorted(files) if p.is_file()]
    chunk_size = 500
    for start in range(0, len(batch), chunk_size):
        subprocess.run(
            [setfacl_bin, "-m", f"u:{user}:r", *batch[start : start + chunk_size]],
            check=True,
        )


def _prune_old_revisions(
    export_root: Path,
    *,
    retain_revisions: int,
) -> dict[str, int]:
    """Keep the most recent N revisions and delete unreferenced snapshots."""

    revisions_dir = export_root / "revisions"
    if not revisions_dir.is_dir():
        return {"pruned_revisions": 0, "pruned_snapshots": 0}
    rev_dirs = sorted(
        (d for d in revisions_dir.iterdir() if d.is_dir() and not d.name.startswith(".")),
        key=lambda p: p.name,
        reverse=True,
    )
    kept = rev_dirs[: max(1, retain_revisions)]
    removed_revs = 0
    for stale in rev_dirs[max(1, retain_revisions) :]:
        shutil.rmtree(stale)
        removed_revs += 1

    referenced_snapshot_dirs: set[Path] = set()
    for rev_dir in kept:
        manifest_path = rev_dir / "manifest.json"
        if not manifest_path.is_file():
            continue
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        for part in manifest.get("partitions", []):
            snap_rel = part.get("snapshot_rel_dir")
            if isinstance(snap_rel, str):
                referenced_snapshot_dirs.add((export_root / snap_rel).resolve())

    snapshots_root = export_root / "snapshots"
    removed_snaps = 0
    if snapshots_root.is_dir():
        for catalog_file in list(snapshots_root.glob("*/*/*/catalog/catalog.sqlite")):
            snap_dir = catalog_file.parent.parent.resolve()
            if snap_dir not in referenced_snapshot_dirs:
                shutil.rmtree(snap_dir)
                removed_snaps += 1
    return {
        "pruned_revisions": removed_revs,
        "pruned_snapshots": removed_snaps,
    }


def render_nginx_config(
    *,
    export_root: Path,
    archive_roots: dict[str, Path],
    listen_addrs: tuple[str, ...] = ("127.0.0.1:18080",),
    server_name: str = "_",
    template_path: Path | None = None,
) -> str:
    """Render read-only Nginx static serving configuration from discovered storage targets."""

    default_template = Path(__file__).resolve().with_name("nginx-mcp-export.conf.template")
    tpl = (template_path or default_template).read_text(encoding="utf-8")

    listen_block = "\n".join(f"    listen {addr};" for addr in listen_addrs)
    archive_blocks: list[str] = []
    for storage_id, target_path in sorted(archive_roots.items()):
        resolved = target_path.resolve()
        for subdir in ("raw", "manifests"):
            archive_blocks.append(
                "\n".join(
                    [
                        f"    location /archive/{storage_id}/{subdir}/ {{",
                        f"        alias {resolved}/{subdir}/;",
                        "        limit_except GET HEAD { deny all; }",
                        "        autoindex off;",
                        "        gzip off;",
                        "        etag on;",
                        "    }",
                    ]
                )
            )

    rendered = tpl.replace("{{LISTEN_DIRECTIVES}}", listen_block)
    rendered = rendered.replace("{{SERVER_NAME}}", server_name)
    rendered = rendered.replace("{{EXPORT_ROOT}}", str(export_root.resolve()))
    rendered = rendered.replace("{{ARCHIVE_LOCATIONS}}", "\n\n".join(archive_blocks))
    return rendered


def publish_export(
    *,
    export_root: Path,
    configs: dict[str, MarketDiscoveryConfig],
    backup_timeout_seconds: float = 10.0,
    retain_revisions: int = 2,
    grant_read_user: str | None = None,
    nginx_conf_out: Path | None = None,
    listen_addrs: tuple[str, ...] = ("127.0.0.1:18080",),
) -> dict[str, Any]:
    """Discover partitions, run bounded SQLite online backups, validate, and atomically switch latest.json."""

    start_monotonic = time.monotonic()
    export_root = export_root.resolve()
    export_root.mkdir(parents=True, exist_ok=True)
    previous_manifest = _load_previous_manifest(export_root)
    previous_by_key = {
        (p.get("market"), str(p.get("source_partition_root"))): p
        for p in previous_manifest.get("partitions", [])
        if isinstance(p, dict)
    }

    now = datetime.now(UTC)
    rev_id = f"{now.strftime('%Y%m%dT%H%M%SZ')}-{int(time.time_ns())}"
    staging_dir = export_root / f".staging-{rev_id}-{os.getpid()}"
    staging_dir.mkdir(parents=True, exist_ok=True)

    discovered_archive_roots: dict[str, Path] = {}
    published_partitions: list[dict[str, Any]] = []
    ready_files_to_grant: set[Path] = set()
    telemetry_partitions: list[dict[str, Any]] = []
    reused_count = 0
    backed_up_count = 0

    try:
        for market in sorted(configs):
            cfg = configs[market]
            for part in discover_partitions(cfg):
                layout = StorageLayout.from_root(part.partition_root)
                if not layout.catalog.is_file():
                    continue
                fp, cat_bytes, wal_bytes, wal_empty = _partition_fingerprint(
                    part, layout, cfg.configured_archive_targets
                )
                prev = previous_by_key.get((part.market, str(part.partition_root)))
                can_reuse = (
                    part.is_cold
                    and wal_empty
                    and prev is not None
                    and prev.get("fingerprint") == fp
                    and prev.get("chunk_metadata_version") == CHUNK_METADATA_VERSION
                    and int((prev.get("counts") or {}).get("pending") or 0) == 0
                    and _snapshot_is_valid(export_root, prev)
                )

                if can_reuse:
                    reused_count += 1
                    assert prev is not None
                    for sid, target_str in (prev.get("resolved_archive_targets") or {}).items():
                        discovered_archive_roots.setdefault(sid, Path(target_str))
                    published_partitions.append(prev)
                    telemetry_partitions.append(
                        {
                            "market": part.market,
                            "partition_id": part.partition_id,
                            "reused": True,
                            "catalog_bytes": cat_bytes,
                            "wal_bytes": wal_bytes,
                            "duration_seconds": 0.0,
                        }
                    )
                    continue

                backed_up_count += 1
                part_t0 = time.monotonic()
                snap_rel = f"snapshots/{part.market}/{part.partition_id}/{fp[:16]}"
                staged_snap = staging_dir / snap_rel
                staged_catalog_dir = staged_snap / "catalog"
                staged_catalog_dir.mkdir(parents=True, exist_ok=True)
                staged_db = staged_catalog_dir / "catalog.sqlite"

                with reader_lock(part.lock_root, exclusive=False):
                    with Catalog.open_live_read_only(layout.catalog) as live_catalog:
                        dst_conn = sqlite3.connect(staged_db)
                        try:
                            bounded_catalog_backup(
                                live_catalog,
                                dst_conn,
                                timeout_seconds=backup_timeout_seconds,
                            )
                            dst_conn.commit()
                        finally:
                            dst_conn.close()

                        ext_conn = sqlite3.connect(
                            f"file:{staged_db}?mode=ro&immutable=1", uri=True
                        )
                        ext_conn.row_factory = sqlite3.Row
                        try:
                            external_roots = _resolve_external_roots(
                                live_catalog, ext_conn, cfg.configured_archive_targets
                            )
                        finally:
                            ext_conn.close()

                _verify_sqlite_integrity(staged_db)
                previous_chunks = _load_previous_chunks_map(export_root, prev)
                counts, chunk_entries, availability, part_grant_files = (
                    _inspect_snapshot_catalog(
                        snapshot_db_path=staged_db,
                        partition=part,
                        external_roots=external_roots,
                        previous_chunks_by_id=previous_chunks,
                    )
                )
                ready_files_to_grant.update(part_grant_files)
                for sid, target_path in external_roots.items():
                    discovered_archive_roots.setdefault(sid, target_path)

                p_meta = partition_metadata(part.partition_root)
                p_meta["market"] = part.market
                p_meta["partition_id"] = part.partition_id
                p_meta["is_cold"] = part.is_cold
                _atomic_write_json(staged_snap / "partition.json", p_meta)
                _atomic_write_json(staged_snap / "availability.json", availability)
                chunks_body = "".join(
                    json.dumps(item, sort_keys=True) + "\n" for item in chunk_entries
                ).encode("utf-8")
                _atomic_write_bytes(staged_snap / "chunks.jsonl", chunks_body)

                final_snap = export_root / snap_rel
                final_snap.parent.mkdir(parents=True, exist_ok=True)
                if final_snap.exists():
                    shutil.rmtree(final_snap)
                shutil.move(str(staged_snap), str(final_snap))

                part_dur = round(time.monotonic() - part_t0, 6)
                files_meta = {
                    "partition_json": {
                        "url_path": f"export/{snap_rel}/partition.json",
                        "sha256": _sha256_file(final_snap / "partition.json"),
                        "bytes": (final_snap / "partition.json").stat().st_size,
                    },
                    "catalog_sqlite": {
                        "url_path": f"export/{snap_rel}/catalog/catalog.sqlite",
                        "sha256": _sha256_file(final_snap / "catalog/catalog.sqlite"),
                        "bytes": (final_snap / "catalog/catalog.sqlite").stat().st_size,
                    },
                    "chunks_jsonl": {
                        "url_path": f"export/{snap_rel}/chunks.jsonl",
                        "sha256": _sha256_file(final_snap / "chunks.jsonl"),
                        "bytes": (final_snap / "chunks.jsonl").stat().st_size,
                    },
                    "availability_json": {
                        "url_path": f"export/{snap_rel}/availability.json",
                        "sha256": _sha256_file(final_snap / "availability.json"),
                        "bytes": (final_snap / "availability.json").stat().st_size,
                    },
                }

                part_entry = {
                    "market": part.market,
                    "partition_id": part.partition_id,
                    "is_cold": part.is_cold,
                    "source_partition_root": str(part.partition_root),
                    "fingerprint": fp,
                    "chunk_metadata_version": CHUNK_METADATA_VERSION,
                    "snapshot_rel_dir": snap_rel,
                    "counts": counts,
                    "resolved_archive_targets": {
                        sid: str(p) for sid, p in sorted(external_roots.items())
                    },
                    "files": files_meta,
                }
                published_partitions.append(part_entry)
                telemetry_partitions.append(
                    {
                        "market": part.market,
                        "partition_id": part.partition_id,
                        "reused": False,
                        "catalog_bytes": cat_bytes,
                        "wal_bytes": wal_bytes,
                        "duration_seconds": part_dur,
                    }
                )

        if not published_partitions:
            raise RuntimeError("no partitions discovered from configured roots")

        total_duration = round(time.monotonic() - start_monotonic, 6)
        rev_dir = export_root / "revisions" / rev_id
        rev_dir.mkdir(parents=True, exist_ok=True)
        rev_manifest = {
            "schema_version": EXPORT_SCHEMA_VERSION,
            "revision_id": rev_id,
            "generated_at_utc": now.isoformat(),
            "markets": {
                market: {
                    "active_roots": [str(p) for p in cfg.active_roots],
                    "cold_roots": [str(p) for p in cfg.cold_roots],
                }
                for market, cfg in sorted(configs.items())
            },
            "archive_targets": {
                sid: {"url_prefix": f"archive/{sid}/", "local_path": str(path)}
                for sid, path in sorted(discovered_archive_roots.items())
            },
            "partitions": published_partitions,
            "telemetry": {
                "duration_seconds": total_duration,
                "reused_partitions": reused_count,
                "backed_up_partitions": backed_up_count,
                "partitions": telemetry_partitions,
            },
        }
        rev_manifest_path = rev_dir / "manifest.json"
        _atomic_write_json(rev_manifest_path, rev_manifest)

        # Self-validate every referenced snapshot before moving latest.json.
        for part_entry in published_partitions:
            if not _snapshot_is_valid(export_root, part_entry):
                raise RuntimeError(
                    f"snapshot validation failed for {part_entry['market']}/{part_entry['partition_id']}"
                )

        if grant_read_user:
            _grant_read_acls(
                grant_read_user,
                export_root=export_root,
                archive_roots=discovered_archive_roots,
                files=ready_files_to_grant,
            )

        if nginx_conf_out is not None:
            rendered_nginx = render_nginx_config(
                export_root=export_root,
                archive_roots=discovered_archive_roots,
                listen_addrs=listen_addrs,
            )
            _atomic_write_bytes(nginx_conf_out, rendered_nginx.encode("utf-8"))

        latest_payload = {
            "schema_version": EXPORT_SCHEMA_VERSION,
            "revision_id": rev_id,
            "generated_at_utc": now.isoformat(),
            "manifest_rel_path": f"revisions/{rev_id}/manifest.json",
            "manifest_url_path": f"export/revisions/{rev_id}/manifest.json",
            "manifest_sha256": _sha256_file(rev_manifest_path),
            "partition_count": len(published_partitions),
            "counts": {
                key: sum(int(p["counts"][key]) for p in published_partitions)
                for key in ("total", "ready", "pending", "quarantined", "gaps")
            },
            "telemetry": rev_manifest["telemetry"],
        }
        _atomic_write_json(export_root / "latest.json", latest_payload)

        prune_stats = _prune_old_revisions(
            export_root, retain_revisions=retain_revisions
        )
        latest_payload["prune"] = prune_stats
        return latest_payload
    finally:
        if staging_dir.exists():
            shutil.rmtree(staging_dir, ignore_errors=True)


def _parse_extra_roots(values: list[str]) -> dict[str, list[Path]]:
    parsed: dict[str, list[Path]] = {}
    for item in values:
        if "=" not in item:
            raise ValueError(f"--extra-root must look like market=/path: {item!r}")
        market, raw_path = item.split("=", 1)
        parsed.setdefault(market.strip(), []).append(Path(raw_path.strip()))
    return parsed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--export-root",
        type=Path,
        default=Path("/srv/recorder-data/S-BMDR/mcp-export"),
    )
    parser.add_argument(
        "--roots-config",
        type=Path,
        default=Path("/etc/sbmdr/roots.json"),
    )
    parser.add_argument(
        "--metadata-roots-config",
        type=Path,
        default=Path("/etc/sbmdr/metadata-roots.json"),
    )
    parser.add_argument(
        "--archive-targets-config",
        type=Path,
        default=Path("/etc/sbmdr/archive-targets.json"),
    )
    parser.add_argument(
        "--legacy-roots-config",
        type=Path,
        default=Path("/etc/sbmdr/legacy-roots.json"),
    )
    parser.add_argument(
        "--extra-root",
        action="append",
        default=[],
        help="Optional additional market root in market=/path format",
    )
    parser.add_argument("--backup-timeout-seconds", type=float, default=10.0)
    parser.add_argument("--retain-revisions", type=int, default=2)
    parser.add_argument(
        "--grant-read-user",
        default=None,
        help="Optional OS user (e.g. www-data) granted read-only ACL on export & verified archives",
    )
    parser.add_argument(
        "--nginx-conf-out",
        type=Path,
        default=None,
        help="Optional path to render the read-only nginx server block",
    )
    parser.add_argument(
        "--listen",
        action="append",
        default=[],
        help="Nginx listen address (default 127.0.0.1:18080)",
    )
    args = parser.parse_args(argv)

    configs = discover_market_configs(
        roots_config=args.roots_config,
        metadata_roots_config=args.metadata_roots_config,
        archive_targets_config=args.archive_targets_config,
        legacy_roots_config=args.legacy_roots_config,
        extra_roots=_parse_extra_roots(args.extra_root),
    )
    listen_addrs = tuple(args.listen) if args.listen else ("127.0.0.1:18080",)
    try:
        result = publish_export(
            export_root=args.export_root,
            configs=configs,
            backup_timeout_seconds=args.backup_timeout_seconds,
            retain_revisions=args.retain_revisions,
            grant_read_user=args.grant_read_user,
            nginx_conf_out=args.nginx_conf_out,
            listen_addrs=listen_addrs,
        )
    except CatalogBackupTimeoutError as exc:
        print(json.dumps({"error": "backup_timeout", "detail": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
