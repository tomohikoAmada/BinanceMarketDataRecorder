"""Shared read-only reconnect-boundary analysis engine.

Scans sealed Raw chunks, manifests, and the lifecycle Catalog to find every
connection_id transition and classify it boundary-locally:

- EXPLICIT_SEQUENCE_GAP: the exact transition pair carries persistent gap
  evidence (frame ``sequence_gap`` on either boundary frame, a single-
  connection old chunk sealed with manifest ``reconnect_gap``, or an
  identity-matched Catalog discontinuity interval);
- BLUE_GREEN_OVERLAP: the exact transition pair carries blue/green overlap
  provenance;
- UNMARKED_RECONNECT: a connection change with no gap evidence at all; the
  sealed interval claims gap=false/complete=true even though exchange-side
  completeness between close and the first new frame cannot be proven;
- UNKNOWN: evidence exists but cannot be attributed to exactly this
  transition (archived Raw, ambiguous manifest flags, multiple matching
  Catalog gaps). Never classified optimistically as explicit.

Classification is boundary-local: a flag anywhere in an adjacent manifest is
never reused to classify an unrelated transition.

The tool is strictly read-only: it never creates directories, never writes
under ``data_root``, opens the Catalog read-only, and works on read-only
mounted trees. JSON canonical output is deterministic: given the same
immutable manifest inventory and cutoff, canonical payload bytes are
byte-identical. Execution metadata (generated_at, canonical SHA-256) is kept
in a non-canonical artifact wrapper.

The historical CLI and installed M22.9 acceptance observer both consume this
classification authority.  The CLI usage is:

    python3.12 tools/audit_reconnect_boundaries.py [--data-root DIR]
        [--market spot|um_perpetual] [--stream NAME]
        [--cutoff-utc-ns NS] [--json]
        [--output historical-reconnect-audit.json]
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import sys
import tempfile
import time
from collections.abc import Mapping
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TextIO, cast

import zstandard

from binance_market_data_recorder.spool.format import (
    FRAME_PREFIX,
    decode_chunk_header,
    decode_envelope,
)
from binance_market_data_recorder.spool.seal import (
    SealError,
    read_strict_manifest,
    validate_sealed_artifact,
)
from binance_market_data_recorder.storage.catalog import (
    LEGACY_SINGLE_SYMBOL,
    Catalog,
    ChunkState,
)
from binance_market_data_recorder.storage.layout import StorageLayout

TOOL_SCHEMA_VERSION = "historical-reconnect-audit.v1"
ARTIFACT_SCHEMA_VERSION = "historical-reconnect-audit-artifact.v1"
GAP_FLAGS = frozenset({"sequence_gap", "reconnect_gap"})

EXPLICIT_SEQUENCE_GAP = "EXPLICIT_SEQUENCE_GAP"
BLUE_GREEN_OVERLAP = "BLUE_GREEN_OVERLAP"
UNMARKED_RECONNECT = "UNMARKED_RECONNECT"
UNKNOWN = "UNKNOWN"

#: Catalog boundary-identity classification strength (P1-B, M21.4.11-R2).
#: Only EXACT_PAIR may prove an inter-chunk boundary by itself; every weaker
#: identity is at best UNKNOWN and never optimistic EXPLICIT.
EXACT_PAIR = "EXACT_PAIR"
PARTIAL_OLD = "PARTIAL_OLD"
PARTIAL_NEW = "PARTIAL_NEW"
TIME_ONLY = "TIME_ONLY"
AMBIGUOUS = "AMBIGUOUS"
NONE = "NONE"

#: WebSocket streams whose connection_id continuity has integrity meaning.
#: REST-polled streams (depth_snapshot, exchange_info, 5m statistics, ...)
#: mint a new connection_id per request; their connection transitions are
#: not reconnect boundaries and are excluded from transition analysis.
WEBSOCKET_STREAMS: dict[str, frozenset[str]] = {
    "spot": frozenset({"diff_depth", "book_ticker", "agg_trade"}),
    "um_perpetual": frozenset(
        {"diff_depth", "book_ticker", "agg_trade", "mark_price", "liquidation"}
    ),
}

#: Expected relative directories below a data root. The audit reports missing
#: inputs but never creates them.
EXPECTED_RELATIVE_DIRECTORIES = (
    "data",
    "data/active",
    "data/sealed",
    "data/manifests",
    "data/checkpoints",
    "data/quarantine",
    "data/reports",
    "state",
)


@dataclass(frozen=True)
class FrameIdentity:
    chunk_id: str
    frame_index: int
    connection_id: str
    receive_time_utc_ns: int
    capture_flags: tuple[str, ...]
    source_sequence: dict[str, int | str]
    payload_sha256: str
    exchange_event_time_ns: int | None


@dataclass
class ChunkScan:
    manifest: dict[str, Any]
    frames: list[FrameIdentity] | None = None
    issue: str | None = None
    deep_scan_allowed: bool = True


@dataclass
class CatalogGapInterval:
    gap_id: str
    market: str
    symbol: str
    stream: str
    started_at_utc_ns: int
    ended_at_utc_ns: int | None
    reason: str | None = None
    original_connection_id: str | None = None
    new_connection_id: str | None = None
    original_generation: int | None = None
    new_generation: int | None = None


@dataclass
class Transition:
    kind: str
    boundary_kind: str
    old_chunk_id: str
    new_chunk_id: str
    old_connection_id: str | None
    new_connection_id: str | None
    last_old_frame: dict[str, Any] | None
    first_new_frame: dict[str, Any] | None
    receive_gap_seconds: float | None
    exchange_event_gap_seconds: float | None
    old_manifest: dict[str, Any]
    new_manifest: dict[str, Any]
    catalog_gap_match: str
    catalog_identity_match: bool
    catalog_identity_match_kind: str
    catalog_matched_gap_id: str | None
    occurred_at_utc_ns: int
    collector_instance_id: str | None
    intervening_manifests: list[dict[str, Any]] | None = None
    frame_detail_unavailable: bool = False
    connection_ids: list[str] | None = None

    def to_dict(self) -> dict[str, Any]:
        document: dict[str, Any] = {
            "kind": self.kind,
            "boundary_kind": self.boundary_kind,
            "old_chunk_id": self.old_chunk_id,
            "new_chunk_id": self.new_chunk_id,
            "old_connection_id": self.old_connection_id,
            "new_connection_id": self.new_connection_id,
            "last_old_frame": self.last_old_frame,
            "first_new_frame": self.first_new_frame,
            "receive_gap_seconds": self.receive_gap_seconds,
            "exchange_event_gap_seconds": self.exchange_event_gap_seconds,
            "old_manifest": {
                "gap": self.old_manifest.get("gap"),
                "complete": self.old_manifest.get("complete"),
                "capture_flags": self.old_manifest.get("capture_flags"),
            },
            "new_manifest": {
                "gap": self.new_manifest.get("gap"),
                "complete": self.new_manifest.get("complete"),
                "capture_flags": self.new_manifest.get("capture_flags"),
            },
            "catalog_gap_match": self.catalog_gap_match,
            "catalog_identity_match": self.catalog_identity_match,
            "catalog_identity_match_kind": self.catalog_identity_match_kind,
            "catalog_matched_gap_id": self.catalog_matched_gap_id,
            "occurred_at_utc_ns": self.occurred_at_utc_ns,
            "collector_instance_id": self.collector_instance_id,
            "frame_detail_unavailable": self.frame_detail_unavailable,
        }
        if self.connection_ids is not None:
            document["connection_ids"] = list(self.connection_ids)
        if self.intervening_manifests:
            document["intervening_manifests"] = list(self.intervening_manifests)
        return document


def read_only_layout(root: Path) -> StorageLayout:
    """Build the StorageLayout view without creating any directory."""
    root = root.resolve()
    data = root / "data"
    return StorageLayout(
        root=root,
        active=data / "active",
        sealed=data / "sealed",
        manifests=data / "manifests",
        checkpoints=data / "checkpoints",
        quarantine=data / "quarantine",
        reports=data / "reports",
        daily_reports=data / "reports" / "daily",
        state=root / "state",
        catalog=root / "state" / "catalog.sqlite",
    )


def missing_inputs(root: Path) -> list[str]:
    """Return expected relative inputs that are absent (never created)."""
    missing: list[str] = []
    if not root.is_dir():
        return ["data_root"]
    for relative in EXPECTED_RELATIVE_DIRECTORIES:
        if not (root / relative).is_dir():
            missing.append(relative)
    if not (root / "state" / "catalog.sqlite").is_file():
        missing.append("state/catalog.sqlite")
    return missing


def _frame_identity(
    chunk_id: str,
    frame_index: int,
    envelope: Any,
) -> FrameIdentity:
    exchange_event_time_ns = envelope.exchange_event_time
    return FrameIdentity(
        chunk_id=chunk_id,
        frame_index=frame_index,
        connection_id=str(envelope.connection_id),
        receive_time_utc_ns=int(envelope.receive_time_utc_ns),
        capture_flags=tuple(envelope.capture_flags),
        source_sequence=dict(envelope.source_sequence),
        payload_sha256=hashlib_sha256(envelope.raw_payload),
        exchange_event_time_ns=(
            int(exchange_event_time_ns) if exchange_event_time_ns is not None else None
        ),
    )


def hashlib_sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def scan_chunk_frames(sealed_path: Path, manifest: dict[str, Any]) -> list[FrameIdentity]:
    """Decompress and decode one sealed chunk in frame order."""
    try:
        raw = zstandard.ZstdDecompressor().decompress(
            sealed_path.read_bytes(), max_output_size=0
        )
    except (OSError, zstandard.ZstdError) as exc:
        raise ValueError(f"cannot decompress {sealed_path}: {exc}") from exc
    source = io.BytesIO(raw)
    try:
        decode_chunk_header(source)
    except Exception as exc:
        raise ValueError(f"invalid chunk header {sealed_path}: {exc}") from exc
    frames: list[FrameIdentity] = []
    index = 0
    while prefix := source.read(FRAME_PREFIX.size):
        body_length, _flags, _reserved, _checksum = FRAME_PREFIX.unpack(prefix)
        body = source.read(body_length)
        if len(body) != body_length:
            raise ValueError(f"truncated frame body in {sealed_path}")
        try:
            envelope = decode_envelope(body)
        except Exception as exc:
            raise ValueError(f"invalid envelope in {sealed_path}: {exc}") from exc
        frames.append(_frame_identity(str(manifest["chunk_id"]), index, envelope))
        index += 1
    return frames


def load_manifest_chunks(
    layout: Any,
    *,
    market: str | None,
    stream: str | None,
    cutoff_utc_ns: int | None = None,
) -> list[ChunkScan]:
    chunks: list[ChunkScan] = []
    manifest_dir = layout.manifests
    if not manifest_dir.is_dir():
        return chunks
    for path in sorted(manifest_dir.glob("*.manifest.json")):
        try:
            document = read_strict_manifest(path, recorder_root=layout.root)
        except (OSError, SealError) as exc:
            raise SealError(f"strict historical manifest load failed for {path}") from exc
        if market is not None and document.get("market") != market:
            continue
        if stream is not None and document.get("stream") != stream:
            continue
        if cutoff_utc_ns is not None:
            created_at = document.get("created_at_utc_ns")
            if not isinstance(created_at, int) or created_at > cutoff_utc_ns:
                continue
        chunks.append(ChunkScan(manifest=document))
    chunks.sort(key=lambda item: int(item.manifest.get("created_at_utc_ns", 0)))
    return chunks


def manifest_inventory(chunks: list[ChunkScan], layout: Any) -> tuple[int, str]:
    """Deterministic SHA-256 over the scanned manifest relative paths."""
    relative_paths: list[str] = []
    for chunk in chunks:
        relative = chunk.manifest.get("relative_path")
        if isinstance(relative, str) and relative:
            relative_paths.append(layout.relative(layout.root / relative))
        else:
            relative_paths.append(str(chunk.manifest.get("chunk_id", "")))
    relative_paths.sort()
    inventory = "\n".join(relative_paths) + "\n"
    digest = hashlib.sha256(inventory.encode("utf-8")).hexdigest()
    return len(relative_paths), digest


def _sealed_path(layout: Any, manifest: dict[str, Any]) -> Path:
    relative = manifest.get("relative_path")
    if not isinstance(relative, str) or not relative:
        raise ValueError("manifest lacks relative_path")
    return Path(layout.root) / relative


def _load_frames(chunks: list[ChunkScan], layout: Any) -> None:
    """Decompress every locally available sealed chunk in frame order."""
    for chunk in chunks:
        if chunk.frames is not None or not chunk.deep_scan_allowed:
            continue
        try:
            sealed = _sealed_path(layout, chunk.manifest)
            if not sealed.is_file():
                chunk.issue = "sealed_file_missing_after_archival"
                continue
            chunk.frames = scan_chunk_frames(sealed, chunk.manifest)
        except (ValueError, OSError) as exc:
            chunk.issue = f"scan_failed: {exc}"


def _last_connection(chunk: ChunkScan) -> tuple[str | None, FrameIdentity | None]:
    if chunk.frames is not None:
        if not chunk.frames:
            return None, None
        return chunk.frames[-1].connection_id, chunk.frames[-1]
    connections = chunk.manifest.get("connection_ids")
    if isinstance(connections, list) and len(connections) == 1:
        return str(connections[0]), None
    return None, None


def _first_connection(chunk: ChunkScan) -> tuple[str | None, FrameIdentity | None]:
    if chunk.frames is not None:
        if not chunk.frames:
            return None, None
        return chunk.frames[0].connection_id, chunk.frames[0]
    connections = chunk.manifest.get("connection_ids")
    if isinstance(connections, list) and len(connections) == 1:
        return str(connections[0]), None
    return None, None


def _manifest_flags(manifest: dict[str, Any]) -> frozenset[str]:
    flags = manifest.get("capture_flags")
    if not isinstance(flags, list):
        return frozenset()
    return frozenset(str(flag) for flag in flags)


def _manifest_has(manifest: dict[str, Any], flag: str) -> bool:
    return flag in _manifest_flags(manifest)


def _catalog_gap_match(
    intervals: list[CatalogGapInterval],
    market: str,
    symbol: str,
    stream: str,
    occurred_at_utc_ns: int,
    old_connection_id: str | None,
    new_connection_id: str | None,
) -> tuple[str, str, str | None]:
    """Match a transition to Catalog discontinuity intervals (P1-B).

    Exact boundary identity is the connection pair:
    transition (old, new) vs interval (original, new). The transition is
    EXACT_PAIR only when all four identities exist and
    ``old == original AND new == interval.new``; a one-sided match is
    PARTIAL_OLD / PARTIAL_NEW and can never prove the boundary by itself.
    Matching is market/symbol/stream specific. Multiple candidates of equal
    strength classify AMBIGUOUS; time overlap alone is TIME_ONLY.

    Returns (match, identity_match_kind, matched_gap_id).
    """
    stream_intervals = [
        interval
        for interval in intervals
        if (
            interval.market == market
            and interval.symbol == symbol
            and interval.stream == stream
        )
    ]

    def within(interval: CatalogGapInterval) -> bool:
        return interval.started_at_utc_ns <= occurred_at_utc_ns and (
            interval.ended_at_utc_ns is None
            or occurred_at_utc_ns <= interval.ended_at_utc_ns
        )

    exact: list[CatalogGapInterval] = []
    partial: list[tuple[CatalogGapInterval, str]] = []
    time_only: list[CatalogGapInterval] = []
    for interval in stream_intervals:
        old_known = (
            old_connection_id is not None
            and interval.original_connection_id is not None
        )
        new_known = (
            new_connection_id is not None and interval.new_connection_id is not None
        )
        old_eq = old_known and interval.original_connection_id == old_connection_id
        new_eq = new_known and interval.new_connection_id == new_connection_id
        if old_eq and new_eq:
            exact.append(interval)
        elif old_eq:
            partial.append((interval, PARTIAL_OLD))
        elif new_eq:
            partial.append((interval, PARTIAL_NEW))
        elif within(interval):
            time_only.append(interval)
    if len(exact) == 1:
        interval = exact[0]
        return (
            "PENDING" if interval.ended_at_utc_ns is None else "MATCHED",
            EXACT_PAIR,
            interval.gap_id,
        )
    if len(exact) > 1:
        return "AMBIGUOUS", AMBIGUOUS, None
    if len(partial) == 1:
        interval, kind = partial[0]
        return (
            "PENDING" if interval.ended_at_utc_ns is None else "MATCHED",
            kind,
            interval.gap_id,
        )
    if len(partial) > 1 or len(time_only) > 1 or (partial and time_only):
        # No exact candidate but multiple partial/time candidates: the
        # evidence cannot be attributed to exactly one boundary.
        return "AMBIGUOUS", AMBIGUOUS, None
    if len(time_only) == 1:
        interval = time_only[0]
        return (
            "PENDING" if interval.ended_at_utc_ns is None else "MATCHED",
            TIME_ONLY,
            interval.gap_id,
        )
    return "UNMATCHED", NONE, None


def _catalog_intervals(
    events: list[dict[str, Any]], *, allow_legacy_symbol: bool = False
) -> list[CatalogGapInterval]:
    intervals: list[CatalogGapInterval] = []
    started: dict[tuple[str, str, str, str], CatalogGapInterval] = {}
    for event in events:
        evidence = event.get("evidence")
        if not isinstance(evidence, dict):
            continue
        gap_id = evidence.get("gap_id")
        if not isinstance(gap_id, str) or not gap_id:
            continue
        market = evidence.get("market")
        symbol = evidence.get("symbol")
        if symbol is None and allow_legacy_symbol:
            symbol = LEGACY_SINGLE_SYMBOL
        stream = evidence.get("stream")
        if (
            not isinstance(market, str)
            or not market
            or not isinstance(symbol, str)
            or not symbol
            or not isinstance(stream, str)
            or not stream
        ):
            continue
        identity = (market, symbol, stream, gap_id)
        if event["event_type"] == "STREAM_DISCONTINUITY_STARTED":
            interval = CatalogGapInterval(
                gap_id=gap_id,
                market=market,
                symbol=symbol,
                stream=stream,
                started_at_utc_ns=int(evidence.get("gap_started_at_utc_ns", 0)),
                ended_at_utc_ns=None,
                reason=(
                    str(evidence["reason"])
                    if isinstance(evidence.get("reason"), str)
                    else None
                ),
                original_connection_id=(
                    str(evidence["original_connection_id"])
                    if isinstance(evidence.get("original_connection_id"), str)
                    else None
                ),
                new_connection_id=(
                    str(evidence["new_connection_id"])
                    if isinstance(evidence.get("new_connection_id"), str)
                    else None
                ),
                original_generation=_optional_int(evidence, "original_generation"),
                new_generation=_optional_int(evidence, "new_generation"),
            )
            started[identity] = interval
            intervals.append(interval)
        elif event["event_type"] == "STREAM_DISCONTINUITY_COMPLETED":
            completed_interval = started.get(identity)
            if completed_interval is None:
                continue
            completed_interval.ended_at_utc_ns = int(
                evidence.get("gap_ended_at_utc_ns", 0)
            )
            if isinstance(evidence.get("new_connection_id"), str):
                completed_interval.new_connection_id = str(
                    evidence["new_connection_id"]
                )
            if isinstance(evidence.get("new_generation"), int):
                completed_interval.new_generation = evidence["new_generation"]
    intervals.sort(key=lambda interval: (interval.started_at_utc_ns, interval.gap_id))
    return intervals


def _optional_int(evidence: dict[str, Any], name: str) -> int | None:
    value = evidence.get(name)
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def _frame_has_gap(frame: FrameIdentity | None) -> bool:
    return frame is not None and "sequence_gap" in frame.capture_flags


def _is_first_frame_of_connection(
    frames: list[FrameIdentity], index: int
) -> bool:
    if index == 0:
        return True
    return frames[index - 1].connection_id != frames[index].connection_id


def _marker_is_end_evidence(
    frames: list[FrameIdentity], index: int
) -> bool:
    """True when a sequence_gap marker on frames[index] is an end marker.

    A marker on a frame that is also the first frame of its connection may
    belong to the earlier boundary (start marker) instead of this one; only
    multi-frame connections produce unambiguous end markers.
    """
    return not _is_first_frame_of_connection(frames, index)


def _frame_deployments(frame: FrameIdentity) -> set[str]:
    return {
        flag for flag in frame.capture_flags if flag.startswith("deployment_id=")
    }


def _overlap_pair(
    last_old: FrameIdentity | None, first_new: FrameIdentity | None
) -> bool:
    """True when blue/green overlap provably covers this exact transition."""
    if last_old is None or first_new is None:
        return False
    if (
        "blue_green_overlap" not in last_old.capture_flags
        or "blue_green_overlap" not in first_new.capture_flags
    ):
        return False
    old_deployments = _frame_deployments(last_old)
    new_deployments = _frame_deployments(first_new)
    if not old_deployments and not new_deployments:
        return True
    return bool(old_deployments & new_deployments)


def _manifest_gap_evidence(manifest: dict[str, Any]) -> bool:
    return bool(manifest.get("gap") is True) or bool(
        _manifest_flags(manifest) & GAP_FLAGS
    )


def _manifest_overlap_evidence(manifest: dict[str, Any]) -> bool:
    return "blue_green_overlap" in _manifest_flags(manifest)


def _zero_record_chunk(chunk: ChunkScan) -> bool:
    """Return whether a manifest proves this chunk contains no Raw frames.

    ``record_count == 0`` remains authoritative when the sealed body has been
    archived and is unavailable.  A contradictory locally scanned non-empty
    body is never treated as an empty marker.
    """

    return chunk.manifest.get("record_count") == 0 and (
        chunk.frames is None or len(chunk.frames) == 0
    )


def _intervening_manifest_info(chunk: ChunkScan) -> dict[str, Any]:
    return {
        "chunk_id": chunk.manifest.get("chunk_id"),
        "record_count": chunk.manifest.get("record_count"),
        "gap": chunk.manifest.get("gap"),
        "complete": chunk.manifest.get("complete"),
        "capture_flags": chunk.manifest.get("capture_flags"),
        "frame_detail_unavailable": chunk.frames is None,
    }


def _classify_inter_chunk(
    old_chunk: ChunkScan,
    new_chunk: ChunkScan,
    last_old: FrameIdentity | None,
    first_new: FrameIdentity | None,
    *,
    catalog_identity_kind: str,
    catalog_matched_gap_id: str | None,
    old_frames: list[FrameIdentity] | None,
    intervening_chunks: list[ChunkScan],
) -> str:
    """Classify one exact old-chunk-end -> new-chunk-start transition.

    Boundary-specific evidence only: a first-new-frame ``sequence_gap``
    marker, an end marker on the old chunk's last frame, a blue/green
    overlap pair on both boundary frames, a single-connection old chunk
    sealed with manifest ``reconnect_gap`` (documents exactly its own end
    boundary), or an EXACT_PAIR Catalog interval. One-sided Catalog
    identities (PARTIAL_OLD/PARTIAL_NEW), time-only matches, and adjacent
    manifest evidence that cannot be attributed to this exact boundary are
    UNKNOWN, never optimistically EXPLICIT and never UNMARKED (P1-B/P2-C).
    """
    if _frame_has_gap(first_new):
        return EXPLICIT_SEQUENCE_GAP
    if (
        _frame_has_gap(last_old)
        and last_old is not None
        and old_frames is not None
        and _marker_is_end_evidence(old_frames, len(old_frames) - 1)
    ):
        # The old chunk's last frame carries an end marker (ingress
        # backpressure boundary frame): boundary-specific for this
        # transition.
        return EXPLICIT_SEQUENCE_GAP
    if _frame_has_gap(last_old) and last_old is not None:
        # A marker on a single-frame connection could belong to the earlier
        # boundary; attribution is ambiguous without frame context.
        return UNKNOWN
    if _overlap_pair(last_old, first_new):
        return BLUE_GREEN_OVERLAP
    old_connections = old_chunk.manifest.get("connection_ids")
    if (
        isinstance(old_connections, list)
        and len(old_connections) == 1
        and _manifest_has(old_chunk.manifest, "reconnect_gap")
    ):
        # The old chunk was sealed at its end by the reconnect-boundary
        # protocol: manifest-level reconnect_gap is boundary-specific for
        # exactly this transition.
        return EXPLICIT_SEQUENCE_GAP
    if any(
        _manifest_has(item.manifest, "reconnect_gap")
        for item in intervening_chunks
    ):
        # A frame-less reconnect marker is ordered wholly between the nearest
        # connection-bearing chunks.  It therefore documents this one logical
        # boundary without fabricating a frame or being borrowed by another
        # transition.
        return EXPLICIT_SEQUENCE_GAP
    if catalog_identity_kind == EXACT_PAIR:
        # The Catalog documents this exact connection pair as a durable
        # discontinuity interval (matched_gap_id provenance).
        return EXPLICIT_SEQUENCE_GAP
    if catalog_identity_kind in {
        PARTIAL_OLD,
        PARTIAL_NEW,
        TIME_ONLY,
        AMBIGUOUS,
    }:
        # Catalog gap evidence exists for this stream at this boundary but
        # cannot be attributed to exactly this connection pair.
        return UNKNOWN
    if _adjacent_unattributable_evidence(
        old_chunk.manifest,
        new_chunk.manifest,
        intervening_manifests=[item.manifest for item in intervening_chunks],
    ):
        # Gap/overlap evidence exists on an adjacent manifest but cannot be
        # attributed to exactly this inter-chunk boundary (for example a
        # multi-connection old chunk sealed with reconnect_gap whose exact
        # transition location is unknown). UNMARKED_RECONNECT means "no
        # evidence exists"; this is evidence that cannot be placed.
        return UNKNOWN
    return UNMARKED_RECONNECT


def _adjacent_unattributable_evidence(
    old_manifest: dict[str, Any],
    new_manifest: dict[str, Any],
    *,
    intervening_manifests: list[dict[str, Any]] | None = None,
) -> bool:
    all_manifests = [old_manifest, new_manifest, *(intervening_manifests or [])]
    return bool(
        any(_manifest_gap_evidence(manifest) for manifest in all_manifests)
        or any(_manifest_overlap_evidence(manifest) for manifest in all_manifests)
    )


def _manifest_level_intra_kind(chunk: ChunkScan) -> str:
    """Classify an intra-chunk boundary when frame detail is unavailable.

    Without frame positions, a manifest-level gap/overlap flag cannot be
    attributed to exactly one transition: such evidence classifies UNKNOWN,
    never optimistically EXPLICIT. A manifest with no gap evidence at all
    stays UNMARKED_RECONNECT.
    """
    flags = _manifest_flags(chunk.manifest)
    if chunk.manifest.get("gap") is True or bool(flags & GAP_FLAGS):
        return UNKNOWN
    if "blue_green_overlap" in flags:
        return UNKNOWN
    return UNMARKED_RECONNECT


def _frame_info(frame: FrameIdentity | None, *, persisted: bool) -> dict[str, Any] | None:
    if frame is None:
        return None
    return {
        "chunk_id": frame.chunk_id,
        "frame_index": frame.frame_index,
        "connection_id": frame.connection_id,
        "receive_time_utc_ns": frame.receive_time_utc_ns,
        "capture_flags": sorted(frame.capture_flags),
        "source_sequence": dict(sorted(frame.source_sequence.items())),
        "payload_sha256": frame.payload_sha256,
        "persisted": persisted,
    }


def _stream_key(market: str, symbol: str, stream: str) -> tuple[str, str, str]:
    return (market, symbol, stream)


def collect_transitions(
    chunks: list[ChunkScan],
    layout: Any,
    catalog: Catalog | None,
    *,
    emit_chunk_ids: frozenset[str] | None = None,
) -> tuple[
    list[Transition], dict[tuple[str, str, str], dict[str, int]], list[CatalogGapInterval]
]:
    """Return deterministic per-stream transitions, summaries, and intervals."""
    _load_frames(chunks, layout)
    intervals = (
        _catalog_intervals(
            catalog.operational_events(),
            allow_legacy_symbol=catalog.legacy_identity_schema,
        )
        if catalog is not None
        else []
    )

    transitions: list[Transition] = []
    summaries: dict[tuple[str, str, str], dict[str, int]] = {}
    previous_by_key: dict[tuple[str, str, str], ChunkScan] = {}
    intervening_by_key: dict[tuple[str, str, str], list[ChunkScan]] = {}
    for chunk in chunks:
        market = str(chunk.manifest["market"])
        symbol = str(chunk.manifest["symbol"])
        stream = str(chunk.manifest["stream"])
        if market not in {"spot", "um_perpetual"}:
            continue
        key = _stream_key(market, symbol, stream)
        if key not in summaries:
            summaries[key] = {
                "chunks_scanned": 0,
                "transitions_total": 0,
                "explicit_gap": 0,
                "blue_green_overlap": 0,
                "unmarked_reconnect": 0,
                "unknown": 0,
                "chunks_with_scan_issues": 0,
            }
        chunk_id = str(chunk.manifest["chunk_id"])
        emit_chunk = emit_chunk_ids is None or chunk_id in emit_chunk_ids
        if emit_chunk:
            summaries[key]["chunks_scanned"] += 1
        if stream not in WEBSOCKET_STREAMS.get(market, frozenset()):
            # REST-polled streams: per-request connection ids are not
            # reconnect boundaries; report chunk counts only.
            continue
        frames = chunk.frames
        if emit_chunk and frames is not None and len(frames) > 1:
            for index in range(1, len(frames)):
                previous_frame = frames[index - 1]
                frame = frames[index]
                if previous_frame.connection_id == frame.connection_id:
                    continue
                if _frame_has_gap(frame):
                    # The first frame of the new connection carries the
                    # recovery marker: boundary-specific for this transition.
                    kind = EXPLICIT_SEQUENCE_GAP
                elif (
                    _frame_has_gap(previous_frame)
                    and _marker_is_end_evidence(frames, index - 1)
                ):
                    # The last frame of the old connection carries an end
                    # marker (ingress backpressure boundary frame):
                    # boundary-specific for this transition.
                    kind = EXPLICIT_SEQUENCE_GAP
                elif _frame_has_gap(previous_frame):
                    # A marker on a single-frame connection could belong to
                    # the earlier boundary; attribution is ambiguous.
                    kind = UNKNOWN
                elif _overlap_pair(previous_frame, frame):
                    kind = BLUE_GREEN_OVERLAP
                else:
                    kind = UNMARKED_RECONNECT
                match, identity_kind, matched_gap_id = _catalog_gap_match(
                    intervals,
                    market,
                    symbol,
                    stream,
                    frame.receive_time_utc_ns,
                    previous_frame.connection_id,
                    frame.connection_id,
                )
                transition = Transition(
                    kind=kind,
                    boundary_kind="intra_chunk",
                    old_chunk_id=chunk.manifest["chunk_id"],
                    new_chunk_id=chunk.manifest["chunk_id"],
                    old_connection_id=previous_frame.connection_id,
                    new_connection_id=frame.connection_id,
                    last_old_frame=_frame_info(previous_frame, persisted=True),
                    first_new_frame=_frame_info(frame, persisted=True),
                    receive_gap_seconds=(
                        (frame.receive_time_utc_ns - previous_frame.receive_time_utc_ns)
                        / 1_000_000_000
                    ),
                    exchange_event_gap_seconds=_exchange_gap(
                        previous_frame, frame
                    ),
                    old_manifest=chunk.manifest,
                    new_manifest=chunk.manifest,
                    catalog_gap_match=match,
                    catalog_identity_match=identity_kind == EXACT_PAIR,
                    catalog_identity_match_kind=identity_kind,
                    catalog_matched_gap_id=matched_gap_id,
                    occurred_at_utc_ns=frame.receive_time_utc_ns,
                    collector_instance_id=_collector_id(chunk.manifest),
                )
                transitions.append(transition)
                _tally(summaries[key], transition.kind)
        elif emit_chunk and frames is None:
            connections = chunk.manifest.get("connection_ids")
            if isinstance(connections, list) and len(connections) > 1:
                # The local sealed copy is unavailable (archived, deleted, or
                # unreadable); frame order inside the chunk is unrecoverable.
                # Only a manifest with no gap evidence at all proves an
                # unmarked boundary; any gap/overlap flag is ambiguous.
                kind = _manifest_level_intra_kind(chunk)
                match, identity_kind, matched_gap_id = _catalog_gap_match(
                    intervals,
                    market,
                    symbol,
                    stream,
                    int(chunk.manifest.get("created_at_utc_ns", 0)),
                    None,
                    None,
                )
                transition = Transition(
                    kind=kind,
                    boundary_kind="intra_chunk_unordered",
                    old_chunk_id=chunk.manifest["chunk_id"],
                    new_chunk_id=chunk.manifest["chunk_id"],
                    old_connection_id=None,
                    new_connection_id=None,
                    last_old_frame=None,
                    first_new_frame=None,
                    receive_gap_seconds=None,
                    exchange_event_gap_seconds=None,
                    old_manifest=chunk.manifest,
                    new_manifest=chunk.manifest,
                    catalog_gap_match=match,
                    catalog_identity_match=identity_kind == EXACT_PAIR,
                    catalog_identity_match_kind=identity_kind,
                    catalog_matched_gap_id=matched_gap_id,
                    occurred_at_utc_ns=int(
                        chunk.manifest.get("created_at_utc_ns", 0)
                    ),
                    collector_instance_id=_collector_id(chunk.manifest),
                    frame_detail_unavailable=True,
                    connection_ids=sorted(str(item) for item in connections),
                )
                if emit_chunk_ids is None or str(chunk.manifest["chunk_id"]) in emit_chunk_ids:
                    transitions.append(transition)
                    _tally(summaries[key], transition.kind)
        if _zero_record_chunk(chunk):
            # Preserve the marker in the logical boundary chain but do not
            # replace the last connection-bearing chunk.  Consecutive empty
            # chunks still describe at most one observed A -> B transition.
            if key in previous_by_key:
                intervening_by_key.setdefault(key, []).append(chunk)
            continue
        previous = previous_by_key.get(key)
        intervening = intervening_by_key.pop(key, [])
        if previous is not None and (
            previous.manifest.get("stream") == stream
            and previous.manifest.get("market") == market
        ):
            old_connection, last_old = _last_connection(previous)
            new_connection, first_new = _first_connection(chunk)
            if (
                old_connection is not None
                and new_connection is not None
                and old_connection != new_connection
            ):
                occurred_at = (
                    first_new.receive_time_utc_ns
                    if first_new is not None
                    else int(chunk.manifest.get("created_at_utc_ns", 0))
                )
                match, identity_kind, matched_gap_id = _catalog_gap_match(
                    intervals,
                    market,
                    symbol,
                    stream,
                    occurred_at,
                    old_connection,
                    new_connection,
                )
                kind = _classify_inter_chunk(
                    previous,
                    chunk,
                    last_old,
                    first_new,
                    catalog_identity_kind=identity_kind,
                    catalog_matched_gap_id=matched_gap_id,
                    old_frames=previous.frames,
                    intervening_chunks=intervening,
                )
                transition = Transition(
                    kind=kind,
                    boundary_kind="inter_chunk",
                    old_chunk_id=previous.manifest["chunk_id"],
                    new_chunk_id=chunk.manifest["chunk_id"],
                    old_connection_id=old_connection,
                    new_connection_id=new_connection,
                    last_old_frame=_frame_info(
                        last_old, persisted=previous.frames is not None
                    ),
                    first_new_frame=_frame_info(
                        first_new, persisted=chunk.frames is not None
                    ),
                    receive_gap_seconds=(
                        (first_new.receive_time_utc_ns - last_old.receive_time_utc_ns)
                        / 1_000_000_000
                        if first_new is not None and last_old is not None
                        else None
                    ),
                    exchange_event_gap_seconds=(
                        _exchange_gap(last_old, first_new)
                        if first_new is not None and last_old is not None
                        else None
                    ),
                    old_manifest=previous.manifest,
                    new_manifest=chunk.manifest,
                    catalog_gap_match=match,
                    catalog_identity_match=identity_kind == EXACT_PAIR,
                    catalog_identity_match_kind=identity_kind,
                    catalog_matched_gap_id=matched_gap_id,
                    occurred_at_utc_ns=occurred_at,
                    collector_instance_id=_collector_id(chunk.manifest),
                    intervening_manifests=[
                        _intervening_manifest_info(item) for item in intervening
                    ]
                    or None,
                    frame_detail_unavailable=(
                        previous.frames is None
                        or chunk.frames is None
                        or any(item.frames is None for item in intervening)
                    ),
                )
                transitions.append(transition)
                _tally(summaries[key], transition.kind)
        previous_by_key[key] = chunk

    for chunk in chunks:
        if chunk.issue is None or chunk.frames is not None:
            continue
        if emit_chunk_ids is not None and str(chunk.manifest["chunk_id"]) not in emit_chunk_ids:
            continue
        if str(chunk.issue).startswith("sealed_file_missing_after_archival"):
            # Normal archived state: frame detail is unavailable and every
            # affected transition already carries
            # frame_detail_unavailable=true; not an anomaly.
            continue
        connections = chunk.manifest.get("connection_ids")
        if isinstance(connections, list) and len(connections) > 1:
            # Already reported as a manifest-level intra-chunk transition.
            continue
        key = _stream_key(
            str(chunk.manifest["market"]),
            str(chunk.manifest["symbol"]),
            str(chunk.manifest["stream"]),
        )
        if key in summaries:
            summaries[key]["chunks_with_scan_issues"] += 1
    transitions.sort(
        key=lambda item: (
            item.occurred_at_utc_ns,
            item.old_chunk_id,
            item.new_chunk_id,
            item.old_connection_id or "",
            item.new_connection_id or "",
            item.boundary_kind,
        )
    )
    return transitions, summaries, intervals


def _exchange_gap(
    last_old: FrameIdentity | None, first_new: FrameIdentity | None
) -> float | None:
    if last_old is None or first_new is None:
        return None
    if last_old.exchange_event_time_ns is None or first_new.exchange_event_time_ns is None:
        return None
    return (first_new.exchange_event_time_ns - last_old.exchange_event_time_ns) / 1_000_000_000


def _collector_id(manifest: dict[str, Any]) -> str | None:
    ids = manifest.get("collector_instance_ids")
    if isinstance(ids, list) and ids:
        return str(ids[0])
    return None


def _tally(summary: dict[str, int], kind: str) -> None:
    summary["transitions_total"] += 1
    if kind == EXPLICIT_SEQUENCE_GAP:
        summary["explicit_gap"] += 1
    elif kind == BLUE_GREEN_OVERLAP:
        summary["blue_green_overlap"] += 1
    elif kind == UNMARKED_RECONNECT:
        summary["unmarked_reconnect"] += 1
    else:
        summary["unknown"] += 1


INCREMENTAL_SCHEMA_VERSION = "m22.9-reconnect-audit-continuation.v1"


def strict_manifest_inventory(
    data_root: Path,
    *,
    market: str | None = None,
    stream: str | None = None,
    deep_scan: bool = True,
) -> tuple[list[ChunkScan], dict[str, Any]]:
    """Read every selected manifest strictly and optionally verify its Raw.

    This is the fail-closed installed-observer inventory.  The historical CLI
    intentionally retains its legacy malformed-manifest presentation behavior,
    while acceptance must never let malformed authority shrink its corpus.
    """

    layout = read_only_layout(data_root)
    chunks: list[ChunkScan] = []
    members: list[dict[str, str]] = []
    absences: list[dict[str, str]] = []
    if not layout.manifests.is_dir():
        raise SealError("Raw manifest authority is unavailable")
    seen_chunk_ids: set[str] = set()
    for manifest_path in sorted(layout.manifests.glob("*.manifest.json")):
        try:
            manifest_bytes = manifest_path.read_bytes()
            strict = read_strict_manifest(manifest_path, recorder_root=layout.root)
        except (OSError, SealError) as exc:
            raise SealError(f"strict manifest inventory failed for {manifest_path}") from exc
        manifest = cast(dict[str, Any], strict)
        if market is not None and manifest["market"] != market:
            continue
        if stream is not None and manifest["stream"] != stream:
            continue
        chunk_id = str(manifest["chunk_id"])
        if chunk_id in seen_chunk_ids:
            raise SealError(f"duplicate Raw manifest chunk_id: {chunk_id}")
        seen_chunk_ids.add(chunk_id)
        relative_manifest = str(manifest_path.resolve().relative_to(layout.root))
        members.append(
            {
                "path": relative_manifest,
                "sha256": hashlib.sha256(manifest_bytes).hexdigest(),
                "chunk_id": chunk_id,
            }
        )
        chunk = ChunkScan(manifest=manifest)
        sealed = _sealed_path(layout, manifest)
        if not sealed.is_file():
            chunk.issue = "sealed_file_missing_after_archival"
            chunk.deep_scan_allowed = False
            absences.append(
                {
                    "chunk_id": str(manifest["chunk_id"]),
                    "manifest_path": relative_manifest,
                    "has_sequence_gap_marker": str(
                        "sequence_gap" in _manifest_flags(manifest)
                    ).lower(),
                }
            )
        elif deep_scan:
            try:
                validate_sealed_artifact(sealed, cast(dict[str, object], manifest))
                chunk.frames = scan_chunk_frames(sealed, manifest)
                chunk.deep_scan_allowed = False
            except (OSError, SealError, ValueError) as exc:
                raise SealError(f"strict Raw verification failed for {sealed}") from exc
        chunks.append(chunk)
    members.sort(key=lambda item: item["path"])
    chunks.sort(
        key=lambda item: (
            int(item.manifest["created_at_utc_ns"]),
            str(item.manifest["chunk_id"]),
        )
    )
    canonical = "".join(
        f"{item['path']}\t{item['sha256']}\n" for item in members
    ).encode()
    return chunks, {
        "count": len(members),
        "sha256": hashlib.sha256(canonical).hexdigest(),
        "members": members,
        "artifact_absences": absences,
    }


def _frame_document(frame: FrameIdentity) -> dict[str, Any]:
    return {
        "chunk_id": frame.chunk_id,
        "frame_index": frame.frame_index,
        "connection_id": frame.connection_id,
        "receive_time_utc_ns": frame.receive_time_utc_ns,
        "capture_flags": list(frame.capture_flags),
        "source_sequence": dict(frame.source_sequence),
        "payload_sha256": frame.payload_sha256,
        "exchange_event_time_ns": frame.exchange_event_time_ns,
    }


def _frame_from_document(document: Mapping[str, object]) -> FrameIdentity:
    flags = document.get("capture_flags")
    sequence = document.get("source_sequence")
    frame_index = document.get("frame_index")
    receive_time = document.get("receive_time_utc_ns")
    exchange_time = document.get("exchange_event_time_ns")
    chunk_id = document.get("chunk_id")
    connection_id = document.get("connection_id")
    payload_sha256 = document.get("payload_sha256")
    if (
        not isinstance(chunk_id, str)
        or not chunk_id
        or not isinstance(connection_id, str)
        or not connection_id
        or not isinstance(payload_sha256, str)
        or len(payload_sha256) != 64
        or any(char not in "0123456789abcdef" for char in payload_sha256)
        or not isinstance(flags, list)
        or any(not isinstance(flag, str) for flag in flags)
        or not isinstance(sequence, dict)
        or not isinstance(frame_index, int)
        or isinstance(frame_index, bool)
        or not isinstance(receive_time, int)
        or isinstance(receive_time, bool)
        or (
            exchange_time is not None
            and (not isinstance(exchange_time, int) or isinstance(exchange_time, bool))
        )
        or any(
            not isinstance(value, (int, str)) or isinstance(value, bool)
            for value in sequence.values()
        )
    ):
        raise SealError("reconnect continuation frame is malformed")
    return FrameIdentity(
        chunk_id=chunk_id,
        frame_index=frame_index,
        connection_id=connection_id,
        receive_time_utc_ns=receive_time,
        capture_flags=tuple(str(item) for item in flags),
        source_sequence={str(key): cast(int | str, value) for key, value in sequence.items()},
        payload_sha256=payload_sha256,
        exchange_event_time_ns=(
            exchange_time if isinstance(exchange_time, int) else None
        ),
    )


def _chunk_context_document(chunk: ChunkScan) -> dict[str, Any]:
    frames = chunk.frames
    return {
        "manifest": chunk.manifest,
        "frames": (
            [_frame_document(frame) for frame in frames[-2:]]
            if frames is not None
            else None
        ),
        "issue": chunk.issue,
    }


def _chunk_from_context(document: Mapping[str, object]) -> ChunkScan:
    manifest = document.get("manifest")
    frames = document.get("frames")
    if not isinstance(manifest, dict) or (frames is not None and not isinstance(frames, list)):
        raise SealError("reconnect continuation chunk is malformed")
    if (
        not isinstance(manifest.get("chunk_id"), str)
        or not isinstance(manifest.get("market"), str)
        or not isinstance(manifest.get("symbol"), str)
        or not isinstance(manifest.get("stream"), str)
        or not manifest["chunk_id"]
        or not manifest["market"]
        or not manifest["symbol"]
        or not manifest["stream"]
        or not isinstance(manifest.get("created_at_utc_ns"), int)
        or isinstance(manifest.get("created_at_utc_ns"), bool)
        or not isinstance(manifest.get("record_count"), int)
        or isinstance(manifest.get("record_count"), bool)
        or not isinstance(manifest.get("connection_ids"), list)
        or any(not isinstance(item, str) for item in manifest["connection_ids"])
    ):
        raise SealError("reconnect continuation manifest is malformed")
    result = ChunkScan(
        manifest={str(key): value for key, value in manifest.items()},
        frames=(
            [_frame_from_document(cast(Mapping[str, object], item)) for item in frames]
            if isinstance(frames, list) and all(isinstance(item, dict) for item in frames)
            else None
        ),
        issue=str(document["issue"]) if isinstance(document.get("issue"), str) else None,
        deep_scan_allowed=False,
    )
    if result.frames is not None and any(
        frame.chunk_id != manifest["chunk_id"] for frame in result.frames
    ):
        raise SealError("reconnect continuation frame identity is mixed")
    return result


def _continuation_contexts(
    continuation: Mapping[str, object] | None,
) -> dict[tuple[str, str, str], list[ChunkScan]]:
    if continuation is None:
        return {}
    if continuation.get("schema_version") != INCREMENTAL_SCHEMA_VERSION:
        raise SealError("reconnect continuation schema is invalid")
    members = continuation.get("manifest_members")
    if not isinstance(members, dict) or any(
        not isinstance(path, str)
        or not path
        or not isinstance(digest, str)
        or len(digest) != 64
        or any(char not in "0123456789abcdef" for char in digest)
        for path, digest in members.items()
    ):
        raise SealError("reconnect continuation manifest inventory is malformed")
    streams = continuation.get("streams")
    if not isinstance(streams, dict):
        raise SealError("reconnect continuation streams are malformed")
    result: dict[tuple[str, str, str], list[ChunkScan]] = {}
    for name, value in streams.items():
        if not isinstance(value, dict):
            raise SealError("reconnect continuation stream is malformed")
        market = value.get("market")
        symbol = value.get("symbol")
        stream = value.get("stream")
        chunks = value.get("chunks")
        legacy_name = (
            isinstance(name, str)
            and isinstance(market, str)
            and isinstance(stream, str)
            and symbol is None
            and name == f"{market}:{stream}"
        )
        if legacy_name:
            symbol = LEGACY_SINGLE_SYMBOL
        expected_name = (
            f"{market}:{stream}" if legacy_name else f"{market}:{symbol}:{stream}"
        )
        if (
            not isinstance(name, str)
            or not isinstance(market, str)
            or not isinstance(symbol, str)
            or not isinstance(stream, str)
            or not name
            or not market
            or not symbol
            or not stream
            or not isinstance(chunks, list)
            or name != expected_name
        ):
            raise SealError("reconnect continuation stream identity is malformed")
        context = [
            _chunk_from_context(cast(Mapping[str, object], item))
            for item in chunks
            if isinstance(item, dict)
        ]
        if len(context) != len(chunks) or any(
            chunk.manifest.get("market") != market
            or chunk.manifest.get("symbol") != symbol
            or chunk.manifest.get("stream") != stream
            for chunk in context
        ):
            raise SealError("reconnect continuation chunk list is malformed")
        result[(market, symbol, stream)] = context
    return result


def validate_incremental_continuation(continuation: Mapping[str, object]) -> None:
    """Validate compact reconnect state before a stage is resumed."""

    _continuation_contexts(continuation)


def _updated_contexts(
    prior: dict[tuple[str, str, str], list[ChunkScan]],
    new_chunks: list[ChunkScan],
) -> dict[tuple[str, str, str], list[ChunkScan]]:
    updated = {key: list(value) for key, value in prior.items()}
    for chunk in new_chunks:
        key = (
            str(chunk.manifest["market"]),
            str(chunk.manifest["symbol"]),
            str(chunk.manifest["stream"]),
        )
        current = updated.setdefault(key, [])
        if _zero_record_chunk(chunk):
            if current:
                current.append(chunk)
        else:
            updated[key] = [chunk]
    return updated


def _audit_output(
    transitions: list[Transition],
    summaries: dict[tuple[str, str, str], dict[str, int]],
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    streams_output: list[dict[str, Any]] = []
    for (market, symbol, stream), summary in sorted(summaries.items()):
        streams_output.append(
            {
                "market": market,
                "symbol": symbol,
                "stream": stream,
                "summary": dict(summary),
                "transitions": [
                    item.to_dict()
                    for item in transitions
                    if item.old_manifest.get("market") == market
                    and item.old_manifest.get("symbol") == symbol
                    and item.old_manifest.get("stream") == stream
                ],
            }
        )
    total = {
        name: sum(summary[name] for summary in summaries.values())
        for name in (
            "chunks_scanned",
            "transitions_total",
            "explicit_gap",
            "blue_green_overlap",
            "unmarked_reconnect",
            "unknown",
            "chunks_with_scan_issues",
        )
    }
    return streams_output, total


def incremental_audit_data_root(
    data_root: Path,
    *,
    continuation: Mapping[str, object] | None = None,
) -> dict[str, Any]:
    """Strict baseline/incremental audit without rescanning verified old Raw."""

    layout = read_only_layout(data_root)
    chunks, inventory = strict_manifest_inventory(data_root, deep_scan=False)
    current_members = {
        str(item["path"]): str(item["sha256"])
        for item in cast(list[dict[str, str]], inventory["members"])
    }
    known_members: dict[str, str] = {}
    if continuation is not None:
        members = continuation.get("manifest_members")
        if not isinstance(members, dict) or any(
            not isinstance(key, str) or not isinstance(value, str)
            for key, value in members.items()
        ):
            raise SealError("reconnect continuation manifest inventory is malformed")
        known_members = {str(key): str(value) for key, value in members.items()}
    integrity_findings = sorted(
        path
        for path, digest in known_members.items()
        if current_members.get(path) != digest
    )
    new_paths = set(current_members) - set(known_members)
    new_chunk_ids = {
        item["chunk_id"]
        for item in cast(list[dict[str, str]], inventory["members"])
        if item["path"] in new_paths
    }
    new_chunks = [
        chunk
        for chunk in chunks
        if str(chunk.manifest["chunk_id"]) in new_chunk_ids
    ]
    for chunk in new_chunks:
        sealed = _sealed_path(layout, chunk.manifest)
        if not sealed.is_file():
            chunk.issue = "sealed_file_missing_after_archival"
            chunk.deep_scan_allowed = False
            continue
        try:
            validate_sealed_artifact(sealed, cast(dict[str, object], chunk.manifest))
            chunk.frames = scan_chunk_frames(sealed, chunk.manifest)
            chunk.deep_scan_allowed = False
        except (OSError, SealError, ValueError) as exc:
            raise SealError(f"strict Raw verification failed for {sealed}") from exc
    prior_contexts = _continuation_contexts(continuation)
    new_by_key: dict[tuple[str, str, str], list[ChunkScan]] = {}
    for chunk in new_chunks:
        key = (
            str(chunk.manifest["market"]),
            str(chunk.manifest["symbol"]),
            str(chunk.manifest["stream"]),
        )
        new_by_key.setdefault(key, []).append(chunk)
    audit_chunks: list[ChunkScan] = []
    for key in sorted(new_by_key):
        audit_chunks.extend(prior_contexts.get(key, []))
        audit_chunks.extend(new_by_key[key])
    catalog: Catalog | None = None
    catalog_findings: list[str] = []
    if layout.catalog.is_file():
        catalog = Catalog(layout.catalog, read_only=True)
    try:
        if catalog is not None:
            if catalog.integrity_check() != ("ok",):
                raise SealError("Catalog integrity check failed")
            if catalog.malformed_discontinuity_events():
                catalog_findings.append("malformed_discontinuity_authority")
            if catalog.degraded_closed_discontinuity_pairs():
                catalog_findings.append("degraded_discontinuity_authority")
            manifests_by_chunk = {
                str(chunk.manifest["chunk_id"]): chunk.manifest for chunk in chunks
            }
            known_manifest_paths = set(current_members)
            for row in catalog.chunks_in_states(*tuple(ChunkState)):
                manifest_path = row.get("manifest_path")
                if (
                    isinstance(manifest_path, str)
                    and manifest_path not in known_manifest_paths
                ):
                    catalog_findings.append(
                        f"catalog_manifest_disagreement:{row.get('chunk_id')}"
                    )
                manifest = manifests_by_chunk.get(str(row.get("chunk_id", "")))
                if manifest is None:
                    continue
                comparisons = {
                    "record_count": manifest["record_count"],
                    "sealed_path": manifest["relative_path"],
                    "stored_bytes": manifest["stored_bytes"],
                    "stored_sha256": manifest["stored_sha256"],
                    "uncompressed_bytes": manifest["uncompressed_bytes"],
                    "uncompressed_sha256": manifest["uncompressed_sha256"],
                }
                if any(row.get(key) != value for key, value in comparisons.items()):
                    catalog_findings.append(
                        f"catalog_manifest_disagreement:{row.get('chunk_id')}"
                    )
        transitions, summaries, _intervals = collect_transitions(
            audit_chunks,
            layout,
            catalog,
            emit_chunk_ids=frozenset(str(chunk.manifest["chunk_id"]) for chunk in new_chunks),
        )
    finally:
        if catalog is not None:
            catalog.close()
    updated = _updated_contexts(prior_contexts, new_chunks)
    continuation_document = {
        "schema_version": INCREMENTAL_SCHEMA_VERSION,
        "manifest_members": current_members,
        "streams": {
            f"{market}:{symbol}:{stream}": {
                "market": market,
                "symbol": symbol,
                "stream": stream,
                "chunks": [_chunk_context_document(chunk) for chunk in context],
            }
            for (market, symbol, stream), context in sorted(updated.items())
        },
    }
    streams_output, summary = _audit_output(transitions, summaries)
    return {
        "schema_version": "m22.9-reconnect-audit.v2",
        "manifest_inventory": inventory,
        "integrity_findings": integrity_findings,
        "catalog_findings": sorted(set(catalog_findings)),
        "deep_scanned_chunk_ids": sorted(
            str(chunk.manifest["chunk_id"]) for chunk in new_chunks
        ),
        "streams": streams_output,
        "summary": summary,
        "continuation": continuation_document,
    }


def audit_data_root(
    data_root: Path,
    *,
    market: str | None = None,
    stream: str | None = None,
    cutoff_utc_ns: int | None = None,
) -> dict[str, Any]:
    """Read-only canonical audit payload (deterministic for a fixed input set).

    Never creates directories, never writes into ``data_root``, and opens the
    Catalog read-only. The returned document contains no wall-clock values;
    execution metadata belongs to the artifact wrapper only.
    """
    if cutoff_utc_ns is not None and cutoff_utc_ns < 0:
        raise ValueError("cutoff_utc_ns must be non-negative")
    layout = read_only_layout(data_root)
    missing = missing_inputs(data_root)
    catalog: Catalog | None = None
    catalog_path = layout.catalog
    if catalog_path.is_file():
        try:
            catalog = Catalog(catalog_path, read_only=True)
        except Exception:
            missing.append("state/catalog.sqlite")
            catalog = None
    try:
        chunks = load_manifest_chunks(
            layout,
            market=market,
            stream=stream,
            cutoff_utc_ns=cutoff_utc_ns,
        )
        inventory_count, inventory_sha256 = manifest_inventory(chunks, layout)
        transitions, summaries, intervals = collect_transitions(chunks, layout, catalog)
        streams_output: list[dict[str, Any]] = []
        for (market_name, symbol_name, stream_name), summary in sorted(
            summaries.items()
        ):
            stream_transitions = [
                transition.to_dict()
                for transition in transitions
                if transition.old_manifest.get("market") == market_name
                and transition.old_manifest.get("symbol") == symbol_name
                and transition.old_manifest.get("stream") == stream_name
            ]
            streams_output.append(
                {
                    "market": market_name,
                    "symbol": symbol_name,
                    "stream": stream_name,
                    "summary": {
                        "chunks_scanned": summary["chunks_scanned"],
                        "transitions_total": summary["transitions_total"],
                        "explicit_gap": summary["explicit_gap"],
                        "blue_green_overlap": summary["blue_green_overlap"],
                        "unmarked_reconnect": summary["unmarked_reconnect"],
                        "unknown": summary["unknown"],
                        "chunks_with_scan_issues": summary[
                            "chunks_with_scan_issues"
                        ],
                    },
                    "transitions": stream_transitions,
                }
            )
        total: dict[str, int] = {
            "chunks_scanned": sum(item["chunks_scanned"] for item in summaries.values()),
            "transitions_total": sum(item["transitions_total"] for item in summaries.values()),
            "explicit_gap": sum(item["explicit_gap"] for item in summaries.values()),
            "blue_green_overlap": sum(
                item["blue_green_overlap"] for item in summaries.values()
            ),
            "unmarked_reconnect": sum(
                item["unmarked_reconnect"] for item in summaries.values()
            ),
            "unknown": sum(item["unknown"] for item in summaries.values()),
            "chunks_with_scan_issues": sum(
                item["chunks_with_scan_issues"] for item in summaries.values()
            ),
        }
        started_by_id: dict[tuple[str, str, str, str], dict[str, Any]] = {}
        completed_by_id: dict[tuple[str, str, str, str], dict[str, Any]] = {}
        for event in catalog.operational_events() if catalog is not None else []:
            evidence = event.get("evidence")
            if not isinstance(evidence, dict):
                continue
            gap_id = evidence.get("gap_id")
            if not isinstance(gap_id, str) or not gap_id:
                continue
            event_market = evidence.get("market")
            event_symbol = evidence.get("symbol")
            if (
                event_symbol is None
                and catalog is not None
                and catalog.legacy_identity_schema
            ):
                event_symbol = LEGACY_SINGLE_SYMBOL
            event_stream = evidence.get("stream")
            if (
                not isinstance(event_market, str)
                or not event_market
                or not isinstance(event_symbol, str)
                or not event_symbol
                or not isinstance(event_stream, str)
                or not event_stream
            ):
                continue
            identity = (event_market, event_symbol, event_stream, gap_id)
            if event["event_type"] == "STREAM_DISCONTINUITY_STARTED":
                started_by_id[identity] = event
            elif event["event_type"] == "STREAM_DISCONTINUITY_COMPLETED":
                completed_by_id[identity] = event
        matched_pairs = len(set(started_by_id) & set(completed_by_id))
        unmatched_started = len(started_by_id) - matched_pairs
        unmatched_completed = len(completed_by_id) - matched_pairs
        return {
            "tool": "audit_reconnect_boundaries",
            "schema_version": TOOL_SCHEMA_VERSION,
            "data_root": str(data_root.resolve()),
            "filters": {"market": market, "stream": stream},
            "audit_cutoff_utc_ns": cutoff_utc_ns,
            "manifest_inventory_count": inventory_count,
            "manifest_inventory_sha256": inventory_sha256,
            "missing_inputs": sorted(set(missing)),
            "summary": total,
            "streams": streams_output,
            "catalog": {
                "discontinuity_started": len(started_by_id),
                "discontinuity_completed": len(completed_by_id),
                "matched_pairs": matched_pairs,
                "unmatched_started": unmatched_started,
                "unmatched_completed": unmatched_completed,
                "gap_intervals": [
                    {
                        "gap_id": interval.gap_id,
                        "market": interval.market,
                        "symbol": interval.symbol,
                        "stream": interval.stream,
                        "started_at_utc_ns": interval.started_at_utc_ns,
                        "ended_at_utc_ns": interval.ended_at_utc_ns,
                        "reason": interval.reason,
                        "original_connection_id": interval.original_connection_id,
                        "new_connection_id": interval.new_connection_id,
                        "original_generation": interval.original_generation,
                        "new_generation": interval.new_generation,
                    }
                    for interval in intervals
                ],
            },
        }
    finally:
        if catalog is not None:
            catalog.close()


def _canonical_body(document: dict[str, Any]) -> str:
    return json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n"


def _validate_output_target(data_root: Path, output: Path) -> None:
    root = data_root.resolve()
    target = output.resolve()
    if target == root or root in target.parents:
        raise ValueError(
            f"--output {output} resolves inside the audited data root {root}"
        )


def _write_artifact(path: Path, body: str) -> None:
    """Atomically write the artifact; the parent is proven external already."""
    descriptor, temporary = tempfile.mkstemp(
        dir=path.parent, prefix=f".{path.name}.", suffix=".tmp"
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as target:
            target.write(body)
            target.flush()
            os.fsync(target.fileno())
        os.replace(temporary, path)
    except BaseException:
        with suppress(OSError):
            os.unlink(temporary)
        raise


def _print_summary(document: dict[str, Any], output: TextIO) -> None:
    summary = document["summary"]
    output.write(
        f"data_root: {document['data_root']}\n"
        f"audit_cutoff_utc_ns: {document['audit_cutoff_utc_ns']}\n"
        f"manifest_inventory_count: {document['manifest_inventory_count']}\n"
        f"manifest_inventory_sha256: {document['manifest_inventory_sha256']}\n"
        f"missing_inputs: {', '.join(document['missing_inputs']) or 'none'}\n"
        f"chunks_scanned: {summary['chunks_scanned']}\n"
        f"transitions_total: {summary['transitions_total']}\n"
        f"  explicit_gap: {summary['explicit_gap']}\n"
        f"  blue_green_overlap: {summary['blue_green_overlap']}\n"
        f"  unmarked_reconnect: {summary['unmarked_reconnect']}\n"
        f"  unknown: {summary['unknown']}\n"
    )
    for stream in document["streams"]:
        item = stream["summary"]
        output.write(
            f"{stream['market']}/{stream['symbol']}/{stream['stream']}: "
            f"chunks={item['chunks_scanned']} transitions={item['transitions_total']} "
            f"(explicit={item['explicit_gap']} overlap={item['blue_green_overlap']} "
            f"unmarked={item['unmarked_reconnect']} unknown={item['unknown']}"
            f" issues={item['chunks_with_scan_issues']})\n"
        )
    for stream in document["streams"]:
        for transition in stream["transitions"]:
            if transition["kind"] in {UNMARKED_RECONNECT, UNKNOWN}:
                output.write(
                    f"  {transition['kind']}: "
                    f"{stream['market']}/{stream['symbol']}/{stream['stream']} "
                    f"chunk {transition['old_chunk_id'][:8]} -> "
                    f"{transition['new_chunk_id'][:8]} "
                    f"conn {_short(transition['old_connection_id'])} -> "
                    f"{_short(transition['new_connection_id'])} "
                    f"at {transition['occurred_at_utc_ns']}\n"
                )


def _short(value: str | None) -> str:
    return value[:8] if value else "-"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Read-only historical reconnect-boundary audit"
    )
    parser.add_argument(
        "--data-root",
        type=Path,
        default=Path("/var/lib/binance-market-data-recorder"),
        help="data root to audit (default: production root)",
    )
    parser.add_argument("--market", choices=["spot", "um_perpetual"], default=None)
    parser.add_argument("--stream", default=None, help="exact stream name")
    parser.add_argument(
        "--cutoff-utc-ns",
        type=int,
        default=None,
        help="include only manifests with created_at_utc_ns <= cutoff",
    )
    parser.add_argument("--json", action="store_true", help="emit the JSON artifact")
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="write the JSON artifact to a file (never into the data root)",
    )
    args = parser.parse_args(argv)
    document = audit_data_root(
        args.data_root,
        market=args.market,
        stream=args.stream,
        cutoff_utc_ns=args.cutoff_utc_ns,
    )
    canonical = _canonical_body(document)
    canonical_sha256 = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    artifact: dict[str, Any] = {
        "tool": "audit_reconnect_boundaries",
        "schema_version": ARTIFACT_SCHEMA_VERSION,
        "canonical_sha256": canonical_sha256,
        "execution": {
            "generated_at_utc_ns": time.time_ns(),
            "generated_at_utc": datetime.now(UTC).isoformat(),
        },
        "canonical": document,
    }
    artifact_body = json.dumps(artifact, sort_keys=True, separators=(",", ":")) + "\n"
    if args.output is not None:
        _validate_output_target(args.data_root, args.output)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        _write_artifact(args.output, artifact_body)
    if args.json:
        sys.stdout.write(artifact_body)
    else:
        _print_summary(document, sys.stdout)
        sys.stdout.write(f"canonical_sha256: {canonical_sha256}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
