"""Read-only historical reconnect-boundary audit (M21.4.11).

Scans sealed Raw chunks, manifests, and the lifecycle Catalog to find every
connection_id transition and classify it:

- EXPLICIT_SEQUENCE_GAP: at least one side carries persistent gap evidence
  (manifest gap / capture_flags ``sequence_gap`` or ``reconnect_gap``);
- BLUE_GREEN_OVERLAP: the chunk carries explicit blue/green overlap
  provenance;
- UNMARKED_RECONNECT: a connection change with no gap evidence at all; the
  sealed interval claims gap=false/complete=true even though exchange-side
  completeness between close and the first new frame cannot be proven;
- UNKNOWN: malformed or unverifiable inputs.

The tool is strictly read-only: it never modifies Raw, manifests, or Catalog.
JSON output is deterministic (sorted keys, stable ordering). Sealed chunks
whose local copy was deleted after verified archival are classified from
manifest evidence alone and marked ``frame_detail_unavailable=true``.

Usage:

    python3.12 tools/audit_reconnect_boundaries.py [--data-root DIR]
        [--market spot|um_perpetual] [--stream NAME]
        [--json] [--output historical-reconnect-audit.json]
"""

from __future__ import annotations

import argparse
import io
import itertools
import json
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TextIO

import zstandard

from binance_market_data_recorder.spool.format import (
    FRAME_PREFIX,
    decode_chunk_header,
    decode_envelope,
)
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout

TOOL_SCHEMA_VERSION = "historical-reconnect-audit.v1"
GAP_FLAGS = frozenset({"sequence_gap", "reconnect_gap"})

EXPLICIT_SEQUENCE_GAP = "EXPLICIT_SEQUENCE_GAP"
BLUE_GREEN_OVERLAP = "BLUE_GREEN_OVERLAP"
UNMARKED_RECONNECT = "UNMARKED_RECONNECT"
UNKNOWN = "UNKNOWN"

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


@dataclass
class CatalogGapInterval:
    gap_id: str
    started_at_utc_ns: int
    ended_at_utc_ns: int | None


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
    occurred_at_utc_ns: int
    collector_instance_id: str | None
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
            "occurred_at_utc_ns": self.occurred_at_utc_ns,
            "collector_instance_id": self.collector_instance_id,
            "frame_detail_unavailable": self.frame_detail_unavailable,
        }
        if self.connection_ids is not None:
            document["connection_ids"] = list(self.connection_ids)
        return document


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
    import hashlib

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
) -> list[ChunkScan]:
    chunks: list[ChunkScan] = []
    manifest_dir = layout.manifests
    if not manifest_dir.is_dir():
        return chunks
    for path in sorted(manifest_dir.glob("*.manifest.json")):
        try:
            document = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(document, dict):
            continue
        if market is not None and document.get("market") != market:
            continue
        if stream is not None and document.get("stream") != stream:
            continue
        chunks.append(ChunkScan(manifest=document))
    chunks.sort(key=lambda item: int(item.manifest.get("created_at_utc_ns", 0)))
    return chunks


def _sealed_path(layout: Any, manifest: dict[str, Any]) -> Path:
    relative = manifest.get("relative_path")
    if not isinstance(relative, str) or not relative:
        raise ValueError("manifest lacks relative_path")
    return Path(layout.root) / relative


def _decompress_multi_connection(chunks: list[ChunkScan], layout: Any) -> None:
    multi = {
        chunk.manifest["chunk_id"]
        for chunk in chunks
        if len(chunk.manifest.get("connection_ids", [])) > 1
    }
    for chunk in chunks:
        if chunk.manifest["chunk_id"] not in multi:
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


def _manifest_gap_evidence(manifest: dict[str, Any]) -> bool:
    flags = manifest.get("capture_flags")
    return bool(
        manifest.get("gap") is True
        or (isinstance(flags, list) and bool(set(flags) & GAP_FLAGS))
    )


def _manifest_overlap(manifest: dict[str, Any]) -> bool:
    flags = manifest.get("capture_flags")
    return isinstance(flags, list) and "blue_green_overlap" in flags


def _catalog_gap_match(
    intervals: list[CatalogGapInterval],
    market: str,
    stream: str,
    occurred_at_utc_ns: int,
) -> str:
    for interval in intervals:
        if interval.started_at_utc_ns <= occurred_at_utc_ns:
            if interval.ended_at_utc_ns is None:
                return "PENDING"
            if occurred_at_utc_ns <= interval.ended_at_utc_ns:
                return "MATCHED"
    return "UNMATCHED"


def _classify(
    old_chunk: ChunkScan,
    new_chunk: ChunkScan,
) -> str:
    if _manifest_gap_evidence(old_chunk.manifest) or _manifest_gap_evidence(
        new_chunk.manifest
    ):
        return EXPLICIT_SEQUENCE_GAP
    if _manifest_overlap(old_chunk.manifest) or _manifest_overlap(new_chunk.manifest):
        return BLUE_GREEN_OVERLAP
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


def _stream_key(market: str, stream: str) -> tuple[str, str]:
    return (market, stream)


def collect_transitions(
    chunks: list[ChunkScan],
    layout: Any,
    catalog: Catalog | None,
) -> tuple[list[Transition], dict[tuple[str, str], dict[str, int]]]:
    """Return deterministic per-stream transitions and per-stream summaries."""
    _decompress_multi_connection(chunks, layout)
    intervals: list[CatalogGapInterval] = []
    if catalog is not None:
        for event in catalog.operational_events():
            evidence = event.get("evidence")
            if not isinstance(evidence, dict):
                continue
            gap_id = evidence.get("gap_id")
            if not isinstance(gap_id, str):
                continue
            if event["event_type"] == "STREAM_DISCONTINUITY_STARTED":
                intervals.append(
                    CatalogGapInterval(
                        gap_id=gap_id,
                        started_at_utc_ns=int(evidence["gap_started_at_utc_ns"]),
                        ended_at_utc_ns=None,
                    )
                )
            elif event["event_type"] == "STREAM_DISCONTINUITY_COMPLETED":
                for interval in intervals:
                    if interval.gap_id == gap_id:
                        interval.ended_at_utc_ns = int(
                            evidence.get("gap_ended_at_utc_ns", 0)
                        )
        intervals.sort(key=lambda interval: interval.started_at_utc_ns)

    transitions: list[Transition] = []
    summaries: dict[tuple[str, str], dict[str, int]] = {}
    previous_by_key: dict[tuple[str, str], ChunkScan] = {}
    for chunk in chunks:
        market = str(chunk.manifest["market"])
        stream = str(chunk.manifest["stream"])
        if market not in {"spot", "um_perpetual"}:
            continue
        key = _stream_key(market, stream)
        if key not in summaries:
            summaries[key] = {
                "chunks_scanned": 0,
                "transitions_total": 0,
                "explicit_gap": 0,
                "blue_green_overlap": 0,
                "unmarked_reconnect": 0,
                "unknown": 0,
            }
        summaries[key]["chunks_scanned"] += 1
        if stream not in WEBSOCKET_STREAMS.get(market, frozenset()):
            # REST-polled streams: per-request connection ids are not
            # reconnect boundaries; report chunk counts only.
            continue
        frames = chunk.frames
        if frames is not None and len(frames) > 1:
            for previous_frame, frame in itertools.pairwise(frames):
                if previous_frame.connection_id == frame.connection_id:
                    continue
                transition = Transition(
                    kind=(
                        EXPLICIT_SEQUENCE_GAP
                        if _manifest_gap_evidence(chunk.manifest)
                        else (
                            BLUE_GREEN_OVERLAP
                            if _manifest_overlap(chunk.manifest)
                            else UNMARKED_RECONNECT
                        )
                    ),
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
                    catalog_gap_match=_catalog_gap_match(
                        intervals, market, stream, frame.receive_time_utc_ns
                    ),
                    occurred_at_utc_ns=frame.receive_time_utc_ns,
                    collector_instance_id=_collector_id(chunk.manifest),
                )
                transitions.append(transition)
                _tally(summaries[key], transition.kind)
        elif frames is None:
            connections = chunk.manifest.get("connection_ids")
            if isinstance(connections, list) and len(connections) > 1:
                # The local sealed copy was deleted after verified archival;
                # frame order inside the chunk is unrecoverable. The manifest
                # alone still proves an unmarked or evidenced connection
                # boundary existed inside this chunk.
                kind = (
                    EXPLICIT_SEQUENCE_GAP
                    if _manifest_gap_evidence(chunk.manifest)
                    else (
                        BLUE_GREEN_OVERLAP
                        if _manifest_overlap(chunk.manifest)
                        else UNMARKED_RECONNECT
                    )
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
                    catalog_gap_match=_catalog_gap_match(
                        intervals,
                        market,
                        stream,
                        int(chunk.manifest.get("created_at_utc_ns", 0)),
                    ),
                    occurred_at_utc_ns=int(
                        chunk.manifest.get("created_at_utc_ns", 0)
                    ),
                    collector_instance_id=_collector_id(chunk.manifest),
                    frame_detail_unavailable=True,
                    connection_ids=sorted(str(item) for item in connections),
                )
                transitions.append(transition)
                _tally(summaries[key], transition.kind)
        previous = previous_by_key.get(key)
        if previous is not None and previous.manifest.get("stream") == stream and (
            previous.manifest.get("market") == market
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
                transition = Transition(
                    kind=_classify(previous, chunk),
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
                    catalog_gap_match=_catalog_gap_match(
                        intervals, market, stream, occurred_at
                    ),
                    occurred_at_utc_ns=occurred_at,
                    collector_instance_id=_collector_id(chunk.manifest),
                )
                transitions.append(transition)
                _tally(summaries[key], transition.kind)
        previous_by_key[key] = chunk

    for chunk in chunks:
        if chunk.issue is not None and chunk.frames is None:
            connections = chunk.manifest.get("connection_ids")
            if isinstance(connections, list) and len(connections) > 1:
                # Already reported as a manifest-level intra-chunk transition.
                continue
            key = _stream_key(str(chunk.manifest["market"]), str(chunk.manifest["stream"]))
            if key in summaries:
                summaries[key]["unknown"] += 1
    transitions.sort(
        key=lambda item: (
            item.occurred_at_utc_ns,
            item.old_chunk_id,
            item.new_chunk_id,
        )
    )
    return transitions, summaries


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


def audit_data_root(
    data_root: Path,
    *,
    market: str | None = None,
    stream: str | None = None,
) -> dict[str, Any]:
    layout = ensure_storage_layout(data_root)
    catalog: Catalog | None = None
    catalog_path = layout.catalog
    if catalog_path.is_file():
        try:
            catalog = Catalog(catalog_path, read_only=True)
        except Exception:
            catalog = None
    try:
        chunks = load_manifest_chunks(
            layout, market=market, stream=stream
        )
        transitions, summaries = collect_transitions(chunks, layout, catalog)
        streams_output: list[dict[str, Any]] = []
        for (market_name, stream_name), summary in sorted(summaries.items()):
            stream_transitions = [
                transition.to_dict()
                for transition in transitions
                if transition.old_manifest.get("market") == market_name
                and transition.old_manifest.get("stream") == stream_name
            ]
            streams_output.append(
                {
                    "market": market_name,
                    "stream": stream_name,
                    "summary": {
                        "chunks_scanned": summary["chunks_scanned"],
                        "transitions_total": summary["transitions_total"],
                        "explicit_gap": summary["explicit_gap"],
                        "blue_green_overlap": summary["blue_green_overlap"],
                        "unmarked_reconnect": summary["unmarked_reconnect"],
                        "unknown": summary["unknown"],
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
        }
        discontinuity_events = (
            catalog.operational_events()
            if catalog is not None
            else []
        )
        started_count = sum(
            1
            for event in discontinuity_events
            if event["event_type"] == "STREAM_DISCONTINUITY_STARTED"
        )
        completed_count = sum(
            1
            for event in discontinuity_events
            if event["event_type"] == "STREAM_DISCONTINUITY_COMPLETED"
        )
        return {
            "tool": "audit_reconnect_boundaries",
            "schema_version": TOOL_SCHEMA_VERSION,
            "data_root": str(data_root.resolve()),
            "filters": {"market": market, "stream": stream},
            "generated_at_utc_ns": time.time_ns(),
            "generated_at_utc": datetime.now(UTC).isoformat(),
            "summary": total,
            "streams": streams_output,
            "catalog": {
                "discontinuity_started": started_count,
                "discontinuity_completed": completed_count,
                "unmatched_started": max(0, started_count - completed_count),
            },
        }
    finally:
        if catalog is not None:
            catalog.close()


def _print_summary(document: dict[str, Any], output: TextIO) -> None:
    summary = document["summary"]
    output.write(
        f"data_root: {document['data_root']}\n"
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
            f"{stream['market']}/{stream['stream']}: "
            f"chunks={item['chunks_scanned']} transitions={item['transitions_total']} "
            f"(explicit={item['explicit_gap']} overlap={item['blue_green_overlap']} "
            f"unmarked={item['unmarked_reconnect']} unknown={item['unknown']})\n"
        )
    for stream in document["streams"]:
        for transition in stream["transitions"]:
            if transition["kind"] in {UNMARKED_RECONNECT, UNKNOWN}:
                output.write(
                    f"  {transition['kind']}: {stream['market']}/{stream['stream']} "
                    f"chunk {transition['old_chunk_id'][:8]} -> "
                    f"{transition['new_chunk_id'][:8]} "
                    f"conn {transition['old_connection_id'][:8]} -> "
                    f"{transition['new_connection_id'][:8]} "
                    f"at {transition['occurred_at_utc_ns']}\n"
                )


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
    parser.add_argument("--json", action="store_true", help="emit deterministic JSON")
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="write the JSON document to a file (never into the data root)",
    )
    args = parser.parse_args(argv)
    document = audit_data_root(
        args.data_root, market=args.market, stream=args.stream
    )
    body = json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n"
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(body, encoding="utf-8")
    if args.json:
        sys.stdout.write(body)
    else:
        _print_summary(document, sys.stdout)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
