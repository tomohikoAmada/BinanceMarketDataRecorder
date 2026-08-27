"""Strict reconnect-boundary audit used by installed M22.9 acceptance code.

The audit is deliberately read-only.  Catalog remains the lifecycle authority,
Raw frames provide boundary markers, and manifests provide sealed completeness.
Malformed evidence is an error, never an omitted denominator entry.
"""

from __future__ import annotations

import hashlib
import io
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import zstandard

from ..spool.format import FRAME_PREFIX, decode_chunk_header, decode_envelope
from ..spool.seal import SealError, read_strict_manifest, validate_sealed_artifact
from ..storage.catalog import Catalog, ChunkState
from ..storage.layout import StorageLayout

EXPLICIT_SEQUENCE_GAP = "EXPLICIT_SEQUENCE_GAP"
BLUE_GREEN_OVERLAP = "BLUE_GREEN_OVERLAP"
UNMARKED_RECONNECT = "UNMARKED_RECONNECT"
UNKNOWN = "UNKNOWN"


@dataclass(frozen=True, slots=True)
class BoundaryFrame:
    connection_id: str
    receive_time_utc_ns: int
    capture_flags: tuple[str, ...]
    source_sequence: dict[str, int | str]
    payload_sha256: str


@dataclass(frozen=True, slots=True)
class StrictChunk:
    path: Path
    manifest_path: Path
    manifest: dict[str, object]
    frames: tuple[BoundaryFrame, ...] | None
    issue: str | None = None


def _read_frames(path: Path) -> tuple[BoundaryFrame, ...]:
    try:
        compressed = path.read_bytes()
        raw = zstandard.ZstdDecompressor().decompress(compressed, max_output_size=0)
    except (OSError, zstandard.ZstdError) as exc:
        raise SealError(f"cannot read sealed Raw artifact {path}") from exc
    stream = io.BytesIO(raw)
    try:
        decode_chunk_header(stream)
    except Exception as exc:
        raise SealError(f"invalid Raw chunk header {path}") from exc
    result: list[BoundaryFrame] = []
    while prefix := stream.read(FRAME_PREFIX.size):
        body_length, _flags, _reserved, _checksum = FRAME_PREFIX.unpack(prefix)
        body = stream.read(body_length)
        if len(body) != body_length:
            raise SealError(f"truncated Raw frame in {path}")
        try:
            envelope = decode_envelope(body)
        except Exception as exc:
            raise SealError(f"invalid Raw envelope in {path}") from exc
        result.append(
            BoundaryFrame(
                connection_id=str(envelope.connection_id),
                receive_time_utc_ns=int(envelope.receive_time_utc_ns),
                capture_flags=tuple(envelope.capture_flags),
                source_sequence=dict(envelope.source_sequence),
                payload_sha256=hashlib.sha256(envelope.raw_payload).hexdigest(),
            )
        )
    return tuple(result)


def strict_manifest_inventory(
    data_root: Path,
    *,
    market: str | None = None,
    stream: str | None = None,
) -> tuple[list[StrictChunk], dict[str, object]]:
    """Inventory every matching manifest with exact path and byte identity."""

    layout = StorageLayout.from_root(data_root.resolve())
    if not layout.manifests.is_dir():
        return [], {"count": 0, "sha256": hashlib.sha256(b"").hexdigest(), "members": []}
    chunks: list[StrictChunk] = []
    members: list[dict[str, str]] = []
    artifact_absences: list[dict[str, str]] = []
    for manifest_path in sorted(layout.manifests.glob("*.manifest.json")):
        # Read bytes here so the inventory binds the exact file, not merely its
        # relative name.  read_strict_manifest repeats the read as the shared
        # parser's authority and detects a concurrent mutation as a mismatch.
        manifest_bytes = manifest_path.read_bytes()
        manifest = read_strict_manifest(manifest_path, recorder_root=layout.root)
        if market is not None and manifest["market"] != market:
            continue
        if stream is not None and manifest["stream"] != stream:
            continue
        relative = str(manifest_path.resolve().relative_to(layout.root))
        members.append({"path": relative, "sha256": hashlib.sha256(manifest_bytes).hexdigest()})
        sealed = (layout.root / str(manifest["relative_path"])).resolve()
        if sealed.is_file():
            validate_sealed_artifact(sealed, manifest)
            frames: tuple[BoundaryFrame, ...] | None = _read_frames(sealed)
            issue = None
        else:
            frames = None
            issue = "sealed_artifact_absent"
            artifact_absences.append(
                {
                    "chunk_id": str(manifest["chunk_id"]),
                    "manifest_path": relative,
                    "has_sequence_gap_marker": str(
                        "sequence_gap" in cast(list[object], manifest["capture_flags"])
                    ).lower(),
                }
            )
        chunks.append(StrictChunk(sealed, manifest_path, manifest, frames, issue))
    members.sort(key=lambda item: item["path"])
    chunks.sort(
        key=lambda chunk: (
            cast(int, chunk.manifest["created_at_utc_ns"]),
            str(chunk.manifest["chunk_id"]),
        )
    )
    canonical = "".join(f"{item['path']}\t{item['sha256']}\n" for item in members).encode()
    return chunks, {
        "count": len(members),
        "sha256": hashlib.sha256(canonical).hexdigest(),
        "members": members,
        "artifact_absences": artifact_absences,
    }


def _boundary_kind(old: BoundaryFrame | None, new: BoundaryFrame | None) -> str:
    if old is None or new is None:
        return UNKNOWN
    old_flags = set(old.capture_flags)
    new_flags = set(new.capture_flags)
    if "sequence_gap" in new_flags:
        return EXPLICIT_SEQUENCE_GAP
    if "sequence_gap" in old_flags or "sequence_gap" in new_flags:
        return UNKNOWN
    if "blue_green_overlap" in old_flags and "blue_green_overlap" in new_flags:
        return BLUE_GREEN_OVERLAP
    return UNMARKED_RECONNECT


def audit_data_root(
    data_root: Path,
    *,
    market: str | None = None,
    stream: str | None = None,
) -> dict[str, object]:
    """Return deterministic strict boundary evidence for one Recorder root."""

    chunks, inventory = strict_manifest_inventory(data_root, market=market, stream=stream)
    transitions: list[dict[str, object]] = []
    for index, chunk in enumerate(chunks):
        if chunk.frames is None:
            continue
        for old, new in zip(chunk.frames, chunk.frames[1:], strict=False):
            if old.connection_id == new.connection_id:
                continue
            kind = _boundary_kind(old, new)
            transitions.append(
                {
                    "kind": kind,
                    "boundary_kind": "intra_chunk",
                    "old_connection_id": old.connection_id,
                    "new_connection_id": new.connection_id,
                    "last_old_frame": {
                        "connection_id": old.connection_id,
                        "payload_sha256": old.payload_sha256,
                    },
                    "first_new_frame": {
                        "connection_id": new.connection_id,
                        "payload_sha256": new.payload_sha256,
                        "source_sequence": new.source_sequence,
                    },
                    "old_manifest": {
                        "gap": chunk.manifest["gap"],
                        "complete": chunk.manifest["complete"],
                    },
                    "new_manifest": {
                        "gap": chunk.manifest["gap"],
                        "complete": chunk.manifest["complete"],
                    },
                }
            )
        if index == 0 or not chunks[index - 1].frames:
            continue
        previous = chunks[index - 1]
        assert previous.frames is not None
        old, new = previous.frames[-1], chunk.frames[0]
        if old.connection_id == new.connection_id:
            continue
        kind = _boundary_kind(old, new)
        transitions.append(
            {
                "kind": kind,
                "boundary_kind": "inter_chunk",
                "old_connection_id": old.connection_id,
                "new_connection_id": new.connection_id,
                "last_old_frame": {
                    "connection_id": old.connection_id,
                    "payload_sha256": old.payload_sha256,
                },
                "first_new_frame": {
                    "connection_id": new.connection_id,
                    "payload_sha256": new.payload_sha256,
                    "source_sequence": new.source_sequence,
                },
                "old_manifest": {
                    "gap": previous.manifest["gap"],
                    "complete": previous.manifest["complete"],
                },
                "new_manifest": {
                    "gap": chunk.manifest["gap"],
                    "complete": chunk.manifest["complete"],
                },
            }
        )
    catalog_findings: list[str] = []
    catalog_path = data_root / "state" / "catalog.sqlite"
    if catalog_path.is_file():
        with Catalog(catalog_path, read_only=True) as catalog:
            if catalog.integrity_check() != ("ok",):
                raise SealError("Catalog integrity check failed")
            if catalog.malformed_discontinuity_events():
                catalog_findings.append("malformed_discontinuity_authority")
            if catalog.degraded_closed_discontinuity_pairs():
                catalog_findings.append("degraded_discontinuity_authority")
            catalog_rows = catalog.chunks_in_states(*tuple(ChunkState))
            inventory_members = inventory.get("members")
            if not isinstance(inventory_members, list):
                raise SealError("manifest inventory members are malformed")
            known_manifest_paths = {
                str(item["path"])
                for item in inventory_members
                if isinstance(item, dict) and isinstance(item.get("path"), str)
            }
            manifests_by_chunk = {
                str(chunk.manifest["chunk_id"]): chunk.manifest for chunk in chunks
            }
            for row in catalog_rows:
                row_market = row.get("market")
                row_stream = row.get("stream")
                if market is not None and row_market != market:
                    continue
                if stream is not None and row_stream != stream:
                    continue
                manifest_path = row.get("manifest_path")
                if isinstance(manifest_path, str) and manifest_path not in known_manifest_paths:
                    catalog_findings.append(f"catalog_manifest_disagreement:{row.get('chunk_id')}")
                manifest = manifests_by_chunk.get(str(row.get("chunk_id", "")))
                if manifest is None:
                    continue
                comparisons = {
                    "market": manifest["market"],
                    "stream": manifest["stream"],
                    "record_count": manifest["record_count"],
                    "sealed_path": manifest["relative_path"],
                    "stored_bytes": manifest["stored_bytes"],
                    "stored_sha256": manifest["stored_sha256"],
                    "uncompressed_bytes": manifest["uncompressed_bytes"],
                    "uncompressed_sha256": manifest["uncompressed_sha256"],
                }
                if any(row.get(key) != value for key, value in comparisons.items()):
                    catalog_findings.append(f"catalog_manifest_disagreement:{row.get('chunk_id')}")
    grouped = [{"market": market or "*", "stream": stream or "*", "transitions": transitions}]
    return {
        "schema_version": "m22.9-reconnect-audit.v1",
        "manifest_inventory": inventory,
        "catalog_findings": sorted(set(catalog_findings)),
        "streams": grouped,
        "summary": {
            "explicit_gap": sum(item["kind"] == EXPLICIT_SEQUENCE_GAP for item in transitions),
            "blue_green_overlap": sum(item["kind"] == BLUE_GREEN_OVERLAP for item in transitions),
            "unmarked_reconnect": sum(item["kind"] == UNMARKED_RECONNECT for item in transitions),
            "unknown": sum(item["kind"] == UNKNOWN for item in transitions),
        },
    }


__all__ = [
    "BLUE_GREEN_OVERLAP",
    "EXPLICIT_SEQUENCE_GAP",
    "UNKNOWN",
    "UNMARKED_RECONNECT",
    "audit_data_root",
    "strict_manifest_inventory",
]
