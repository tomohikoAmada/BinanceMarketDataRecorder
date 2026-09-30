"""Constant-state reconnect projection from exact streamed Raw summaries."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from ..audit.reconnect_boundaries import (
    NONE,
    UNKNOWN,
    UNMARKED_RECONNECT,
    WEBSOCKET_STREAMS,
    ChunkScan,
    _catalog_gap_match,
    _catalog_intervals,
    _classify_inter_chunk,
    _frame_from_document,
)
from .acceptance import AcceptanceError


def stream_key(manifest: Mapping[str, Any]) -> str:
    return f"{manifest['market']}:{manifest['symbol']}:{manifest['stream']}"


def manifest_projection(manifest: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: manifest[key]
        for key in (
            "chunk_id",
            "market",
            "symbol",
            "stream",
            "created_at_utc_ns",
            "record_count",
            "connection_ids",
            "capture_flags",
            "gap",
            "overlap",
            "complete",
        )
    }


def advance_reconnect(
    contexts: dict[str, Any],
    manifest: Mapping[str, Any],
    proof: Mapping[str, Any],
    *,
    discontinuity_rows: list[dict[str, Any]],
    t0_utc_ns: int | None,
    context_bound: int,
) -> tuple[list[str], dict[str, Any]]:
    market, stream = manifest["market"], manifest["stream"]
    if stream not in WEBSOCKET_STREAMS.get(market, frozenset()):
        return [], {"kind": "NOT_WEBSOCKET", "occurred_at_utc_ns": None}
    raw = proof.get("local") or proof.get("archive")
    if not isinstance(raw, dict):
        raise AcceptanceError("reconnect Raw summary is unavailable")
    key = stream_key(manifest)
    context = contexts.get(key)
    findings = []
    for kind in (UNKNOWN, UNMARKED_RECONNECT):
        latest = raw["intra_transition_time_ranges"][kind]["max"]
        if latest is not None and t0_utc_ns is not None and latest >= t0_utc_ns:
            findings.append("unknown_reconnect" if kind == UNKNOWN else "unmarked_reconnect")
    first, last = raw["first_frame"], raw["last_frame"]
    boundary: dict[str, Any] = {"kind": "NO_CONNECTION_CHANGE", "occurred_at_utc_ns": None}
    if first is None:
        if context is not None:
            flags = set(context["intervening_flags"]) | set(manifest["capture_flags"])
            # Only boundary classification flags survive; no marker list grows with N.
            context["intervening_flags"] = sorted(
                flags
                & {
                    "reconnect_gap",
                    "sequence_gap",
                    "blue_green_overlap",
                }
            )
            context["intervening_gap"] = context["intervening_gap"] or manifest["gap"]
        return findings, {"kind": "ZERO_RECORD_MARKER", "occurred_at_utc_ns": None}
    new_frame = _frame_from_document(first)
    if context is not None:
        old_frame = _frame_from_document(context["last_frame"])
        if old_frame.connection_id != new_frame.connection_id:
            old_frames = [old_frame]
            if context["penultimate_frame"] is not None:
                old_frames.insert(0, _frame_from_document(context["penultimate_frame"]))
            events = [
                {**row, "evidence": json.loads(row["evidence_json"])} for row in discontinuity_rows
            ]
            intervals = _catalog_intervals(events)
            _match, identity_kind, gap_id = (
                _catalog_gap_match(
                    intervals,
                    market,
                    manifest["symbol"],
                    stream,
                    new_frame.receive_time_utc_ns,
                    old_frame.connection_id,
                    new_frame.connection_id,
                )
                if intervals
                else ("UNMATCHED", NONE, None)
            )
            intervening = []
            if context["intervening_flags"] or context["intervening_gap"]:
                intervening.append(
                    ChunkScan(
                        manifest={
                            "capture_flags": context["intervening_flags"],
                            "gap": context["intervening_gap"],
                        }
                    )
                )
            kind = _classify_inter_chunk(
                ChunkScan(manifest=context["manifest"]),
                ChunkScan(manifest=dict(manifest)),
                old_frame,
                new_frame,
                catalog_identity_kind=identity_kind,
                catalog_matched_gap_id=gap_id,
                old_frames=old_frames,
                intervening_chunks=intervening,
            )
            boundary = {
                "kind": kind,
                "occurred_at_utc_ns": new_frame.receive_time_utc_ns,
                "old_chunk_id": old_frame.chunk_id,
                "new_chunk_id": new_frame.chunk_id,
                "catalog_identity_kind": identity_kind,
                "catalog_gap_id": gap_id,
            }
            if t0_utc_ns is not None and new_frame.receive_time_utc_ns >= t0_utc_ns:
                if kind == UNMARKED_RECONNECT:
                    findings.append("unmarked_reconnect")
                elif kind == UNKNOWN:
                    findings.append("unknown_reconnect")
    contexts[key] = {
        "manifest": manifest_projection(manifest),
        "last_frame": last,
        "penultimate_frame": raw["penultimate_frame"],
        "intervening_flags": [],
        "intervening_gap": False,
    }
    if len(contexts) > context_bound:
        raise AcceptanceError("reconnect topology bound exceeded")
    return findings, boundary
