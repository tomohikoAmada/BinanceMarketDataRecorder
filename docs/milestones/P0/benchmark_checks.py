"""Checks for the finite P0 workload, independent of capture/runtime implementation."""

from __future__ import annotations

import hashlib
import math
import sqlite3
import struct
from collections import Counter
from itertools import pairwise
from pathlib import Path
from threading import RLock
from typing import Any

from binance_market_data_recorder.normalize.raw import SourceChunk, iter_source_records


def verify_physical_records(
    chunks: list[SourceChunk], catalog_path: Path, expected: dict[str, Any]
) -> tuple[dict[str, Any], dict[str, int], dict[str, int], list[dict[str, Any]]]:
    """Read physical ordinals in Catalog ACTIVE/SEALED lifecycle order, never frame-sort."""
    with sqlite3.connect(catalog_path) as connection:
        rows = connection.execute(
            "SELECT chunk_id, MIN(CASE WHEN to_state='ACTIVE' THEN transition_id END), "
            "MIN(CASE WHEN to_state='SEALED' THEN transition_id END) "
            "FROM chunk_transitions GROUP BY chunk_id"
        ).fetchall()
    lifecycle = {row[0]: (row[1], row[2]) for row in rows}
    for chunk in chunks:
        active, sealed = lifecycle.get(chunk.chunk_id, (None, None))
        if active is None or sealed is None or not active < sealed:
            raise ValueError(f"missing/invalid chunk lifecycle: {chunk.chunk_id}")
    ordered = sorted(chunks, key=lambda chunk: lifecycle[chunk.chunk_id][0])
    prior_seal: dict[tuple[str, ...], int] = {}
    prior_receive: dict[tuple[str, ...], int] = {}
    actual: dict[str, Any] = {}
    flags: Counter[str] = Counter()
    counts: Counter[str] = Counter()
    evidence = []
    for chunk in ordered:
        m = chunk.manifest
        stream = (str(m["market"]), str(m["symbol"]), str(m["stream"]))
        instances = m["collector_instance_ids"]
        if not isinstance(instances, list) or len(instances) != 1:
            raise ValueError("finite fixture requires one writer incarnation per chunk")
        owner = (*stream, str(instances[0]))
        active, sealed = lifecycle[chunk.chunk_id]
        if active <= prior_seal.get(owner, -1):
            raise ValueError(f"overlapping stream writer lifecycles: {owner}")
        prior_seal[owner] = sealed
        physical_count = 0
        for record in iter_source_records(chunk):
            if record.ordinal != physical_count:
                raise ValueError("physical ordinal discontinuity")
            physical_count += 1
            e = record.envelope
            label = "/".join(stream)
            counts[label] += 1
            flags.update(e.capture_flags)
            connection_identity = (*owner, e.connection_id)
            previous = prior_receive.get(connection_identity)
            if previous is not None and e.receive_monotonic_ns < previous:
                raise ValueError(
                    f"physical receive order inversion: {chunk.chunk_id}:{record.ordinal}"
                )
            prior_receive[connection_identity] = e.receive_monotonic_ns
            if label in expected:
                item = actual.setdefault(label, {"frames": 0, "digest": hashlib.sha256()})
                item["frames"] += 1
                item["digest"].update(struct.pack("!I", len(e.raw_payload)))
                item["digest"].update(e.raw_payload)
        if physical_count != m["record_count"]:
            raise ValueError("physical frame count differs from manifest")
        evidence.append(
            dict(
                chunk_id=chunk.chunk_id,
                active_transition_id=active,
                sealed_transition_id=sealed,
                physical_records=physical_count,
            )
        )
    actual = {
        k: {"frames": v["frames"], "sha256": v["digest"].hexdigest()} for k, v in actual.items()
    }
    if actual != expected or flags.get("malformed", 0):
        raise ValueError("physical Raw payload/order/count differs from fixed input")
    return actual, dict(flags), dict(counts), evidence


class WorkloadProgress:
    """Payload-byte accounting for this finite fixture; no production metrics changes."""

    def __init__(self, inputs: dict[Any, Any], intervals: dict[str, float]) -> None:
        self.lock = RLock()
        self.intervals = intervals
        self.sizes = {
            "/".join(key): [len(frame) for frame in frames] for key, frames in inputs.items()
        }
        self.values = {
            key: {
                phase + suffix: 0
                for phase in ("sent", "received", "enqueued", "written", "persisted")
                for suffix in ("_frames", "_payload_bytes")
            }
            for key in self.sizes
        }
        self.last_sent = dict.fromkeys(self.sizes, 0.0)
        self.writers: dict[str, tuple[str, int, int, int, int]] = {}

    def register_model_source(self, key: str) -> None:
        """Observe produced REST/snapshot models at Raw ingress, not a fake HTTP receive."""
        with self.lock:
            if key not in self.values:
                self.values[key] = dict.fromkeys(next(iter(self.values.values())), 0)
                self.last_sent[key] = 0.0

    def record(self, phase: str, key: str, size: int, elapsed: float = 0.0) -> None:
        with self.lock:
            self.values[key][phase + "_frames"] += 1
            self.values[key][phase + "_payload_bytes"] += size
            if phase == "sent":
                self.last_sent[key] = elapsed

    def written(self, writer: str, key: str, size: int) -> None:
        with self.lock:
            self.record("written", key, size)
            _, frames, total, synced_frames, synced_bytes = self.writers.get(
                writer, (key, 0, 0, 0, 0)
            )
            self.writers[writer] = key, frames + 1, total + size, synced_frames, synced_bytes

    def sync_target(self, writer: str) -> tuple[str, int, int] | None:
        with self.lock:
            row = self.writers.get(writer)
            return None if row is None else row[:3]

    def synced(self, writer: str, target: tuple[str, int, int] | None) -> None:
        if target is None:
            return
        with self.lock:
            key, frames, total = target
            _, written_frames, written_bytes, old_frames, old_bytes = self.writers[writer]
            if (
                not old_frames <= frames <= written_frames
                or not old_bytes <= total <= written_bytes
            ):
                raise ValueError("nonmonotonic fsync acknowledgement")
            self.values[key]["persisted_frames"] += frames - old_frames
            self.values[key]["persisted_payload_bytes"] += total - old_bytes
            self.writers[writer] = key, written_frames, written_bytes, frames, total

    def snapshot(self, elapsed: float) -> dict[str, Any]:
        with self.lock:
            streams = {}
            for key, values in self.values.items():
                sizes = self.sizes.get(key)
                due = (
                    min(
                        len(sizes),
                        max(0, math.floor(elapsed / self.intervals[key.split("/")[-1]]) + 1),
                    )
                    if sizes is not None
                    else 0
                )
                streams[key] = dict(
                    values,
                    protocol_kind="fixed_ws" if sizes is not None else "model_at_raw_ingress",
                    network_send_receive_observed=sizes is not None,
                    receipt_queue=None,
                    receipt_owned_payload_bytes=0,
                    spool_queue_depth=0,
                    protocol_due_frames=due if sizes is not None else None,
                    total_input_frames=len(sizes) if sizes is not None else None,
                    last_sent_elapsed=self.last_sent[key],
                    input_not_sent_frames=(
                        len(sizes) - values["sent_frames"] if sizes is not None else None
                    ),
                    due_not_sent_frames=max(0, due - values["sent_frames"]),
                    pending_receive_frames=values["sent_frames"] - values["received_frames"],
                    pending_write_frames=values["received_frames"] - values["written_frames"],
                    pending_write_payload_bytes=(
                        values["received_payload_bytes"] - values["written_payload_bytes"]
                    ),
                    spool_owned_frames=values["enqueued_frames"] - values["written_frames"],
                    spool_owned_payload_bytes=(
                        values["enqueued_payload_bytes"] - values["written_payload_bytes"]
                    ),
                    written_not_persisted_frames=values["written_frames"]
                    - values["persisted_frames"],
                    written_not_persisted_payload_bytes=(
                        values["written_payload_bytes"] - values["persisted_payload_bytes"]
                    ),
                )
            return {"elapsed": elapsed, "streams": streams}


def classify_steady(boundaries: list[dict[str, Any]], scheduled_end: float) -> dict[str, Any]:
    """Final drain correctness cannot retroactively turn a lagging window into steady."""
    reasons = []
    last = boundaries[-1]["streams"]
    for key, row in last.items():
        if row["protocol_kind"] != "fixed_ws":
            continue
        if (
            row["sent_frames"] != row["total_input_frames"]
            or row["last_sent_elapsed"] > scheduled_end
        ):
            reasons.append(f"input_not_completed_in_protocol:{key}")
    # At least three separated boundaries; count append and fsync backlog independently.
    for field in (
        "due_not_sent_frames",
        "pending_write_frames",
        "written_not_persisted_frames",
        "receipt_owned_payload_bytes",
        "spool_owned_payload_bytes",
    ):
        values = [sum(row[field] for row in point["streams"].values()) for point in boundaries]
        if (
            len(values) >= 3
            and values[-1] > values[0]
            and all(left <= right for left, right in pairwise(values))
        ):
            reasons.append(f"sustained_backlog_growth:{field}")
    return {
        "status": "NON_STEADY" if reasons else "STEADY",
        "performance_comparable": not reasons,
        "reasons": reasons,
        "rule": "input by deadline; no monotonic backlog growth across >=3 boundaries",
    }
