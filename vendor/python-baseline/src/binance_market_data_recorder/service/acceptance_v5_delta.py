"""Pure V5 cursor/causality replay, shared by producer and independent reader.

The reader reconstructs continuation from input rows; published cursor and
finding summaries are compared to this reconstruction, never used as inputs.
"""

from __future__ import annotations

import copy
import json
from collections.abc import Mapping
from typing import Any

from ..audit.reconnect_boundaries import WEBSOCKET_STREAMS
from ..storage.acceptance_delta import CURSORS, SQL_PAGE_CAP
from ..storage.catalog import ALLOWED_TRANSITIONS, ARCHIVE_CHUNK_STATES
from .acceptance import AcceptanceError, canonical_json, sha256_bytes
from .acceptance_v5_reconnect import advance_reconnect, stream_key

MAX_PENDING_CAUSAL_REFERENCES = 256
BOUNDED_BATCH_POLICY = {
    "version": "bounded-batches.v1",
    "sql_page_cap": SQL_PAGE_CAP,
    "max_pages_per_family": 4,
    "causal_reference_cap": 4 * SQL_PAGE_CAP,
    "max_canonical_delta_bytes": 7 * 1024 * 1024,
}
COMPACT_BATCH_POLICY = {
    **BOUNDED_BATCH_POLICY,
    "version": "bounded-batches.v2",
    "archive_companions": "shared-transaction.v1",
    "causal_reference_bound": "observation-final",
}
DELTA_WORK_BUDGET_NS = 240_000_000_000
FAMILIES = ("operational", "chunk", "archive")
BACKLOG_STATES = ("COPYING", "VERIFYING", "VERIFIED", "LOCAL_DELETE_PENDING")
ARCHIVE_STEPS = (
    ("reserve", "COPYING", "ARCHIVE_COPYING"),
    ("archive-verifying", "VERIFYING", "ARCHIVE_VERIFYING"),
    ("archive-verified", "VERIFIED", "ARCHIVED_VERIFIED"),
    ("local-delete-pending", "LOCAL_DELETE_PENDING", "LOCAL_DELETE_PENDING"),
    ("local-deleted", "LOCAL_DELETED", "LOCAL_DELETED"),
)


class DependencyPending(AcceptanceError):
    """Do not acknowledge the selected row; its cursor cannot skip it."""


def delta_limits(start: Mapping[str, Any]) -> tuple[int, int]:
    """Old starts retain one-page semantics; new policy is frozen at T0."""
    if "delta_policy" not in start:
        return 1, SQL_PAGE_CAP
    if start["delta_policy"] not in (BOUNDED_BATCH_POLICY, COMPACT_BATCH_POLICY):
        raise AcceptanceError("unsupported V5 delta policy")
    return 4, 4 * SQL_PAGE_CAP


def row_digest(row: Mapping[str, Any]) -> str:
    return sha256_bytes(canonical_json(dict(row)))


def cursor_id(family: str, row: Mapping[str, Any]) -> int:
    value = row.get(CURSORS[family][1])
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise AcceptanceError("malformed delta primary identity")
    return value


def empty_continuation(
    cursors: Mapping[str, int],
    *,
    products: list[list[str]] | None = None,
) -> dict[str, Any]:
    if set(cursors) != set(FAMILIES) or any(
        not isinstance(value, int) or isinstance(value, bool) or value < 0
        for value in cursors.values()
    ):
        raise AcceptanceError("invalid baseline cursor tuple")
    return {
        "processed": dict(cursors),
        "observed_high_water": dict(cursors),
        "pending_causal_references": [],
        "open_discontinuities": {},
        "reconnect": {},
        "rolling": {family: "0" * 64 for family in FAMILIES},
        "discontinuity_context": {},
        "integrity_findings": [],
        "archive_counts": {state: 0 for state in BACKLOG_STATES},
        "context_bound": sum(len(WEBSOCKET_STREAMS.get(product[0], ())) for product in products)
        if products
        else 256,
    }


def reference(
    family: str,
    row: Mapping[str, Any],
    *,
    transaction_id: str | None,
    chunk_id: str | None,
    source_family: str,
    source_id: int,
) -> dict[str, Any]:
    return {
        "family": family,
        "numeric_id": cursor_id(family, row),
        "primary_identity": row.get("event_id")
        if family == "operational"
        else row[CURSORS[family][1]],
        "canonical_row_sha256": row_digest(row),
        "transaction_id": transaction_id,
        "chunk_id": chunk_id,
        "state": row.get("to_state", row.get("event_type")),
        "source_family": source_family,
        "source_id": source_id,
    }


def verify_archive_lifecycle(
    bundle: Mapping[str, Any],
    high_water: Mapping[str, int],
) -> list[tuple[str, dict[str, Any]]]:
    transaction, chunk = bundle.get("transaction"), bundle.get("chunk")
    if not isinstance(transaction, dict) or not isinstance(chunk, dict):
        raise DependencyPending("missing archive chunk/transaction authority")
    tx_id, chunk_id = transaction.get("transaction_id"), transaction.get("chunk_id")
    if not isinstance(tx_id, str) or chunk.get("chunk_id") != chunk_id:
        raise AcceptanceError("archive chunk identity disagreement")
    states = [step[1] for step in ARCHIVE_STEPS]
    if transaction.get("state") not in states:
        raise AcceptanceError("illegal archive state")
    through = states.index(transaction["state"])
    if chunk.get("state") != str(ARCHIVE_CHUNK_STATES[transaction["state"]]):
        raise AcceptanceError("archive/chunk state disagreement")
    events, transitions = bundle.get("events"), bundle.get("transitions")
    if not isinstance(events, list) or not isinstance(transitions, list):
        raise AcceptanceError("malformed archive companions")
    if len(events) > 5 or len(transitions) > 5:
        raise AcceptanceError("archive causal fan-out exceeded")
    result: list[tuple[str, dict[str, Any]]] = []
    for index, (prefix, archive_state, chunk_state) in enumerate(ARCHIVE_STEPS):
        key = f"{prefix}:{tx_id}"
        chunk_key = f"archive-reserve:{tx_id}" if index == 0 else f"chunk:{key}"
        matching_event = [row for row in events if row.get("idempotency_key") == key]
        matching_chunk = [row for row in transitions if row.get("idempotency_key") == chunk_key]
        if index > through:
            if matching_event or matching_chunk:
                raise AcceptanceError("archive event beyond current state")
            continue
        if len(matching_event) != 1 or len(matching_chunk) != 1:
            raise DependencyPending("missing archive lifecycle step")
        event, transition = matching_event[0], matching_chunk[0]
        if (
            cursor_id("archive", event) > high_water["archive"]
            or cursor_id("chunk", transition) > high_water["chunk"]
        ):
            raise DependencyPending("archive companion beyond high-water")
        expected_archive_from = None if index == 0 else ARCHIVE_STEPS[index - 1][1]
        expected_chunk_from = "SEALED" if index == 0 else ARCHIVE_STEPS[index - 1][2]
        if (
            event.get("transaction_id") != tx_id
            or event.get("from_state") != expected_archive_from
            or event.get("to_state") != archive_state
            or transition.get("chunk_id") != chunk_id
            or transition.get("from_state") != expected_chunk_from
            or transition.get("to_state") != chunk_state
            or event.get("occurred_at_utc_ns") != transition.get("occurred_at_utc_ns")
        ):
            raise AcceptanceError("conflicting archive lifecycle step")
        result.extend((("archive", event), ("chunk", transition)))
    if len(events) != through + 1 or len(transitions) != through + 1:
        raise AcceptanceError("unrecognized/duplicate archive lifecycle authority")
    if bundle.get("target") is None:
        raise DependencyPending("unregistered archive target")
    return result


def _operational_body(row: Mapping[str, Any]) -> dict[str, Any]:
    try:
        body = json.loads(row["evidence_json"])
    except (ValueError, TypeError, KeyError) as exc:
        raise AcceptanceError("malformed operational event") from exc
    if not isinstance(body, dict):
        raise AcceptanceError("non-object operational event")
    return body


def _companions(
    family: str,
    row: Mapping[str, Any],
    companions: Mapping[str, Any],
    high_water: Mapping[str, int],
) -> list[tuple[str, dict[str, Any]]]:
    if family in {"chunk", "archive"}:
        archive = companions.get("archive")
        chunk = (
            companions.get("chunk")
            if family == "chunk"
            else (archive.get("chunk") if isinstance(archive, dict) else None)
        )
        if not isinstance(chunk, dict):
            raise DependencyPending("missing exact chunk row")
        if family == "chunk":
            if row.get("chunk_id") != chunk.get("chunk_id"):
                raise AcceptanceError("chunk companion identity disagreement")
            before, after = row.get("from_state"), row.get("to_state")
            if before is None:
                if after not in {"ACTIVE", "SEALING"}:
                    raise AcceptanceError("illegal initial chunk transition")
            elif before != after and after not in ALLOWED_TRANSITIONS.get(before, set()):
                raise AcceptanceError("illegal chunk transition")
        if archive is not None:
            referenced = verify_archive_lifecycle(archive, high_water)
            if family == "archive" and not any(
                item_family == family and item_row == row for item_family, item_row in referenced
            ):
                raise AcceptanceError("selected archive event is not a legal lifecycle step")
            if (
                family == "chunk"
                and row.get("to_state") in set(ARCHIVE_CHUNK_STATES.values())
                and not any(
                    item_family == family and item_row == row
                    for item_family, item_row in referenced
                )
            ):
                raise AcceptanceError("selected archive chunk transition is not corroborated")
            return referenced
        if family == "archive" or row.get("to_state") in set(ARCHIVE_CHUNK_STATES.values()):
            raise DependencyPending("missing archive transaction")
        return []
    _operational_body(row)
    event_type = row.get("event_type")
    if event_type not in {"STREAM_DISCONTINUITY_STARTED", "STREAM_DISCONTINUITY_COMPLETED"}:
        return []
    pair = companions.get("discontinuity")
    if not isinstance(pair, list) or not 1 <= len(pair) <= 2:
        raise DependencyPending("missing discontinuity pair")
    started = [item for item in pair if item.get("event_type") == "STREAM_DISCONTINUITY_STARTED"]
    completed = [
        item for item in pair if item.get("event_type") == "STREAM_DISCONTINUITY_COMPLETED"
    ]
    if len(started) != 1 or len(completed) > 1:
        raise AcceptanceError("ambiguous discontinuity pair")
    if event_type == "STREAM_DISCONTINUITY_COMPLETED" and len(completed) != 1:
        raise DependencyPending("missing discontinuity start")
    if not any(item == row for item in pair):
        raise AcceptanceError("selected discontinuity event is absent from pair")
    for item in pair:
        if cursor_id("operational", item) > high_water["operational"]:
            raise DependencyPending("discontinuity companion beyond high-water")
        if any(item.get(key) != row.get(key) for key in ("market", "symbol", "stream", "gap_id")):
            raise AcceptanceError("discontinuity pair identity disagreement")
        body = _operational_body(item)
        if any(
            body.get(key) != item.get(key) or not isinstance(body.get(key), str) or not body[key]
            for key in ("market", "symbol", "stream", "gap_id")
        ):
            raise AcceptanceError("discontinuity body/row identity disagreement")
        prefix = "original" if item["event_type"] == "STREAM_DISCONTINUITY_STARTED" else "new"
        instant_key = "gap_started_at_utc_ns" if prefix == "original" else "gap_ended_at_utc_ns"
        instant, generation, connection = (
            body.get(instant_key),
            body.get(f"{prefix}_generation"),
            body.get(f"{prefix}_connection_id"),
        )
        if (
            not isinstance(instant, int)
            or isinstance(instant, bool)
            or not isinstance(generation, int)
            or isinstance(generation, bool)
            or generation < 0
            or not isinstance(connection, str)
            or not connection
        ):
            raise AcceptanceError("malformed discontinuity time/connection authority")
    if completed and completed[0]["occurred_at_utc_ns"] < started[0]["occurred_at_utc_ns"]:
        raise AcceptanceError("discontinuity interval regressed")
    return [("operational", item) for item in pair]


def replay_delta(
    prior: Mapping[str, Any],
    high_water: Mapping[str, int],
    pages: Mapping[str, list[dict[str, Any]]],
    *,
    t0_utc_ns: int | None = None,
    row_cap: int = SQL_PAGE_CAP,
    causal_cap_at_end: bool = False,
) -> dict[str, Any]:
    """Replay bounded acknowledged rows plus explicit first pending row per family.

    A row has row/companions/unit/status. Status is reconstructed: pending
    dependencies cannot be turned into acknowledgement by a producer summary.
    Raw unit proof is checked independently against terminal exact Raw replay.
    """
    if row_cap not in {SQL_PAGE_CAP, 4 * SQL_PAGE_CAP}:
        raise AcceptanceError("unsupported delta row cap")
    state = copy.deepcopy(dict(prior))
    if set(high_water) != set(FAMILIES) or set(pages) != set(FAMILIES):
        raise AcceptanceError("invalid delta families")
    if any(
        not isinstance(value, int) or isinstance(value, bool) or value < 0
        for value in high_water.values()
    ):
        raise AcceptanceError("malformed delta high-water")
    for family in FAMILIES:
        if high_water[family] < state["observed_high_water"][family]:
            raise AcceptanceError("high-water regression")
        if len(pages[family]) > row_cap:
            raise AcceptanceError("SQL page cap exceeded")
        last = state["processed"][family]
        for index, entry in enumerate(pages[family]):
            row, companions = entry.get("row"), entry.get("companions")
            if not isinstance(row, dict) or not isinstance(companions, dict):
                raise AcceptanceError("invalid cursor row/companions")
            numeric_id = cursor_id(family, row)
            if not last < numeric_id <= high_water[family]:
                raise AcceptanceError("cursor row out of order/boundary")
            pending_reason: str | None = None
            try:
                referenced = _companions(family, row, companions, high_water)
            except DependencyPending as exc:
                referenced = []
                pending_reason = str(exc)
            needs_raw = family == "chunk" and row.get("to_state") == "SEALED"
            unit = entry.get("unit")
            if needs_raw and unit is None:
                pending_reason = pending_reason or "raw_unit_pending"
            if entry.get("status") == "budget_pending":
                pending_reason = pending_reason or "delta_work_budget_exhausted"
            if pending_reason:
                if (
                    entry.get("status") not in {"pending", "budget_pending"}
                    or index != len(pages[family]) - 1
                ):
                    raise AcceptanceError("unverified dependency skipped/acknowledged")
                break
            if entry.get("status") != "acknowledged":
                raise AcceptanceError("unsupported delta status")
            if family == "archive":
                before, after = row["from_state"], row["to_state"]
                if before in state["archive_counts"]:
                    state["archive_counts"][before] -= 1
                    if state["archive_counts"][before] < 0:
                        raise AcceptanceError(
                            "archive aggregate regressed below baseline authority"
                        )
                if after in state["archive_counts"]:
                    state["archive_counts"][after] += 1
            if family == "operational":
                event_type = row["event_type"]
                if event_type.startswith("STREAM_DISCONTINUITY_"):
                    key = stream_key(row)
                    pair = companions["discontinuity"]
                    state["discontinuity_context"][key] = pair
                    if event_type == "STREAM_DISCONTINUITY_STARTED":
                        existing_open = state["open_discontinuities"].get(key)
                        if existing_open is not None and existing_open["gap_id"] != row["gap_id"]:
                            raise AcceptanceError("overlapping open discontinuities")
                        state["open_discontinuities"][key] = row
                    else:
                        state["open_discontinuities"].pop(key, None)
                    if len(state["discontinuity_context"]) > state["context_bound"]:
                        raise AcceptanceError("discontinuity topology bound exceeded")
                if (
                    t0_utc_ns is not None
                    and row["occurred_at_utc_ns"] >= t0_utc_ns
                    and event_type
                    in {
                        "SERVICE_FAILED",
                        "SERVICE_STOPPED",
                        "CORE_MARKET_TERMINAL_FAILURE",
                        "DISK_EMERGENCY_STOP",
                    }
                ):
                    state["integrity_findings"] = sorted(
                        set(state["integrity_findings"]) | {f"terminal_event:{event_type}"}
                    )
            if needs_raw:
                if not isinstance(unit, dict) or unit["manifest"]["chunk_id"] != row["chunk_id"]:
                    raise AcceptanceError("new manifest/transition identity differs")
                manifest = unit["manifest"]
                reconnect_findings, _boundary = advance_reconnect(
                    state["reconnect"],
                    manifest,
                    unit["raw"],
                    discontinuity_rows=state["discontinuity_context"].get(stream_key(manifest), []),
                    t0_utc_ns=t0_utc_ns,
                    context_bound=state["context_bound"],
                )
                state["integrity_findings"] = sorted(
                    set(state["integrity_findings"]) | set(reconnect_findings)
                )
            tx = companions.get("archive")
            transaction_id = tx["transaction"]["transaction_id"] if tx else None
            chunk_id = row.get("chunk_id") or (tx["transaction"]["chunk_id"] if tx else None)
            refs = state["pending_causal_references"]
            selected_ref = reference(
                family,
                row,
                transaction_id=transaction_id,
                chunk_id=chunk_id,
                source_family=family,
                source_id=numeric_id,
            )
            for bound in refs:
                if (
                    bound["family"] == family
                    and bound["numeric_id"] == numeric_id
                    and any(
                        bound[key] != selected_ref[key]
                        for key in (
                            "primary_identity",
                            "canonical_row_sha256",
                            "transaction_id",
                            "chunk_id",
                            "state",
                        )
                    )
                ):
                    raise AcceptanceError("cross-cursor causal replay mismatch")
            refs[:] = [
                bound
                for bound in refs
                if not (bound["family"] == family and bound["numeric_id"] == numeric_id)
            ]
            for other_family, other_row in referenced:
                other_id = cursor_id(other_family, other_row)
                if other_id <= state["processed"][other_family] or (
                    other_family == family and other_id == numeric_id
                ):
                    continue
                bound = reference(
                    other_family,
                    other_row,
                    transaction_id=transaction_id,
                    chunk_id=chunk_id,
                    source_family=family,
                    source_id=numeric_id,
                )
                existing = [
                    item
                    for item in refs
                    if item["family"] == other_family and item["numeric_id"] == other_id
                ]
                if existing:
                    if any(
                        existing[0][key] != bound[key]
                        for key in (
                            "primary_identity",
                            "canonical_row_sha256",
                            "transaction_id",
                            "chunk_id",
                            "state",
                        )
                    ):
                        raise AcceptanceError("conflicting causal reference")
                else:
                    refs.append(bound)
                if not causal_cap_at_end and len(refs) > row_cap:
                    raise AcceptanceError("pending causal reference cap exceeded")
            state["rolling"][family] = sha256_bytes(
                bytes.fromhex(state["rolling"][family]) + bytes.fromhex(row_digest(row))
            )
            last = numeric_id
            state["processed"][family] = last
    # The v2 cap applies to persisted continuation. Intra-observation references
    # are still finite: <= three bounded row batches times fixed companion fan-out.
    # Later families must corroborate/discharge them, never silently discard them.
    if len(state["pending_causal_references"]) > row_cap:
        raise AcceptanceError("pending causal reference cap exceeded")
    state["observed_high_water"] = dict(high_water)
    state["pending_causal_references"].sort(key=lambda item: (item["family"], item["numeric_id"]))
    return state
