"""Source availability intervals, watermarks, discontinuities, and operational events."""

from __future__ import annotations

import json
import sqlite3
import time
from collections.abc import Mapping, Sequence
from typing import Any, cast

from .base import CatalogBase
from .types import LEGACY_SINGLE_SYMBOL, CatalogStateError

_DISCONTINUITY_EVENT_TYPES = (
    "STREAM_DISCONTINUITY_STARTED",
    "STREAM_DISCONTINUITY_COMPLETED",
)

_SYMBOL_SPECIFIC_SIDE_DATA_EVENT_TYPES = (
    "SIDE_DATA_EMPTY_RESPONSE",
    "SIDE_DATA_UNRECOVERABLE_GAP",
    "SIDE_DATA_RANGE_EVALUATED",
)


def stream_discontinuity_event_id(
    *, event_type: str, market: str, symbol: str, stream: str, gap_id: str
) -> str:
    """Build the idempotency key for one symbol-aware lifecycle event."""

    if event_type not in _DISCONTINUITY_EVENT_TYPES:
        raise ValueError(f"unsupported discontinuity event type: {event_type}")
    if not all(isinstance(value, str) and value for value in (market, symbol, stream, gap_id)):
        raise ValueError("discontinuity event identity must be non-empty text")
    suffix = "started" if event_type == "STREAM_DISCONTINUITY_STARTED" else "completed"
    return f"stream-discontinuity-{suffix}:{market}:{symbol}:{stream}:{gap_id}"


def _closed_lifecycle_identity_valid(
    started_evidence: dict[str, object],
    completed_evidence: dict[str, object],
) -> bool:
    """True when a paired CLOSED lifecycle has well-formed identity fields.

    Wall timestamps are NOT part of identity validity: an inverted or equal
    wall pair is still exact lifecycle authority (M21.4.11-R3.2).
    """
    started_at = started_evidence.get("gap_started_at_utc_ns")
    ended_at = completed_evidence.get("gap_ended_at_utc_ns")
    original_connection = started_evidence.get("original_connection_id")
    original_generation = started_evidence.get("original_generation")
    new_connection = completed_evidence.get("new_connection_id")
    new_generation = completed_evidence.get("new_generation")
    return (
        isinstance(started_at, int)
        and not isinstance(started_at, bool)
        and isinstance(ended_at, int)
        and not isinstance(ended_at, bool)
        and isinstance(original_connection, str)
        and bool(original_connection)
        and not isinstance(original_generation, bool)
        and isinstance(original_generation, int)
        and original_generation >= 0
        and isinstance(new_connection, str)
        and bool(new_connection)
        and not isinstance(new_generation, bool)
        and isinstance(new_generation, int)
        and new_generation >= 0
    )


def _discontinuity_identity(
    evidence: object, *, allow_legacy_symbol: bool
) -> tuple[str, str, str, str] | None:
    """Extract the complete lifecycle key from one event evidence object."""

    if not isinstance(evidence, dict):
        return None
    market = evidence.get("market")
    stream = evidence.get("stream")
    gap_id = evidence.get("gap_id")
    if "symbol" not in evidence:
        symbol: object = LEGACY_SINGLE_SYMBOL if allow_legacy_symbol else None
    else:
        symbol = evidence.get("symbol")
    if not all(
        isinstance(value, str) and bool(value) for value in (market, symbol, stream, gap_id)
    ):
        return None
    return cast(tuple[str, str, str, str], (market, symbol, stream, gap_id))


def _optional_identity_text(value: object) -> str | None:
    """Return one valid identity field without rewriting malformed evidence."""

    if isinstance(value, str) and value:
        return value
    return None


class CatalogAvailabilityMixin(CatalogBase):
    """Operational events, availability intervals, shared watermarks, and side-data cursors."""

    def record_operational_event(
        self,
        *,
        event_id: str,
        event_type: str,
        occurred_at_utc_ns: int,
        evidence: Mapping[str, object],
        symbol: str | None = None,
    ) -> bool:
        self._require_writable()
        if not event_id or not event_type or occurred_at_utc_ns < 0:
            raise ValueError("invalid operational event")
        evidence_document = dict(evidence)
        identity_values: tuple[str | None, str | None, str | None, str | None]
        if event_type in (
            *_DISCONTINUITY_EVENT_TYPES,
            *_SYMBOL_SPECIFIC_SIDE_DATA_EVENT_TYPES,
        ):
            if not isinstance(symbol, str) or not symbol:
                raise ValueError("symbol must be explicit for symbol-specific events")
            evidence_symbol = evidence_document.get("symbol")
            if evidence_symbol is not None and (
                not isinstance(evidence_symbol, str)
                or not evidence_symbol
                or evidence_symbol != symbol
            ):
                raise ValueError("symbol-specific event evidence symbol is invalid")
            evidence_document["symbol"] = symbol
            if event_type in _DISCONTINUITY_EVENT_TYPES:
                identity_values = (
                    _optional_identity_text(evidence_document.get("market")),
                    symbol,
                    _optional_identity_text(evidence_document.get("stream")),
                    _optional_identity_text(evidence_document.get("gap_id")),
                )
            else:
                identity_values = (None, None, None, None)
        else:
            identity_values = (None, None, None, None)
        body = json.dumps(evidence_document, sort_keys=True, separators=(",", ":"))
        with self._lock:
            try:
                cursor = self._connection.execute(
                    """
                    INSERT OR IGNORE INTO operational_events(
                        event_id, event_type, occurred_at_utc_ns, evidence_json,
                        market, symbol, stream, gap_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event_id,
                        event_type,
                        occurred_at_utc_ns,
                        body,
                        identity_values[0],
                        identity_values[1],
                        identity_values[2],
                        identity_values[3],
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise CatalogStateError(
                    f"operational event identity is not valid: {event_id}"
                ) from exc
            inserted = cursor.rowcount == 1
            if not inserted:
                existing = self._connection.execute(
                    """
                    SELECT event_type, occurred_at_utc_ns, evidence_json
                    FROM operational_events WHERE event_id = ?
                    """,
                    (event_id,),
                ).fetchone()
                if existing is None or (
                    str(existing["event_type"]) != event_type
                    or int(existing["occurred_at_utc_ns"]) != occurred_at_utc_ns
                    or str(existing["evidence_json"]) != body
                ):
                    raise CatalogStateError(f"operational event identity conflict: {event_id}")
        return inserted

    def ensure_operational_event(
        self,
        *,
        event_id: str,
        event_type: str,
        occurred_at_utc_ns: int,
        evidence: Mapping[str, object],
        symbol: str | None = None,
    ) -> bool:
        """Insert an event or prove an existing event is byte-semantically exact.

        Reconnect lifecycle callers may treat idempotent replay as success only
        after this check.  The explicit readback also protects recovery from a
        false/ignored insert result supplied by a fault-injection wrapper: no
        materialization action is reported unless the requested event is
        actually durable with identical type, timestamp, and evidence.
        """

        inserted = self.record_operational_event(
            event_id=event_id,
            event_type=event_type,
            occurred_at_utc_ns=occurred_at_utc_ns,
            evidence=evidence,
            symbol=symbol,
        )
        if inserted:
            return True
        expected_evidence = dict(evidence)
        if event_type in (
            *_DISCONTINUITY_EVENT_TYPES,
            *_SYMBOL_SPECIFIC_SIDE_DATA_EVENT_TYPES,
        ):
            # Match record_operational_event's explicit-symbol normalization on
            # idempotent replay, including callers that omitted the redundant
            # evidence field while supplying the required API argument.
            expected_evidence["symbol"] = symbol
        expected_body = json.dumps(expected_evidence, sort_keys=True, separators=(",", ":"))
        with self._lock:
            existing = self._connection.execute(
                """
                SELECT event_type, occurred_at_utc_ns, evidence_json
                FROM operational_events WHERE event_id = ?
                """,
                (event_id,),
            ).fetchone()
        if existing is None or (
            str(existing["event_type"]) != event_type
            or int(existing["occurred_at_utc_ns"]) != occurred_at_utc_ns
            or str(existing["evidence_json"]) != expected_body
        ):
            raise CatalogStateError(f"operational event was not durably validated: {event_id}")
        return False

    def operational_events(self, *, event_type: str | None = None) -> list[dict[str, object]]:
        query = (
            "SELECT event_id, event_type, occurred_at_utc_ns, evidence_json FROM operational_events"
        )
        parameters: tuple[object, ...] = ()
        if event_type is not None:
            query += " WHERE event_type = ?"
            parameters = (event_type,)
        query += " ORDER BY occurred_at_utc_ns, event_id"
        with self._lock:
            rows = self._connection.execute(query, parameters).fetchall()
        output: list[dict[str, object]] = []
        for row in rows:
            document = dict(row)
            evidence_json = document.pop("evidence_json")
            document["evidence"] = json.loads(str(evidence_json))
            output.append(document)
        return output

    def operational_event_by_id(self, event_id: str) -> dict[str, object] | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT event_id, event_type, occurred_at_utc_ns, evidence_json "
                "FROM operational_events WHERE event_id=?",
                (event_id,),
            ).fetchone()
        if row is None:
            return None
        return {
            "event_id": row["event_id"],
            "event_type": row["event_type"],
            "occurred_at_utc_ns": row["occurred_at_utc_ns"],
            "evidence": json.loads(row["evidence_json"]),
        }

    def discontinuity_authority_snapshot(self, *, as_of_utc_ns: int) -> dict[str, object]:
        """Return one coherent, read-only discontinuity authority snapshot.

        The bounded event read is one SQLite statement.  All lifecycle pairing,
        malformed/degraded validation, and terminal-event selection is then
        derived from that immutable row set, so a writer cannot commit between
        the open and closed queries of one Acceptance observation.  The read
        lock is released before this in-memory derivation and never spans any
        filesystem or Raw scan.
        """
        if isinstance(as_of_utc_ns, bool) or not isinstance(as_of_utc_ns, int) or as_of_utc_ns < 0:
            raise ValueError("as_of_utc_ns must be a non-negative integer")
        rows = self._read_discontinuity_event_rows(as_of_utc_ns=as_of_utc_ns)
        authority = self._discontinuity_authority_from_rows(rows)
        authority["as_of_utc_ns"] = as_of_utc_ns
        return authority

    def _read_discontinuity_event_rows(
        self, *, as_of_utc_ns: int | None = None
    ) -> list[sqlite3.Row]:
        if as_of_utc_ns is not None and (
            isinstance(as_of_utc_ns, bool) or not isinstance(as_of_utc_ns, int) or as_of_utc_ns < 0
        ):
            raise ValueError("as_of_utc_ns must be a non-negative integer")
        query = "SELECT * FROM operational_events"
        parameters: tuple[object, ...] = ()
        if as_of_utc_ns is not None:
            query += " WHERE occurred_at_utc_ns <= ?"
            parameters = (as_of_utc_ns,)
        query += " ORDER BY occurred_at_utc_ns, event_id"
        with self._lock:
            return self._connection.execute(query, parameters).fetchall()

    def _discontinuity_authority_from_rows(self, rows: Sequence[sqlite3.Row]) -> dict[str, object]:
        event_documents: list[dict[str, object]] = []
        lifecycle_rows: list[tuple[dict[str, object], object, bool]] = []
        for row in rows:
            row_document = dict(row)
            event_type = str(row_document["event_type"])
            parse_failed = False
            try:
                evidence = json.loads(str(row_document["evidence_json"]))
            except (TypeError, json.JSONDecodeError):
                evidence = None
                parse_failed = True
            row_document["evidence"] = evidence
            event_documents.append(
                {
                    "event_id": str(row_document["event_id"]),
                    "event_type": event_type,
                    "occurred_at_utc_ns": int(row_document["occurred_at_utc_ns"]),
                    "evidence": evidence,
                }
            )
            if event_type in _DISCONTINUITY_EVENT_TYPES:
                lifecycle_rows.append((row_document, evidence, parse_failed))

        malformed: list[dict[str, object]] = []
        valid_rows: list[
            tuple[
                dict[str, object],
                dict[str, object],
                tuple[str, str, str, str],
            ]
        ] = []
        for row_document, evidence, parse_failed in lifecycle_rows:
            event_id = str(row_document["event_id"])
            event_type = str(row_document["event_type"])
            reason: str | None = None
            if parse_failed:
                reason = "evidence_not_json"
            elif not isinstance(evidence, dict):
                reason = "evidence_not_object"
            else:
                market = evidence.get("market")
                symbol = evidence.get("symbol")
                stream = evidence.get("stream")
                gap_id = evidence.get("gap_id")
                if not isinstance(market, str) or not market:
                    reason = "missing_market"
                elif not isinstance(symbol, str) or not symbol:
                    if not (self._legacy_identity_schema and "symbol" not in evidence):
                        reason = "missing_symbol"
                elif not isinstance(stream, str) or not stream:
                    reason = "missing_stream"
                elif not isinstance(gap_id, str) or not gap_id:
                    reason = "missing_gap_id"
            if reason is not None:
                malformed.append(
                    {
                        "event_id": event_id,
                        "event_type": event_type,
                        "reason": reason,
                    }
                )
                continue
            assert isinstance(evidence, dict)
            evidence_document = cast(dict[str, object], evidence)
            identity = _discontinuity_identity(
                evidence_document,
                allow_legacy_symbol=self._legacy_identity_schema,
            )
            if identity is not None:
                valid_rows.append((row_document, evidence_document, identity))
        malformed.sort(key=lambda item: str(item["event_id"]))

        started_by_gap: dict[
            tuple[str, str, str, str], tuple[dict[str, object], dict[str, object]]
        ] = {}
        completed_by_gap: dict[
            tuple[str, str, str, str], tuple[dict[str, object], dict[str, object]]
        ] = {}
        for row_document, evidence, identity in valid_rows:
            if str(row_document["event_type"]) == "STREAM_DISCONTINUITY_COMPLETED":
                completed_by_gap[identity] = (row_document, evidence)
            else:
                started_by_gap[identity] = (row_document, evidence)

        degraded: list[dict[str, object]] = []
        for key in sorted(set(started_by_gap) & set(completed_by_gap)):
            started_evidence = started_by_gap[key][1]
            completed_evidence = completed_by_gap[key][1]
            if _closed_lifecycle_identity_valid(started_evidence, completed_evidence):
                continue
            market, symbol, stream, gap_id = key
            degraded.append(
                {
                    "market": market,
                    "symbol": symbol,
                    "stream": stream,
                    "gap_id": gap_id,
                    "reason": "malformed_lifecycle_identity",
                }
            )

        closed: dict[tuple[str, str, str], list[dict[str, object]]] = {}
        for (market, symbol, stream, gap_id), (
            _started_row,
            started_evidence,
        ) in sorted(started_by_gap.items()):
            completed = completed_by_gap.get((market, symbol, stream, gap_id))
            if completed is None:
                continue
            _completed_row, completed_evidence = completed
            if not _closed_lifecycle_identity_valid(started_evidence, completed_evidence):
                continue
            started_at = cast(int, started_evidence["gap_started_at_utc_ns"])
            ended_at = cast(int, completed_evidence["gap_ended_at_utc_ns"])
            closed.setdefault((market, symbol, stream), []).append(
                {
                    "market": market,
                    "symbol": symbol,
                    "stream": stream,
                    "gap_id": gap_id,
                    "started_at_utc_ns": started_at,
                    "ended_at_utc_ns": ended_at,
                    "wall_time_order": ("NORMAL" if ended_at > started_at else "NON_MONOTONIC"),
                    "original_connection_id": started_evidence["original_connection_id"],
                    "original_generation": started_evidence["original_generation"],
                    "new_connection_id": completed_evidence["new_connection_id"],
                    "new_generation": completed_evidence["new_generation"],
                }
            )

        completed_gap_ids = set(completed_by_gap)
        unclosed: dict[tuple[str, str, str], list[dict[str, object]]] = {}
        for row_document, _evidence, identity in valid_rows:
            if str(row_document["event_type"]) == "STREAM_DISCONTINUITY_COMPLETED":
                continue
            if identity in completed_gap_ids:
                continue
            market, symbol, stream, _gap_id = identity
            document = dict(row_document)
            document.pop("evidence_json", None)
            unclosed.setdefault((market, symbol, stream), []).append(document)

        terminal_types = {
            "SERVICE_FAILED",
            "SERVICE_STOPPED",
            "CORE_MARKET_TERMINAL_FAILURE",
        }
        terminal_events = [
            dict(event)
            for event in event_documents
            if str(event.get("event_type")) in terminal_types
        ]
        return {
            "operational_events": event_documents,
            "terminal_events": terminal_events,
            "malformed_events": malformed,
            "degraded_pairs": degraded,
            "unclosed": unclosed,
            "closed": closed,
        }

    def unclosed_stream_discontinuities(
        self, *, market: str, symbol: str, stream: str
    ) -> list[dict[str, object]]:
        """Return STARTED evidence for one symbol without a matching completion."""

        if not market or not symbol or not stream:
            raise ValueError("market, symbol and stream must be non-empty")
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT * FROM operational_events
                WHERE event_type IN (
                    'STREAM_DISCONTINUITY_STARTED',
                    'STREAM_DISCONTINUITY_COMPLETED'
                )
                ORDER BY occurred_at_utc_ns, event_id
                """
            ).fetchall()
        started: list[dict[str, object]] = []
        completed_gap_ids: set[tuple[str, str, str, str]] = set()
        for row in rows:
            document = dict(row)
            evidence_json = document.pop("evidence_json")
            try:
                evidence = json.loads(str(evidence_json))
            except (TypeError, json.JSONDecodeError):
                continue
            if not isinstance(evidence, dict):
                continue
            identity = _discontinuity_identity(
                evidence, allow_legacy_symbol=self._legacy_identity_schema
            )
            if identity is None or identity[:3] != (market, symbol, stream):
                continue
            document["evidence"] = evidence
            if document["event_type"] == "STREAM_DISCONTINUITY_COMPLETED":
                completed_gap_ids.add(identity)
            else:
                started.append(document)
        return [
            event
            for event in started
            if not (
                isinstance(event["evidence"], dict)
                and _discontinuity_identity(
                    event["evidence"],
                    allow_legacy_symbol=self._legacy_identity_schema,
                )
                in completed_gap_ids
            )
        ]

    def closed_stream_discontinuity_intervals_by_stream(
        self,
    ) -> dict[tuple[str, str, str], list[dict[str, object]]]:
        """Return paired intervals grouped by (market, symbol, stream).

        Each interval is exactly one logical reconnect discontinuity whose
        lifecycle is CLOSED, paired strictly by exact ``gap_id``.  The
        interval exposes the full durable identity evidence: the wall-clock
        timestamps (observational only, never causal-ordering authority),
        the STARTED-side original connection/generation, and the
        COMPLETED-side replacement connection/generation
        (M21.4.11-R3.2).  A pair whose COMPLETED wall timestamp is not
        after its STARTED wall timestamp is still returned, flagged
        ``wall_time_order == NON_MONOTONIC``: inverted wall clocks never
        remove exact lifecycle authority from the classification universe.
        Intervals lacking a well-formed identity field are omitted here and
        surfaced by ``degraded_closed_discontinuity_pairs`` instead:
        classification must never guess from partial evidence.
        """
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT event_type, occurred_at_utc_ns, evidence_json
                FROM operational_events
                WHERE event_type IN (
                    'STREAM_DISCONTINUITY_STARTED',
                    'STREAM_DISCONTINUITY_COMPLETED'
                )
                ORDER BY occurred_at_utc_ns, event_id
                """
            ).fetchall()
        started_by_gap: dict[tuple[str, str, str, str], dict[str, object]] = {}
        completed_by_gap: dict[tuple[str, str, str, str], dict[str, object]] = {}
        for row in rows:
            evidence = json.loads(str(row["evidence_json"]))
            if not isinstance(evidence, dict):
                continue
            identity = _discontinuity_identity(
                evidence, allow_legacy_symbol=self._legacy_identity_schema
            )
            if identity is None:
                continue
            market, symbol, stream, _gap_id = identity
            document = {
                "occurred_at_utc_ns": int(row["occurred_at_utc_ns"]),
                "evidence": evidence,
            }
            if str(row["event_type"]) == "STREAM_DISCONTINUITY_COMPLETED":
                completed_by_gap[identity] = document
            else:
                started_by_gap[identity] = document
        grouped: dict[tuple[str, str, str], list[dict[str, object]]] = {}
        for (market, symbol, stream, gap_id), started in sorted(started_by_gap.items()):
            completed = completed_by_gap.get((market, symbol, stream, gap_id))
            if completed is None:
                continue
            started_evidence = started["evidence"]
            completed_evidence = completed["evidence"]
            if not isinstance(started_evidence, dict) or not isinstance(completed_evidence, dict):
                continue
            if not _closed_lifecycle_identity_valid(started_evidence, completed_evidence):
                continue
            started_at = cast(int, started_evidence["gap_started_at_utc_ns"])
            ended_at = cast(int, completed_evidence["gap_ended_at_utc_ns"])
            original_connection = started_evidence["original_connection_id"]
            original_generation = started_evidence["original_generation"]
            new_connection = completed_evidence["new_connection_id"]
            new_generation = completed_evidence["new_generation"]
            grouped.setdefault((market, symbol, stream), []).append(
                {
                    "market": market,
                    "symbol": symbol,
                    "stream": stream,
                    "gap_id": gap_id,
                    "started_at_utc_ns": started_at,
                    "ended_at_utc_ns": ended_at,
                    "wall_time_order": ("NORMAL" if ended_at > started_at else "NON_MONOTONIC"),
                    "original_connection_id": original_connection,
                    "original_generation": original_generation,
                    "new_connection_id": new_connection,
                    "new_generation": new_generation,
                }
            )
        return grouped

    def degraded_closed_discontinuity_pairs(
        self,
    ) -> list[dict[str, object]]:
        """Return exactly-paired CLOSED lifecycles with malformed identity.

        A STARTED/COMPLETED pair keyed by the same ``gap_id`` whose identity
        fields (connections, generations, timestamps) are not well-formed
        cannot serve as positive-proof authority.  R3.3 surfaces each such
        pair as an explicit degraded-authority predecision blocker: malformed
        evidence must widen uncertainty and can never reduce the possible
        historical universe (M21.4.11-R3.3).
        """
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT event_type, evidence_json
                FROM operational_events
                WHERE event_type IN (
                    'STREAM_DISCONTINUITY_STARTED',
                    'STREAM_DISCONTINUITY_COMPLETED'
                )
                """
            ).fetchall()
        started_by_gap: dict[tuple[str, str, str, str], dict[str, object]] = {}
        completed_by_gap: dict[tuple[str, str, str, str], dict[str, object]] = {}
        for row in rows:
            evidence = json.loads(str(row["evidence_json"]))
            if not isinstance(evidence, dict):
                continue
            identity = _discontinuity_identity(
                evidence, allow_legacy_symbol=self._legacy_identity_schema
            )
            if identity is None:
                continue
            if str(row["event_type"]) == "STREAM_DISCONTINUITY_COMPLETED":
                completed_by_gap[identity] = evidence
            else:
                started_by_gap[identity] = evidence
        degraded: list[dict[str, object]] = []
        for key in sorted(set(started_by_gap) & set(completed_by_gap)):
            started_evidence = started_by_gap[key]
            completed_evidence = completed_by_gap[key]
            if _closed_lifecycle_identity_valid(started_evidence, completed_evidence):
                continue
            market, symbol, stream, gap_id = key
            degraded.append(
                {
                    "market": market,
                    "symbol": symbol,
                    "stream": stream,
                    "gap_id": gap_id,
                    "reason": "malformed_lifecycle_identity",
                }
            )
        return degraded

    def malformed_discontinuity_events(self) -> list[dict[str, object]]:
        """Inventory every STARTED/COMPLETED row that cannot be keyed safely.

        A lifecycle event whose evidence is not a JSON object, or whose
        ``market``/``symbol``/``stream``/``gap_id`` identity is missing or not text,
        cannot participate in exact gap_id pairing.  R3.3 surfaces each such
        row as an explicit degraded-authority predecision blocker instead of
        silently skipping it: malformed evidence must widen uncertainty and
        can never shrink the searched parent universe
        (M21.4.11-R3.3 REV-001).
        """
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT event_id, event_type, evidence_json
                FROM operational_events
                WHERE event_type IN (
                    'STREAM_DISCONTINUITY_STARTED',
                    'STREAM_DISCONTINUITY_COMPLETED'
                )
                ORDER BY event_id
                """
            ).fetchall()
        output: list[dict[str, object]] = []
        for row in rows:
            event_id = str(row["event_id"])
            event_type = str(row["event_type"])
            reason: str | None = None
            try:
                evidence = json.loads(str(row["evidence_json"]))
            except (json.JSONDecodeError, TypeError):
                reason = "evidence_not_json"
            if reason is None and not isinstance(evidence, dict):
                reason = "evidence_not_object"
            if reason is None:
                market = evidence.get("market")
                symbol = evidence.get("symbol")
                stream = evidence.get("stream")
                gap_id = evidence.get("gap_id")
                if not isinstance(market, str) or not market:
                    reason = "missing_market"
                elif not isinstance(symbol, str) or not symbol:
                    if not (self._legacy_identity_schema and "symbol" not in evidence):
                        reason = "missing_symbol"
                elif not isinstance(stream, str) or not stream:
                    reason = "missing_stream"
                elif not isinstance(gap_id, str) or not gap_id:
                    reason = "missing_gap_id"
            if reason is None:
                continue
            output.append(
                {
                    "event_id": event_id,
                    "event_type": event_type,
                    "reason": reason,
                }
            )
        return output

    def unclosed_stream_discontinuities_by_stream(
        self,
    ) -> dict[tuple[str, str, str], list[dict[str, object]]]:
        """Return STARTED events without a matching COMPLETED, grouped by
        (market, symbol, stream), in one pass (M21.4.11-R3.2)."""

        with self._lock:
            rows = self._connection.execute(
                """
                SELECT * FROM operational_events
                WHERE event_type IN (
                    'STREAM_DISCONTINUITY_STARTED',
                    'STREAM_DISCONTINUITY_COMPLETED'
                )
                ORDER BY occurred_at_utc_ns, event_id
                """
            ).fetchall()
        started: list[dict[str, object]] = []
        completed_gap_ids: set[tuple[str, str, str, str]] = set()
        for row in rows:
            document = dict(row)
            evidence_json = document.pop("evidence_json")
            try:
                evidence = json.loads(str(evidence_json))
            except (TypeError, json.JSONDecodeError):
                continue
            if not isinstance(evidence, dict):
                continue
            identity = _discontinuity_identity(
                evidence, allow_legacy_symbol=self._legacy_identity_schema
            )
            if identity is None:
                continue
            document["evidence"] = evidence
            if document["event_type"] == "STREAM_DISCONTINUITY_COMPLETED":
                completed_gap_ids.add(identity)
            else:
                started.append(document)
        grouped: dict[tuple[str, str, str], list[dict[str, object]]] = {}
        for event in started:
            evidence = event.get("evidence")
            if not isinstance(evidence, dict):
                continue
            identity = _discontinuity_identity(
                evidence, allow_legacy_symbol=self._legacy_identity_schema
            )
            if identity is None:
                continue
            market, symbol, stream, _gap_id = identity
            if identity in completed_gap_ids:
                continue
            grouped.setdefault((market, symbol, stream), []).append(event)
        return grouped

    def stream_discontinuity_lifecycle(
        self, *, market: str, symbol: str, stream: str, gap_id: str
    ) -> str:
        """Return the durable lifecycle of exactly one gap identifier.

        One of:

        - ``ABSENT``: no STARTED and no COMPLETED record for the gap.
        - ``OPEN``: a STARTED record exists without a matching COMPLETED.
        - ``CLOSED``: a COMPLETED record exists for the gap.

        Startup recovery keys every seal-intent decision by this lifecycle
        (M21.4.11-R2.1/REQ-100): a CLOSED gap is historical and never
        conflicts with a later OPEN gap, while an OPEN gap must agree
        exactly with its seal intent.
        """
        if not market or not symbol or not stream or not gap_id:
            raise ValueError("market, symbol, stream and gap_id must be non-empty")
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT event_type, evidence_json FROM operational_events
                WHERE event_type IN (
                    'STREAM_DISCONTINUITY_STARTED',
                    'STREAM_DISCONTINUITY_COMPLETED'
                )
                """
            ).fetchall()
        started = False
        completed = False
        for row in rows:
            try:
                evidence = json.loads(str(row["evidence_json"]))
            except (TypeError, json.JSONDecodeError):
                continue
            if not isinstance(evidence, dict):
                continue
            identity = _discontinuity_identity(
                evidence, allow_legacy_symbol=self._legacy_identity_schema
            )
            if identity != (market, symbol, stream, gap_id):
                continue
            if str(row["event_type"]) == "STREAM_DISCONTINUITY_COMPLETED":
                completed = True
            else:
                started = True
        if completed:
            return "CLOSED"
        if started:
            return "OPEN"
        return "ABSENT"

    def side_data_cursor(self, kind: str, symbol: str) -> dict[str, object] | None:
        if not kind or not symbol:
            raise ValueError("side-data cursor kind and symbol must be non-empty")
        with self._lock:
            if self._legacy_identity_schema:
                if symbol != LEGACY_SINGLE_SYMBOL:
                    return None
                row = self._connection.execute(
                    "SELECT kind, ? AS symbol, last_persisted_period_timestamp, "
                    "updated_at_utc_ns, source_retention_window, retention_window_ms "
                    "FROM side_data_cursors WHERE kind = ?",
                    (LEGACY_SINGLE_SYMBOL, kind),
                ).fetchone()
            else:
                row = self._connection.execute(
                    "SELECT * FROM side_data_cursors WHERE kind = ? AND symbol = ?",
                    (kind, symbol),
                ).fetchone()
        return dict(row) if row is not None else None

    def advance_side_data_cursor(
        self,
        *,
        kind: str,
        symbol: str,
        last_persisted_period_timestamp: int,
        updated_at_utc_ns: int,
        source_retention_window: str,
        retention_window_ms: int,
    ) -> bool:
        if (
            not kind
            or not symbol
            or last_persisted_period_timestamp < 0
            or updated_at_utc_ns < 0
            or not source_retention_window
            or retention_window_ms <= 0
        ):
            raise ValueError("invalid side-data cursor")
        with self._transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM side_data_cursors WHERE kind = ? AND symbol = ?",
                (kind, symbol),
            ).fetchone()
            if existing is not None:
                current = int(existing["last_persisted_period_timestamp"])
                if last_persisted_period_timestamp < current:
                    raise CatalogStateError("side-data cursor cannot move backward")
                if last_persisted_period_timestamp == current:
                    return False
            connection.execute(
                """
                INSERT INTO side_data_cursors(
                    kind, symbol, last_persisted_period_timestamp,
                    updated_at_utc_ns, source_retention_window, retention_window_ms
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(kind, symbol) DO UPDATE SET
                    last_persisted_period_timestamp =
                        excluded.last_persisted_period_timestamp,
                    updated_at_utc_ns = excluded.updated_at_utc_ns,
                    source_retention_window = excluded.source_retention_window,
                    retention_window_ms = excluded.retention_window_ms
                """,
                (
                    kind,
                    symbol,
                    last_persisted_period_timestamp,
                    updated_at_utc_ns,
                    source_retention_window,
                    retention_window_ms,
                ),
            )
        return True

    def source_availability_watermark(self, selection_id: str | None = None) -> int | None:
        with self._lock:
            if (
                self._connection.execute(
                    "SELECT 1 FROM sqlite_master "
                    "WHERE type='table' AND name='source_availability_watermark'"
                ).fetchone()
                is None
            ):
                return None
            if selection_id is not None:
                row = self._connection.execute(
                    "SELECT evaluated_through_utc_ns FROM source_availability_watermark "
                    "WHERE selection_id = ?",
                    (selection_id,),
                ).fetchone()
            else:
                row = self._connection.execute(
                    "SELECT evaluated_through_utc_ns FROM source_availability_watermark "
                    "ORDER BY updated_at_utc_ns DESC LIMIT 1"
                ).fetchone()
        return int(row[0]) if row is not None else None

    def source_availability(self, selection_id: str | None = None) -> dict[str, dict[str, Any]]:
        with self._lock:
            if (
                self._connection.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='source_availability'"
                ).fetchone()
                is None
            ):
                return {}
            rows = self._connection.execute("SELECT * FROM source_availability").fetchall()
            watermarks: dict[str, int] = {}
            if self._connection.execute(
                "SELECT 1 FROM sqlite_master "
                "WHERE type='table' AND name='source_availability_watermark'"
            ).fetchone():
                watermarks = {
                    str(row[0]): int(row[1])
                    for row in self._connection.execute(
                        "SELECT selection_id, evaluated_through_utc_ns "
                        "FROM source_availability_watermark"
                    ).fetchall()
                }
        result: dict[str, dict[str, Any]] = {}
        for row in rows:
            data = json.loads(row["state_json"])
            membership = data.get("selection_id")
            watermark_ns = (
                watermarks.get(membership)
                if (selection_id is None or membership == selection_id)
                else None
            )
            if (
                watermark_ns is not None
                and data.get("unavailable_since_utc_ns") is None
                and not data.get("dormant")
            ):
                data["observed_at_utc_ns"] = max(
                    int(data.get("observed_at_utc_ns", 0)), watermark_ns
                )
            result[str(row["source_id"])] = data
        return result

    def persist_source_availability_watermark(
        self, selection_id: str, evaluated_through_utc_ns: int
    ) -> None:
        self.persist_source_availability([], watermark=(selection_id, evaluated_through_utc_ns))

    def persist_source_availability(
        self,
        batch: list[dict[str, Any]],
        *,
        watermark: tuple[str, int] | None = None,
    ) -> None:
        now = time.time_ns()
        with self._transaction() as connection:
            for item in batch:
                state, gap = item["state"], item["gap"]
                connection.execute(
                    "INSERT INTO source_availability VALUES (?, ?) "
                    "ON CONFLICT(source_id) DO UPDATE SET state_json=excluded.state_json",
                    (state["source_id"], json.dumps(state, sort_keys=True)),
                )
                eligibility = item["eligibility"]
                connection.execute(
                    "INSERT INTO source_availability_lifecycle VALUES (?, ?, ?, ?) "
                    "ON CONFLICT(interval_id) DO UPDATE SET "
                    "ended_at_utc_ns=excluded.ended_at_utc_ns",
                    (
                        eligibility["interval_id"],
                        eligibility["source_id"],
                        eligibility["started_at_utc_ns"],
                        eligibility["ended_at_utc_ns"],
                    ),
                )
                if gap is not None:
                    connection.execute(
                        "INSERT INTO source_availability_gaps VALUES (?, ?, ?, ?, ?) "
                        "ON CONFLICT(gap_id) DO UPDATE SET "
                        "ended_at_utc_ns=excluded.ended_at_utc_ns, "
                        "evidence_json=excluded.evidence_json",
                        (
                            gap["gap_id"],
                            gap["source_id"],
                            gap["started_at_utc_ns"],
                            gap["ended_at_utc_ns"],
                            json.dumps(gap, sort_keys=True),
                        ),
                    )
            if watermark is not None:
                selection_id, evaluated_through_utc_ns = watermark
                connection.execute(
                    "INSERT INTO source_availability_watermark("
                    "selection_id, evaluated_through_utc_ns, updated_at_utc_ns) "
                    "VALUES (?, ?, ?) "
                    "ON CONFLICT(selection_id) DO UPDATE SET "
                    "evaluated_through_utc_ns=excluded.evaluated_through_utc_ns, "
                    "updated_at_utc_ns=excluded.updated_at_utc_ns",
                    (selection_id, evaluated_through_utc_ns, now),
                )

    def source_eligibility_intervals(self, source_id: str) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._connection.execute(
                "SELECT * FROM source_availability_lifecycle WHERE source_id=? "
                "ORDER BY started_at_utc_ns",
                (source_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def source_availability_intervals(
        self,
        source_id: str,
        start: int,
        end: int,
    ) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._connection.execute(
                "SELECT * FROM source_availability_gaps WHERE source_id=? "
                "AND started_at_utc_ns<? AND (ended_at_utc_ns IS NULL OR ended_at_utc_ns>?) "
                "ORDER BY started_at_utc_ns",
                (source_id, end, start),
            ).fetchall()
        return [dict(row) for row in rows]
