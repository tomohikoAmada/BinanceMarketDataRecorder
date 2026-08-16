"""M21.4.11-R3/R3.1/R3.2/R3.3: orphan reconnect seal intents must not exist.

A reconnect boundary that merely extends an already-open pending logical gap
(a closing attempt that never delivered a reliable first-new frame) must not
persist a durable seal intent with a freshly minted gap identity.  The 72h
production window proved the live extension path keeps exactly one Catalog
STARTED/COMPLETED pair, but it also persisted a SEALING marker intent whose
fresh gap_id had no lifecycle.  Startup recovery scans every historical
SEALING intent and materializes a phantom STREAM_DISCONTINUITY_STARTED for
such an orphan (REQ-103 shape), which would contaminate the next restart
(the 168h gate requires a controlled service restart).

Three properties are verified independently:

PREVENTION: the corrected runtime reuses the canonical pending-gap identity
for extension seal intents (with attempt-level metadata in ``extension``),
and every R3.3+ intent carries the durable ``intent_schema`` provenance
(``reconnect-seal-intent.v2``).
LEGACY_RECOVERY: already-persisted old orphan shapes (closed or still-open
parent gap) are resolved through the R3.3 exhaustive three-way partition
(PROVEN_LEGITIMATE / PROVEN_EXTENSION / AMBIGUOUS, never a default
materialization or ignore); UTC wall-clock ordering never gates
classification; unversioned legacy intents NEVER use "no parent found" as
positive proof; the operator authority (schema v3, strongly bound to the
exact persisted classification evidence: chunk_id + seal intent +
verified_frames) resolves ONLY ambiguous candidates and can never override
durable proofs; startup performs the global pre-decision before any legacy
lifecycle mutation; malformed lifecycle authority is surfaced as explicit
degraded-authority blockers.
PREFLIGHT: the read-only ``recovery legacy-reconnect-preflight`` command
enumerates every candidate deterministically through the same shared
decision engine, reports first-corrected-startup eligibility, is
intrinsically read-only (it never creates storage layout), and exits
nonzero when ineligible.

No production-specific UUIDs or timestamps are hard-coded anywhere.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, cast

import pytest

from binance_market_data_recorder.binance.spot.schema import SpotStream
from binance_market_data_recorder.binance.spot.websocket import (
    SpotStreamCollector,
)
from binance_market_data_recorder.binance.usdm.schema import UsdMStream
from binance_market_data_recorder.binance.usdm.websocket import (
    WebSocketConnection,
)
from binance_market_data_recorder.spool.legacy_reconnect import (
    LEGACY_CLASSIFICATION_FILENAME,
)
from binance_market_data_recorder.spool.recovery import (
    RecoveryConflictError,
    recover_storage,
)
from binance_market_data_recorder.spool.seal import RECONNECT_GAP_FLAG
from binance_market_data_recorder.spool.stream import StreamSpool
from binance_market_data_recorder.spool.writer import RotationPolicy
from binance_market_data_recorder.storage.catalog import Catalog, ChunkState
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tests.integration.test_reconnect_boundary_integrity import (
    ScriptedSocket,
    _durable_intent,
    _record_gap,
    book_ticker,
    discontinuity_events,
    envelopes,
    make_collector,
    manifests,
    sealing_intent,
)


def _make_spot_collector(
    root: Path,
    *,
    opener: Any,
) -> tuple[SpotStreamCollector, Catalog, StreamSpool]:
    layout = ensure_storage_layout(root)
    catalog = Catalog(layout.catalog)
    spool = StreamSpool(
        layout=layout,
        catalog=catalog,
        market="spot",
        symbol="BTCUSDT",
        stream="book_ticker",
        collector_instance_id="m21-4-11-r3-test",
        collector_version="0.1.0+test",
        queue_capacity=32,
        rotation=RotationPolicy(seconds=60),
        durability_interval_seconds=0,
        max_frame_bytes=1024 * 1024,
    )
    collector = SpotStreamCollector(
        stream=SpotStream.BOOK_TICKER,
        wire_name="btcusdt@bookTicker",
        spool=spool,
        collector_instance_id="m21-4-11-r3-test",
        collector_version="0.1.0+test",
        logger=logging.getLogger("test.m21-4-11-r3"),
        opener=opener,
    )
    return collector, catalog, spool


def _seal_zero_record_marker(
    root: Path,
    catalog: Catalog,
    intent: dict[str, Any],
    *,
    stream: str,
) -> str:
    """Persist a zero-record boundary marker carrying ``intent``.

    Mirrors the runtime no-active-writer path (AUDIT-001): the marker's Raw
    body holds zero frames and the SEALING transition evidence carries the
    exact seal intent.
    """
    spool = StreamSpool(
        layout=ensure_storage_layout(root),
        catalog=catalog,
        market="um_perpetual",
        symbol="BTCUSDT",
        stream=stream,
        collector_instance_id="m21-4-11-r3-test",
        collector_version="0.1.0+test",
        queue_capacity=32,
        rotation=RotationPolicy(seconds=60),
        durability_interval_seconds=0,
        max_frame_bytes=1024 * 1024,
    )
    manifest = spool._seal_current(seal_intent=dict(intent))
    assert manifest is not None
    assert manifest["record_count"] == 0
    assert manifest["gap"] is True
    assert manifest["complete"] is False
    assert RECONNECT_GAP_FLAG in cast(list[Any], manifest["capture_flags"])
    assert manifest["connection_ids"] == []
    return str(manifest["chunk_id"])


def _seal_intents(root: Path) -> list[dict[str, Any]]:
    with Catalog(
        ensure_storage_layout(root).catalog, read_only=True
    ) as catalog:
        results: list[dict[str, Any]] = []
        for _chunk_id, evidence in catalog.sealing_transition_evidence():
            intent = evidence.get("seal_intent")
            if isinstance(intent, dict):
                results.append(cast(dict[str, Any], intent))
        return results


def _started_events(catalog: Catalog) -> list[dict[str, Any]]:
    return [
        cast(dict[str, Any], event)
        for event in catalog.operational_events()
        if str(event["event_type"]) == "STREAM_DISCONTINUITY_STARTED"
    ]


def test_usdm_session_restart_extension_reuses_pending_gap_identity(
    tmp_path: Path,
) -> None:
    """TEST-001: a session_restart extension of an open pending gap must
    persist the canonical pending-gap identity in its marker intent."""
    attempts = 0
    extension_socket_open = asyncio.Event()

    @asynccontextmanager
    async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            yield ScriptedSocket(
                [book_ticker(1)], error=OSError("first disconnect")
            )
        else:
            extension_socket_open.set()
            yield ScriptedSocket([], block_on_exhaustion=True)

    async def extend() -> dict[str, Any]:
        stop = asyncio.Event()
        session_restart = asyncio.Event()
        collector, catalog, _spool = make_collector(tmp_path, opener=opener)
        try:
            async def trigger_restart() -> None:
                await extension_socket_open.wait()
                session_restart.set()
                stop.set()

            asyncio.get_running_loop().create_task(trigger_restart())
            await asyncio.wait_for(
                collector.run(stop, session_restart), timeout=5
            )
            with Catalog(
                ensure_storage_layout(tmp_path).catalog, read_only=True
            ) as readonly:
                events = discontinuity_events(readonly)
                assert [event["event_type"] for event in events] == [
                    "STREAM_DISCONTINUITY_STARTED"
                ]
                started = cast(dict[str, Any], events[0]["evidence"])
                return started
        finally:
            catalog.close()

    started = asyncio.run(extend())

    # Startup recovery must not fail closed on the extension marker, and the
    # replacement generation completes exactly the one canonical gap.
    layout = ensure_storage_layout(tmp_path)
    recovered = Catalog(layout.catalog)
    actions = recover_storage(layout=layout, catalog=recovered)
    recovered.close()
    assert not any(
        action.action == "pending_discontinuity_materialized"
        for action in actions
    )

    async def deliver() -> None:
        stop = asyncio.Event()

        @asynccontextmanager
        async def replacement(_url: str) -> AsyncIterator[WebSocketConnection]:
            yield ScriptedSocket([book_ticker(2)], stop=stop)

        collector, catalog, _spool = make_collector(
            tmp_path, opener=replacement
        )
        try:
            await asyncio.wait_for(collector.run(stop), timeout=5)
        finally:
            catalog.close()

    asyncio.run(deliver())

    documents = manifests(tmp_path)
    assert len(documents) == 3
    old_chunk, marker, new_chunk = documents
    assert old_chunk["gap"] is True and old_chunk["complete"] is False
    assert marker["record_count"] == 0
    assert marker["gap"] is True
    assert marker["complete"] is False
    assert RECONNECT_GAP_FLAG in marker["capture_flags"]
    assert marker["connection_ids"] == []
    assert new_chunk["gap"] is True and new_chunk["complete"] is False
    assert "sequence_gap" in new_chunk["capture_flags"]

    with Catalog(layout.catalog, read_only=True) as catalog:
        events = discontinuity_events(catalog)
        assert [event["event_type"] for event in events] == [
            "STREAM_DISCONTINUITY_STARTED",
            "STREAM_DISCONTINUITY_COMPLETED",
        ]
        evidence = cast(dict[str, Any], events[0]["evidence"])
        completed = cast(dict[str, Any], events[1]["evidence"])
        assert evidence["gap_id"] == started["gap_id"]
        assert completed["gap_id"] == started["gap_id"]
        assert completed["historical_continuity_restored"] is False

        marker_intent = sealing_intent(catalog, str(marker["chunk_id"]))
        # INV-003: canonical identity fields agree with the pending gap.
        assert marker_intent["gap_id"] == started["gap_id"]
        assert marker_intent["reason"] == started["reason"]
        assert (
            marker_intent["original_connection_id"]
            == started["original_connection_id"]
        )
        assert (
            marker_intent["original_generation"]
            == started["original_generation"]
        )
        assert (
            marker_intent["gap_started_at_utc_ns"]
            == started["gap_started_at_utc_ns"]
        )
        # INV-004: attempt-level metadata stays separate, never canonical.
        extension = marker_intent.get("extension")
        assert isinstance(extension, dict)
        assert extension["attempt_reason"] == "session_restart"
        assert isinstance(extension["attempt_connection_id"], str)
        assert extension["attempt_connection_id"]
        assert extension["attempt_connection_id"] != started[
            "original_connection_id"
        ]
        assert isinstance(extension["attempt_generation"], int)
        assert extension["attempt_generation"] == started[
            "original_generation"
        ] + 1

    persisted = envelopes(tmp_path)
    assert [item.raw_payload for item in persisted] == [
        book_ticker(1),
        book_ticker(2),
    ]
    boundary = [
        item for item in persisted if "sequence_gap" in item.capture_flags
    ]
    assert len(boundary) == 1
    assert boundary[0].raw_payload == book_ticker(2)

    # Every durable seal intent in this fixture carries the canonical gap.
    for intent in _seal_intents(tmp_path):
        assert intent["gap_id"] == started["gap_id"]


def test_spot_session_restart_extension_reuses_pending_gap_identity(
    tmp_path: Path,
) -> None:
    """TEST-002: the Spot path has the same extension-intent semantics."""
    attempts = 0
    extension_socket_open = asyncio.Event()

    @asynccontextmanager
    async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            yield ScriptedSocket(
                [book_ticker(1)], error=OSError("first disconnect")
            )
        else:
            extension_socket_open.set()
            yield ScriptedSocket([], block_on_exhaustion=True)

    async def extend() -> dict[str, Any]:
        stop = asyncio.Event()
        session_restart = asyncio.Event()
        collector, catalog, _spool = _make_spot_collector(
            tmp_path, opener=opener
        )
        try:
            async def trigger_restart() -> None:
                await extension_socket_open.wait()
                session_restart.set()
                stop.set()

            asyncio.get_running_loop().create_task(trigger_restart())
            await asyncio.wait_for(
                collector.run(stop, session_restart), timeout=5
            )
            with Catalog(
                ensure_storage_layout(tmp_path).catalog, read_only=True
            ) as readonly:
                events = discontinuity_events(readonly)
                assert [event["event_type"] for event in events] == [
                    "STREAM_DISCONTINUITY_STARTED"
                ]
                return cast(dict[str, Any], events[0]["evidence"])
        finally:
            catalog.close()

    started = asyncio.run(extend())

    layout = ensure_storage_layout(tmp_path)
    recovered = Catalog(layout.catalog)
    actions = recover_storage(layout=layout, catalog=recovered)
    recovered.close()
    assert not any(
        action.action == "pending_discontinuity_materialized"
        for action in actions
    )

    async def deliver() -> None:
        stop = asyncio.Event()

        @asynccontextmanager
        async def replacement(_url: str) -> AsyncIterator[WebSocketConnection]:
            yield ScriptedSocket([book_ticker(2)], stop=stop)

        collector, catalog, _spool = _make_spot_collector(
            tmp_path, opener=replacement
        )
        try:
            await asyncio.wait_for(collector.run(stop), timeout=5)
        finally:
            catalog.close()

    asyncio.run(deliver())

    documents = manifests(tmp_path)
    assert len(documents) == 3
    marker = documents[1]
    assert marker["record_count"] == 0
    assert marker["gap"] is True and marker["complete"] is False

    with Catalog(layout.catalog, read_only=True) as catalog:
        events = discontinuity_events(catalog)
        assert [event["event_type"] for event in events] == [
            "STREAM_DISCONTINUITY_STARTED",
            "STREAM_DISCONTINUITY_COMPLETED",
        ]
        marker_intent = sealing_intent(catalog, str(marker["chunk_id"]))
        assert marker_intent["gap_id"] == started["gap_id"]
        assert marker_intent["reason"] == started["reason"]
        assert (
            marker_intent["original_connection_id"]
            == started["original_connection_id"]
        )
        extension = marker_intent.get("extension")
        assert isinstance(extension, dict)
        assert extension["attempt_reason"] == "session_restart"


def test_connect_failures_before_first_new_frame_leave_no_orphan_intents(
    tmp_path: Path,
) -> None:
    """TEST-003/004: repeated zero-frame attempts keep one canonical gap and
    no durable intent mints a second identity."""
    attempts = 0

    async def exercise() -> None:
        stop = asyncio.Event()

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                yield ScriptedSocket(
                    [book_ticker(1)], error=OSError("A disconnected")
                )
            if attempts in (2, 3):
                raise OSError(f"connect attempt {attempts} failed")
            yield ScriptedSocket([book_ticker(4)], stop=stop)

        collector, catalog, _spool = make_collector(tmp_path, opener=opener)
        try:
            await asyncio.wait_for(collector.run(stop), timeout=5)
        finally:
            catalog.close()
        assert attempts == 4

    asyncio.run(exercise())

    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog, read_only=True) as catalog:
        events = discontinuity_events(catalog)
        assert [event["event_type"] for event in events] == [
            "STREAM_DISCONTINUITY_STARTED",
            "STREAM_DISCONTINUITY_COMPLETED",
        ]
        started = cast(dict[str, Any], events[0]["evidence"])
        assert started["gap_id"]
        for intent in _seal_intents(tmp_path):
            assert intent["gap_id"] == started["gap_id"]

    documents = manifests(tmp_path)
    assert len(documents) == 2


def test_multiple_consecutive_session_restart_extensions_keep_one_gap(
    tmp_path: Path,
) -> None:
    """TEST-004: several consecutive session_restart extensions (each with
    zero delivered frames) seal multiple markers; every marker intent must
    reuse the single canonical pending-gap identity."""
    round_index = 0

    async def boundary_round() -> dict[str, Any]:
        stop = asyncio.Event()
        session_restart = asyncio.Event()
        attempts = 0
        extension_socket_open = asyncio.Event()

        @asynccontextmanager
        async def opener(
            _url: str,
        ) -> AsyncIterator[WebSocketConnection]:
            nonlocal attempts
            attempts += 1
            if round_index == 0 and attempts == 1:
                yield ScriptedSocket(
                    [book_ticker(1)], error=OSError("first disconnect")
                )
            else:
                extension_socket_open.set()
                yield ScriptedSocket([], block_on_exhaustion=True)

        collector, catalog, _spool = make_collector(tmp_path, opener=opener)
        try:
            async def trigger_restart() -> None:
                await extension_socket_open.wait()
                session_restart.set()
                stop.set()

            asyncio.get_running_loop().create_task(trigger_restart())
            await asyncio.wait_for(
                collector.run(stop, session_restart), timeout=5
            )
            with Catalog(
                ensure_storage_layout(tmp_path).catalog, read_only=True
            ) as readonly:
                events = discontinuity_events(readonly)
                assert [event["event_type"] for event in events] == [
                    "STREAM_DISCONTINUITY_STARTED"
                ]
                return cast(dict[str, Any], events[0]["evidence"])
        finally:
            catalog.close()

    started = asyncio.run(boundary_round())
    round_index = 1
    second = asyncio.run(boundary_round())
    third = asyncio.run(boundary_round())
    assert second["gap_id"] == started["gap_id"]
    assert third["gap_id"] == started["gap_id"]

    layout = ensure_storage_layout(tmp_path)
    recovered = Catalog(layout.catalog)
    actions = recover_storage(layout=layout, catalog=recovered)
    recovered.close()
    assert not any(
        action.action == "pending_discontinuity_materialized"
        for action in actions
    )

    async def deliver() -> None:
        stop = asyncio.Event()

        @asynccontextmanager
        async def replacement(_url: str) -> AsyncIterator[WebSocketConnection]:
            yield ScriptedSocket([book_ticker(2)], stop=stop)

        collector, catalog, _spool = make_collector(
            tmp_path, opener=replacement
        )
        try:
            await asyncio.wait_for(collector.run(stop), timeout=5)
        finally:
            catalog.close()

    asyncio.run(deliver())

    documents = manifests(tmp_path)
    assert len(documents) == 5
    assert [document["record_count"] for document in documents[1:4]] == [
        0,
        0,
        0,
    ]

    with Catalog(layout.catalog, read_only=True) as catalog:
        events = discontinuity_events(catalog)
        assert [event["event_type"] for event in events] == [
            "STREAM_DISCONTINUITY_STARTED",
            "STREAM_DISCONTINUITY_COMPLETED",
        ]
        for document in documents[1:4]:
            intent = sealing_intent(catalog, str(document["chunk_id"]))
            assert intent["gap_id"] == started["gap_id"]
            assert isinstance(intent.get("extension"), dict)


def test_legitimate_intent_only_crash_materializes_exactly_once(
    tmp_path: Path,
) -> None:
    """TEST-005/006: the REQ-103 crash case stays intact: a genuine
    R3.3-versioned marker intent with no lifecycle and no parent interval
    materializes exactly one STARTED, and repeated recovery never
    duplicates it."""
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        intent = _durable_intent(
            gap_id="genuine-g1",
            reason="planned_rotation",
            connection_id="conn-g1",
            generation=0,
            started_at_utc_ns=5_000_000_000,
            intent_schema="reconnect-seal-intent.v2",
        )
        _seal_zero_record_marker(
            tmp_path, catalog, intent, stream=intent["stream"]
        )

    recovered = Catalog(layout.catalog)
    first_actions = recover_storage(layout=layout, catalog=recovered)
    recovered.close()
    assert any(
        action.action == "pending_discontinuity_materialized"
        and action.detail == "genuine-g1"
        for action in first_actions
    )

    with Catalog(layout.catalog, read_only=True) as catalog:
        started = _started_events(catalog)
        assert [event["evidence"]["gap_id"] for event in started] == [
            "genuine-g1"
        ]

    recovered = Catalog(layout.catalog)
    second_actions = recover_storage(layout=layout, catalog=recovered)
    recovered.close()
    assert not any(
        action.action == "pending_discontinuity_materialized"
        for action in second_actions
    )
    with Catalog(layout.catalog, read_only=True) as catalog:
        started = _started_events(catalog)
        assert [event["evidence"]["gap_id"] for event in started] == [
            "genuine-g1"
        ]


def _legacy_orphan_fixture(
    root: Path,
) -> tuple[dict[str, Any], dict[str, Any], str, str]:
    """Persist the production legacy orphan SHAPE deterministically.

    Parent gap CLOSED (generation 0 -> 1, completing connection
    ``conn-parent-2``); orphan intent with a freshly minted gap identity,
    the failed attempt connection, the parent's replacement generation, and
    a wall timestamp strictly inside the parent interval.

    Returns (parent, orphan, orphan_chunk_id, classification_evidence_sha256).
    """
    layout = ensure_storage_layout(root)
    with Catalog(layout.catalog) as catalog:
        parent = _durable_intent(
            gap_id="parent-g1",
            reason="unexpected_disconnect",
            connection_id="conn-parent",
            generation=0,
            started_at_utc_ns=1_000_000_000,
        )
        _record_gap(catalog, parent, completed=False)
        _record_gap(
            catalog,
            parent,
            completed=True,
            new_connection_id="conn-parent-2",
            new_generation=1,
            gap_ended_at_utc_ns=3_000_000_000,
        )
        orphan = _durable_intent(
            gap_id="orphan-g2",
            reason="session_restart",
            connection_id="conn-attempt",
            generation=1,
            started_at_utc_ns=2_000_000_000,
        )
        orphan_chunk_id = _seal_zero_record_marker(
            root, catalog, orphan, stream=orphan["stream"]
        )
        orphan_digest = _seal_intent_digest(catalog, orphan_chunk_id)
    return parent, orphan, orphan_chunk_id, orphan_digest


def _seal_intent_digest(catalog: Catalog, chunk_id: str) -> str:
    from binance_market_data_recorder.spool.legacy_reconnect import (
        classification_evidence_sha256,
    )

    evidence = catalog.latest_transition_evidence(
        chunk_id, ChunkState.SEALING
    )
    assert evidence is not None
    return classification_evidence_sha256(
        chunk_id=chunk_id,
        intent=sealing_intent(catalog, chunk_id),
        verified_frames=evidence.get("verified_frames"),
    )


def _write_classification_authority(
    root: Path, entries: list[dict[str, Any]]
) -> None:
    document = {
        "schema": "legacy-reconnect-classification.v3",
        "classifications": entries,
    }
    (root / "legacy_reconnect_classifications.json").write_text(
        json.dumps(document, indent=2) + "\n", encoding="utf-8"
    )


def _orphan_classification_entry(
    root: Path,
    chunk_id: str,
    *,
    classification: str,
) -> dict[str, Any]:
    with Catalog(ensure_storage_layout(root).catalog, read_only=True) as catalog:
        intent = sealing_intent(catalog, chunk_id)
        return {
            "gap_id": str(intent["gap_id"]),
            "market": str(intent["market"]),
            "stream": str(intent["stream"]),
            "chunk_id": chunk_id,
            "classification_evidence_sha256": _seal_intent_digest(
                catalog, chunk_id
            ),
            "classification": classification,
            "note": "test-reviewed classification",
        }


def test_legacy_orphan_with_closed_parent_never_materializes(
    tmp_path: Path,
) -> None:
    """TEST-008: the production orphan shape (parent gap CLOSED, orphan
    intent with the failed attempt connection, zero-record marker, wall
    timestamp inside the parent interval, same replacement generation) is
    ignored by startup recovery instead of materializing a phantom STARTED.

    The suppression is proven by the explicit operator classification
    authority only: durable identity alone (attempt connection differs from
    the parent's completing connection, generation matches) cannot
    distinguish the legacy extension from a legitimate post-restart boundary
    with a reused generation, so unclassified recovery must fail closed
    (R3.1/R3.2). The authority entry is strongly bound to the exact
    persisted intent (chunk_id + canonical seal-intent digest, schema v2)."""
    _parent, _orphan, orphan_chunk_id, _orphan_digest = _legacy_orphan_fixture(
        tmp_path
    )
    _write_classification_authority(
        tmp_path,
        [
            _orphan_classification_entry(
                tmp_path,
                orphan_chunk_id,
                classification="extension_orphan",
            )
        ],
    )

    layout = ensure_storage_layout(tmp_path)
    recovered = Catalog(layout.catalog)
    actions = recover_storage(layout=layout, catalog=recovered)
    recovered.close()
    assert any(
        action.action == "extension_orphan_ignored"
        and action.detail == "orphan-g2"
        for action in actions
    )
    assert not any(
        action.action == "pending_discontinuity_materialized"
        for action in actions
    )

    with Catalog(layout.catalog, read_only=True) as catalog:
        started = _started_events(catalog)
        assert [event["evidence"]["gap_id"] for event in started] == [
            "parent-g1"
        ]
        assert (
            catalog.stream_discontinuity_lifecycle(
                market="um_perpetual",
                stream="book_ticker",
                gap_id="orphan-g2",
            )
            == "ABSENT"
        )

    recovered = Catalog(layout.catalog)
    repeated_actions = recover_storage(layout=layout, catalog=recovered)
    recovered.close()
    assert any(
        action.action == "extension_orphan_ignored"
        and action.detail == "orphan-g2"
        for action in repeated_actions
    )
    assert not any(
        action.action == "pending_discontinuity_materialized"
        for action in repeated_actions
    )


def test_unclassified_legacy_orphan_with_closed_parent_fails_closed(
    tmp_path: Path,
) -> None:
    """TEST-008a: without the explicit operator classification the legacy
    orphan shape cannot be proven an extension from durable identity alone
    (a legitimate post-restart boundary can carry the same generation
    number, a different connection, zero frames and an overlapping wall
    timestamp): startup recovery must fail closed, never silently suppress
    it and never silently materialize a phantom STARTED."""
    _legacy_orphan_fixture(tmp_path)

    layout = ensure_storage_layout(tmp_path)
    with pytest.raises(RecoveryConflictError, match="RECOVERY_LEGACY_ORPHAN_AMBIGUOUS"):
        recover_storage(layout=layout, catalog=Catalog(layout.catalog))


def test_wall_clock_rollback_after_completion_still_materializes(
    tmp_path: Path,
) -> None:
    """R3.1-001: REQ-103 survives wall-clock rollback.

    A genuine boundary H is detected causally AFTER the parent completed but
    its wall timestamp falls INSIDE the parent interval.  The connection
    identity is the complete causal proof: H closed the same connection that
    completed the parent, which an extension attempt can never do.  H must
    materialize exactly one STARTED."""
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        parent = _durable_intent(
            gap_id="parent-g1",
            reason="unexpected_disconnect",
            connection_id="conn-parent",
            generation=0,
            started_at_utc_ns=1_000_000_000,
        )
        _record_gap(catalog, parent, completed=False)
        _record_gap(
            catalog,
            parent,
            completed=True,
            new_connection_id="conn-parent-2",
            new_generation=1,
            gap_ended_at_utc_ns=3_000_000_000,
        )
        genuine = _durable_intent(
            gap_id="genuine-g2",
            reason="planned_rotation",
            connection_id="conn-parent-2",
            generation=1,
            started_at_utc_ns=2_000_000_000,
        )
        _seal_zero_record_marker(
            tmp_path, catalog, genuine, stream=genuine["stream"]
        )

    recovered = Catalog(layout.catalog)
    actions = recover_storage(layout=layout, catalog=recovered)
    recovered.close()
    assert any(
        action.action == "pending_discontinuity_materialized"
        and action.detail == "genuine-g2"
        for action in actions
    )
    assert not any(
        action.action == "extension_orphan_ignored" for action in actions
    )
    with Catalog(layout.catalog, read_only=True) as catalog:
        started = _started_events(catalog)
        assert [event["evidence"]["gap_id"] for event in started] == [
            "parent-g1",
            "genuine-g2",
        ]


def test_generation_reuse_after_restart_never_silently_suppressed(
    tmp_path: Path,
) -> None:
    """R3.1-002: a legitimate intent-only boundary after a service restart
    (different connection, numerically reused generation, zero frames, wall
    timestamp inside an old CLOSED interval due clock rollback) must never
    be silently ignored as an extension orphan.

    Durable identity cannot distinguish it from a legacy extension: recovery
    fails closed and requires explicit operator classification."""
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        parent = _durable_intent(
            gap_id="parent-g1",
            reason="unexpected_disconnect",
            connection_id="conn-parent",
            generation=0,
            started_at_utc_ns=1_000_000_000,
        )
        _record_gap(catalog, parent, completed=False)
        _record_gap(
            catalog,
            parent,
            completed=True,
            new_connection_id="conn-parent-2",
            new_generation=1,
            gap_ended_at_utc_ns=3_000_000_000,
        )
        genuine = _durable_intent(
            gap_id="genuine-g2",
            reason="planned_rotation",
            connection_id="conn-new-process",
            generation=1,
            started_at_utc_ns=2_000_000_000,
        )
        _seal_zero_record_marker(
            tmp_path, catalog, genuine, stream=genuine["stream"]
        )

    recovered = Catalog(layout.catalog)
    with pytest.raises(
        RecoveryConflictError, match="RECOVERY_LEGACY_ORPHAN_AMBIGUOUS"
    ):
        recover_storage(layout=layout, catalog=recovered)
    recovered.close()


def test_generation_reuse_after_restart_resolvable_via_authority(
    tmp_path: Path,
) -> None:
    """R3.1-003: the explicit operator classification authority resolves
    the ambiguous post-restart shape: a ``legitimate_req103`` entry
    materializes exactly one STARTED and repeated recovery stays
    idempotent."""
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        parent = _durable_intent(
            gap_id="parent-g1",
            reason="unexpected_disconnect",
            connection_id="conn-parent",
            generation=0,
            started_at_utc_ns=1_000_000_000,
        )
        _record_gap(catalog, parent, completed=False)
        _record_gap(
            catalog,
            parent,
            completed=True,
            new_connection_id="conn-parent-2",
            new_generation=1,
            gap_ended_at_utc_ns=3_000_000_000,
        )
        genuine = _durable_intent(
            gap_id="genuine-g2",
            reason="planned_rotation",
            connection_id="conn-new-process",
            generation=1,
            started_at_utc_ns=2_000_000_000,
        )
        genuine_chunk_id = _seal_zero_record_marker(
            tmp_path, catalog, genuine, stream=genuine["stream"]
        )
    _write_classification_authority(
        tmp_path,
        [
            _orphan_classification_entry(
                tmp_path,
                genuine_chunk_id,
                classification="legitimate_req103",
            )
        ],
    )

    recovered = Catalog(layout.catalog)
    actions = recover_storage(layout=layout, catalog=recovered)
    recovered.close()
    assert any(
        action.action == "pending_discontinuity_materialized"
        and action.detail == "genuine-g2"
        for action in actions
    )
    recovered = Catalog(layout.catalog)
    repeated_actions = recover_storage(layout=layout, catalog=recovered)
    recovered.close()
    assert not any(
        action.action == "pending_discontinuity_materialized"
        for action in repeated_actions
    )
    with Catalog(layout.catalog, read_only=True) as catalog:
        started = _started_events(catalog)
        assert [event["evidence"]["gap_id"] for event in started] == [
            "parent-g1",
            "genuine-g2",
        ]


def test_frame_bearing_intent_inside_closed_interval_materializes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """R3.1-004: trustworthy SEALING evidence with ``verified_frames > 0``
    proves the boundary drained a frame-bearing generation; a legacy
    extension attempt always seals a zero-record marker.  The intent must
    materialize even when its wall timestamp falls inside the parent
    interval."""
    from tests.integration.test_reconnect_boundary_integrity import (
        _seal_chunk_with_intent,
    )

    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        parent = _durable_intent(
            gap_id="parent-g1",
            reason="unexpected_disconnect",
            connection_id="conn-parent",
            generation=0,
            started_at_utc_ns=1_000_000_000,
        )
        _record_gap(catalog, parent, completed=False)
        _record_gap(
            catalog,
            parent,
            completed=True,
            new_connection_id="conn-parent-2",
            new_generation=1,
            gap_ended_at_utc_ns=3_000_000_000,
        )
        genuine = _durable_intent(
            gap_id="genuine-g2",
            reason="planned_rotation",
            connection_id="conn-attempt",
            generation=1,
            started_at_utc_ns=2_000_000_000,
        )
        _seal_chunk_with_intent(
            layout,
            catalog,
            monkeypatch,
            intent=genuine,
            frame_payload=book_ticker(7),
        )

    recovered = Catalog(layout.catalog)
    actions = recover_storage(layout=layout, catalog=recovered)
    recovered.close()
    assert any(
        action.action == "pending_discontinuity_materialized"
        and action.detail == "genuine-g2"
        for action in actions
    )


def test_classification_authority_malformed_fails_closed(
    tmp_path: Path,
) -> None:
    """R3.1-005: a malformed operator classification authority file must
    fail closed instead of silently proceeding without authority."""
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        intent = _durable_intent(
            gap_id="genuine-g1",
            reason="planned_rotation",
            connection_id="conn-g1",
            generation=0,
            started_at_utc_ns=5_000_000_000,
        )
        _seal_zero_record_marker(
            tmp_path, catalog, intent, stream=intent["stream"]
        )

    authority = layout.root / "legacy_reconnect_classifications.json"
    authority.write_text("{not json", encoding="utf-8")
    with pytest.raises(RecoveryConflictError, match="LEGACY_CLASSIFICATION"):
        recover_storage(layout=layout, catalog=Catalog(layout.catalog))

    authority.write_text(
        json.dumps(
            {
                "schema": "legacy-reconnect-classification.v2",
                "classifications": [
                    {
                        "gap_id": "genuine-g1",
                        "market": "um_perpetual",
                        "stream": "book_ticker",
                        "chunk_id": "unknown-chunk",
                        "seal_intent_sha256": "a" * 64,
                        "classification": "extension_orphan",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(RecoveryConflictError, match="LEGACY_CLASSIFICATION"):
        recover_storage(layout=layout, catalog=Catalog(layout.catalog))

    authority.write_text(
        json.dumps(
            {
                "schema": "legacy-reconnect-classification.v3",
                "classifications": [
                    {
                        "gap_id": "genuine-g1",
                        "market": "um_perpetual",
                        "stream": "book_ticker",
                        "chunk_id": "unknown-chunk",
                        "classification_evidence_sha256": "a" * 64,
                        "classification": "unknown-kind",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(RecoveryConflictError, match="LEGACY_CLASSIFICATION"):
        recover_storage(layout=layout, catalog=Catalog(layout.catalog))

    authority.write_text(
        json.dumps(
            {
                "schema": "legacy-reconnect-classification.v1",
                "classifications": [],
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(RecoveryConflictError, match="LEGACY_CLASSIFICATION"):
        recover_storage(layout=layout, catalog=Catalog(layout.catalog))

    authority.write_text(
        json.dumps(
            {
                "schema": "legacy-reconnect-classification.v3",
                "classifications": [
                    {
                        "gap_id": "genuine-g1",
                        "market": "um_perpetual",
                        "stream": "book_ticker",
                        "chunk_id": "unknown-chunk",
                        "classification_evidence_sha256": "not-a-digest",
                        "classification": "extension_orphan",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(RecoveryConflictError, match="LEGACY_CLASSIFICATION"):
        recover_storage(layout=layout, catalog=Catalog(layout.catalog))


def test_closed_interval_history_loaded_once_per_pass(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """R3.1-006 (P2): startup recovery builds the CLOSED interval index once
    per recovery pass, never once per candidate intent (no O(K x E)
    rebuild)."""
    from binance_market_data_recorder.storage import catalog as catalog_module

    layout = ensure_storage_layout(tmp_path)
    orphan_chunk_ids: list[str] = []
    with Catalog(layout.catalog) as catalog:
        parent = _durable_intent(
            gap_id="parent-g1",
            reason="unexpected_disconnect",
            connection_id="conn-parent",
            generation=0,
            started_at_utc_ns=1_000_000_000,
        )
        _record_gap(catalog, parent, completed=False)
        _record_gap(
            catalog,
            parent,
            completed=True,
            new_connection_id="conn-parent-2",
            new_generation=1,
            gap_ended_at_utc_ns=3_000_000_000,
        )
        for index in range(3):
            orphan = _durable_intent(
                gap_id=f"orphan-g{index}",
                reason="session_restart",
                connection_id=f"conn-attempt-{index}",
                generation=1,
                started_at_utc_ns=2_000_000_000,
            )
            orphan_chunk_id = _seal_zero_record_marker(
                tmp_path, catalog, orphan, stream=orphan["stream"]
            )
            orphan_chunk_ids.append(orphan_chunk_id)
    _write_classification_authority(
        tmp_path,
        [
            _orphan_classification_entry(
                tmp_path,
                orphan_chunk_ids[index],
                classification="extension_orphan",
            )
            for index in range(3)
        ],
    )

    calls = 0
    original = catalog_module.Catalog.closed_stream_discontinuity_intervals_by_stream

    def counting(
        catalog_self: Any, **kwargs: Any
    ) -> dict[tuple[str, str], list[dict[str, Any]]]:
        nonlocal calls
        calls += 1
        return original(catalog_self, **kwargs)

    monkeypatch.setattr(
        catalog_module.Catalog,
        "closed_stream_discontinuity_intervals_by_stream",
        counting,
    )

    recovered = Catalog(layout.catalog)
    actions = recover_storage(layout=layout, catalog=recovered)
    recovered.close()
    assert calls == 1
    assert sum(
        1
        for action in actions
        if action.action == "extension_orphan_ignored"
    ) == 3


def test_legacy_orphan_with_open_parent_is_ignored_not_conflict(
    tmp_path: Path,
) -> None:
    """TEST-008b: an orphan marker intent whose parent gap is still OPEN is
    an extension of that parent (zero-record marker, next generation, later
    timestamp) and must be ignored, not turned into a hard conflict."""
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        parent = _durable_intent(
            gap_id="parent-g1",
            reason="unexpected_disconnect",
            connection_id="conn-parent",
            generation=0,
            started_at_utc_ns=1_000_000_000,
        )
        _record_gap(catalog, parent, completed=False)
        orphan = _durable_intent(
            gap_id="orphan-g2",
            reason="session_restart",
            connection_id="conn-attempt",
            generation=1,
            started_at_utc_ns=2_000_000_000,
        )
        _seal_zero_record_marker(
            tmp_path, catalog, orphan, stream=orphan["stream"]
        )

    recovered = Catalog(layout.catalog)
    actions = recover_storage(layout=layout, catalog=recovered)
    recovered.close()
    assert any(
        action.action == "extension_orphan_ignored"
        and action.detail == "orphan-g2"
        for action in actions
    )
    with Catalog(layout.catalog, read_only=True) as catalog:
        started = _started_events(catalog)
        assert [event["evidence"]["gap_id"] for event in started] == [
            "parent-g1"
        ]
        open_gaps = catalog.unclosed_stream_discontinuities(
            market="um_perpetual", stream="book_ticker"
        )
        assert [
            cast(dict[str, Any], event["evidence"])["gap_id"]
            for event in open_gaps
        ] == ["parent-g1"]


def test_open_parent_with_frame_bearing_intent_still_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """TEST-009: an ABSENT intent with an OPEN parent gap whose marker chunk
    carried frames cannot be an extension (extensions never persist frames);
    the genuine ambiguity must stay a hard RecoveryConflictError."""
    from tests.integration.test_reconnect_boundary_integrity import (
        _seal_chunk_with_intent,
    )

    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        parent = _durable_intent(
            gap_id="parent-g1",
            reason="unexpected_disconnect",
            connection_id="conn-parent",
            generation=0,
            started_at_utc_ns=1_000_000_000,
        )
        _record_gap(catalog, parent, completed=False)
        orphan = _durable_intent(
            gap_id="orphan-g2",
            reason="session_restart",
            connection_id="conn-attempt",
            generation=1,
            started_at_utc_ns=2_000_000_000,
        )
        _seal_chunk_with_intent(
            layout,
            catalog,
            monkeypatch,
            intent=orphan,
            frame_payload=book_ticker(7),
        )

    with pytest.raises(RecoveryConflictError):
        recover_storage(layout=layout, catalog=Catalog(layout.catalog))


def test_legitimate_marker_intent_after_closed_gap_still_materializes(
    tmp_path: Path,
) -> None:
    """TEST-008c: a genuine versioned (R3.3+) intent-only crash whose
    timestamp follows (and is not contained by) the stream's closed gap
    history must still materialize exactly one STARTED."""
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        older = _durable_intent(
            gap_id="older-g1",
            reason="unexpected_disconnect",
            connection_id="conn-older",
            generation=0,
            started_at_utc_ns=1_000_000_000,
        )
        _record_gap(catalog, older, completed=False)
        _record_gap(
            catalog,
            older,
            completed=True,
            new_connection_id="conn-older-2",
            new_generation=1,
            gap_ended_at_utc_ns=3_000_000_000,
        )
        genuine = _durable_intent(
            gap_id="genuine-g2",
            reason="planned_rotation",
            connection_id="conn-genuine",
            generation=2,
            started_at_utc_ns=5_000_000_000,
            intent_schema="reconnect-seal-intent.v2",
        )
        _seal_zero_record_marker(
            tmp_path, catalog, genuine, stream=genuine["stream"]
        )

    recovered = Catalog(layout.catalog)
    actions = recover_storage(layout=layout, catalog=recovered)
    recovered.close()
    assert any(
        action.action == "pending_discontinuity_materialized"
        and action.detail == "genuine-g2"
        for action in actions
    )
    with Catalog(layout.catalog, read_only=True) as catalog:
        started = _started_events(catalog)
        assert [event["evidence"]["gap_id"] for event in started] == [
            "older-g1",
            "genuine-g2",
        ]


def test_extension_orphan_generation_mismatch_still_materializes_legit_crash(
    tmp_path: Path,
) -> None:
    """TEST-009b: the containment rule requires the orphan's generation to
    equal the parent's replacement generation.  A genuine versioned R3.3+
    crash intent whose generation does not match never risks being
    suppressed."""
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        parent = _durable_intent(
            gap_id="parent-g1",
            reason="unexpected_disconnect",
            connection_id="conn-parent",
            generation=0,
            started_at_utc_ns=1_000_000_000,
        )
        _record_gap(catalog, parent, completed=False)
        _record_gap(
            catalog,
            parent,
            completed=True,
            new_connection_id="conn-parent-2",
            new_generation=1,
            gap_ended_at_utc_ns=3_000_000_000,
        )
        genuine = _durable_intent(
            gap_id="genuine-g2",
            reason="planned_rotation",
            connection_id="conn-genuine",
            generation=5,
            started_at_utc_ns=2_000_000_000,
            intent_schema="reconnect-seal-intent.v2",
        )
        _seal_zero_record_marker(
            tmp_path, catalog, genuine, stream=genuine["stream"]
        )

    recovered = Catalog(layout.catalog)
    actions = recover_storage(layout=layout, catalog=recovered)
    recovered.close()
    assert any(
        action.action == "pending_discontinuity_materialized"
        and action.detail == "genuine-g2"
        for action in actions
    )


# ---------------------------------------------------------------------------
# M21.4.11-R3.2: exhaustive legacy partition, no UTC gate, strongly bound
# authority, deterministic read-only preflight.
# ---------------------------------------------------------------------------


def test_orphan_outside_parent_wall_interval_fails_closed(
    tmp_path: Path,
) -> None:
    """R3.2-001 (P1-001): a true pre-R3 extension orphan whose wall
    timestamp falls OUTSIDE the parent's numeric interval (wall clock
    stepped backward before the extension) must never materialize a phantom
    STARTED merely because no UTC-contained interval exists.

    Without an explicit classification startup fails closed (AMBIGUOUS)."""
    layout = ensure_storage_layout(tmp_path)
    orphan_chunk_id = ""
    with Catalog(layout.catalog) as catalog:
        parent = _durable_intent(
            gap_id="parent-g1",
            reason="unexpected_disconnect",
            connection_id="conn-parent",
            generation=0,
            started_at_utc_ns=1_000_000_000,
        )
        _record_gap(catalog, parent, completed=False)
        _record_gap(
            catalog,
            parent,
            completed=True,
            new_connection_id="conn-parent-2",
            new_generation=1,
            gap_ended_at_utc_ns=3_000_000_000,
        )
        orphan = _durable_intent(
            gap_id="orphan-g2",
            reason="session_restart",
            connection_id="conn-attempt",
            generation=1,
            started_at_utc_ns=500_000_000,
        )
        orphan_chunk_id = _seal_zero_record_marker(
            tmp_path, catalog, orphan, stream=orphan["stream"]
        )

    with pytest.raises(RecoveryConflictError, match="RECOVERY_LEGACY_ORPHAN_AMBIGUOUS"):
        recover_storage(layout=layout, catalog=Catalog(layout.catalog))

    _write_classification_authority(
        tmp_path,
        [
            _orphan_classification_entry(
                tmp_path,
                orphan_chunk_id,
                classification="extension_orphan",
            )
        ],
    )
    recovered = Catalog(layout.catalog)
    actions = recover_storage(layout=layout, catalog=recovered)
    recovered.close()
    assert any(
        action.action == "extension_orphan_ignored"
        and action.detail == "orphan-g2"
        for action in actions
    )
    assert not any(
        action.action == "pending_discontinuity_materialized"
        for action in actions
    )


def test_inverted_wall_clock_parent_fails_closed(
    tmp_path: Path,
) -> None:
    """R3.2-002 (P1-001): a CLOSED lifecycle pair whose COMPLETED wall
    timestamp is not after its STARTED wall timestamp remains exact
    lifecycle authority.  A zero-frame legacy candidate whose generation
    matches that parent must stay AMBIGUOUS, never become automatically
    materializable because an interval builder dropped the inverted pair."""
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        parent = _durable_intent(
            gap_id="parent-g1",
            reason="unexpected_disconnect",
            connection_id="conn-parent",
            generation=0,
            started_at_utc_ns=3_000_000_000,
        )
        _record_gap(catalog, parent, completed=False)
        _record_gap(
            catalog,
            parent,
            completed=True,
            new_connection_id="conn-parent-2",
            new_generation=1,
            gap_ended_at_utc_ns=2_000_000_000,
        )
        orphan = _durable_intent(
            gap_id="orphan-g2",
            reason="session_restart",
            connection_id="conn-attempt",
            generation=1,
            started_at_utc_ns=2_500_000_000,
        )
        _seal_zero_record_marker(
            tmp_path, catalog, orphan, stream=orphan["stream"]
        )

    with pytest.raises(RecoveryConflictError, match="RECOVERY_LEGACY_ORPHAN_AMBIGUOUS"):
        recover_storage(layout=layout, catalog=Catalog(layout.catalog))


def test_authority_cannot_override_frame_bearing_legitimacy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """R3.2-003 (P1-002 TEST-A): verified_frames > 0 is durable proof of a
    legitimate boundary; an ``extension_orphan`` classification must never
    override it.  Startup fails closed instead of ignoring."""
    from tests.integration.test_reconnect_boundary_integrity import (
        _seal_chunk_with_intent,
    )

    layout = ensure_storage_layout(tmp_path)
    chunk_id = ""
    with Catalog(layout.catalog) as catalog:
        parent = _durable_intent(
            gap_id="parent-g1",
            reason="unexpected_disconnect",
            connection_id="conn-parent",
            generation=0,
            started_at_utc_ns=1_000_000_000,
        )
        _record_gap(catalog, parent, completed=False)
        _record_gap(
            catalog,
            parent,
            completed=True,
            new_connection_id="conn-parent-2",
            new_generation=1,
            gap_ended_at_utc_ns=3_000_000_000,
        )
        genuine = _durable_intent(
            gap_id="genuine-g2",
            reason="planned_rotation",
            connection_id="conn-attempt",
            generation=1,
            started_at_utc_ns=2_000_000_000,
        )
        chunk_id = _seal_chunk_with_intent(
            layout,
            catalog,
            monkeypatch,
            intent=genuine,
            frame_payload=book_ticker(7),
        )
    _write_classification_authority(
        tmp_path,
        [
            _orphan_classification_entry(
                tmp_path, chunk_id, classification="extension_orphan"
            )
        ],
    )

    with pytest.raises(RecoveryConflictError, match="RECOVERY_LEGACY_PREDECISION"):
        recover_storage(layout=layout, catalog=Catalog(layout.catalog))


def test_authority_cannot_override_completing_connection_legitimacy(
    tmp_path: Path,
) -> None:
    """R3.2-004 (P1-002 TEST-B): the completing-connection identity proof
    makes the intent legitimate; an ``extension_orphan`` classification
    must never override it."""
    layout = ensure_storage_layout(tmp_path)
    chunk_id = ""
    with Catalog(layout.catalog) as catalog:
        parent = _durable_intent(
            gap_id="parent-g1",
            reason="unexpected_disconnect",
            connection_id="conn-parent",
            generation=0,
            started_at_utc_ns=1_000_000_000,
        )
        _record_gap(catalog, parent, completed=False)
        _record_gap(
            catalog,
            parent,
            completed=True,
            new_connection_id="conn-parent-2",
            new_generation=1,
            gap_ended_at_utc_ns=3_000_000_000,
        )
        genuine = _durable_intent(
            gap_id="genuine-g2",
            reason="planned_rotation",
            connection_id="conn-parent-2",
            generation=1,
            started_at_utc_ns=2_000_000_000,
        )
        chunk_id = _seal_zero_record_marker(
            tmp_path, catalog, genuine, stream=genuine["stream"]
        )
    _write_classification_authority(
        tmp_path,
        [
            _orphan_classification_entry(
                tmp_path, chunk_id, classification="extension_orphan"
            )
        ],
    )

    with pytest.raises(RecoveryConflictError, match="RECOVERY_LEGACY_PREDECISION"):
        recover_storage(layout=layout, catalog=Catalog(layout.catalog))


def test_authority_cannot_override_proven_open_parent_extension(
    tmp_path: Path,
) -> None:
    """R3.2-005 (P1-002 TEST-C): the OPEN-parent extension shape is proven
    from durable identity alone; a ``legitimate_req103`` classification must
    never override it into a second STARTED."""
    layout = ensure_storage_layout(tmp_path)
    chunk_id = ""
    with Catalog(layout.catalog) as catalog:
        parent = _durable_intent(
            gap_id="parent-g1",
            reason="unexpected_disconnect",
            connection_id="conn-parent",
            generation=0,
            started_at_utc_ns=1_000_000_000,
        )
        _record_gap(catalog, parent, completed=False)
        orphan = _durable_intent(
            gap_id="orphan-g2",
            reason="session_restart",
            connection_id="conn-attempt",
            generation=1,
            started_at_utc_ns=2_000_000_000,
        )
        chunk_id = _seal_zero_record_marker(
            tmp_path, catalog, orphan, stream=orphan["stream"]
        )
    _write_classification_authority(
        tmp_path,
        [
            _orphan_classification_entry(
                tmp_path, chunk_id, classification="legitimate_req103"
            )
        ],
    )

    with pytest.raises(RecoveryConflictError, match="RECOVERY_LEGACY_PREDECISION"):
        recover_storage(layout=layout, catalog=Catalog(layout.catalog))


def test_authority_binding_wrong_chunk_rejected(tmp_path: Path) -> None:
    """R3.2-006: an authority entry whose chunk_id does not match the
    persisted intent chunk is stale and fails closed."""
    _parent, _orphan, _chunk_id, _digest = _legacy_orphan_fixture(tmp_path)
    _write_classification_authority(
        tmp_path,
        [
            {
                "gap_id": "orphan-g2",
                "market": "um_perpetual",
                "stream": "book_ticker",
                "chunk_id": "00000000-0000-0000-0000-000000000000",
                "classification_evidence_sha256": "a" * 64,
                "classification": "extension_orphan",
            }
        ],
    )
    with pytest.raises(RecoveryConflictError, match="RECOVERY_LEGACY_PREDECISION"):
        recover_storage(
            layout=ensure_storage_layout(tmp_path),
            catalog=Catalog(ensure_storage_layout(tmp_path).catalog),
        )


def test_authority_binding_wrong_digest_rejected(tmp_path: Path) -> None:
    """R3.2-007: an authority entry whose digest does not match the exact
    persisted seal intent is stale and fails closed."""
    _parent, _orphan, chunk_id, _digest = _legacy_orphan_fixture(tmp_path)
    _write_classification_authority(
        tmp_path,
        [
            {
                "gap_id": "orphan-g2",
                "market": "um_perpetual",
                "stream": "book_ticker",
                "chunk_id": chunk_id,
                "classification_evidence_sha256": "b" * 64,
                "classification": "extension_orphan",
            }
        ],
    )
    with pytest.raises(RecoveryConflictError, match="RECOVERY_LEGACY_PREDECISION"):
        recover_storage(
            layout=ensure_storage_layout(tmp_path),
            catalog=Catalog(ensure_storage_layout(tmp_path).catalog),
        )


def test_authority_duplicate_binding_rejected(tmp_path: Path) -> None:
    """R3.2-008: two entries binding the same intent (one with a different
    digest) conflict and fail closed at authority load."""
    _parent, _orphan, chunk_id, digest = _legacy_orphan_fixture(tmp_path)
    _write_classification_authority(
        tmp_path,
        [
            {
                "gap_id": "orphan-g2",
                "market": "um_perpetual",
                "stream": "book_ticker",
                "chunk_id": chunk_id,
                "classification_evidence_sha256": digest,
                "classification": "extension_orphan",
            },
            {
                "gap_id": "orphan-g2",
                "market": "um_perpetual",
                "stream": "book_ticker",
                "chunk_id": chunk_id,
                "classification_evidence_sha256": "c" * 64,
                "classification": "legitimate_req103",
            },
        ],
    )
    with pytest.raises(RecoveryConflictError, match="LEGACY_CLASSIFICATION"):
        recover_storage(
            layout=ensure_storage_layout(tmp_path),
            catalog=Catalog(ensure_storage_layout(tmp_path).catalog),
        )


def test_authority_unmatched_entry_makes_startup_ineligible(
    tmp_path: Path,
) -> None:
    """R3.2-009: an authority entry with no corresponding candidate (no such
    intent exists) makes startup ineligible: authority must never silently
    classify records it does not bind."""
    _legacy_orphan_fixture(tmp_path)
    layout = ensure_storage_layout(tmp_path)
    from binance_market_data_recorder.spool.legacy_reconnect import (
        classification_evidence_sha256,
    )

    unused_intent = _durable_intent(
        gap_id="never-persisted-g1",
        reason="planned_rotation",
        connection_id="conn-nowhere",
        generation=0,
        started_at_utc_ns=1,
    )
    digest = classification_evidence_sha256(
        chunk_id="11111111-2222-3333-4444-555555555555",
        intent=unused_intent,
        verified_frames=0,
    )
    _write_classification_authority(
        tmp_path,
        [
            {
                "gap_id": "never-persisted-g1",
                "market": "um_perpetual",
                "stream": "book_ticker",
                "chunk_id": "11111111-2222-3333-4444-555555555555",
                "classification_evidence_sha256": digest,
                "classification": "extension_orphan",
            }
        ],
    )
    with pytest.raises(RecoveryConflictError, match="RECOVERY_LEGACY_PREDECISION"):
        recover_storage(layout=layout, catalog=Catalog(layout.catalog))


def test_startup_does_not_partially_mutate_before_ambiguity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """R3.2-010 (P1-003): startup performs the full legacy pre-decision
    before ANY legacy lifecycle mutation.  A proven-legitimate candidate A
    plus an unresolved ambiguous candidate B must fail startup without
    materializing A's STARTED."""
    from tests.integration.test_reconnect_boundary_integrity import (
        _seal_chunk_with_intent,
    )

    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        parent = _durable_intent(
            gap_id="parent-g1",
            reason="unexpected_disconnect",
            connection_id="conn-parent",
            generation=0,
            started_at_utc_ns=1_000_000_000,
        )
        _record_gap(catalog, parent, completed=False)
        _record_gap(
            catalog,
            parent,
            completed=True,
            new_connection_id="conn-parent-2",
            new_generation=1,
            gap_ended_at_utc_ns=3_000_000_000,
        )
        legitimate = _durable_intent(
            gap_id="legit-a",
            reason="planned_rotation",
            connection_id="conn-legit",
            generation=7,
            started_at_utc_ns=5_000_000_000,
        )
        _seal_chunk_with_intent(
            layout,
            catalog,
            monkeypatch,
            intent=legitimate,
            frame_payload=book_ticker(7),
        )
        ambiguous = _durable_intent(
            gap_id="ambig-b",
            reason="session_restart",
            connection_id="conn-attempt",
            generation=1,
            started_at_utc_ns=2_000_000_000,
        )
        _seal_zero_record_marker(
            tmp_path, catalog, ambiguous, stream=ambiguous["stream"]
        )

    with pytest.raises(RecoveryConflictError, match="RECOVERY_LEGACY_PREDECISION"):
        recover_storage(layout=layout, catalog=Catalog(layout.catalog))

    with Catalog(layout.catalog, read_only=True) as catalog:
        started = _started_events(catalog)
        assert [event["evidence"]["gap_id"] for event in started] == [
            "parent-g1"
        ]


def test_preflight_enumerates_all_legacy_candidates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """R3.2-011 (P1-003): the read-only preflight enumerates every legacy
    candidate regardless of UTC shape and reports deterministic counts."""
    from binance_market_data_recorder.spool.legacy_reconnect import (
        LegacyClassificationAuthority,
        evaluate_legacy_reconnect_decisions,
    )

    layout = ensure_storage_layout(tmp_path)
    chunk_ids: dict[str, str] = {}
    with Catalog(layout.catalog) as catalog:
        parent = _durable_intent(
            gap_id="parent-g1",
            reason="unexpected_disconnect",
            connection_id="conn-parent",
            generation=0,
            started_at_utc_ns=1_000_000_000,
        )
        _record_gap(catalog, parent, completed=False)
        _record_gap(
            catalog,
            parent,
            completed=True,
            new_connection_id="conn-parent-2",
            new_generation=1,
            gap_ended_at_utc_ns=3_000_000_000,
        )
        contained = _durable_intent(
            gap_id="orphan-contained",
            reason="session_restart",
            connection_id="conn-attempt-1",
            generation=1,
            started_at_utc_ns=2_000_000_000,
        )
        chunk_ids["contained"] = _seal_zero_record_marker(
            tmp_path, catalog, contained, stream=contained["stream"]
        )
        outside = _durable_intent(
            gap_id="orphan-outside",
            reason="session_restart",
            connection_id="conn-attempt-2",
            generation=1,
            started_at_utc_ns=500_000_000,
        )
        chunk_ids["outside"] = _seal_zero_record_marker(
            tmp_path, catalog, outside, stream=outside["stream"]
        )
        from tests.integration.test_reconnect_boundary_integrity import (
            _seal_chunk_with_intent,
        )

        legit_frames = _durable_intent(
            gap_id="legit-frames",
            reason="planned_rotation",
            connection_id="conn-legit-1",
            generation=9,
            started_at_utc_ns=5_000_000_000,
            stream="agg_trade",
        )
        _seal_chunk_with_intent(
            layout,
            catalog,
            monkeypatch,
            intent=legit_frames,
            frame_payload=book_ticker(7),
            stream=UsdMStream.AGG_TRADE,
        )
        legit_completing = _durable_intent(
            gap_id="legit-completing",
            reason="planned_rotation",
            connection_id="conn-parent-2",
            generation=1,
            started_at_utc_ns=2_500_000_000,
        )
        _seal_zero_record_marker(
            tmp_path, catalog, legit_completing, stream=legit_completing["stream"]
        )

    with Catalog(layout.catalog, read_only=True) as catalog:
        authority = LegacyClassificationAuthority.load(
            layout.root / LEGACY_CLASSIFICATION_FILENAME
        )
        report = evaluate_legacy_reconnect_decisions(
            catalog=catalog, authority=authority
        )
    public = report.public_dict()
    assert public["candidate_count"] == 4
    assert public["proven_legitimate_count"] == 2
    assert public["ambiguous_count"] == 2
    assert public["unclassified_ambiguous_count"] == 2
    assert public["first_corrected_startup_eligible"] is False
    candidates = cast(list[dict[str, Any]], public["candidates"])
    assert {item["gap_id"] for item in candidates} == {
        "orphan-contained",
        "orphan-outside",
        "legit-frames",
        "legit-completing",
    }


def test_preflight_and_startup_share_one_decision_engine(
    tmp_path: Path,
) -> None:
    """R3.2-012: preflight and startup use the same decision engine.  A
    fully classified fixture produces identical decisions in the read-only
    report and in startup execution."""
    from binance_market_data_recorder.spool.legacy_reconnect import (
        LegacyClassificationAuthority,
        evaluate_legacy_reconnect_decisions,
    )

    _parent, _orphan, orphan_chunk_id, _digest = _legacy_orphan_fixture(tmp_path)
    _write_classification_authority(
        tmp_path,
        [
            _orphan_classification_entry(
                tmp_path,
                orphan_chunk_id,
                classification="extension_orphan",
            )
        ],
    )
    layout = ensure_storage_layout(tmp_path)

    with Catalog(layout.catalog, read_only=True) as catalog:
        report = evaluate_legacy_reconnect_decisions(
            catalog=catalog,
            authority=LegacyClassificationAuthority.load(
                layout.root / LEGACY_CLASSIFICATION_FILENAME
            ),
        )
    assert report.first_corrected_startup_eligible is True
    finals = {
        decision.candidate.gap_id: decision.final
        for decision in report.decisions
    }
    assert finals["orphan-g2"] == "classified_extension_orphan"

    recovered = Catalog(layout.catalog)
    actions = recover_storage(layout=layout, catalog=recovered)
    recovered.close()
    assert any(
        action.action == "extension_orphan_ignored"
        and action.detail == "orphan-g2"
        for action in actions
    )
    assert not any(
        action.action == "pending_discontinuity_materialized"
        for action in actions
    )


def test_preflight_is_read_only(tmp_path: Path) -> None:
    """R3.2-013: the preflight mutates neither the Catalog nor the authority
    file nor any artifact."""
    from binance_market_data_recorder.spool.legacy_reconnect import (
        LegacyClassificationAuthority,
        evaluate_legacy_reconnect_decisions,
    )

    _legacy_orphan_fixture(tmp_path)
    layout = ensure_storage_layout(tmp_path)
    before = (layout.catalog).read_bytes()
    with Catalog(layout.catalog, read_only=True) as catalog:
        authority = LegacyClassificationAuthority.load(
            layout.root / LEGACY_CLASSIFICATION_FILENAME
        )
        report = evaluate_legacy_reconnect_decisions(
            catalog=catalog, authority=authority
        )
    assert report.first_corrected_startup_eligible is False
    assert (layout.catalog).read_bytes() == before
    assert not (layout.root / "legacy_reconnect_classifications.json").exists()


def test_preflight_cli_is_machine_readable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """R3.2-014: the CLI exposes the preflight as deterministic
    machine-readable JSON and never mutates the data root."""
    import binance_market_data_recorder.cli as cli_module

    _parent, _orphan, orphan_chunk_id, _digest = _legacy_orphan_fixture(tmp_path)
    _write_classification_authority(
        tmp_path,
        [
            _orphan_classification_entry(
                tmp_path,
                orphan_chunk_id,
                classification="extension_orphan",
            )
        ],
    )
    monkeypatch.setenv("BINANCE_MARKET_RECORDER_DATA_ROOT", str(tmp_path))
    catalog_path = tmp_path / "state" / "catalog.sqlite"
    before = catalog_path.read_bytes()

    assert (
        cli_module.main(
            ["recovery", "legacy-reconnect-preflight"]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema_version"] == "legacy-reconnect-preflight.v1"
    assert payload["first_corrected_startup_eligible"] is True
    assert payload["candidate_count"] == 1
    assert catalog_path.read_bytes() == before


def test_preflight_complete_inventory_all_ten_shapes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """R3.2-015 (P1-003): one fixture containing every required legacy
    shape; the read-only preflight enumerates ALL of them with exact
    deterministic counts.  No candidate may disappear because of its
    wall-clock shape."""
    from binance_market_data_recorder.spool.legacy_reconnect import (
        LegacyClassificationAuthority,
        evaluate_legacy_reconnect_decisions,
    )
    from tests.integration.test_reconnect_boundary_integrity import (
        _seal_chunk_with_intent,
    )

    layout = ensure_storage_layout(tmp_path)
    chunk_ids: dict[str, str] = {}
    with Catalog(layout.catalog) as catalog:
        parent = _durable_intent(
            gap_id="parent-g1",
            reason="unexpected_disconnect",
            connection_id="conn-parent",
            generation=0,
            started_at_utc_ns=1_000_000_000,
        )
        _record_gap(catalog, parent, completed=False)
        _record_gap(
            catalog,
            parent,
            completed=True,
            new_connection_id="conn-parent-2",
            new_generation=1,
            gap_ended_at_utc_ns=3_000_000_000,
        )
        inverted = _durable_intent(
            gap_id="parent-inverted",
            reason="unexpected_disconnect",
            connection_id="conn-inv",
            generation=0,
            started_at_utc_ns=3_000_000_000,
        )
        _record_gap(catalog, inverted, completed=False)
        _record_gap(
            catalog,
            inverted,
            completed=True,
            new_connection_id="conn-inv-2",
            new_generation=1,
            gap_ended_at_utc_ns=2_000_000_000,
        )
        contained = _durable_intent(
            gap_id="shape-contained-orphan",
            reason="session_restart",
            connection_id="conn-attempt-contained",
            generation=1,
            started_at_utc_ns=2_000_000_000,
        )
        chunk_ids["contained"] = _seal_zero_record_marker(
            tmp_path, catalog, contained, stream=contained["stream"]
        )
        outside = _durable_intent(
            gap_id="shape-outside-orphan",
            reason="session_restart",
            connection_id="conn-attempt-outside",
            generation=1,
            started_at_utc_ns=500_000_000,
        )
        chunk_ids["outside"] = _seal_zero_record_marker(
            tmp_path, catalog, outside, stream=outside["stream"]
        )
        inverted_orphan = _durable_intent(
            gap_id="shape-inverted-parent-orphan",
            reason="session_restart",
            connection_id="conn-attempt-inverted",
            generation=1,
            started_at_utc_ns=2_500_000_000,
        )
        _seal_zero_record_marker(
            tmp_path, catalog, inverted_orphan, stream=inverted_orphan["stream"]
        )
        completing = _durable_intent(
            gap_id="shape-completing-connection",
            reason="planned_rotation",
            connection_id="conn-parent-2",
            generation=1,
            started_at_utc_ns=2_500_000_000,
        )
        _seal_zero_record_marker(
            tmp_path, catalog, completing, stream=completing["stream"]
        )
        reuse_parent = _durable_intent(
            gap_id="reuse-parent",
            reason="unexpected_disconnect",
            connection_id="conn-reuse-parent",
            generation=0,
            started_at_utc_ns=1_000_000_000,
            stream="mark_price",
        )
        _record_gap(catalog, reuse_parent, completed=False)
        _record_gap(
            catalog,
            reuse_parent,
            completed=True,
            new_connection_id="conn-reuse-parent-2",
            new_generation=1,
            gap_ended_at_utc_ns=3_000_000_000,
        )
        reuse = _durable_intent(
            gap_id="shape-generation-reuse",
            reason="planned_rotation",
            connection_id="conn-new-process",
            generation=1,
            started_at_utc_ns=2_500_000_000,
            stream="mark_price",
        )
        chunk_ids["reuse"] = _seal_zero_record_marker(
            tmp_path, catalog, reuse, stream=reuse["stream"]
        )
        frames = _durable_intent(
            gap_id="shape-frame-bearing",
            reason="planned_rotation",
            connection_id="conn-legit-frames",
            generation=9,
            started_at_utc_ns=5_000_000_000,
            stream="agg_trade",
        )
        _seal_chunk_with_intent(
            layout,
            catalog,
            monkeypatch,
            intent=frames,
            frame_payload=book_ticker(7),
            stream=UsdMStream.AGG_TRADE,
        )
        open_parent = _durable_intent(
            gap_id="shape-open-parent",
            reason="unexpected_disconnect",
            connection_id="conn-open",
            generation=0,
            started_at_utc_ns=1_000_000_000,
            stream="diff_depth",
        )
        _record_gap(catalog, open_parent, completed=False)
        competing = _durable_intent(
            gap_id="shape-open-conflict",
            reason="planned_rotation",
            connection_id="conn-open-attempt",
            generation=2,
            started_at_utc_ns=2_000_000_000,
            stream="diff_depth",
        )
        _seal_chunk_with_intent(
            layout,
            catalog,
            monkeypatch,
            intent=competing,
            frame_payload=book_ticker(8),
            stream=UsdMStream.DIFF_DEPTH,
        )

    _write_classification_authority(
        tmp_path,
        [
            _orphan_classification_entry(
                tmp_path,
                chunk_ids["contained"],
                classification="extension_orphan",
            ),
            _orphan_classification_entry(
                tmp_path,
                chunk_ids["reuse"],
                classification="legitimate_req103",
            ),
            {
                "gap_id": "shape-outside-orphan",
                "market": "um_perpetual",
                "stream": "book_ticker",
                "chunk_id": chunk_ids["outside"],
                "classification_evidence_sha256": "d" * 64,
                "classification": "extension_orphan",
            },
        ],
    )

    with Catalog(layout.catalog, read_only=True) as catalog:
        report = evaluate_legacy_reconnect_decisions(
            catalog=catalog,
            authority=LegacyClassificationAuthority.load(
                layout.root / LEGACY_CLASSIFICATION_FILENAME
            ),
        )
    public = report.public_dict()
    candidates = cast(list[dict[str, Any]], public["candidates"])
    assert {item["gap_id"] for item in candidates} == {
        "shape-contained-orphan",
        "shape-outside-orphan",
        "shape-inverted-parent-orphan",
        "shape-generation-reuse",
        "shape-completing-connection",
        "shape-frame-bearing",
        "shape-open-conflict",
    }
    assert public["candidate_count"] == 7
    assert public["proven_legitimate_count"] == 2
    assert public["proven_extension_count"] == 0
    assert public["ambiguous_count"] == 4
    assert public["classified_ambiguous_count"] == 2
    assert public["unclassified_ambiguous_count"] == 2
    assert public["stale_authority_count"] == 1
    assert public["unmatched_authority_count"] == 0
    assert public["contradiction_count"] == 0
    assert public["conflict_count"] == 1
    assert public["first_corrected_startup_eligible"] is False
    decisions = {item["gap_id"]: item for item in candidates}
    assert decisions["shape-contained-orphan"]["final_decision"] == (
        "classified_extension_orphan"
    )
    assert decisions["shape-generation-reuse"]["final_decision"] == (
        "classified_legitimate_req103"
    )
    assert decisions["shape-completing-connection"]["final_decision"] == (
        "proven_legitimate"
    )
    assert decisions["shape-frame-bearing"]["final_decision"] == (
        "proven_legitimate"
    )
    assert decisions["shape-inverted-parent-orphan"]["automatic_decision"] == (
        "ambiguous"
    )
    assert decisions["shape-outside-orphan"]["automatic_decision"] == (
        "ambiguous"
    )
    assert decisions["shape-outside-orphan"]["authority_state"] == "STALE"
    assert decisions["shape-open-conflict"]["automatic_decision"] == "conflict"
