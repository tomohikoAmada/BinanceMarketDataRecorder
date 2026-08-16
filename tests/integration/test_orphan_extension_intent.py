"""M21.4.11-R3 P1-001: orphan reconnect seal intents must not exist.

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
for extension seal intents (with attempt-level metadata in ``extension``).
LEGACY_RECOVERY: already-persisted old orphan shapes (closed or still-open
parent gap) are resolved from durable identity evidence plus the explicit
operator-reviewed classification authority (M21.4.11-R3.1); UTC wall-clock
containment is never suppression authority, ambiguous shapes fail closed,
and the legitimate intent-only crash case (REQ-103) is preserved exactly.
COMPLEXITY: the CLOSED interval history is built once per recovery pass.

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
from binance_market_data_recorder.binance.usdm.websocket import (
    WebSocketConnection,
)
from binance_market_data_recorder.spool.recovery import (
    RecoveryConflictError,
    recover_storage,
)
from binance_market_data_recorder.spool.seal import RECONNECT_GAP_FLAG
from binance_market_data_recorder.spool.stream import StreamSpool
from binance_market_data_recorder.spool.writer import RotationPolicy
from binance_market_data_recorder.storage.catalog import Catalog
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
    """TEST-005/006: the REQ-103 crash case stays intact: a genuine marker
    intent with no lifecycle and no parent interval materializes exactly one
    STARTED, and repeated recovery never duplicates it."""
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
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Persist the production legacy orphan SHAPE deterministically.

    Parent gap CLOSED (generation 0 -> 1, completing connection
    ``conn-parent-2``); orphan intent with a freshly minted gap identity,
    the failed attempt connection, the parent's replacement generation, and
    a wall timestamp strictly inside the parent interval.
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
        _seal_zero_record_marker(
            root, catalog, orphan, stream=orphan["stream"]
        )
    return parent, orphan


def _write_classification_authority(
    root: Path, entries: list[dict[str, Any]]
) -> None:
    document = {
        "schema": "legacy-reconnect-classification.v1",
        "classifications": entries,
    }
    (root / "legacy_reconnect_classifications.json").write_text(
        json.dumps(document, indent=2) + "\n", encoding="utf-8"
    )


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
    (R3.1)."""
    _legacy_orphan_fixture(tmp_path)
    _write_classification_authority(
        tmp_path,
        [
            {
                "gap_id": "orphan-g2",
                "market": "um_perpetual",
                "stream": "book_ticker",
                "classification": "extension_orphan",
                "note": "proven pre-fix extension of parent-g1",
            }
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
        _seal_zero_record_marker(
            tmp_path, catalog, genuine, stream=genuine["stream"]
        )
    _write_classification_authority(
        tmp_path,
        [
            {
                "gap_id": "genuine-g2",
                "market": "um_perpetual",
                "stream": "book_ticker",
                "classification": "legitimate_req103",
                "note": "post-restart genuine boundary",
            }
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
                "schema": "legacy-reconnect-classification.v1",
                "classifications": [
                    {
                        "gap_id": "genuine-g1",
                        "market": "um_perpetual",
                        "stream": "book_ticker",
                        "classification": "unknown-kind",
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
            _seal_zero_record_marker(
                tmp_path, catalog, orphan, stream=orphan["stream"]
            )
    _write_classification_authority(
        tmp_path,
        [
            {
                "gap_id": f"orphan-g{index}",
                "market": "um_perpetual",
                "stream": "book_ticker",
                "classification": "extension_orphan",
                "note": "legacy extension of parent-g1",
            }
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
    """TEST-008c: a genuine intent-only crash whose timestamp follows (and is
    not contained by) the stream's closed gap history must still materialize
    exactly one STARTED."""
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
    equal the parent's replacement generation.  A genuine crash intent whose
    generation does not match never risks being suppressed."""
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
