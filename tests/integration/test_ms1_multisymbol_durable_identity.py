"""Focused MS1 migration and cross-symbol identity coverage."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import cast

import pytest

from binance_market_data_recorder.storage.catalog import (
    Catalog,
    CatalogStateError,
    stream_discontinuity_event_id,
)

TARGET_SYMBOLS = (
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "XRPUSDT",
    "DOGEUSDT",
    "SUIUSDT",
    "LINKUSDT",
)
LEGACY_SCHEMA = Path(__file__).parents[1] / "fixtures" / "pre_m22_catalog_schema.sql"


def _seed_legacy_catalog(
    path: Path,
    *,
    market: str = "spot",
    malformed: str | None = None,
    partial: bool = False,
) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.executescript(LEGACY_SCHEMA.read_text(encoding="utf-8"))
        started = {
            "gap_id": "legacy-gap-1",
            "market": market,
            "stream": "depth",
            "reason": "historical_disconnect",
            "gap_started_at_utc_ns": 100,
            "metadata": {"preserve": "this"},
        }
        completed = {
            **started,
            "gap_ended_at_utc_ns": 200,
            "new_connection_id": "legacy-connection-2",
        }
        second_started = {
            "gap_id": "legacy-gap-2",
            "market": market,
            "stream": "depth",
            "reason": "historical_rotation",
            "gap_started_at_utc_ns": 300,
            "original_generation": 9,
        }
        events = [
            (
                "legacy-started-1",
                "STREAM_DISCONTINUITY_STARTED",
                100,
                json.dumps(started),
            ),
            (
                "legacy-completed-1",
                "STREAM_DISCONTINUITY_COMPLETED",
                200,
                json.dumps(completed),
            ),
            (
                "legacy-started-2",
                "STREAM_DISCONTINUITY_STARTED",
                300,
                json.dumps(second_started),
            ),
            (
                "legacy-side-empty",
                "SIDE_DATA_EMPTY_RESPONSE",
                325,
                json.dumps(
                    {
                        "kind": "basis_5m",
                        "requested_start_timestamp": 325,
                    }
                ),
            ),
            ("legacy-service-event", "SERVICE_STARTED", 400, '{"value": 1}'),
        ]
        if malformed == "missing_gap_id":
            events[0] = (
                events[0][0],
                events[0][1],
                events[0][2],
                json.dumps({"market": market, "stream": "depth"}),
            )
        elif malformed == "invalid_json":
            events[0] = (events[0][0], events[0][1], events[0][2], "{not-json")
        elif malformed == "invalid_side_data_json":
            events[3] = (events[3][0], events[3][1], events[3][2], "{not-json")
        connection.executemany(
            "INSERT INTO operational_events(event_id, event_type, "
            "occurred_at_utc_ns, evidence_json) VALUES (?, ?, ?, ?)",
            events,
        )
        connection.execute(
            "INSERT INTO side_data_cursors(kind, last_persisted_period_timestamp, "
            "updated_at_utc_ns, source_retention_window, retention_window_ms) "
            "VALUES (?, ?, ?, ?, ?)",
            ("basis_5m", 123_000, 456_000, "latest_30_days", 30 * 24 * 60 * 60 * 1000),
        )
        intent = {
            "required_forced_flags": ["reconnect_gap"],
            "gap_id": "legacy-gap-2",
            "reason": "historical_rotation",
            "market": market,
            "stream": "depth",
            "original_connection_id": "legacy-connection-1",
            "original_generation": 8,
            "gap_started_at_utc_ns": 300,
        }
        connection.execute(
            "INSERT INTO chunk_transitions(chunk_id, from_state, to_state, "
            "occurred_at_utc_ns, evidence_json, idempotency_key) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                "legacy-sealed-chunk",
                "ACTIVE",
                "SEALING",
                350,
                json.dumps({"seal_intent": intent}),
                "legacy-sealing-transition",
            ),
        )
        if partial:
            connection.execute("ALTER TABLE operational_events ADD COLUMN symbol TEXT")
        connection.commit()
    finally:
        connection.close()


def _snapshot(path: Path) -> dict[str, list[tuple[object, ...]]]:
    connection = sqlite3.connect(path)
    try:
        cursor_columns = {
            str(row[1])
            for row in connection.execute("PRAGMA table_info(side_data_cursors)")
        }
        cursor_symbol = "symbol" if "symbol" in cursor_columns else "NULL"
        return {
            "events": connection.execute(
                "SELECT event_id, event_type, occurred_at_utc_ns, evidence_json "
                "FROM operational_events ORDER BY event_id"
            ).fetchall(),
            "cursors": connection.execute(
                f"SELECT kind, {cursor_symbol}, last_persisted_period_timestamp, "
                "updated_at_utc_ns, source_retention_window, retention_window_ms "
                "FROM side_data_cursors ORDER BY kind"
            ).fetchall(),
            "transitions": connection.execute(
                "SELECT transition_id, evidence_json FROM chunk_transitions "
                "ORDER BY transition_id"
            ).fetchall(),
        }
    finally:
        connection.close()


def _cursor_timestamp(catalog: Catalog, symbol: str) -> int:
    cursor = catalog.side_data_cursor("basis_5m", symbol)
    assert cursor is not None
    value = cursor["last_persisted_period_timestamp"]
    assert isinstance(value, int)
    return value


@pytest.mark.parametrize("market", ["spot", "um_perpetual"])
def test_legacy_catalog_migrates_to_btc_and_is_idempotent(
    tmp_path: Path, market: str
) -> None:
    path = tmp_path / "catalog.sqlite"
    _seed_legacy_catalog(path, market=market)
    before = _snapshot(path)

    with Catalog(path) as catalog:
        assert catalog.table_columns("operational_events") >= {
            "market",
            "symbol",
            "stream",
            "gap_id",
        }
        assert catalog.table_columns("side_data_cursors") >= {
            "kind",
            "symbol",
        }
        events = {str(event["event_id"]): event for event in catalog.operational_events()}
        assert events["legacy-started-1"]["occurred_at_utc_ns"] == 100
        started_evidence = cast(
            dict[str, object], events["legacy-started-1"]["evidence"]
        )
        assert started_evidence["symbol"] == "BTCUSDT"
        assert started_evidence["metadata"] == {
            "preserve": "this"
        }
        side_evidence = cast(
            dict[str, object], events["legacy-side-empty"]["evidence"]
        )
        assert side_evidence["symbol"] == "BTCUSDT"
        assert [
            cast(dict[str, object], event["evidence"])["gap_id"]
            for event in catalog.unclosed_stream_discontinuities(
                market=market, symbol="BTCUSDT", stream="depth"
            )
        ] == ["legacy-gap-2"]
        assert catalog.unclosed_stream_discontinuities(
            market=market, symbol="ETHUSDT", stream="depth"
        ) == []
        cursor = catalog.side_data_cursor("basis_5m", "BTCUSDT")
        assert cursor is not None
        assert cursor["last_persisted_period_timestamp"] == 123_000
        assert catalog.side_data_cursor("basis_5m", "ETHUSDT") is None

    first_migration = _snapshot(path)
    assert [row[:3] for row in first_migration["events"]] == [
        row[:3] for row in before["events"]
    ]
    assert [row[0] for row in first_migration["cursors"]] == ["basis_5m"]
    assert first_migration["transitions"][0][1] != before["transitions"][0][1]
    migrated_intent = json.loads(str(first_migration["transitions"][0][1]))[
        "seal_intent"
    ]
    assert migrated_intent["symbol"] == "BTCUSDT"

    with Catalog(path) as reopened:
        assert reopened.side_data_cursor("basis_5m", "BTCUSDT") is not None
        assert reopened.side_data_cursor("basis_5m", "ETHUSDT") is None
    assert _snapshot(path) == first_migration


@pytest.mark.parametrize(
    "malformed", ["missing_gap_id", "invalid_json", "invalid_side_data_json"]
)
def test_legacy_migration_fails_closed_without_rewriting_malformed_catalog(
    tmp_path: Path, malformed: str
) -> None:
    path = tmp_path / "catalog.sqlite"
    _seed_legacy_catalog(path, malformed=malformed)
    before = _snapshot(path)

    with pytest.raises(CatalogStateError, match="malformed"):
        Catalog(path)

    connection = sqlite3.connect(path)
    try:
        columns = {
            str(row[1])
            for row in connection.execute("PRAGMA table_info(operational_events)")
        }
        assert columns == {"event_id", "event_type", "occurred_at_utc_ns", "evidence_json"}
        assert not connection.execute(
            "SELECT 1 FROM sqlite_master WHERE name LIKE '%__ms1_legacy'"
        ).fetchall()
    finally:
        connection.close()
    assert _snapshot(path)["events"] == before["events"]
    assert _snapshot(path)["cursors"] == before["cursors"]


def test_partial_identity_schema_fails_closed(tmp_path: Path) -> None:
    path = tmp_path / "catalog.sqlite"
    _seed_legacy_catalog(path, partial=True)
    with pytest.raises(CatalogStateError, match="partial symbol identity"):
        Catalog(path)
    connection = sqlite3.connect(path)
    try:
        assert connection.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE name = 'operational_events__ms1_legacy'"
        ).fetchone() == (0,)
        assert connection.execute(
            "SELECT COUNT(*) FROM side_data_cursors"
        ).fetchone() == (1,)
    finally:
        connection.close()


def _record_lifecycle_event(
    catalog: Catalog,
    *,
    market: str,
    symbol: str,
    stream: str,
    gap_id: str,
    event_type: str,
    occurred_at_utc_ns: int,
) -> None:
    evidence: dict[str, object] = {
        "gap_id": gap_id,
        "market": market,
        "symbol": symbol,
        "stream": stream,
        "reason": "ms1-test",
        "interval_classification": "UNRELIABLE",
        "gap_started_at_utc_ns": 1_000,
        "original_connection_id": f"old-{symbol}",
        "original_generation": 0,
    }
    if event_type == "STREAM_DISCONTINUITY_COMPLETED":
        evidence.update(
            {
                "gap_ended_at_utc_ns": occurred_at_utc_ns,
                "new_connection_id": f"new-{symbol}",
                "new_generation": 1,
            }
        )
    catalog.record_operational_event(
        event_id=stream_discontinuity_event_id(
            event_type=event_type,
            market=market,
            symbol=symbol,
            stream=stream,
            gap_id=gap_id,
        ),
        event_type=event_type,
        occurred_at_utc_ns=occurred_at_utc_ns,
        evidence=evidence,
        symbol=symbol,
    )


@pytest.mark.parametrize("market", ["spot", "um_perpetual"])
def test_discontinuity_identity_isolated_for_every_frozen_symbol(
    tmp_path: Path, market: str
) -> None:
    path = tmp_path / "catalog.sqlite"
    with Catalog(path) as catalog:
        for index, symbol in enumerate(TARGET_SYMBOLS):
            _record_lifecycle_event(
                catalog,
                market=market,
                symbol=symbol,
                stream="depth",
                gap_id="same-gap-id",
                event_type="STREAM_DISCONTINUITY_STARTED",
                occurred_at_utc_ns=1_000 + index,
            )

        for symbol in TARGET_SYMBOLS:
            open_events = catalog.unclosed_stream_discontinuities(
                market=market, symbol=symbol, stream="depth"
            )
            assert [
                cast(dict[str, object], event["evidence"])["symbol"]
                for event in open_events
            ] == [symbol]

        grouped = catalog.unclosed_stream_discontinuities_by_stream()
        assert set(grouped) == {(market, symbol, "depth") for symbol in TARGET_SYMBOLS}

        _record_lifecycle_event(
            catalog,
            market=market,
            symbol="BTCUSDT",
            stream="depth",
            gap_id="same-gap-id",
            event_type="STREAM_DISCONTINUITY_COMPLETED",
            occurred_at_utc_ns=2_000,
        )
        assert catalog.unclosed_stream_discontinuities(
            market=market, symbol="BTCUSDT", stream="depth"
        ) == []
        assert len(
            catalog.unclosed_stream_discontinuities(
                market=market, symbol="ETHUSDT", stream="depth"
            )
        ) == 1
        assert catalog.stream_discontinuity_lifecycle(
            market=market,
            symbol="BTCUSDT",
            stream="depth",
            gap_id="same-gap-id",
        ) == "CLOSED"
        assert catalog.stream_discontinuity_lifecycle(
            market=market,
            symbol="ETHUSDT",
            stream="depth",
            gap_id="same-gap-id",
        ) == "OPEN"

        _record_lifecycle_event(
            catalog,
            market=market,
            symbol="ETHUSDT",
            stream="depth",
            gap_id="same-gap-id",
            event_type="STREAM_DISCONTINUITY_COMPLETED",
            occurred_at_utc_ns=2_001,
        )
        closed = catalog.closed_stream_discontinuity_intervals_by_stream()
        assert set(closed) == {
            (market, "BTCUSDT", "depth"),
            (market, "ETHUSDT", "depth"),
        }

    with Catalog(path) as reopened:
        assert reopened.stream_discontinuity_lifecycle(
            market=market,
            symbol="BTCUSDT",
            stream="depth",
            gap_id="same-gap-id",
        ) == "CLOSED"
        assert len(
            reopened.unclosed_stream_discontinuities(
                market=market, symbol="SOLUSDT", stream="depth"
            )
        ) == 1


def test_side_data_cursor_identity_isolated_for_every_frozen_symbol(
    tmp_path: Path,
) -> None:
    path = tmp_path / "catalog.sqlite"
    with Catalog(path) as catalog:
        for index, symbol in enumerate(TARGET_SYMBOLS):
            assert catalog.advance_side_data_cursor(
                kind="basis_5m",
                symbol=symbol,
                last_persisted_period_timestamp=10_000 + index,
                updated_at_utc_ns=20_000 + index,
                source_retention_window="latest_30_days",
                retention_window_ms=30 * 24 * 60 * 60 * 1000,
            )

        assert catalog.advance_side_data_cursor(
            kind="basis_5m",
            symbol="BTCUSDT",
            last_persisted_period_timestamp=99_000,
            updated_at_utc_ns=99_001,
            source_retention_window="latest_30_days",
            retention_window_ms=30 * 24 * 60 * 60 * 1000,
        )
        assert _cursor_timestamp(catalog, "BTCUSDT") == 99_000
        assert _cursor_timestamp(catalog, "ETHUSDT") == 10_001
        assert _cursor_timestamp(catalog, "SOLUSDT") == 10_002

        assert catalog.advance_side_data_cursor(
            kind="basis_5m",
            symbol="ETHUSDT",
            last_persisted_period_timestamp=88_000,
            updated_at_utc_ns=88_001,
            source_retention_window="latest_30_days",
            retention_window_ms=30 * 24 * 60 * 60 * 1000,
        )
        assert _cursor_timestamp(catalog, "BTCUSDT") == 99_000
        assert _cursor_timestamp(catalog, "ETHUSDT") == 88_000
        assert catalog.side_data_cursor("funding_info", "ETHUSDT") is None

    connection = sqlite3.connect(path)
    try:
        assert connection.execute(
            "SELECT COUNT(*) FROM side_data_cursors WHERE kind = 'basis_5m'"
        ).fetchone() == (len(TARGET_SYMBOLS),)
    finally:
        connection.close()

    with Catalog(path) as reopened:
        assert _cursor_timestamp(reopened, "BTCUSDT") == 99_000
        assert _cursor_timestamp(reopened, "ETHUSDT") == 88_000


def test_new_symbol_specific_records_require_explicit_symbol(tmp_path: Path) -> None:
    with Catalog(tmp_path / "catalog.sqlite") as catalog, pytest.raises(
        ValueError, match="symbol must be explicit"
    ):
        catalog.record_operational_event(
            event_id="missing-symbol",
            event_type="STREAM_DISCONTINUITY_STARTED",
            occurred_at_utc_ns=1,
            evidence={
                "gap_id": "gap",
                "market": "spot",
                "stream": "depth",
            },
        )
    with Catalog(tmp_path / "side-catalog.sqlite") as catalog, pytest.raises(
        ValueError, match="symbol must be explicit"
    ):
        catalog.record_operational_event(
            event_id="missing-side-symbol",
            event_type="SIDE_DATA_EMPTY_RESPONSE",
            occurred_at_utc_ns=1,
            evidence={"kind": "basis_5m"},
        )


def test_ensure_operational_event_replay_preserves_explicit_symbol_normalization(
    tmp_path: Path,
) -> None:
    evidence = {
        "gap_id": "ensure-gap",
        "market": "spot",
        "stream": "depth",
    }
    with Catalog(tmp_path / "catalog.sqlite") as catalog:
        assert catalog.ensure_operational_event(
            event_id="ensure-gap-start",
            event_type="STREAM_DISCONTINUITY_STARTED",
            occurred_at_utc_ns=1,
            evidence=evidence,
            symbol="ETHUSDT",
        )
        assert not catalog.ensure_operational_event(
            event_id="ensure-gap-start",
            event_type="STREAM_DISCONTINUITY_STARTED",
            occurred_at_utc_ns=1,
            evidence=evidence,
            symbol="ETHUSDT",
        )
