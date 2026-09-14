from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import cast

import pytest

from binance_market_data_recorder.storage.catalog import Catalog, CatalogStateError

STORAGE_ID = "11111111-2222-3333-4444-555555555555"


def _catalog_files(root: Path) -> dict[str, tuple[int, bytes]]:
    return {
        path.name: (path.stat().st_mtime_ns, path.read_bytes())
        for path in root.iterdir()
        if path.name.startswith("catalog.sqlite")
    }


def _create_catalog(path: Path) -> None:
    with Catalog(path) as catalog:
        catalog.register_storage_target(
            storage_id=STORAGE_ID,
            volume_uuid="AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE",
            volume_name="Read Only Test",
            filesystem_type="testfs",
            relative_path="Archive",
            marker_nonce="read-only-marker-nonce",
            registered_at_utc_ns=1,
        )
        assert catalog.begin_storage_eject(
            storage_id=STORAGE_ID,
            request_id="read-only-control",
            occurred_at_utc_ns=2,
        ) == []


def _record_gap_event(
    catalog: Catalog,
    *,
    event_id: str,
    event_type: str,
    occurred_at_utc_ns: int,
    evidence: dict[str, object],
) -> None:
    catalog.record_operational_event(
        event_id=event_id,
        event_type=event_type,
        occurred_at_utc_ns=occurred_at_utc_ns,
        evidence=evidence,
        symbol="BTCUSDT",
    )


def _started_evidence(gap_id: str, started_at_utc_ns: int) -> dict[str, object]:
    return {
        "market": "um_perpetual",
        "symbol": "BTCUSDT",
        "stream": "book_ticker",
        "gap_id": gap_id,
        "gap_started_at_utc_ns": started_at_utc_ns,
        "original_connection_id": "connection-a",
        "original_generation": 1,
    }


def _completed_evidence(
    gap_id: str, ended_at_utc_ns: int, *, malformed: bool = False
) -> dict[str, object]:
    evidence: dict[str, object] = {
        "market": "um_perpetual",
        "symbol": "BTCUSDT",
        "stream": "book_ticker",
        "gap_id": gap_id,
        "gap_ended_at_utc_ns": ended_at_utc_ns,
        "new_connection_id": "connection-b",
    }
    if not malformed:
        evidence["new_generation"] = 2
    return evidence


def _snapshot(path: Path, as_of_utc_ns: int) -> dict[str, object]:
    with Catalog(path, read_only=True) as catalog:
        return catalog.discontinuity_authority_snapshot(as_of_utc_ns=as_of_utc_ns)


def test_discontinuity_snapshot_excludes_a_future_lifecycle_completely(
    tmp_path: Path,
) -> None:
    path = tmp_path / "catalog.sqlite"
    with Catalog(path) as catalog:
        _record_gap_event(
            catalog,
            event_id="future-start",
            event_type="STREAM_DISCONTINUITY_STARTED",
            occurred_at_utc_ns=101,
            evidence=_started_evidence("future-gap", 101),
        )
        _record_gap_event(
            catalog,
            event_id="future-complete",
            event_type="STREAM_DISCONTINUITY_COMPLETED",
            occurred_at_utc_ns=102,
            evidence=_completed_evidence("future-gap", 102),
        )

    snapshot = _snapshot(path, 100)
    assert snapshot["operational_events"] == []
    assert snapshot["malformed_events"] == []
    assert snapshot["degraded_pairs"] == []
    assert snapshot["unclosed"] == {}
    assert snapshot["closed"] == {}


def test_discontinuity_snapshot_future_completion_leaves_open_until_next_boundary(
    tmp_path: Path,
) -> None:
    path = tmp_path / "catalog.sqlite"
    with Catalog(path) as catalog:
        _record_gap_event(
            catalog,
            event_id="open-start",
            event_type="STREAM_DISCONTINUITY_STARTED",
            occurred_at_utc_ns=99,
            evidence=_started_evidence("open-gap", 99),
        )
        _record_gap_event(
            catalog,
            event_id="open-complete",
            event_type="STREAM_DISCONTINUITY_COMPLETED",
            occurred_at_utc_ns=101,
            evidence=_completed_evidence("open-gap", 101),
        )

    at_boundary = _snapshot(path, 100)
    unclosed = cast(dict[tuple[str, str, str], list[dict[str, object]]], at_boundary["unclosed"])
    assert set(unclosed) == {
        ("um_perpetual", "BTCUSDT", "book_ticker")
    }
    assert at_boundary["closed"] == {}

    after_completion = _snapshot(path, 101)
    assert after_completion["unclosed"] == {}
    closed = after_completion["closed"]
    assert isinstance(closed, dict)
    assert closed[("um_perpetual", "BTCUSDT", "book_ticker")][0]["gap_id"] == "open-gap"


def test_discontinuity_snapshot_completed_before_boundary_is_closed(
    tmp_path: Path,
) -> None:
    path = tmp_path / "catalog.sqlite"
    with Catalog(path) as catalog:
        _record_gap_event(
            catalog,
            event_id="closed-start",
            event_type="STREAM_DISCONTINUITY_STARTED",
            occurred_at_utc_ns=98,
            evidence=_started_evidence("closed-gap", 98),
        )
        _record_gap_event(
            catalog,
            event_id="closed-complete",
            event_type="STREAM_DISCONTINUITY_COMPLETED",
            occurred_at_utc_ns=99,
            evidence=_completed_evidence("closed-gap", 99),
        )

    snapshot = _snapshot(path, 100)
    assert snapshot["unclosed"] == {}
    closed = snapshot["closed"]
    assert isinstance(closed, dict)
    assert closed[("um_perpetual", "BTCUSDT", "book_ticker")][0]["ended_at_utc_ns"] == 99


def test_discontinuity_snapshot_excludes_future_terminal_event(
    tmp_path: Path,
) -> None:
    path = tmp_path / "catalog.sqlite"
    with Catalog(path) as catalog:
        catalog.record_operational_event(
            event_id="future-stop",
            event_type="SERVICE_STOPPED",
            occurred_at_utc_ns=101,
            evidence={},
        )

    before = _snapshot(path, 100)
    assert before["operational_events"] == []
    assert before["terminal_events"] == []
    after = _snapshot(path, 101)
    terminal = after["terminal_events"]
    assert isinstance(terminal, list)
    assert [event["event_type"] for event in terminal] == ["SERVICE_STOPPED"]


def test_discontinuity_snapshot_bounds_malformed_and_degraded_authority(
    tmp_path: Path,
) -> None:
    path = tmp_path / "catalog.sqlite"
    with Catalog(path) as catalog:
        _record_gap_event(
            catalog,
            event_id="degraded-start",
            event_type="STREAM_DISCONTINUITY_STARTED",
            occurred_at_utc_ns=99,
            evidence=_started_evidence("degraded-gap", 99),
        )
        _record_gap_event(
            catalog,
            event_id="future-degraded-complete",
            event_type="STREAM_DISCONTINUITY_COMPLETED",
            occurred_at_utc_ns=101,
            evidence=_completed_evidence("degraded-gap", 101, malformed=True),
        )
        _record_gap_event(
            catalog,
            event_id="future-malformed",
            event_type="STREAM_DISCONTINUITY_STARTED",
            occurred_at_utc_ns=102,
            evidence={
                "market": "um_perpetual",
                "symbol": "BTCUSDT",
                "gap_id": "malformed-gap",
            },
        )

    before = _snapshot(path, 100)
    assert before["degraded_pairs"] == []
    assert before["malformed_events"] == []
    unclosed = cast(dict[tuple[str, str, str], list[dict[str, object]]], before["unclosed"])
    assert set(unclosed) == {
        ("um_perpetual", "BTCUSDT", "book_ticker")
    }

    after = _snapshot(path, 102)
    degraded = after["degraded_pairs"]
    assert isinstance(degraded, list)
    assert degraded[0]["gap_id"] == "degraded-gap"
    malformed = after["malformed_events"]
    assert isinstance(malformed, list)
    assert malformed == [
        {
            "event_id": "future-malformed",
            "event_type": "STREAM_DISCONTINUITY_STARTED",
            "reason": "missing_stream",
        }
    ]


def test_discontinuity_snapshot_uses_one_bounded_operational_event_read(
    tmp_path: Path,
) -> None:
    path = tmp_path / "catalog.sqlite"
    with Catalog(path) as catalog:
        _record_gap_event(
            catalog,
            event_id="bounded-start",
            event_type="STREAM_DISCONTINUITY_STARTED",
            occurred_at_utc_ns=99,
            evidence=_started_evidence("bounded-gap", 99),
        )
        _record_gap_event(
            catalog,
            event_id="bounded-complete",
            event_type="STREAM_DISCONTINUITY_COMPLETED",
            occurred_at_utc_ns=101,
            evidence=_completed_evidence("bounded-gap", 101),
        )

    before = _catalog_files(tmp_path)
    statements: list[str] = []
    with Catalog(path, read_only=True) as catalog:
        catalog._connection.set_trace_callback(statements.append)
        snapshot = catalog.discontinuity_authority_snapshot(as_of_utc_ns=100)
    assert _catalog_files(tmp_path) == before

    event_reads = [
        statement
        for statement in statements
        if "FROM operational_events" in statement
    ]
    assert len(event_reads) == 1
    assert "occurred_at_utc_ns <= 100" in event_reads[0]
    unclosed = cast(dict[tuple[str, str, str], list[dict[str, object]]], snapshot["unclosed"])
    assert set(unclosed) == {
        ("um_perpetual", "BTCUSDT", "book_ticker")
    }
    assert snapshot["closed"] == {}


def test_read_only_catalog_reads_aggregate_targets_and_control_without_mutation(
    tmp_path: Path,
) -> None:
    catalog_path = tmp_path / "catalog.sqlite"
    _create_catalog(catalog_path)
    before = _catalog_files(tmp_path)

    with Catalog(catalog_path, read_only=True) as catalog:
        query_only = catalog._connection.execute("PRAGMA query_only").fetchone()
        assert query_only is not None
        assert query_only[0] == 1
        assert catalog.archive_aggregate(STORAGE_ID)["backlog_files"] == 0
        assert catalog.storage_targets()[0]["storage_id"] == STORAGE_ID
        assert catalog.storage_control(STORAGE_ID)["state"] == "EJECT_PENDING"

    assert _catalog_files(tmp_path) == before


@pytest.mark.parametrize(
    "write",
    [
        lambda catalog: catalog._transaction().__enter__(),
        lambda catalog: catalog._initialize(),
        lambda catalog: catalog.record_operational_event(
            event_id="event",
            event_type="TEST",
            occurred_at_utc_ns=1,
            evidence={},
        ),
        lambda catalog: catalog.begin_archive_attempt("missing-transaction"),
        lambda catalog: catalog.record_archive_error("missing-transaction", "error"),
        lambda catalog: catalog.register_quarantined_artifact(
            artifact_id="artifact",
            relative_path="quarantine/artifact",
            reason="test",
            sha256="0" * 64,
        ),
        lambda catalog: catalog.register_orderbook_checkpoint(
            checkpoint_id="checkpoint",
            market="spot",
            symbol="BTCUSDT",
            update_id=1,
            book_hash="1" * 64,
            relative_path="checkpoints/test",
            created_at_utc_ns=1,
        ),
        lambda catalog: catalog.checkpoint(),
    ],
)
def test_read_only_catalog_rejects_every_write_entrypoint_outside_transactions(
    tmp_path: Path,
    write: Callable[[Catalog], object],
) -> None:
    catalog_path = tmp_path / "catalog.sqlite"
    _create_catalog(catalog_path)

    with (
        Catalog(catalog_path, read_only=True) as catalog,
        pytest.raises(CatalogStateError, match="read-only"),
    ):
        write(catalog)


def test_read_only_catalog_missing_path_does_not_create_file_or_parent(
    tmp_path: Path,
) -> None:
    catalog_path = tmp_path / "missing" / "catalog.sqlite"

    with pytest.raises(CatalogStateError, match="does not exist"):
        Catalog(catalog_path, read_only=True)

    assert not catalog_path.exists()
    assert not catalog_path.parent.exists()
