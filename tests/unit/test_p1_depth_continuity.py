from __future__ import annotations

import json
from collections.abc import Callable

import pytest

from binance_market_data_recorder.binance.spot import schema as spot
from binance_market_data_recorder.binance.usdm import schema as usdm
from binance_market_data_recorder.domain.event import EventEnvelope, Market
from binance_market_data_recorder.orderbook.parser import (
    depth_update_from_envelope,
    snapshot_from_envelope,
)
from binance_market_data_recorder.orderbook.reconstructor import (
    LocalBookReconstructor,
    SynchronizeResult,
)
from binance_market_data_recorder.supervisor.depth_continuity import DepthContinuity
from tests.unit.test_collector_readiness import _envelope


def depth(market: Market, first: int, last: int, previous: int) -> EventEnvelope:
    return _envelope(
        market=market,
        stream="diff_depth",
        payload={
            "s": "BTCUSDT",
            "U": first,
            "u": last,
            "pu": previous,
            "b": [["1", "1"]],
            "a": [["2", "1"]],
        },
        ordinal=last,
    ).model_copy(update={"source_sequence": {"U": first, "u": last, "pu": previous}})


def snapshot(market: Market, last: int) -> EventEnvelope:
    return _envelope(
        market=market,
        stream="depth_snapshot",
        payload={
            "response": {
                "model": {
                    "lastUpdateId": last,
                    "bids": [["1", "1"]],
                    "asks": [["2", "1"]],
                }
            },
        },
    )


@pytest.mark.parametrize("market", ["spot", "um_perpetual"])
def test_sequence_bridge_duplicates_gaps_and_recovery_match_offline_book(market: Market) -> None:
    observed = []
    lightweight = DepthContinuity(
        market, capacity=8, audit_observer=lambda audit, at: observed.append(audit.kind)
    )
    book = LocalBookReconstructor(market)
    for envelope in (depth(market, 99, 101, 98), depth(market, 102, 103, 101)):
        lightweight.offer(envelope)
        book.offer(depth_update_from_envelope(envelope))
    for last in (95, 100):
        assert lightweight.synchronize(snapshot(market, last)) == book.synchronize(
            snapshot_from_envelope(snapshot(market, last))
        )
    assert lightweight.reliable_update_id == book.reliable_update_id == 103
    for envelope in (
        depth(market, 102, 103, 101),  # duplicate
        depth(market, 104, 105, 103),
        depth(market, 108, 110, 107),  # gap
    ):
        lightweight.offer(envelope)
        book.offer(depth_update_from_envelope(envelope))
        assert lightweight.reliable_update_id == book.reliable_update_id
    assert "sequence_gap" in observed and "duplicate_or_stale_depth" in observed
    assert (
        lightweight.synchronize(snapshot(market, 108))
        == book.synchronize(snapshot_from_envelope(snapshot(market, 108)))
        == SynchronizeResult.SYNCHRONIZED
    )
    assert lightweight.reliable_update_id == book.reliable_update_id == 110


def test_sequence_buffer_is_bounded_and_retains_no_prices_or_payloads() -> None:
    observer = []
    state = DepthContinuity(
        "spot", capacity=2, audit_observer=lambda audit, at: observer.append(audit.kind)
    )
    for i in range(100):
        state.offer(depth("spot", i, i + 1, max(0, i - 1)))
    assert state.bootstrap_buffer_overflowed
    assert len(state._buffer) == 0
    assert observer == ["bootstrap_buffer_near_capacity", "bootstrap_buffer_overflow"]
    assert state.reliable_update_id is None


@pytest.mark.parametrize("market", ["spot", "um_perpetual"])
def test_unknown_fields_and_unparseable_received_bytes_are_preserved(market: Market) -> None:
    payload = {
        "e": "bookTicker",
        "s": "BTCUSDT",
        "st": 1,
        "ps": "BTCUSDT",
        "E": 42,
        "T": 41,
        "u": 20,
        "b": "1",
        "B": "2",
        "a": "2",
        "A": "3",
        "future_field": {"new": [1, 2, 3]},
    }
    for raw, malformed in (
        (json.dumps(payload).encode(), False),
        (b"[" * 1500 + b"]" * 1500, True),
        (b"{not json\xff", True),
    ):
        factory: Callable[..., EventEnvelope] = (
            spot.envelope_from_websocket_frame
            if market == "spot"
            else (usdm.envelope_from_websocket_frame)
        )
        stream = spot.SpotStream.BOOK_TICKER if market == "spot" else usdm.UsdMStream.BOOK_TICKER
        envelope = factory(
            symbol="BTCUSDT",
            stream=stream,
            raw_payload=raw,
            connection_id="connection",
            collector_instance_id="collector",
            collector_version="test",
            receive_time_utc_ns=1234,
            receive_monotonic_ns=5678,
        )
        assert envelope.raw_payload == raw
        assert envelope.receive_time_utc_ns == 1234
        assert envelope.receive_monotonic_ns == 5678
        assert ("malformed" in envelope.capture_flags) is malformed


def test_capture_readiness_needs_no_full_book_and_malformed_depth_invalidates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from binance_market_data_recorder.orderbook.model import OrderBook
    from binance_market_data_recorder.supervisor.readiness import CollectorReadiness

    def no_full_book(*args: object, **kwargs: object) -> None:
        raise AssertionError("capture must not construct a full book")

    monkeypatch.setattr(OrderBook, "__init__", no_full_book)
    audits = []
    readiness = CollectorReadiness(
        market="spot",
        symbol="BTCUSDT",
        collector_instance_id="candidate-1",
        collector_version="0.2.0+candidate",
        audit_observer=lambda audit, at: audits.append(audit.kind),
    )
    readiness.observe_persisted(depth("spot", 100, 101, 99))
    assert readiness.observe_snapshot_persisted(snapshot("spot", 100)) == (
        SynchronizeResult.SYNCHRONIZED
    )
    assert readiness.snapshot().orderbook_synchronized
    public = readiness.snapshot().public_dict()
    assert public["full_orderbook_evaluated"] is False
    assert public["depth_sequence_synchronized"] is True
    readiness.observe_persisted(
        depth("spot", 102, 103, 101).model_copy(
            update={"capture_flags": ("malformed",), "raw_payload": b"invalid raw bytes"}
        )
    )
    assert not readiness.snapshot().orderbook_synchronized
    assert audits == ["sequence_gap"]
