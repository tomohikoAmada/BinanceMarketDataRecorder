from __future__ import annotations

import json

import pytest

from binance_market_data_recorder.domain.event import EventEnvelope, Market
from binance_market_data_recorder.supervisor import CollectorReadiness


def _envelope(
    *,
    market: Market,
    stream: str,
    payload: dict[str, object],
    instance_id: str = "candidate-1",
    version: str = "0.2.0+candidate",
    ordinal: int = 1,
) -> EventEnvelope:
    return EventEnvelope(
        market=market,
        symbol="BTCUSDT",
        stream=stream,
        module=f"binance.{market}.test",
        connection_id=f"connection-{stream}",
        collector_instance_id=instance_id,
        collector_version=version,
        receive_time_utc_ns=1_700_000_000_000_000_000 + ordinal,
        receive_monotonic_ns=5_000_000_000 + ordinal,
        raw_payload=json.dumps(payload, separators=(",", ":")).encode(),
    )


@pytest.mark.parametrize("market", ["spot", "um_perpetual"])
def test_readiness_requires_all_streams_persisted_snapshot_and_book_sync(
    market: Market,
) -> None:
    readiness = CollectorReadiness(
        market=market,
        collector_instance_id="candidate-1",
        collector_version="0.2.0+candidate",
    )
    for stream in ("diff_depth", "agg_trade", "book_ticker"):
        readiness.observe_connected(stream)
    readiness.observe_persisted(
        _envelope(
            market=market,
            stream="diff_depth",
            payload={
                "s": "BTCUSDT",
                "U": 100,
                "u": 101,
                **({"pu": 99} if market == "um_perpetual" else {}),
                "b": [["1", "1"]],
                "a": [["2", "1"]],
            },
        )
    )
    readiness.observe_persisted(
        _envelope(
            market=market,
            stream="agg_trade",
            payload={"s": "BTCUSDT"},
            ordinal=2,
        )
    )
    readiness.observe_persisted(
        _envelope(
            market=market,
            stream="book_ticker",
            payload={"s": "BTCUSDT"},
            ordinal=3,
        )
    )
    before_snapshot = readiness.snapshot()
    assert before_snapshot.ready is False
    assert before_snapshot.persisted_streams == {
        "diff_depth",
        "agg_trade",
        "book_ticker",
    }
    readiness.observe_snapshot_persisted(
        _envelope(
            market=market,
            stream="depth_snapshot",
            payload={
                "response": {
                    "model": {
                        "lastUpdateId": 100,
                        "bids": [["1", "1"]],
                        "asks": [["2", "1"]],
                    }
                }
            },
            ordinal=4,
        )
    )
    assert readiness.snapshot().ready is True


def test_readiness_rejects_another_instance_identity() -> None:
    readiness = CollectorReadiness(
        market="spot",
        collector_instance_id="candidate-1",
        collector_version="0.2.0+candidate",
    )
    with pytest.raises(ValueError, match="identity"):
        readiness.observe_persisted(
            _envelope(
                market="spot",
                stream="agg_trade",
                payload={},
                instance_id="different",
            )
        )


def test_disconnect_requires_a_new_persisted_event() -> None:
    readiness = CollectorReadiness(
        market="spot",
        collector_instance_id="candidate-1",
        collector_version="0.2.0+candidate",
    )
    for ordinal, stream in enumerate(
        ("diff_depth", "agg_trade", "book_ticker"), start=1
    ):
        readiness.observe_connected(stream)
        readiness.observe_persisted(
            _envelope(
                market="spot",
                stream=stream,
                payload={
                    "s": "BTCUSDT",
                    **(
                        {
                            "U": 100,
                            "u": 101,
                            "b": [["1", "1"]],
                            "a": [["2", "1"]],
                        }
                        if stream == "diff_depth"
                        else {}
                    ),
                },
                ordinal=ordinal,
            )
        )
    readiness.observe_snapshot_persisted(
        _envelope(
            market="spot",
            stream="depth_snapshot",
            payload={
                "response": {
                    "model": {
                        "lastUpdateId": 100,
                        "bids": [["1", "1"]],
                        "asks": [["2", "1"]],
                    }
                }
            },
            ordinal=4,
        )
    )
    assert readiness.snapshot().ready is True

    readiness.observe_disconnected("agg_trade")
    readiness.observe_connected("agg_trade")
    assert readiness.snapshot().ready is False

    readiness.observe_persisted(
        _envelope(
            market="spot",
            stream="agg_trade",
            payload={"s": "BTCUSDT"},
            ordinal=5,
        )
    )
    assert readiness.snapshot().ready is True
