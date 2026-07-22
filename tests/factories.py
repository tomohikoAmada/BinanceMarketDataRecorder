from __future__ import annotations

from binance_market_data_recorder.domain.event import EventEnvelope


def event(
    ordinal: int = 1,
    *,
    payload: bytes | None = None,
    flags: tuple[str, ...] = (),
) -> EventEnvelope:
    return EventEnvelope(
        market="spot",
        symbol="BTCUSDT",
        stream="diff_depth",
        module="binance.spot.v1",
        connection_id="connection-1",
        collector_instance_id="collector-1",
        collector_version="0.1.0+test",
        receive_time_utc_ns=1_700_000_000_000_000_000 + ordinal,
        receive_monotonic_ns=5_000_000_000 + ordinal,
        exchange_event_time=1_700_000_000_000 + ordinal,
        source_sequence={"U": ordinal, "u": ordinal},
        raw_payload=payload if payload is not None else f'{{"u":{ordinal}}}'.encode(),
        capture_flags=flags,
    )
