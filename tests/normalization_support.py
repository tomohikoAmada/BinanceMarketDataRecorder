from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from binance_market_data_recorder.domain.event import EventEnvelope, Market
from binance_market_data_recorder.spool.seal import seal_partial
from binance_market_data_recorder.spool.writer import RawChunkWriter
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import StorageLayout

BASE_NS = 1_767_225_600_000_000_000
FIXTURES = Path(__file__).parent / "fixtures" / "binance"


def fixture(market: str, name: str) -> bytes:
    return (FIXTURES / market / name).read_bytes().rstrip()


def envelope(
    *,
    market: Market,
    stream: str,
    raw_payload: bytes,
    ordinal: int,
    module: str | None = None,
    source_sequence: dict[str, int | str] | None = None,
    flags: tuple[str, ...] = (),
    collector_instance_id: str = "normalization-fixture",
    receive_time_utc_ns: int | None = None,
    exchange_event_time: int | None = 1_672_515_782_136,
    exchange_transaction_time: int | None = None,
    exchange_trade_time: int | None = None,
) -> EventEnvelope:
    return EventEnvelope(
        market=market,
        symbol="BTCUSDT",
        stream=stream,
        module=module
        or (
            f"binance.{'spot' if market == 'spot' else 'usdm'}.websocket.v1"
        ),
        connection_id=f"fixture-connection-{ordinal}",
        collector_instance_id=collector_instance_id,
        collector_version="0.1.0+normalization-test",
        receive_time_utc_ns=(
            BASE_NS + ordinal * 1_000_000
            if receive_time_utc_ns is None
            else receive_time_utc_ns
        ),
        receive_monotonic_ns=ordinal * 1_000_000,
        exchange_event_time=exchange_event_time,
        exchange_transaction_time=exchange_transaction_time,
        exchange_trade_time=exchange_trade_time,
        source_sequence=source_sequence or {},
        raw_payload=raw_payload,
        capture_flags=flags,
        payload_encoding=(
            "utf-8-json-provenance" if ".rest." in (module or "") else "utf-8-json"
        ),
    )


def provenance(
    *,
    schema_version: str,
    model: object,
    path: str,
    kind: str | None = None,
    parameters: dict[str, object] | None = None,
) -> bytes:
    document: dict[str, Any] = {
        "schema_version": schema_version,
        "request": {
            "method": "GET",
            "path": path,
            "parameters": parameters or {},
            "request_time_utc_ns": BASE_NS - 1_000,
        },
        "response": {
            "status": 200,
            "headers": {},
            "model": model,
            "receive_time_utc_ns": BASE_NS,
        },
        "transport": {
            "kind": "official_sdk_parsed_model",
            "raw_http_body_available": False,
        },
    }
    if kind is not None:
        document["kind"] = kind
    return json.dumps(document, sort_keys=True, separators=(",", ":")).encode()


def seal_events(
    *,
    layout: StorageLayout,
    catalog: Catalog,
    events: list[EventEnvelope],
) -> dict[str, object]:
    first = events[0]
    writer = RawChunkWriter(
        layout=layout,
        catalog=catalog,
        market=first.market,
        symbol=first.symbol,
        stream=first.stream,
        collector_instance_id=first.collector_instance_id,
        collector_version=first.collector_version,
        durability_interval_seconds=0,
    )
    for event in events:
        writer.append(event)
    writer.close()
    return seal_partial(writer.path, layout=layout, catalog=catalog)
