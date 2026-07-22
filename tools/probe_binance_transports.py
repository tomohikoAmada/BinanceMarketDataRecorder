#!/usr/bin/env python3
"""Report M2 transport capabilities; network access is opt-in with --online-rest."""

from __future__ import annotations

import argparse
import inspect
import json
from collections.abc import Callable, Sequence
from importlib.metadata import version
from typing import Any

SPOT_SDK = "binance-sdk-spot"
USDM_SDK = "binance-sdk-derivatives-trading-usds-futures"
GENERIC_WEBSOCKET = "websockets"


def _field_aliases(model: type[Any]) -> set[str]:
    return {field.alias or name for name, field in model.model_fields.items()}


def offline_capability_report() -> dict[str, object]:
    """Inspect pinned packages without opening a socket or reading credentials."""

    from binance_common.websocket import WebSocketCommon
    from binance_sdk_derivatives_trading_usds_futures.websocket_streams.models.diff_book_depth_streams_response import (  # noqa: E501
        DiffBookDepthStreamsResponse,
    )
    from binance_sdk_spot.websocket_streams.models.diff_book_depth_response import (
        DiffBookDepthResponse,
    )
    from websockets.asyncio.client import connect

    receive_source = inspect.getsource(WebSocketCommon.receive_loop)
    init_source = inspect.getsource(WebSocketCommon.init_connection)
    connect_signature = inspect.signature(connect)
    spot_ids = _field_aliases(DiffBookDepthResponse)
    usdm_ids = _field_aliases(DiffBookDepthStreamsResponse)

    sdk_checks = {
        "raw_payload_bytes": {
            "passed": False,
            "evidence": "receive_loop calls json.loads(msg.data) before dispatch",
        },
        "receive_timestamp_boundary": {
            "passed": False,
            "evidence": "no callback receives msg.data or a timestamp at socket receipt",
        },
        "depth_update_ids": {
            "passed": {"U", "u"} <= spot_ids and {"U", "u", "pu"} <= usdm_ids,
            "evidence": {"spot": sorted(spot_ids), "usdm": sorted(usdm_ids)},
        },
        "blocking_callback_backpressure": {
            "passed": False,
            "evidence": (
                "receive_loop invokes synchronous callback(parsed) inline with no "
                "bounded handoff"
            ),
        },
        "connection_rotation_control": {
            "passed": False,
            "evidence": "init_connection creates an internal fixed 23-hour reconnect task",
        },
        "fault_injection_surface": {
            "passed": False,
            "evidence": "public configuration has no transport/connection factory injection",
        },
    }
    source_guards = {
        "json_decode_before_callback": "json.loads(msg.data)" in receive_source,
        "synchronous_callback": "callback(parsed)" in receive_source,
        "fixed_rotation": "23 * 3600" in init_source,
    }
    if not all(source_guards.values()):
        raise RuntimeError(f"pinned SDK source no longer matches probe: {source_guards}")

    generic_parameters = set(connect_signature.parameters)
    generic_checks = {
        "raw_payload_bytes": {
            "passed": True,
            "evidence": (
                "recv(decode=False) yields payload bytes; local round-trip test is required"
            ),
        },
        "receive_timestamp_boundary": {
            "passed": True,
            "evidence": "caller controls the statement immediately after await recv()",
        },
        "depth_update_ids": {
            "passed": True,
            "evidence": "the library returns the full message without schema projection",
        },
        "bounded_transport_queue": {
            "passed": "max_queue" in generic_parameters,
            "evidence": "connect exposes max_queue high/low watermarks",
        },
        "connection_rotation_control": {
            "passed": True,
            "evidence": "caller owns context lifetime, close, cancellation, and reconnect loop",
        },
        "fault_injection_surface": {
            "passed": "create_connection" in generic_parameters,
            "evidence": "connect exposes create_connection and supports local servers",
        },
    }
    return {
        "mode": "offline",
        "network_accessed": False,
        "packages": {
            SPOT_SDK: version(SPOT_SDK),
            USDM_SDK: version(USDM_SDK),
            GENERIC_WEBSOCKET: version(GENERIC_WEBSOCKET),
        },
        "official_sdk_websocket": {
            "selected": False,
            "checks": sdk_checks,
            "source_guards": source_guards,
        },
        "generic_websocket": {
            "selected": True,
            "checks": generic_checks,
        },
    }


def online_public_rest_smoke() -> dict[str, object]:
    """Call only unsigned public BTCUSDT depth endpoints through official SDKs."""

    from binance_sdk_derivatives_trading_usds_futures.derivatives_trading_usds_futures import (
        DerivativesTradingUsdsFutures,
    )
    from binance_sdk_spot.spot import Spot

    results: dict[str, object] = {}
    calls: dict[str, Callable[[], Any]] = {
        "spot": lambda: Spot().rest_api.depth("BTCUSDT", 5),
        "usdm": lambda: DerivativesTradingUsdsFutures().rest_api.order_book(
            "BTCUSDT", 5
        ),
    }
    for market, call in calls.items():
        response = call()
        data = response.data().model_dump(by_alias=True)
        required = {"lastUpdateId", "bids", "asks"}
        if response.status != 200 or not required <= set(data):
            raise RuntimeError(f"invalid {market} public depth response")
        results[market] = {
            "status": response.status,
            "last_update_id_type": type(data["lastUpdateId"]).__name__,
            "bid_levels": len(data["bids"]),
            "ask_levels": len(data["asks"]),
            "credentials_supplied": False,
        }
    return {
        "mode": "online_public_rest",
        "network_accessed": True,
        "account_api_accessed": False,
        "credentials_read": False,
        "results": results,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--online-rest",
        action="store_true",
        help="opt in to one unsigned public depth request per V1 market",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    report = offline_capability_report()
    if args.online_rest:
        report["online_rest"] = online_public_rest_smoke()
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
