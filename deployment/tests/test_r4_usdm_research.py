"""Regression tests for R4 USD-M C2 alignment, research export, and parity."""

from __future__ import annotations

import base64
import json
import shutil
import sqlite3
import sys
from pathlib import Path

import pyarrow.parquet as pq  # type: ignore[import-untyped]
import pytest

ROOT = Path(__file__).resolve().parents[2]
PROTOTYPE = ROOT / "prototype"
BASELINE_SRC = ROOT / "vendor/python-baseline/src"

for entry in (str(PROTOTYPE), str(BASELINE_SRC)):
    if entry not in sys.path:
        sys.path.insert(0, entry)

import normalize  # noqa: E402
import usdm_research  # noqa: E402
from binance_market_data_recorder.archive.drain import archive_drain  # noqa: E402
from binance_market_data_recorder.domain.event import EventEnvelope  # noqa: E402
from binance_market_data_recorder.spool.writer import RawChunkWriter  # noqa: E402
from binance_market_data_recorder.storage.catalog import Catalog  # noqa: E402
from binance_market_data_recorder.storage.layout import (  # noqa: E402
    ensure_storage_layout,
)
from binance_market_data_recorder.storage.macos.model import VolumeInfo  # noqa: E402
from binance_market_data_recorder.storage.macos.registry import (  # noqa: E402
    StorageRegistry,
)


class _StubVolumes:
    def __init__(self, mountpoint: Path) -> None:
        self._info = VolumeInfo(
            disk_id="disk-r4",
            volume_uuid="vol-r4-test",
            name="r4-archive",
            filesystem_type="apfs",
            mountpoint=mountpoint,
            writable=True,
            internal=False,
            removable=True,
            total_bytes=100 * 1024**3,
            free_bytes=50 * 1024**3,
            observed_at_utc_ns=1_000_000_000,
        )

    def inventory(self) -> list[VolumeInfo]:
        return [self._info]

    def inspect_path(self, path: Path) -> VolumeInfo:
        return self._info


def _ws_envelope(
    *,
    stream: str,
    symbol: str,
    payload: dict,
    receive_ns: int,
    exchange_tx_ms: int | None = None,
    combined: bool = True,
) -> EventEnvelope:
    wire = (
        {"stream": f"{symbol.lower()}@{stream}", "data": payload}
        if combined
        else payload
    )
    return EventEnvelope(
        market="um_perpetual",
        symbol=symbol,
        stream=stream,
        module="binance.public.websocket.v1",
        connection_id="ws-market-1",
        collector_instance_id="inst-r4",
        collector_version="r4-test",
        receive_time_utc_ns=receive_ns,
        receive_monotonic_ns=receive_ns - 10,
        exchange_event_time=payload.get("E"),
        exchange_transaction_time=exchange_tx_ms,
        payload_encoding="utf-8-json-combined.v1" if combined else "utf-8-json",
        raw_payload=json.dumps(wire, separators=(",", ":")).encode("utf-8"),
    )


def _rest_envelope(
    *,
    source_stream: str,
    path: str,
    symbol: str,
    parameters: dict,
    body_obj: object,
    receive_ns: int,
    status: int = 200,
    valid: bool = True,
    error_type: str | None = None,
) -> EventEnvelope:
    raw_body = json.dumps(body_obj, separators=(",", ":")).encode("utf-8")
    provenance = {
        "schema_version": "binance-public-rest-provenance.v1",
        "representation": "http-body-base64.v1",
        "family": "D01" if source_stream == "exchange_info" else "D12",
        "source_stream": source_stream,
        "scope": "__GLOBAL__" if symbol == "__GLOBAL__" else symbol,
        "venue_market": "usdm",
        "request": {
            "request_id": f"req-{source_stream}-{receive_ns}",
            "method": "GET",
            "path": path,
            "parameters": parameters,
            "request_time_utc_ns": receive_ns - 5000,
            "request_monotonic_ns": receive_ns - 5000,
            "timeout_ms": 10000,
        },
        "response": {
            "status": status,
            "headers": {"content-type": "application/json"},
            "raw_body_base64": base64.b64encode(raw_body).decode("ascii"),
            "valid": valid,
            "error_type": error_type,
            "receive_time_utc_ns": receive_ns,
            "receive_monotonic_ns": receive_ns,
        },
    }
    return EventEnvelope(
        market="um_perpetual",
        symbol=symbol,
        stream="rest_observations",
        module="binance.public.rest.v1",
        connection_id="rest-1",
        collector_instance_id="inst-r4",
        collector_version="r4-test",
        receive_time_utc_ns=receive_ns,
        receive_monotonic_ns=receive_ns,
        source_sequence={"sourceStream": source_stream, "sourceFamily": "D12"},
        capture_flags=() if valid else ("malformed",),
        payload_encoding="utf-8-json",
        raw_payload=json.dumps(provenance, separators=(",", ":")).encode("utf-8"),
    )


def test_usdm_agg_trade_nq_and_mark_price_t_semantics_live_and_legacy() -> None:
    fixtures = usdm_research.build_usdm_parity_fixtures()
    agg_with_nq = _ws_envelope(
        stream="agg_trade",
        symbol="BTCUSDT",
        payload=fixtures["ws_payloads"][0]["data"],
        receive_ns=1_762_992_000_100_000_000,
    )
    agg_zero_nq = _ws_envelope(
        stream="agg_trade",
        symbol="BTCUSDT",
        payload=fixtures["ws_payloads"][1]["data"],
        receive_ns=1_762_992_000_200_000_000,
    )
    legacy_no_nq_payload = dict(fixtures["ws_payloads"][0]["data"])
    del legacy_no_nq_payload["nq"]
    agg_legacy = _ws_envelope(
        stream="agg_trade",
        symbol="BTCUSDT",
        payload=legacy_no_nq_payload,
        receive_ns=1_762_992_000_300_000_000,
        combined=False,
    )

    p1 = usdm_research.parse_usdm_envelope(agg_with_nq)[0]
    p2 = usdm_research.parse_usdm_envelope(agg_zero_nq)[0]
    p3 = usdm_research.parse_usdm_envelope(agg_legacy)[0]
    assert p1.c2_event["normal_quantity"] == "0.125"
    assert p2.c2_event["normal_quantity"] == "0"
    assert p3.c2_event["normal_quantity"] is None
    assert normalize.parse_capture_envelope(agg_zero_nq)[0].fields["normal_quantity"] == "0"
    assert normalize.parse_capture_envelope(agg_legacy)[0].fields["normal_quantity"] is None

    # Legacy Raw stored markPrice T in exchange_transaction_time_ms; R4 must clear it.
    mp_payload = fixtures["ws_payloads"][2]["data"]
    mp_legacy = _ws_envelope(
        stream="mark_price",
        symbol="BTCUSDT",
        payload=mp_payload,
        receive_ns=1_762_992_001_000_000_000,
        exchange_tx_ms=mp_payload["T"],
        combined=True,
    )
    mp_rec = usdm_research.parse_usdm_envelope(mp_legacy)[0]
    assert mp_rec.parquet_row["next_funding_time_ms"] == 1763020800000
    assert mp_rec.parquet_row["mark_price_moving_average"] == "90115.10"
    assert mp_rec.parquet_row["exchange_event_time_ms"] == 1762992001000
    assert mp_rec.parquet_row["exchange_transaction_time_ms"] is None
    assert mp_rec.parquet_row["exchange_trade_time_ms"] is None
    assert mp_rec.c2_event["metadata"]["exchange_transaction_time_ms"] is None
    assert mp_rec.c2_event["metadata"]["exchange_trade_time_ms"] is None
    adapter_mp = normalize.parse_capture_envelope(mp_legacy)[0]
    assert adapter_mp.exchange_times == {
        "exchange_event_time_ms": 1762992001000,
        "exchange_transaction_time_ms": None,
        "exchange_trade_time_ms": None,
    }
    assert adapter_mp.fields["mark_price_moving_average"] == "90115.10"


def test_usdm_exchange_info_splits_configured_symbols_and_rest_sources() -> None:
    fixtures = usdm_research.build_usdm_parity_fixtures()
    ex_env = _rest_envelope(
        source_stream="exchange_info",
        path="/fapi/v1/exchangeInfo",
        symbol="__GLOBAL__",
        parameters={},
        body_obj=fixtures["exchange_info"],
        receive_ns=1_762_992_000_000_000_000,
    )
    # Request BTCUSDT, ETHUSDT, and an absent symbol SOLUSDT.
    recs = usdm_research.parse_usdm_envelope(
        ex_env, configured_symbols=("BTCUSDT", "ETHUSDT", "SOLUSDT")
    )
    assert [r.symbol for r in recs] == ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    assert all(r.symbol != "__GLOBAL__" for r in recs)
    btc = recs[0].c2_event
    assert btc["status"] == "VALID"
    assert btc["source_kind"] == "INSTRUMENT"
    assert btc["instrument"]["base_asset"] == "BTC"
    assert btc["instrument"]["price_precision"] == 2
    assert btc["instrument"]["filters"][0]["tick_size"] == "0.10"
    assert btc["instrument"]["rate_limits"][0]["limit"] == 2400
    assert btc["instrument"]["server_time_ms"] == 1762992000000
    sol = recs[2].c2_event
    assert sol["status"] == "ERROR"
    assert sol["error_code"] == "SYMBOL_MISSING_IN_EXCHANGE_INFO"

    # Empty periodic REST response without startTime/endTime parameters:
    empty_env = _rest_envelope(
        source_stream="top_long_short_position_ratio_5m",
        path="/futures/data/topLongShortPositionRatio",
        symbol="BTCUSDT",
        parameters={"symbol": "BTCUSDT", "period": "5m", "limit": 1},
        body_obj=[],
        receive_ns=1_762_992_000_500_000_000,
    )
    empty_rec = usdm_research.parse_usdm_envelope(empty_env)[0]
    assert empty_rec.c2_event["status"] == "EMPTY"
    assert empty_rec.c2_event["source_time_ms"] is None
    assert empty_rec.c2_event["receive_time_utc_ns"] == 1_762_992_000_500_000_000

    # Transport error preserves source-scoped ERROR without synthetic business payload.
    err_env = _rest_envelope(
        source_stream="open_interest",
        path="/fapi/v1/openInterest",
        symbol="BTCUSDT",
        parameters={"symbol": "BTCUSDT"},
        body_obj={"msg": "unavailable"},
        receive_ns=1_762_992_000_600_000_000,
        status=503,
        valid=False,
        error_type="Http503",
    )
    err_rec = usdm_research.parse_usdm_envelope(err_env)[0]
    assert err_rec.c2_event["status"] == "ERROR"
    assert err_rec.c2_event["error_code"] == "Http503"
    assert "open_interest" not in err_rec.c2_event


def test_profile_and_upfront_validation_rejects_unsupported_inputs(tmp_path: Path) -> None:
    profile = Path(
        "/Users/amada/Documents/Development/Crypto/Milestone Plan/examples/usdm-parity-profile.json"
    )
    if profile.is_file():
        market, syms, sources = usdm_research.load_profile(profile)
        assert market == "um_perpetual"
        assert syms == usdm_research.DEFAULT_PARITY_SYMBOLS
        assert "mark_price" in sources
        assert "taker_buy_sell_volume_5m" in sources

    with pytest.raises(ValueError, match="unsupported research market"):
        usdm_research.validate_export_request(market="cm_delivery")
    with pytest.raises(ValueError, match="unsupported research source"):
        usdm_research.validate_export_request(sources=["not_a_real_stream"])
    with pytest.raises(ValueError, match="unsupported research field"):
        usdm_research.validate_export_request(
            fields_by_source={"agg_trade": ["nonexistent_field"]}
        )


def test_end_to_end_hot_cold_archived_usdm_research_export_and_book_parity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import binance_market_data_recorder.storage.platform as platform_mod

    ext_mount = tmp_path / "ext"
    ext_slot = ext_mount / "archive_slot"
    ext_slot.mkdir(parents=True)
    volumes = _StubVolumes(ext_mount)
    monkeypatch.setattr(platform_mod, "volume_adapter", lambda: volumes)
    monkeypatch.setattr(usdm_research, "volume_adapter", lambda: volumes)

    container = tmp_path / "hot" / "usdm"
    p_cold_src = container / "partitions" / "2025-11-12"
    p_open = container / "partitions" / "2025-11-13"
    for p, state, mn, mx in (
        (p_cold_src, "closed", 1_762_992_000_000_000_000, 1_762_992_001_190_000_000),
        (p_open, "open", 1_762_992_001_200_000_000, 1_762_992_002_000_000_000),
    ):
        for d in ("state", "data/active", "data/sealed", "data/manifests"):
            (p / d).mkdir(parents=True, exist_ok=True)
        (p / "partition.json").write_text(
            json.dumps(
                {
                    "schema_version": "partition.v1",
                    "partition_id": p.name,
                    "created_at_utc_ns": mn,
                    "receive_time_utc_range_ns": {"min": mn, "max": mx},
                    "state": state,
                }
            ),
            encoding="utf-8",
        )
    (container / "CURRENT").write_text("2025-11-13\n", encoding="utf-8")

    fixtures = usdm_research.build_usdm_parity_fixtures()

    def write_envs(part_root: Path, envs: list[EventEnvelope], stream: str, symbol: str) -> None:
        layout = ensure_storage_layout(part_root)
        with Catalog(layout.catalog) as catalog:
            writer = RawChunkWriter(
                layout=layout,
                catalog=catalog,
                market="um_perpetual",
                symbol=symbol,
                stream=stream,
                collector_instance_id="inst-r4",
                collector_version="r4-test",
                created_at_utc_ns=envs[0].receive_time_utc_ns,
            )
            for item in envs:
                writer.append(item)
            writer.sync()
            writer.seal()

    # Partition 1 (closed -> archived -> migrated to cold_root):
    # holds exchangeInfo, aggTrades, markPrice, bookTicker, depth_snapshot, diff_depth #1, + 5 REST sources.
    ex_env = _rest_envelope(
        source_stream="exchange_info",
        path="/fapi/v1/exchangeInfo",
        symbol="__GLOBAL__",
        parameters={},
        body_obj=fixtures["exchange_info"],
        receive_ns=1_762_992_000_000_000_000,
    )
    snap_env = _rest_envelope(
        source_stream="depth_snapshot",
        path="/fapi/v1/depth",
        symbol="BTCUSDT",
        parameters={"symbol": "BTCUSDT", "limit": 1000},
        body_obj=fixtures["depth_snapshot"],
        receive_ns=1_762_992_001_150_000_000,
    )
    rest_envs = [
        ex_env,
        snap_env,
        _rest_envelope(
            source_stream="open_interest",
            path="/fapi/v1/openInterest",
            symbol="BTCUSDT",
            parameters={"symbol": "BTCUSDT"},
            body_obj=fixtures["rest_observations"]["open_interest"],
            receive_ns=1_762_992_000_050_000_000,
        ),
        _rest_envelope(
            source_stream="top_long_short_account_ratio_5m",
            path="/futures/data/topLongShortAccountRatio",
            symbol="BTCUSDT",
            parameters={"symbol": "BTCUSDT", "period": "5m", "limit": 1},
            body_obj=fixtures["rest_observations"]["top_long_short_account_ratio_5m"],
            receive_ns=1_762_992_000_060_000_000,
        ),
        _rest_envelope(
            source_stream="top_long_short_position_ratio_5m",
            path="/futures/data/topLongShortPositionRatio",
            symbol="BTCUSDT",
            parameters={"symbol": "BTCUSDT", "period": "5m", "limit": 1},
            body_obj=fixtures["rest_observations"]["top_long_short_position_ratio_5m"],
            receive_ns=1_762_992_000_070_000_000,
        ),
        _rest_envelope(
            source_stream="global_long_short_ratio_5m",
            path="/futures/data/globalLongShortAccountRatio",
            symbol="BTCUSDT",
            parameters={"symbol": "BTCUSDT", "period": "5m", "limit": 1},
            body_obj=fixtures["rest_observations"]["global_long_short_ratio_5m"],
            receive_ns=1_762_992_000_080_000_000,
        ),
        _rest_envelope(
            source_stream="taker_buy_sell_volume_5m",
            path="/futures/data/takerlongshortRatio",
            symbol="BTCUSDT",
            parameters={"symbol": "BTCUSDT", "period": "5m", "limit": 1},
            body_obj=fixtures["rest_observations"]["taker_buy_sell_volume_5m"],
            receive_ns=1_762_992_000_090_000_000,
        ),
    ]
    write_envs(p_cold_src, [rest_envs[0]], "rest_observations", "__GLOBAL__")
    write_envs(p_cold_src, rest_envs[1:], "rest_observations", "BTCUSDT")
    write_envs(
        p_cold_src,
        [
            _ws_envelope(
                stream="agg_trade",
                symbol="BTCUSDT",
                payload=fixtures["ws_payloads"][0]["data"],
                receive_ns=1_762_992_000_100_000_000,
            ),
            _ws_envelope(
                stream="agg_trade",
                symbol="BTCUSDT",
                payload=fixtures["ws_payloads"][1]["data"],
                receive_ns=1_762_992_000_200_000_000,
            ),
        ],
        "agg_trade",
        "BTCUSDT",
    )
    write_envs(
        p_cold_src,
        [
            _ws_envelope(
                stream="mark_price",
                symbol="BTCUSDT",
                payload=fixtures["ws_payloads"][2]["data"],
                receive_ns=1_762_992_001_000_000_000,
                exchange_tx_ms=fixtures["ws_payloads"][2]["data"]["T"],
            )
        ],
        "mark_price",
        "BTCUSDT",
    )
    write_envs(
        p_cold_src,
        [
            _ws_envelope(
                stream="book_ticker",
                symbol="BTCUSDT",
                payload=fixtures["ws_payloads"][3]["data"],
                receive_ns=1_762_992_001_100_000_000,
            )
        ],
        "book_ticker",
        "BTCUSDT",
    )
    write_envs(
        p_cold_src,
        [
            _ws_envelope(
                stream="diff_depth",
                symbol="BTCUSDT",
                payload=fixtures["ws_payloads"][4]["data"],
                receive_ns=1_762_992_001_140_000_000,
            )
        ],
        "diff_depth",
        "BTCUSDT",
    )

    # Archive partition 1 Raw to external volume and move metadata to cold_root!
    layout1 = ensure_storage_layout(p_cold_src)
    with Catalog(layout1.catalog) as cat1:
        reg1 = StorageRegistry(catalog=cat1, volumes=volumes)
        reg1.register(ext_slot)
        storage_id = str(reg1.observe_statuses()[0]["storage_id"])
        archive_drain(
            layout=layout1,
            catalog=cat1,
            storage_id=storage_id,
            max_runtime_seconds=30.0,
            max_files=20,
            volumes=volumes,
        )
    cold = tmp_path / "cold"
    cold.mkdir()
    shutil.move(str(p_cold_src), str(cold / "2025-11-12"))

    # Partition 2 (open hot partition): diff_depth #2 bridges cleanly with #1!
    write_envs(
        p_open,
        [
            _ws_envelope(
                stream="diff_depth",
                symbol="BTCUSDT",
                payload=fixtures["ws_payloads"][5]["data"],
                receive_ns=1_762_992_001_300_000_000,
            )
        ],
        "diff_depth",
        "BTCUSDT",
    )

    out = tmp_path / "export"
    manifest = usdm_research.export_usdm_research(
        container,
        out,
        cold_root=cold,
        symbols=["BTCUSDT", "ETHUSDT"],
    )

    assert len(manifest["resolved_roots"]) == 2
    assert manifest["stream_counts"]["exchange_info"] == 2  # BTCUSDT + ETHUSDT split
    assert manifest["stream_counts"]["agg_trade"] == 2
    assert manifest["stream_counts"]["mark_price"] == 1
    assert manifest["stream_counts"]["diff_depth"] == 2
    assert manifest["stream_counts"]["open_interest"] == 1
    assert manifest["stream_counts"]["top_long_short_account_ratio_5m"] == 1
    assert manifest["stream_counts"]["top_long_short_position_ratio_5m"] == 1
    assert manifest["stream_counts"]["global_long_short_ratio_5m"] == 1
    assert manifest["stream_counts"]["taker_buy_sell_volume_5m"] == 1

    # Parquet schema & values check:
    agg_rows = pq.read_table(out / "parquet/agg_trade.parquet").to_pylist()
    assert [r["normal_quantity"] for r in agg_rows] == ["0.125", "0"]
    mp_rows = pq.read_table(out / "parquet/mark_price.parquet").to_pylist()
    assert mp_rows[0]["mark_price_moving_average"] == "90115.10"
    assert mp_rows[0]["exchange_transaction_time_ms"] is None

    # L2 OrderBook parity across cold+archived partition #1 and hot partition #2:
    btc_book = manifest["books"]["BTCUSDT"]
    assert btc_book["synchronized"] is True
    assert btc_book["last_update_id"] == 103
    # Bids: 90120.00 deleted ("0"), 90119.50 added ("1.000"), 90119.00 ("4.000") kept.
    assert btc_book["bids"] == [["90119.5", "1"], ["90119", "4"]]
    # Asks: 90121.00 ("2.500"), 90122.00 ("3.000").
    assert btc_book["asks"] == [["90121", "2.5"], ["90122", "3"]]
