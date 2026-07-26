from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq  # type: ignore[import-untyped]

from binance_market_data_recorder.domain.event import EventEnvelope
from binance_market_data_recorder.normalize import Normalizer
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tests.normalization_support import envelope, provenance, seal_events
from tests.unit.test_m19_normalized_parser import FIVE_MINUTE_MODELS


def _m19_envelope(
    stream: str,
    model: object,
    ordinal: int,
) -> EventEnvelope:
    return envelope(
        market="um_perpetual",
        stream=stream,
        raw_payload=provenance(
            schema_version="binance-usdm-side-rest-provenance.v1",
            model=model,
            path=f"/futures/data/{stream}",
            kind=stream,
            parameters={
                "symbol": "BTCUSDT",
                "period": "5m",
                "startTime": 300_000,
                "endTime": 899_999,
            },
        ),
        ordinal=ordinal,
        module="binance.usdm.side_rest.v1",
        exchange_event_time=None,
    )


def _spot_exchange_envelope(ordinal: int) -> EventEnvelope:
    model = {
        "timezone": "UTC",
        "serverTime": 1_725_000_000_000,
        "rateLimits": [{"rateLimitType": "REQUEST_WEIGHT", "limit": 6000}],
        "symbols": [
            {
                "symbol": "BTCUSDT",
                "status": "TRADING",
                "orderTypes": ["LIMIT", "MARKET"],
                "filters": [
                    {"filterType": "PRICE_FILTER", "tickSize": "0.01000000"}
                ],
                "permissions": ["SPOT"],
                "permissionSets": [["SPOT"]],
            }
        ],
    }
    return envelope(
        market="spot",
        stream="exchange_info",
        raw_payload=provenance(
            schema_version="binance-spot-exchange-info-provenance.v1",
            model=model,
            path="/api/v3/exchangeInfo",
        ),
        ordinal=ordinal,
        module="binance.spot.rest.exchange_info.v1",
        exchange_event_time=1_725_000_000_000,
    )


def _rows_by_stream(
    root: Path, build: dict[str, Any]
) -> dict[str, list[dict[str, Any]]]:
    output: dict[str, list[dict[str, Any]]] = {}
    for partition in build["partitions"]:
        path = root / partition["relative_path"]
        rows = pq.read_table(path).to_pylist()
        output.setdefault(str(partition["stream"]), []).extend(rows)
    return output


def test_m19_raw_to_parquet_is_supported_deduplicated_and_conflict_visible(
    tmp_path: Path,
) -> None:
    layout = ensure_storage_layout(tmp_path)
    duplicate_ratio = FIVE_MINUTE_MODELS["global_long_short_ratio_5m"]
    conflicting_ratio = json.loads(
        json.dumps(FIVE_MINUTE_MODELS["top_long_short_account_ratio_5m"])
    )
    conflicting_ratio[0]["longAccount"] = "0.99999999"
    taker_requested = FIVE_MINUTE_MODELS["taker_buy_sell_volume_5m"][0]
    taker_overlap = {
        **taker_requested,
        "timestamp": 0,
    }
    malformed_oi = [
        {
            "symbol": "BTCUSDT",
            "sumOpenInterest": "1.00000000",
            "timestamp": 900_000,
        }
    ]
    groups = [
        [_spot_exchange_envelope(1)],
        [
            _m19_envelope(
                "open_interest_statistics_5m",
                FIVE_MINUTE_MODELS["open_interest_statistics_5m"],
                2,
            ),
            _m19_envelope(
                "open_interest_statistics_5m",
                malformed_oi,
                3,
            ),
        ],
        [
            _m19_envelope(
                "taker_buy_sell_volume_5m",
                [taker_overlap, taker_requested],
                4,
            ),
            _m19_envelope(
                "taker_buy_sell_volume_5m",
                [taker_requested],
                5,
            ),
        ],
        [
            _m19_envelope("global_long_short_ratio_5m", duplicate_ratio, 6),
            _m19_envelope("global_long_short_ratio_5m", duplicate_ratio, 7),
        ],
        [
            _m19_envelope(
                "top_long_short_account_ratio_5m",
                FIVE_MINUTE_MODELS["top_long_short_account_ratio_5m"],
                8,
            ),
            _m19_envelope(
                "top_long_short_account_ratio_5m",
                conflicting_ratio,
                9,
            ),
        ],
        [
            _m19_envelope(
                "top_long_short_position_ratio_5m",
                FIVE_MINUTE_MODELS["top_long_short_position_ratio_5m"],
                10,
            )
        ],
        [
            _m19_envelope("basis_5m", FIVE_MINUTE_MODELS["basis_5m"], 11),
            _m19_envelope("basis_5m", [], 12),
        ],
    ]
    with Catalog(layout.catalog) as catalog:
        for events in groups:
            seal_events(
                layout=layout,
                catalog=catalog,
                events=events,
            )
        result = Normalizer(layout=layout, catalog=catalog).run()
        assert result.status == "BUILT"
        assert result.partitions == 7
        assert result.normalized_rows == 12
        assert result.duplicate_rows_removed == 2
        assert result.identity_conflicts == 2
        assert result.build_manifest is not None
        build = json.loads(
            (layout.root / result.build_manifest).read_text(encoding="utf-8")
        )

    rows = _rows_by_stream(layout.root, build)
    assert set(rows) == {
        "exchange_info",
        "open_interest_statistics_5m",
        "taker_buy_sell_volume_5m",
        "global_long_short_ratio_5m",
        "top_long_short_account_ratio_5m",
        "top_long_short_position_ratio_5m",
        "basis_5m",
    }
    assert rows["exchange_info"][0]["order_types_json"] == '["LIMIT","MARKET"]'
    assert len(rows["open_interest_statistics_5m"]) == 3
    assert sum(
        row["valid"] is False
        for row in rows["open_interest_statistics_5m"]
    ) == 1
    assert {
        (row["timestamp_ms"], row["duplicate_count"])
        for row in rows["taker_buy_sell_volume_5m"]
    } == {(0, 1), (300_000, 2)}
    assert rows["global_long_short_ratio_5m"][0]["duplicate_count"] == 2
    assert {
        row["identity_conflict"]
        for row in rows["top_long_short_account_ratio_5m"]
    } == {True}
    empty = next(row for row in rows["basis_5m"] if row["observation_empty"])
    assert empty["timestamp_ms"] is None
