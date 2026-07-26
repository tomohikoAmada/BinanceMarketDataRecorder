from __future__ import annotations

import json

import pytest

from binance_market_data_recorder.normalize.model import SUPPORTED_STREAMS
from binance_market_data_recorder.normalize.parser import parse_envelope
from tests.normalization_support import envelope, fixture, provenance


def _models() -> dict[tuple[str, str], bytes]:
    depth_model = {
        "lastUpdateId": 160,
        "bids": [["1.0", "2.0"]],
        "asks": [["2.0", "3.0"]],
    }
    return {
        ("spot", "diff_depth"): fixture("spot", "diff_depth.json"),
        ("spot", "agg_trade"): fixture("spot", "agg_trade.json"),
        ("spot", "book_ticker"): fixture("spot", "book_ticker.json"),
        ("spot", "depth_snapshot"): provenance(
            schema_version="binance-spot-depth-snapshot-provenance.v1",
            model=depth_model,
            path="/api/v3/depth",
        ),
        ("um_perpetual", "diff_depth"): fixture("usdm", "diff_depth.json"),
        ("um_perpetual", "agg_trade"): fixture("usdm", "agg_trade.json"),
        ("um_perpetual", "book_ticker"): fixture("usdm", "book_ticker.json"),
        ("um_perpetual", "depth_snapshot"): provenance(
            schema_version="binance-usdm-depth-snapshot-provenance.v1",
            model=depth_model,
            path="/fapi/v1/depth",
        ),
        ("um_perpetual", "mark_price"): fixture("usdm", "mark_price.json"),
        ("um_perpetual", "liquidation"): fixture("usdm", "liquidation.json"),
        ("um_perpetual", "premium_index_snapshot"): provenance(
            schema_version="binance-usdm-side-rest-provenance.v1",
            model=json.loads(fixture("usdm", "premium_index.json")),
            path="/fapi/v1/premiumIndex",
            kind="premium_index_snapshot",
        ),
        ("um_perpetual", "funding_history"): provenance(
            schema_version="binance-usdm-side-rest-provenance.v1",
            model=json.loads(fixture("usdm", "funding_history.json")),
            path="/fapi/v1/fundingRate",
            kind="funding_history",
        ),
        ("um_perpetual", "funding_info"): provenance(
            schema_version="binance-usdm-side-rest-provenance.v1",
            model=json.loads(fixture("usdm", "funding_info.json")),
            path="/fapi/v1/fundingInfo",
            kind="funding_info",
        ),
        ("um_perpetual", "open_interest"): provenance(
            schema_version="binance-usdm-side-rest-provenance.v1",
            model=json.loads(fixture("usdm", "open_interest.json")),
            path="/fapi/v1/openInterest",
            kind="open_interest",
        ),
        ("um_perpetual", "exchange_info"): provenance(
            schema_version="binance-usdm-side-rest-provenance.v1",
            model=json.loads(fixture("usdm", "exchange_info.json")),
            path="/fapi/v1/exchangeInfo",
            kind="exchange_info",
        ),
    }


@pytest.mark.parametrize(("identity", "payload"), sorted(_models().items()))
def test_every_raw_stream_has_a_versioned_parser(
    identity: tuple[str, str], payload: bytes
) -> None:
    market, stream = identity
    parsed = parse_envelope(
        envelope(
            market=market,  # type: ignore[arg-type]
            stream=stream,
            raw_payload=payload,
            ordinal=1,
            module=(
                f"binance.{'spot' if market == 'spot' else 'usdm'}.rest.v1"
                if stream
                in {
                    "depth_snapshot",
                    "premium_index_snapshot",
                    "funding_history",
                    "funding_info",
                    "open_interest",
                    "exchange_info",
                }
                else None
            ),
        )
    )
    assert parsed
    assert all(row.valid for row in parsed)


def test_supported_stream_matrix_is_complete() -> None:
    assert {
        "spot": frozenset(
            {
                "diff_depth",
                "agg_trade",
                "book_ticker",
                "depth_snapshot",
                "exchange_info",
            }
        ),
        "um_perpetual": frozenset(
            {
                "diff_depth",
                "agg_trade",
                "book_ticker",
                "depth_snapshot",
                "mark_price",
                "liquidation",
                "premium_index_snapshot",
                "funding_history",
                "funding_info",
                "open_interest",
                "exchange_info",
                "open_interest_statistics_5m",
                "taker_buy_sell_volume_5m",
                "global_long_short_ratio_5m",
                "top_long_short_account_ratio_5m",
                "top_long_short_position_ratio_5m",
                "basis_5m",
            }
        ),
    } == SUPPORTED_STREAMS


def test_funding_history_empty_and_funding_info_absence_are_explicit() -> None:
    empty_history = parse_envelope(
        envelope(
            market="um_perpetual",
            stream="funding_history",
            raw_payload=provenance(
                schema_version="binance-usdm-side-rest-provenance.v1",
                model=[],
                path="/fapi/v1/fundingRate",
                kind="funding_history",
            ),
            ordinal=1,
            module="binance.usdm.side_rest.v1",
        )
    )
    absent_info = parse_envelope(
        envelope(
            market="um_perpetual",
            stream="funding_info",
            raw_payload=provenance(
                schema_version="binance-usdm-side-rest-provenance.v1",
                model=json.loads(fixture("usdm", "funding_info.json")),
                path="/fapi/v1/fundingInfo",
                kind="funding_info",
            ),
            ordinal=2,
            module="binance.usdm.side_rest.v1",
        )
    )
    assert empty_history[0].fields["observation_empty"] is True
    assert absent_info[0].fields["symbol_present"] is False


def test_malformed_payload_becomes_invalid_row_instead_of_disappearing() -> None:
    parsed = parse_envelope(
        envelope(
            market="spot",
            stream="agg_trade",
            raw_payload=b"{broken",
            ordinal=1,
            flags=("malformed",),
        )
    )
    assert len(parsed) == 1
    assert parsed[0].valid is False
    assert parsed[0].error_code == "UPSTREAM_MALFORMED"
