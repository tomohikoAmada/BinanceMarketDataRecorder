from __future__ import annotations

import os

import pytest

from binance_market_data_recorder.binance.spot.exchange_info import (
    capture_spot_exchange_info,
)
from binance_market_data_recorder.binance.usdm.side_data_rest import (
    RestSideDataKind,
    capture_rest_side_data,
)

pytestmark = [
    pytest.mark.online,
    pytest.mark.skipif(
        os.environ.get("BINANCE_MARKET_RECORDER_ONLINE") != "1",
        reason="set BINANCE_MARKET_RECORDER_ONLINE=1 for unsigned public M19 smoke",
    ),
]

def test_spot_exchange_info_unsigned_public_smoke() -> None:
    envelope = capture_spot_exchange_info(
        collector_instance_id="m19-online-spot",
        collector_version="test",
    )
    assert envelope.stream == "exchange_info"
    assert "official_sdk_model_no_raw_http_body" in envelope.capture_flags


@pytest.mark.parametrize(
    "kind",
    [
        RestSideDataKind.OPEN_INTEREST_STATISTICS,
        RestSideDataKind.TAKER_BUY_SELL_VOLUME,
        RestSideDataKind.GLOBAL_LONG_SHORT_RATIO,
        RestSideDataKind.TOP_LONG_SHORT_ACCOUNT_RATIO,
        RestSideDataKind.TOP_LONG_SHORT_POSITION_RATIO,
        RestSideDataKind.BASIS,
    ],
)
def test_usdm_five_minute_statistics_unsigned_public_smoke(
    kind: RestSideDataKind,
) -> None:
    envelope = capture_rest_side_data(
        kind=kind,
        collector_instance_id="m19-online-usdm",
        collector_version="test",
    )
    assert envelope.stream == kind.value
    assert envelope.source_sequence["period"] == "5m"
