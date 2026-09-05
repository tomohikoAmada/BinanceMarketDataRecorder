from __future__ import annotations

import os
import time

import pytest

from binance_market_data_recorder.binance.spot.exchange_info import (
    capture_spot_exchange_info,
)
from binance_market_data_recorder.binance.usdm.side_data_rest import (
    FIVE_MINUTE_PERIOD_MS,
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
    # These public statistics do not all become available at the same instant.
    # Use a fully closed window one hour behind wall clock so the online smoke
    # tests API semantics rather than transient publication latency.
    request_start = (
        int(time.time() * 1000) // FIVE_MINUTE_PERIOD_MS
    ) * FIVE_MINUTE_PERIOD_MS - 12 * FIVE_MINUTE_PERIOD_MS
    envelope = capture_rest_side_data(
        kind=kind,
        symbol="BTCUSDT",
        collector_instance_id="m19-online-usdm",
        collector_version="test",
        period_start_ms=request_start,
        period_end_ms=request_start + FIVE_MINUTE_PERIOD_MS - 1,
    )
    assert envelope.stream == kind.value
    assert envelope.source_sequence["period"] == "5m"
    record_count = envelope.source_sequence["requestedRecordCount"]
    first_timestamp = envelope.source_sequence.get("firstRequestedTimestamp")
    last_timestamp = envelope.source_sequence.get("lastRequestedTimestamp")
    requested_start = envelope.source_sequence["requestedStartTimestamp"]
    requested_end = envelope.source_sequence["requestedEndTimestamp"]
    assert isinstance(record_count, int) and record_count >= 1
    assert isinstance(first_timestamp, int)
    assert isinstance(last_timestamp, int)
    assert isinstance(requested_start, int)
    assert isinstance(requested_end, int)
    assert requested_start <= first_timestamp <= last_timestamp <= requested_end
