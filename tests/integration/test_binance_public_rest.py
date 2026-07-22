from __future__ import annotations

import os
from typing import Any, cast

import pytest

from tools.probe_binance_transports import online_public_rest_smoke

pytestmark = pytest.mark.online


@pytest.mark.skipif(
    os.environ.get("BINANCE_MARKET_RECORDER_ONLINE") != "1",
    reason="set BINANCE_MARKET_RECORDER_ONLINE=1 for unsigned public API smoke",
)
def test_spot_and_usdm_public_depth_snapshots() -> None:
    result = cast(dict[str, Any], online_public_rest_smoke())
    assert result["credentials_read"] is False
    assert result["account_api_accessed"] is False
    assert result["results"]["spot"]["status"] == 200
    assert result["results"]["usdm"]["status"] == 200
