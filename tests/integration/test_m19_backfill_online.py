from __future__ import annotations

import os
import urllib.request
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from urllib.parse import urlparse

import pyarrow.parquet as pq  # type: ignore[import-untyped]
import pytest

from binance_market_data_recorder.backfill.importer import HistoricalImporter
from binance_market_data_recorder.backfill.planner import PlanEntry, build_plan

pytestmark = [
    pytest.mark.online,
    pytest.mark.skipif(
        os.environ.get("BINANCE_MARKET_RECORDER_ONLINE") != "1",
        reason="set BINANCE_MARKET_RECORDER_ONLINE=1 for official backfill smoke",
    ),
]

KNOWN_DATE = date(2024, 1, 1)
CHECKSUM_ENTRIES = (
    *build_plan("baseline-bars", KNOWN_DATE, KNOWN_DATE).entries,
    *build_plan("microstructure-trades", KNOWN_DATE, KNOWN_DATE).entries,
)


@pytest.mark.parametrize(
    "entry",
    CHECKSUM_ENTRIES,
    ids=lambda entry: f"{entry.market}-{entry.data_type}",
)
def test_official_historical_checksum_url_exists(entry: PlanEntry) -> None:
    request = urllib.request.Request(
        entry.checksum_url,
        headers={"User-Agent": "BinanceMarketDataRecorder/0.1"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        body = response.read()
        assert response.status == 200
    filename = Path(urlparse(entry.zip_url).path).name
    digest, _text = HistoricalImporter._parse_checksum(body, filename)
    assert len(digest) == 64


def test_download_verify_and_parse_small_spot_one_minute_day(
    tmp_path: Path,
) -> None:
    entry = next(
        item
        for item in build_plan(
            "baseline-bars", KNOWN_DATE, KNOWN_DATE
        ).entries
        if item.market == "spot" and item.data_type == "klines"
    )
    importer = HistoricalImporter(data_root=tmp_path)
    result = importer.import_entry(entry)
    assert result.status == "IMPORTED"
    assert result.normalized_path is not None
    parquet = pq.ParquetFile(result.normalized_path)
    assert parquet.metadata.num_rows > 0
    timestamps = parquet.read(
        columns=["archive_event_time_utc_ns"]
    ).column(0).to_pylist()
    day_start = int(
        datetime.combine(KNOWN_DATE, datetime.min.time(), tzinfo=UTC).timestamp()
        * 1_000_000_000
    )
    day_end = day_start + int(timedelta(days=1).total_seconds() * 1_000_000_000)
    assert min(timestamps) >= day_start
    assert max(timestamps) < day_end
    assert importer.verify()["verified_source_revisions"] == 1
