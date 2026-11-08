"""On-demand official daily klines, using the frozen checksum/revision importer."""

import argparse
import json
import re
from datetime import date
from pathlib import Path

from binance_market_data_recorder.backfill.importer import HistoricalImporter
from binance_market_data_recorder.backfill.planner import PlanEntry


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("root", type=Path)
    p.add_argument("--market", choices=["spot", "usdm"], required=True)
    p.add_argument("--symbol", required=True)
    p.add_argument("--date", required=True)
    p.add_argument(
        "--interval",
        default="1m",
        choices=[
            "1m",
            "3m",
            "5m",
            "15m",
            "30m",
            "1h",
            "2h",
            "4h",
            "6h",
            "8h",
            "12h",
            "1d",
            "3d",
            "1w",
            "1mo",
        ],
    )
    a = p.parse_args()
    day = date.fromisoformat(a.date)
    if not re.fullmatch("[A-Z0-9]+", a.symbol):
        p.error("symbol must be uppercase ASCII")
    market = "spot" if a.market == "spot" else "futures/um"
    filename = f"{a.symbol}-{a.interval}-{day.isoformat()}.zip"
    url = f"https://data.binance.vision/data/{market}/daily/klines/{a.symbol}/{a.interval}/{filename}"
    entry = PlanEntry(
        market=market,
        symbol=a.symbol,
        data_type="klines",
        interval=a.interval,
        granularity="daily",
        period=day.isoformat(),
        start_date=day.isoformat(),
        end_date=day.isoformat(),
        timestamp_unit="microseconds"
        if a.market == "spot" and day >= date(2025, 1, 1)
        else "milliseconds",
        zip_url=url,
        checksum_url=url + ".CHECKSUM",
        estimated_bytes=3000000,
    )
    result = HistoricalImporter(data_root=a.root).import_entry(entry)
    print(json.dumps(result.public_dict(), indent=2))
    if result.status == "GAP":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
