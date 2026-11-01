"""P4-only preflight: freeze the public directory and complete expected source plan."""

from __future__ import annotations

import hashlib
import json
import sys
import time
from pathlib import Path

from binance_market_data_recorder.binance.products import ProductDirectory
from binance_market_data_recorder.binance.public_rest import (
    PublicRestClient,
    capture_public_response,
)
from binance_market_data_recorder.binance.public_sources import (
    directory_source,
    nominal_derivative_weight_per_minute,
    source_plan,
    source_selection_report,
)
from binance_market_data_recorder.config import load_config


def main() -> None:
    config_path, evidence_path = map(Path, sys.argv[1:])
    config = load_config(config_file=config_path).config
    coverage = config.coverage
    assert coverage is not None and coverage.underlying_assets == ("BTC",)
    directory = ProductDirectory(coverage)
    directories = []
    for venue in ("spot", "usdm", "coinm"):
        source = directory_source(venue, coverage)
        result = capture_public_response(
            source,
            PublicRestClient(venue, config.proxy_policy()).request,
            collector_instance_id="P4-preflight",
            collector_version="P4-preflight",
        )
        if not result.valid:
            raise RuntimeError(f"{venue} directory failed: {result.error}")
        directory.apply(venue, result.document)
        directories.append(source)
        with (evidence_path / f"directory-{venue}.json").open("xb") as handle:
            handle.write(result.envelope.raw_payload)
    now = time.time_ns()
    products = tuple(
        p
        for p in directory.products.values()
        if p.status == "TRADING"
        and (p.perpetual or p.delivery_time_ms is None or p.delivery_time_ms * 1_000_000 > now)
    )
    plan = (*directories, *source_plan(products, coverage))
    weight = nominal_derivative_weight_per_minute(tuple(plan[3:]), coverage)
    assert coverage.max_steady_derivative_weight_per_minute is not None
    assert weight <= coverage.max_steady_derivative_weight_per_minute
    assert len(products) <= coverage.max_products
    assert sum(s.transport == "ws" for s in plan) <= coverage.max_streams
    frozen = {
        "schema_version": "p4-run-freeze.v1",
        "frozen_at_utc_ns": now,
        "source_git_sha": "ff865cd2c24cd9b8e41d5088725d9fd166cbafff",
        "config_sha256": hashlib.sha256(config_path.read_bytes()).hexdigest(),
        "config": config.model_dump(mode="json"),
        "products": [p.public_dict() for p in products],
        "directory_products": [p.public_dict() for p in directory.products.values()],
        "sources": [s.public_dict() for s in plan],
        "source_ids": sorted("/".join(s.key) for s in plan),
        "source_selection": source_selection_report(products, coverage, tuple(plan[3:])),
        "nominal_derivative_weight_per_minute": weight,
        "preparation_limit_seconds": 900,
        "sample_seconds": 30,
        "checkpoint_seconds": [7200, 86400],
        "threshold": 0.995,
        "production_qualified": False,
        "dormant_policy": "after_delivery sources are ineligible until their dated delivery",
        "capacity_policy": "ordinary P3 hard reserve max(5GiB,2%capacity,2*rotation)",
    }
    with (evidence_path / "freeze.json").open("x") as handle:
        json.dump(frozen, handle, sort_keys=True, indent=2)
    print(json.dumps({"products": len(products), "sources": len(plan), "weight": weight}))


if __name__ == "__main__":
    main()
