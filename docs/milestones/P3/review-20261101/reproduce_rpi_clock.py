"""Offline R3 re-review: RPI callbacks must not clear known clock uncertainty.

Run with PYTHONPATH=. from the repository root. Only the fixture's Python clock
is patched; the host clock, network, service and production data are untouched.
"""

from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from binance_market_data_recorder.binance.public_rest import capture_public_response
from binance_market_data_recorder.binance.public_sources import product_sources
from binance_market_data_recorder.binance.public_websocket import (
    DepthSequenceObserver,
    envelope_for_group,
    stream_groups,
)
from binance_market_data_recorder.collector.coverage import ProductCapture
from binance_market_data_recorder.metrics.continuity import continuity_report
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tests.unit.test_p2_coverage import SMALL, Response, products
from tests.unit.test_p3_availability import BASE, flush, owner_at


def scenario(root: Path, *, frames_during_uncertainty: bool) -> dict[str, object]:
    layout = ensure_storage_layout(root)
    wall = [BASE]
    monotonic = 0
    with patch("time.time_ns", lambda: wall[0]), Catalog(layout.catalog) as catalog:
        owner = owner_at(root, catalog)
        owner.sources.clear()
        product = next(p for p in products() if p.key.market == "um_perpetual")
        sources = product_sources(product, SMALL)
        source = next(s for s in sources if s.stream == "rpi_depth")
        snapshot_source = next(s for s in sources if s.stream == "rpi_depth_snapshot")
        for item in (source, snapshot_source):
            owner.add_source(item)
        capture = ProductCapture(owner, product)
        owner.collectors[product.key] = capture
        state = owner.sources[source.key]
        state.connected = True
        group = stream_groups((source,), 128)[0]
        sequence = DepthSequenceObserver()
        spool = owner.spool(source.market, source.symbol, group.name)
        key = "/".join(source.key)

        def receive(first: int, last: int, previous: int) -> None:
            nonlocal monotonic
            monotonic += 1_000_000
            raw = json.dumps({"stream": group.wire_names[0], "data": {
                "s": product.key.symbol, "ps": product.pair, "st": 1,
                "e": "depthUpdate", "E": 1000, "T": 999,
                "U": first, "u": last, "pu": previous, "b": [], "a": [],
            }}).encode()
            envelope = sequence.observe(envelope_for_group(
                group, product, raw_payload=raw, connection_id="c",
                collector_instance_id="i", collector_version="v",
                receive_time_utc_ns=wall[0], receive_monotonic_ns=monotonic,
            ))
            spool.enqueue(envelope)
            spool.drain_all()
            capture.persisted(envelope)

        receive(9, 10, 8)
        capture.snapshot_persisted(capture_public_response(
            snapshot_source,
            lambda s, p: Response({"lastUpdateId": 9, "bids": [], "asks": []}),
            collector_instance_id="i", collector_version="v",
        ).envelope)
        assert capture.rpi.is_reliable
        for second in range(11):
            wall[0] = BASE + second * 1_000_000_000
            owner._observe_availability(wall[0])

        observations = []
        # Wall time moves back ten seconds; frame monotonic timestamps keep increasing.
        for second in range(10):
            wall[0] = BASE + second * 1_000_000_000
            owner._observe_availability(wall[0])
            before = owner.availability.sources[key].reason
            if frames_during_uncertainty:
                wall[0] += 1_000_000  # A valid persisted RPI frame 1ms after the monitor.
                receive(11 + 2 * second, 12 + 2 * second, 10 + 2 * second)
            observations.append({
                "second": second,
                "clock_still_unreliable": wall[0] < owner._clock_unreliable_until,
                "before_callback_reason": before,
                "after_callback_reason": owner.availability.sources[key].reason,
            })

        # Persist a healthy checkpoint after the uncertainty ends. Do not confuse
        # a report's unobserved tail with the already observed ten-second interval.
        for second in range(10, 22):
            wall[0] = BASE + second * 1_000_000_000
            owner._observe_availability(wall[0])
        flush(owner.availability, catalog)
        report = continuity_report(
            catalog, start=BASE, end=BASE + 10_000_000_000, source_ids=[key],
        )["sources"][0]
        spool.close_and_seal()
        return {
            "frames_during_uncertainty": frames_during_uncertainty,
            "expected_unknown_ns": 10_000_000_000,
            "actual_unknown_ns": report["unavailable_or_unknown_ns"],
            "coverage": report["coverage"],
            "unavailable_intervals": report["unavailable_intervals"],
            "observations": observations,
            "finding_reproduced": report["unavailable_or_unknown_ns"] < 10_000_000_000,
        }


if __name__ == "__main__":
    with TemporaryDirectory(prefix="p3-r3-clock-review-") as temporary:
        root = Path(temporary)
        results = [
            scenario(root / "control", frames_during_uncertainty=False),
            scenario(root / "rpi_frames", frames_during_uncertainty=True),
        ]
    print(json.dumps({"finding": "P3-R3 residual", "cases": results}, indent=2))
