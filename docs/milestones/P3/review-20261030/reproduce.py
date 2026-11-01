"""Offline independent review probes; run with PYTHONPATH=. from the repo root.

Only fresh TemporaryDirectory fixtures are touched. Reports observations rather
than redefining a passing product test around the currently broken behavior.
"""

from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory

from binance_market_data_recorder.binance.public_rest import capture_public_response
from binance_market_data_recorder.binance.public_sources import product_sources
from binance_market_data_recorder.binance.public_websocket import (
    DepthSequenceObserver,
    envelope_for_group,
    stream_groups,
)
from binance_market_data_recorder.collector.coverage import ProductCapture
from binance_market_data_recorder.metrics.continuity import continuity_report
from binance_market_data_recorder.service.verification import verify_new
from binance_market_data_recorder.spool.seal import RECONNECT_GAP_FLAG, seal_partial
from binance_market_data_recorder.spool.writer import RawChunkWriter
from binance_market_data_recorder.storage.catalog import Catalog, ChunkState
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tests.factories import event
from tests.unit.test_p2_coverage import SMALL, Response, products
from tests.unit.test_p3_availability import BASE, flush, owner_at


def manifest_gap(root: Path, *, empty: bool) -> dict[str, object]:
    layout = ensure_storage_layout(root)
    with Catalog(layout.catalog) as catalog:
        writer = RawChunkWriter(
            layout=layout, catalog=catalog, market="spot", symbol="BTCUSDT",
            stream="diff_depth", collector_instance_id="review", collector_version="test",
            durability_interval_seconds=0,
        )
        if not empty:
            writer.append(event(1))
        writer.close()
        manifest = seal_partial(
            writer.path, layout=layout, catalog=catalog,
            forced_flags=frozenset({RECONNECT_GAP_FLAG}),
            seal_intent={"required_forced_flags": [RECONNECT_GAP_FLAG]},
        )
        control = verify_new(layout, catalog)
        assert control["verified_files"] == 1
        chunk_id = str(manifest["chunk_id"])
        row = catalog.chunk(chunk_id)
        assert row is not None
        manifest.update(capture_flags=[], gap=False, complete=True)
        (layout.root / str(row["manifest_path"])).write_text(json.dumps(manifest))
        catalog.enroll_raw_verification(chunk_id)
        result = verify_new(layout, catalog)
        return {
            "finding": "P3-R1", "record_count": manifest["record_count"],
            "valid_control_verified": control["verified_files"],
            "after_false_complete_verified": result["verified_files"],
            "after_false_complete_failed": result["failed_files"],
            "catalog_sealing_evidence": catalog.latest_transition_evidence(
                chunk_id, ChunkState.SEALING
            ),
            "expected": "Reject missing durable reconnect flags; never publish VERIFIED.",
            "finding_reproduced": result["verified_files"] == 1,
        }


def sparse_writer(root: Path) -> dict[str, object]:
    layout = ensure_storage_layout(root)
    with Catalog(layout.catalog) as catalog:
        owner = owner_at(root, catalog)
        owner.sources.clear()
        product = next(p for p in products() if p.key.market == "um_perpetual")
        source = next(s for s in product_sources(product, SMALL) if s.stream == "liquidation")
        owner.add_source(source)
        state = owner.sources[source.key]
        state.first_due, state.connected = BASE, True
        collector = owner.make_group(
            stream_groups((source,), 128)[0], product, lambda e: None, lambda e: None
        )
        collector._received_total = collector._persisted_total = 1
        collector._last_write_utc_ns = BASE
        for second in range(60):
            owner._observe_availability(BASE + second * 1_000_000_000)
        collector._received_total = 2
        collector._pending_since_utc_ns = BASE + 59_900_000_000
        owner._observe_availability(BASE + 60_000_000_000)
        flush(owner.availability, catalog)
        report = continuity_report(
            catalog, start=BASE, end=BASE + 60_000_000_000,
            source_ids=["/".join(source.key)],
        )["sources"][0]
        return {
            "finding": "P3-R2", "pending_seconds": 0.1,
            "reason": state.availability["reason"],
            "unavailable_seconds": report["unavailable_or_unknown_ns"] / 1e9,
            "expected": "No writer stall after only 0.1s pending; quiet time stays healthy.",
            "finding_reproduced": state.availability["reason"] == "WRITER_STALLED",
        }


def brief_depth_gap(root: Path, stream: str) -> dict[str, object]:
    layout = ensure_storage_layout(root)
    with Catalog(layout.catalog) as catalog:
        owner = owner_at(root, catalog)
        owner.sources.clear()
        product = next(p for p in products() if p.key.market == "um_perpetual")
        sources = product_sources(product, SMALL)
        source = next(s for s in sources if s.stream == stream)
        snapshot_source = next(s for s in sources if s.stream == (
            "rpi_depth_snapshot" if stream == "rpi_depth" else "depth_snapshot"
        ))
        for item in (source, snapshot_source):
            owner.add_source(item)
        capture = ProductCapture(owner, product)
        owner.collectors[product.key] = capture
        state = owner.sources[source.key]
        state.first_due, state.connected = BASE, True
        group = stream_groups((source,), 128)[0]
        sequence = DepthSequenceObserver()

        def receive(first: int, last: int, previous: int, elapsed: int) -> None:
            raw = json.dumps({"stream": group.wire_names[0], "data": {
                "s": product.key.symbol, "ps": product.pair, "st": 1,
                "e": "depthUpdate", "E": 1000, "T": 999,
                "U": first, "u": last, "pu": previous, "b": [], "a": [],
            }}).encode()
            capture.persisted(sequence.observe(envelope_for_group(
                group, product, raw_payload=raw, connection_id="c",
                collector_instance_id="i", collector_version="v",
                receive_time_utc_ns=BASE + elapsed, receive_monotonic_ns=elapsed,
            )))

        def snapshot(last: int) -> None:
            capture.snapshot_persisted(capture_public_response(
                snapshot_source,
                lambda s, p: Response({"lastUpdateId": last, "bids": [], "asks": []}),
                collector_instance_id="i", collector_version="v",
            ).envelope)

        receive(9, 10, 8, 0)
        snapshot(9)
        receive(11, 12, 10, 1)
        owner._observe_availability(BASE)
        receive(15, 16, 14, 100_000_000)
        unreliable = not (capture.rpi.is_reliable if stream == "rpi_depth" else
                          capture.readiness_snapshot().orderbook_synchronized)
        snapshot(15)
        receive(17, 18, 16, 300_000_000)
        recovered = (capture.rpi.is_reliable if stream == "rpi_depth" else
                     capture.readiness_snapshot().orderbook_synchronized)
        # Keep normal one-second monitoring; persist the ten-second healthy checkpoint.
        for second in range(1, 12):
            owner._observe_availability(BASE + second * 1_000_000_000)
        flush(owner.availability, catalog)
        report = continuity_report(
            catalog, start=BASE, end=BASE + 10_000_000_000,
            source_ids=["/".join(source.key)],
        )["sources"][0]
        return {
            "finding": "P3-R3", "stream": stream,
            "sequence_failure_observed": unreliable, "recovered_before_monitor": recovered,
            "unavailable_ns": report["unavailable_or_unknown_ns"],
            "coverage": report["coverage"],
            "expected": "Retain the observed sequence failure even when recovery is quick.",
            "finding_reproduced": unreliable and recovered and report["coverage"] == 1,
        }


if __name__ == "__main__":
    with TemporaryDirectory(prefix="p3-independent-review-") as temporary:
        root = Path(temporary)
        probes = [
            manifest_gap(root / "nonempty", empty=False),
            manifest_gap(root / "empty", empty=True),
            sparse_writer(root / "sparse"),
            brief_depth_gap(root / "depth", "diff_depth"),
            brief_depth_gap(root / "rpi", "rpi_depth"),
        ]
    print(json.dumps({"probes": probes}, indent=2))
