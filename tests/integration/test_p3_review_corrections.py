from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

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
from tests.unit.test_p3_availability import BASE, owner_at


@pytest.mark.parametrize("empty", [False, True])
@pytest.mark.parametrize("authority", ["flags", "intent", "recovery"])
def test_p3_r1_manifest_cannot_erase_durable_incompleteness(
    tmp_path: Path,
    empty: bool,
    authority: str,
) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        writer = RawChunkWriter(
            layout=layout,
            catalog=catalog,
            market="spot",
            symbol="BTCUSDT",
            stream="diff_depth",
            collector_instance_id="review",
            collector_version="test",
            durability_interval_seconds=0,
        )
        if not empty:
            writer.append(event(1))
        writer.close()
        chunk_id = str(writer.header.chunk_id)
        if authority == "recovery":
            catalog.transition(
                chunk_id,
                ChunkState.RECOVERED,
                idempotency_key="recovered:" + chunk_id,
                evidence={"verified_frames": int(not empty), "reason": "retained_tail"},
            )
        manifest = seal_partial(
            writer.path,
            layout=layout,
            catalog=catalog,
            forced_flags=frozenset({RECONNECT_GAP_FLAG})
            if authority != "recovery"
            else frozenset(),
            seal_intent={"required_forced_flags": [RECONNECT_GAP_FLAG]}
            if authority == "intent"
            else None,
        )
        if authority == "intent":
            # Isolate the intent authority, rather than accidentally testing only seal_forced_flags.
            evidence = catalog.latest_transition_evidence(chunk_id, ChunkState.SEALING)
            assert evidence is not None
            evidence.pop("seal_forced_flags")
            catalog._connection.execute(
                "UPDATE chunk_transitions SET evidence_json=? WHERE chunk_id=? AND to_state=?",
                (json.dumps(evidence), chunk_id, ChunkState.SEALING),
            )
        assert verify_new(layout, catalog)["verified_files"] == 1  # Correct incomplete original.
        row = catalog.chunk(chunk_id)
        assert row is not None
        artifact = layout.root / str(row["sealed_path"])
        manifest_path = layout.root / str(row["manifest_path"])
        original_raw, original_manifest = artifact.read_bytes(), manifest_path.read_bytes()
        manifest.update(capture_flags=[], gap=False, complete=True, recovered=False, recovery=None)
        corrupted = json.dumps(manifest).encode()
        manifest_path.write_bytes(corrupted)
        catalog.enroll_raw_verification(chunk_id)
        checked = verify_new(layout, catalog)
        assert checked["verified_files"] == 0 and checked["failed_files"] == 1
        assert artifact.read_bytes() == original_raw and manifest_path.read_bytes() == corrupted
        assert len(catalog.operational_events(event_type="RAW_VERIFICATION_FAILED")) == 1
        # Retrying the correct original does not delete either the old proof or failure evidence.
        manifest_path.write_bytes(original_manifest)
        catalog.enroll_raw_verification(chunk_id)
        assert verify_new(layout, catalog)["verified_files"] == 1
        assert len(catalog.operational_events(event_type="RAW_VERIFICATION_FAILED")) == 1


@pytest.mark.parametrize("recent_progress", [False, True])
def test_p3_r2_idle_or_recent_progress_does_not_backdate_a_stall(
    tmp_path: Path,
    recent_progress: bool,
) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        owner = owner_at(tmp_path, catalog)
        owner.sources.clear()
        product = next(p for p in products() if p.key.market == "um_perpetual")
        source = next(s for s in product_sources(product, SMALL) if s.stream == "liquidation")
        peer = next(s for s in product_sources(product, SMALL) if s.stream == "book_ticker")
        for selected in (source, peer):
            owner.add_source(selected)
            owner.sources[selected.key].first_due = BASE
            owner.sources[selected.key].connected = True
        collector = owner.make_group(
            stream_groups((source,), 128)[0], product, lambda e: None, lambda e: None
        )
        collector._received_total = collector._persisted_total = 1
        collector._last_write_utc_ns = BASE
        for second in range(60):
            owner._observe_availability(BASE + second * 1_000_000_000)
        collector._received_total = 2
        pending = BASE + (50_000_000_000 if recent_progress else 59_900_000_000)
        collector._pending_since_utc_ns = pending
        if recent_progress:
            collector._last_write_utc_ns = BASE + 59_900_000_000
        boundary = BASE + 59_900_000_000
        owner._observe_availability(BASE + 60_000_000_000)
        state = owner.sources[source.key]
        assert state.availability["unavailable_since_utc_ns"] is None
        assert all(entry["is_transition"] for entry in owner.availability.pending)
        asyncio.run(owner._flush_availability(all_pending=True))
        report = continuity_report(catalog, start=BASE, end=BASE + 60_000_000_000)["sources"]
        assert all(row["unavailable_or_unknown_ns"] == 0 for row in report)
        for second in range(61, 71):
            owner._observe_availability(BASE + second * 1_000_000_000)
        assert state.availability["reason"] == "WRITER_STALLED"
        assert state.availability["unavailable_since_utc_ns"] == boundary
        assert owner.sources[peer.key].availability["unavailable_since_utc_ns"] is None
        collector._persisted_total = 2
        collector._pending_since_utc_ns = None
        owner._observe_availability(BASE + 71_000_000_000)
        asyncio.run(owner._flush_availability(all_pending=True))
        report = continuity_report(
            catalog, start=BASE, end=BASE + 71_000_000_000, source_ids=["/".join(source.key)]
        )["sources"][0]
        assert report["unavailable_intervals"] == [(boundary, BASE + 71_000_000_000)]
        assert state.availability["unavailable_since_utc_ns"] is None


@pytest.mark.parametrize("stream", ["rpi_depth", "diff_depth"])
@pytest.mark.parametrize("snapshot_bridges", [False, True])
def test_p3_r3_submonitor_depth_gap_survives_snapshot_recovery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    stream: str,
    snapshot_bridges: bool,
) -> None:
    import time

    from binance_market_data_recorder.normalize.raw import iter_source_records, load_source_chunks

    elapsed = 0
    monkeypatch.setattr(time, "time_ns", lambda: BASE + elapsed)
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        owner = owner_at(tmp_path, catalog)
        owner.sources.clear()
        product = next(p for p in products() if p.key.market == "um_perpetual")
        sources = product_sources(product, SMALL)
        source = next(s for s in sources if s.stream == stream)
        snapshot_source = next(
            s
            for s in sources
            if s.stream == ("rpi_depth_snapshot" if stream == "rpi_depth" else "depth_snapshot")
        )
        for selected in (source, snapshot_source):
            owner.add_source(selected)
        capture = ProductCapture(owner, product)
        owner.collectors[product.key] = capture
        state = owner.sources[source.key]
        state.connected = True
        group = stream_groups((source,), 128)[0]
        sequence = DepthSequenceObserver()
        spool = owner.spool(source.market, source.symbol, group.name)
        raw_frames: list[bytes] = []

        def receive(first: int, last: int, previous: int) -> None:
            raw = json.dumps(
                {
                    "stream": group.wire_names[0],
                    "data": {
                        "s": product.key.symbol,
                        "ps": product.pair,
                        "st": 1,
                        "e": "depthUpdate",
                        "E": 1000,
                        "T": 999,
                        "U": first,
                        "u": last,
                        "pu": previous,
                        "b": [],
                        "a": [],
                    },
                }
            ).encode()
            envelope = sequence.observe(
                envelope_for_group(
                    group,
                    product,
                    raw_payload=raw,
                    connection_id="c",
                    collector_instance_id="i",
                    collector_version="v",
                    receive_time_utc_ns=BASE + elapsed,
                    receive_monotonic_ns=elapsed,
                )
            )
            spool.enqueue(envelope)
            spool.drain_all()
            raw_frames.append(raw)
            capture.persisted(envelope)

        def snapshot(last: int) -> None:
            capture.snapshot_persisted(
                capture_public_response(
                    snapshot_source,
                    lambda s, p: Response({"lastUpdateId": last, "bids": [], "asks": []}),
                    collector_instance_id="i",
                    collector_version="v",
                ).envelope
            )

        receive(9, 10, 8)
        snapshot(9)
        elapsed = 1
        receive(11, 12, 10)
        owner._observe_availability(BASE + elapsed)
        elapsed = 100_000_000
        receive(15, 16, 14)
        if stream == "rpi_depth":
            assert not capture.rpi.is_reliable
            assert (
                owner.availability.sources["/".join(source.key)].unavailable_since == BASE + elapsed
            )
        elapsed = 200_000_000
        snapshot(15 if snapshot_bridges else 1)
        if stream == "rpi_depth":
            assert capture.rpi.is_reliable is snapshot_bridges
            if not snapshot_bridges:
                assert (
                    owner.availability.sources["/".join(source.key)].unavailable_since is not None
                )
        elapsed = 300_000_000
        snapshot(15)
        receive(17, 18, 16)
        for second in range(1, 12):
            elapsed = second * 1_000_000_000
            owner._observe_availability(BASE + elapsed)
        asyncio.run(owner._flush_availability(all_pending=True))
        report = continuity_report(
            catalog, start=BASE + 1, end=BASE + 10_000_000_000, source_ids=["/".join(source.key)]
        )["sources"][0]
        assert report["unavailable_or_unknown_ns"] > 0 and report["coverage"] < 1
        if stream == "rpi_depth":
            assert report["unavailable_intervals"] == [
                (BASE + 100_000_000, BASE + (200_000_000 if snapshot_bridges else 300_000_000))
            ]
            assert state.sequence_reliable is True
        spool.close_and_seal()
        restored = [
            r.envelope
            for chunk in load_source_chunks(layout=layout, catalog=catalog)
            for r in iter_source_records(chunk)
        ]
        assert [r.raw_payload for r in restored] == raw_frames
        assert "sequence_gap" in restored[2].capture_flags


@pytest.mark.parametrize("corrupt_manifest", [False, True])
def test_p3_r1_prior_verifier_proofs_are_retained_but_rechecked_in_the_enrolled_queue(
    tmp_path: Path,
    corrupt_manifest: bool,
) -> None:
    from binance_market_data_recorder.service.verification import verify_chunk
    from tests.archive_support import prepare_archive

    fixture = prepare_archive(tmp_path)
    with Catalog(fixture.layout.catalog) as catalog:
        chunk_id = fixture.chunk_ids[0]
        row = catalog.chunk(chunk_id)
        assert row is not None
        legacy = verify_chunk(fixture.layout, catalog, row, {})
        legacy["verifier_version"] = "raw-independent.v1"
        legacy.pop("durable_evidence")
        catalog.record_raw_verification(chunk_id, evidence=legacy)
        old = catalog._connection.execute(
            "SELECT result_id, evidence_json FROM raw_verification_results"
        ).fetchall()
        assert catalog.raw_verification_status()["pending_files"] == 1
        assert catalog.raw_verification_status()["verified_files"] == 0
        if corrupt_manifest:
            path = fixture.layout.root / str(row["manifest_path"])
            document = json.loads(path.read_bytes())
            document["record_count"] += 1
            path.write_text(json.dumps(document))
        result = verify_new(
            fixture.layout, catalog
        )  # No explicit enrollment, and no corpus inventory.
        assert result["verified_files"] == int(not corrupt_manifest)
        assert result["failed_files"] == int(corrupt_manifest)
        for identity, body in old:
            assert (
                catalog._connection.execute(
                    "SELECT evidence_json FROM raw_verification_results WHERE result_id=?",
                    (identity,),
                ).fetchone()[0]
                == body
            )


@pytest.mark.parametrize("malformed", [False, True])
def test_p3_r1_changed_or_invalid_control_evidence_never_publishes_a_proof(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    malformed: bool,
) -> None:
    from binance_market_data_recorder.service import verification
    from binance_market_data_recorder.spool.readback import read_artifact
    from tests.archive_support import prepare_archive

    fixture = prepare_archive(tmp_path)
    with Catalog(fixture.layout.catalog) as catalog:
        chunk_id = fixture.chunk_ids[0]

        def replace_authority() -> None:
            catalog._connection.execute(
                "UPDATE chunk_transitions SET evidence_json=? WHERE chunk_id=? AND to_state=?",
                (
                    json.dumps(
                        {"seal_forced_flags": "invalid" if malformed else [RECONNECT_GAP_FLAG]}
                    ),
                    chunk_id,
                    ChunkState.SEALING,
                ),
            )

        if malformed:
            replace_authority()
        else:

            def changed(path: Path, **kwargs: object) -> object:
                result = read_artifact(path, **kwargs)  # type: ignore[arg-type]
                replace_authority()
                return result

            monkeypatch.setattr(verification, "read_artifact", changed)
        result = verify_new(fixture.layout, catalog)
        assert result["verified_files"] == 0 and result["failed_files"] == 1


def test_p3_r1_v1_schema_upgrade_preserves_proofs_and_read_only_observation(tmp_path: Path) -> None:
    from binance_market_data_recorder.service.verification import verify_chunk
    from tests.archive_support import prepare_archive

    fixture = prepare_archive(tmp_path)
    with Catalog(fixture.layout.catalog) as catalog:
        row = catalog.chunk(fixture.chunk_ids[0])
        assert row is not None
        legacy = verify_chunk(fixture.layout, catalog, row, {})
        legacy["verifier_version"] = "raw-independent.v1"
        legacy.pop("durable_evidence")
        catalog.record_raw_verification(fixture.chunk_ids[0], evidence=legacy)
        catalog._connection.execute(
            "ALTER TABLE raw_verification_results DROP COLUMN verifier_version"
        )
        before = catalog._connection.execute("SELECT * FROM raw_verification_results").fetchall()
        saved = [tuple(item) for item in before]
    with Catalog(fixture.layout.catalog, read_only=True) as observer:
        assert observer.raw_verification_status()["verified_files"] == 0
        assert observer.raw_verification_status()["pending_files"] == 1
        assert "verifier_version" not in observer.table_columns("raw_verification_results")
    with Catalog(fixture.layout.catalog) as upgraded:
        assert [
            tuple(item)[:3]
            for item in upgraded._connection.execute("SELECT * FROM raw_verification_results")
        ] == saved
        assert verify_new(fixture.layout, upgraded)["verified_files"] == 1
