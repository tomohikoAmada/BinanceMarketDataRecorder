from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from binance_market_data_recorder.binance.usdm.schema import (
    UsdMStream,
    envelope_from_websocket_frame,
)
from binance_market_data_recorder.spool.seal import OVERLAP_FLAG, seal_partial
from binance_market_data_recorder.spool.writer import RawChunkWriter, RotationPolicy
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tools.audit_reconnect_boundaries import (
    BLUE_GREEN_OVERLAP,
    EXPLICIT_SEQUENCE_GAP,
    UNMARKED_RECONNECT,
    audit_data_root,
    load_manifest_chunks,
    scan_chunk_frames,
)


def book_ticker(update_id: int) -> bytes:
    return json.dumps(
        {
            "e": "bookTicker",
            "u": update_id,
            "s": "BTCUSDT",
            "b": "100.0",
            "B": "1.0",
            "a": "101.0",
            "A": "2.0",
            "T": update_id,
            "E": update_id,
        },
        separators=(",", ":"),
    ).encode()


def usdm_envelope(
    connection_id: str,
    update_id: int,
    flags: tuple[str, ...] = (),
) -> Any:
    return envelope_from_websocket_frame(
        raw_payload=book_ticker(update_id),
        stream=UsdMStream.BOOK_TICKER,
        connection_id=connection_id,
        collector_instance_id="audit-test",
        collector_version="0.1.0+test",
        receive_time_utc_ns=1_000_000_000 + update_id,
        receive_monotonic_ns=update_id,
        additional_capture_flags=flags,
    )


def seal_chunk(
    layout: Any,
    catalog: Catalog,
    envelopes: list[Any],
) -> dict[str, Any]:
    writer = RawChunkWriter(
        layout=layout,
        catalog=catalog,
        market="um_perpetual",
        symbol="BTCUSDT",
        stream="book_ticker",
        collector_instance_id="audit-test",
        collector_version="0.1.0+test",
        rotation=RotationPolicy(seconds=60),
        durability_interval_seconds=0,
    )
    for envelope in envelopes:
        writer.append(envelope)
    writer.close()
    return seal_partial(writer.path, layout=layout, catalog=catalog)


def legacy_manifest(layout: Any, manifest: dict[str, Any]) -> None:
    """Overwrite a manifest with a legacy artifact that hid its boundary.

    The current seal defense fails closed to reconnect_gap, so an unmarked
    multi-connection chunk cannot be produced by the fixed artifact. The
    historical silent-gap artifacts were sealed before the fix; this helper
    reproduces their gap=false/complete=true manifest for audit fixtures.
    """
    legacy = dict(manifest)
    legacy["capture_flags"] = []
    legacy["gap"] = False
    legacy["complete"] = True
    manifest_dir = layout.manifests
    path = manifest_dir / f"{manifest['chunk_id'].replace('-', '')}.manifest.json"
    path.write_text(json.dumps(legacy, sort_keys=True) + "\n", encoding="utf-8")


def build_fixture(root: Path) -> Catalog:
    layout = ensure_storage_layout(root)
    catalog = Catalog(layout.catalog)
    # 1. single-connection complete chunk (no transition).
    seal_chunk(layout, catalog, [usdm_envelope("conn-a", 1), usdm_envelope("conn-a", 2)])
    # 2. unmarked reconnect: two connections, legacy manifest without gap.
    manifest = seal_chunk(
        layout,
        catalog,
        [usdm_envelope("conn-b", 3), usdm_envelope("conn-c", 4)],
    )
    legacy_manifest(layout, manifest)
    # 3. explicit gap: two connections with sequence_gap evidence on both
    #    boundary frames and the manifest forced via sequence_gap flag.
    seal_chunk(
        layout,
        catalog,
        [
            usdm_envelope("conn-d", 5, ("sequence_gap",)),
            usdm_envelope("conn-e", 6, ("sequence_gap",)),
        ],
    )
    # 4. blue/green overlap: two connections with explicit provenance.
    seal_chunk(
        layout,
        catalog,
        [
            usdm_envelope("conn-f", 7, (OVERLAP_FLAG, "deployment_id=d")),
            usdm_envelope("conn-g", 8, (OVERLAP_FLAG, "deployment_id=d")),
        ],
    )
    return catalog


def test_audit_classifies_all_transition_kinds_deterministically(tmp_path: Path) -> None:
    build_fixture(tmp_path)
    first = audit_data_root(tmp_path)
    second = audit_data_root(tmp_path)
    for document in (first, second):
        document.pop("generated_at_utc_ns")
        document.pop("generated_at_utc")
    assert json.dumps(first, sort_keys=True) == json.dumps(second, sort_keys=True)
    streams = first["streams"]
    assert len(streams) == 1
    assert streams[0]["market"] == "um_perpetual"
    assert streams[0]["stream"] == "book_ticker"
    kinds = {transition["kind"] for transition in streams[0]["transitions"]}
    assert kinds == {
        UNMARKED_RECONNECT,
        EXPLICIT_SEQUENCE_GAP,
        BLUE_GREEN_OVERLAP,
    }
    by_kind: dict[str, list[dict[str, Any]]] = {}
    for transition in streams[0]["transitions"]:
        by_kind.setdefault(transition["kind"], []).append(transition)
    assert len(by_kind[UNMARKED_RECONNECT]) == 2
    assert len(by_kind[EXPLICIT_SEQUENCE_GAP]) == 3
    assert len(by_kind[BLUE_GREEN_OVERLAP]) == 1
    unmarked = next(
        transition
        for transition in by_kind[UNMARKED_RECONNECT]
        if transition["boundary_kind"] == "intra_chunk"
    )
    assert unmarked["old_connection_id"] == "conn-b"
    assert unmarked["new_connection_id"] == "conn-c"
    assert unmarked["old_manifest"]["complete"] is True
    assert unmarked["catalog_gap_match"] == "UNMATCHED"
    assert unmarked["last_old_frame"]["connection_id"] == "conn-b"
    assert unmarked["first_new_frame"]["connection_id"] == "conn-c"
    assert unmarked["first_new_frame"]["payload_sha256"]
    explicit = by_kind[EXPLICIT_SEQUENCE_GAP][0]
    assert explicit["old_manifest"]["gap"] or explicit["new_manifest"]["gap"]
    overlap = by_kind[BLUE_GREEN_OVERLAP][0]
    assert "blue_green_overlap" in overlap["old_manifest"]["capture_flags"]
    assert first["summary"]["unmarked_reconnect"] == 2
    assert first["summary"]["explicit_gap"] == 3
    assert first["summary"]["blue_green_overlap"] == 1


def test_audit_correlates_catalog_discontinuity_intervals(tmp_path: Path) -> None:
    build_fixture(tmp_path)
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        chunks = load_manifest_chunks(layout, market="um_perpetual", stream=None)
        assert len(chunks) == 4
        catalog.record_operational_event(
            event_id="stream-discontinuity-started:gap-1",
            event_type="STREAM_DISCONTINUITY_STARTED",
            occurred_at_utc_ns=1_000_000_003,
            evidence={
                "gap_id": "gap-1",
                "market": "um_perpetual",
                "stream": "book_ticker",
                "gap_started_at_utc_ns": 1_000_000_003,
            },
        )
        catalog.record_operational_event(
            event_id="stream-discontinuity-completed:gap-1",
            event_type="STREAM_DISCONTINUITY_COMPLETED",
            occurred_at_utc_ns=1_000_000_004,
            evidence={
                "gap_id": "gap-1",
                "market": "um_perpetual",
                "stream": "book_ticker",
                "gap_ended_at_utc_ns": 1_000_000_004,
            },
        )
    document = audit_data_root(tmp_path)
    assert document["catalog"]["discontinuity_started"] == 1
    assert document["catalog"]["discontinuity_completed"] == 1
    assert document["catalog"]["unmatched_started"] == 0
    unmarked = [
        transition
        for stream in document["streams"]
        for transition in stream["transitions"]
        if transition["kind"] == UNMARKED_RECONNECT
    ]
    assert unmarked[0]["catalog_gap_match"] == "MATCHED"


def test_audit_skips_rest_polled_streams_and_unknown_streams(tmp_path: Path) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        writer = RawChunkWriter(
            layout=layout,
            catalog=catalog,
            market="spot",
            symbol="BTCUSDT",
            stream="depth_snapshot",
            collector_instance_id="audit-test",
            collector_version="0.1.0+test",
            rotation=RotationPolicy(seconds=60),
            durability_interval_seconds=0,
        )
        from binance_market_data_recorder.domain.event import EventEnvelope

        for request_index in range(3):
            writer.append(
                EventEnvelope(
                    market="spot",
                    symbol="BTCUSDT",
                    stream="depth_snapshot",
                    module="usdm_rest",
                    connection_id=f"rest-{request_index}",
                    collector_instance_id="audit-test",
                    collector_version="0.1.0+test",
                    receive_time_utc_ns=1_000_000_000 + request_index,
                    receive_monotonic_ns=request_index,
                    raw_payload=book_ticker(request_index),
                )
            )
        writer.close()
        seal_partial(writer.path, layout=layout, catalog=catalog)
    document = audit_data_root(tmp_path, market="spot", stream="depth_snapshot")
    assert document["streams"][0]["transitions"] == []
    assert document["streams"][0]["summary"]["chunks_scanned"] == 1


def test_audit_scan_rejects_corrupt_sealed_artifact(tmp_path: Path) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        seal_chunk(
            layout,
            catalog,
            [usdm_envelope("conn-h", 9), usdm_envelope("conn-i", 10)],
        )
    sealed = next((tmp_path / "data" / "sealed").glob("*.bmdr.zst"))
    sealed.write_bytes(b"corrupt-not-zstd")
    with pytest.raises(ValueError, match="cannot decompress"):
        scan_chunk_frames(sealed, {"chunk_id": "corrupt"})


def test_audit_missing_sealed_file_reports_manifest_level_transition(
    tmp_path: Path,
) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        manifest = seal_chunk(
            layout,
            catalog,
            [usdm_envelope("conn-j", 11), usdm_envelope("conn-k", 12)],
        )
        legacy_manifest(layout, manifest)
    sealed = next((tmp_path / "data" / "sealed").glob("*.bmdr.zst"))
    sealed.unlink()
    document = audit_data_root(tmp_path)
    transitions = [
        transition
        for stream in document["streams"]
        for transition in stream["transitions"]
    ]
    assert len(transitions) == 1
    transition = transitions[0]
    assert transition["kind"] == UNMARKED_RECONNECT
    assert transition["boundary_kind"] == "intra_chunk_unordered"
    assert transition["frame_detail_unavailable"] is True
    assert transition["last_old_frame"] is None


def test_audit_ignores_malformed_manifest_files(tmp_path: Path) -> None:
    build_fixture(tmp_path)
    manifest_dir = tmp_path / "data" / "manifests"
    corrupt = manifest_dir / "corrupt.manifest.json"
    corrupt.write_text("{not valid json", encoding="utf-8")
    document = audit_data_root(tmp_path)
    assert sum(stream["summary"]["chunks_scanned"] for stream in document["streams"]) == 4


def test_audit_stream_and_market_filters_are_exclusive(tmp_path: Path) -> None:
    build_fixture(tmp_path)
    document = audit_data_root(tmp_path, market="spot")
    assert document["streams"] == []
    document = audit_data_root(tmp_path, stream="agg_trade")
    assert document["streams"] == []
