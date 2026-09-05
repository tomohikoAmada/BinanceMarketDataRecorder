from __future__ import annotations

import json
import os
import stat
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from binance_market_data_recorder.binance.usdm.schema import (
    UsdMStream,
    envelope_from_websocket_frame,
)
from binance_market_data_recorder.spool.seal import OVERLAP_FLAG, SealError, seal_partial
from binance_market_data_recorder.spool.writer import RawChunkWriter, RotationPolicy
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tools.audit_reconnect_boundaries import (
    BLUE_GREEN_OVERLAP,
    EXPLICIT_SEQUENCE_GAP,
    UNKNOWN,
    UNMARKED_RECONNECT,
    audit_data_root,
    load_manifest_chunks,
    scan_chunk_frames,
)

AUDIT_TOOL = Path(__file__).resolve().parents[2] / "tools" / "audit_reconnect_boundaries.py"


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
    *,
    forced_flags: frozenset[str] = frozenset(),
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
    return seal_partial(
        writer.path, layout=layout, catalog=catalog, forced_flags=forced_flags
    )


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


def build_fixture(root: Path) -> None:
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
    catalog.close()


def canonical(document: dict[str, Any]) -> str:
    return json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n"


def all_transitions(document: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        transition
        for stream in document["streams"]
        for transition in stream["transitions"]
    ]


def test_audit_classifies_all_transition_kinds_deterministically(tmp_path: Path) -> None:
    build_fixture(tmp_path)
    first = audit_data_root(tmp_path)
    second = audit_data_root(tmp_path)
    # The canonical payload is byte-identical across runs: it carries no
    # wall-clock execution metadata.
    assert canonical(first) == canonical(second)
    for document in (first, second):
        assert "generated_at_utc_ns" not in document
        assert "generated_at_utc" not in document
        assert document["audit_cutoff_utc_ns"] is None
        assert document["manifest_inventory_count"] == 4
        assert len(document["manifest_inventory_sha256"]) == 64
        assert document["missing_inputs"] == []
    streams = first["streams"]
    assert len(streams) == 1
    assert streams[0]["market"] == "um_perpetual"
    assert streams[0]["stream"] == "book_ticker"
    kinds = {transition["kind"] for transition in streams[0]["transitions"]}
    assert kinds == {
        UNMARKED_RECONNECT,
        EXPLICIT_SEQUENCE_GAP,
        BLUE_GREEN_OVERLAP,
        UNKNOWN,
    }
    by_kind: dict[str, list[dict[str, Any]]] = {}
    for transition in streams[0]["transitions"]:
        by_kind.setdefault(transition["kind"], []).append(transition)
    assert len(by_kind[UNMARKED_RECONNECT]) == 2
    assert len(by_kind[EXPLICIT_SEQUENCE_GAP]) == 2
    assert len(by_kind[BLUE_GREEN_OVERLAP]) == 1
    assert len(by_kind[UNKNOWN]) == 1
    unmarked = next(
        transition
        for transition in by_kind[UNMARKED_RECONNECT]
        if transition["boundary_kind"] == "intra_chunk"
    )
    assert unmarked["old_connection_id"] == "conn-b"
    assert unmarked["new_connection_id"] == "conn-c"
    assert unmarked["old_manifest"]["complete"] is True
    assert unmarked["catalog_gap_match"] == "UNMATCHED"
    assert unmarked["catalog_identity_match"] is False
    assert unmarked["last_old_frame"]["connection_id"] == "conn-b"
    assert unmarked["first_new_frame"]["connection_id"] == "conn-c"
    assert unmarked["first_new_frame"]["payload_sha256"]
    explicit = by_kind[EXPLICIT_SEQUENCE_GAP][0]
    assert explicit["old_manifest"]["gap"] or explicit["new_manifest"]["gap"]
    overlap = by_kind[BLUE_GREEN_OVERLAP][0]
    assert "blue_green_overlap" in overlap["old_manifest"]["capture_flags"]
    unknown = by_kind[UNKNOWN][0]
    assert unknown["boundary_kind"] == "inter_chunk"
    assert first["summary"]["unmarked_reconnect"] == 2
    assert first["summary"]["explicit_gap"] == 2
    assert first["summary"]["blue_green_overlap"] == 1
    assert first["summary"]["unknown"] == 1


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
                "symbol": "BTCUSDT",
                "stream": "book_ticker",
                "gap_started_at_utc_ns": 1_000_000_003,
            },
            symbol="BTCUSDT",
        )
        catalog.record_operational_event(
            event_id="stream-discontinuity-completed:gap-1",
            event_type="STREAM_DISCONTINUITY_COMPLETED",
            occurred_at_utc_ns=1_000_000_004,
            evidence={
                "gap_id": "gap-1",
                "market": "um_perpetual",
                "symbol": "BTCUSDT",
                "stream": "book_ticker",
                "gap_ended_at_utc_ns": 1_000_000_004,
            },
            symbol="BTCUSDT",
        )
    document = audit_data_root(tmp_path)
    assert document["catalog"]["discontinuity_started"] == 1
    assert document["catalog"]["discontinuity_completed"] == 1
    assert document["catalog"]["matched_pairs"] == 1
    assert document["catalog"]["unmatched_started"] == 0
    assert document["catalog"]["unmatched_completed"] == 0
    assert document["catalog"]["gap_intervals"][0]["gap_id"] == "gap-1"
    unmarked = [
        transition
        for transition in all_transitions(document)
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
    transitions = all_transitions(document)
    assert len(transitions) == 1
    transition = transitions[0]
    assert transition["kind"] == UNMARKED_RECONNECT
    assert transition["boundary_kind"] == "intra_chunk_unordered"
    assert transition["frame_detail_unavailable"] is True
    assert transition["last_old_frame"] is None


def test_audit_rejects_malformed_manifest_files(tmp_path: Path) -> None:
    build_fixture(tmp_path)
    manifest_dir = tmp_path / "data" / "manifests"
    corrupt = manifest_dir / "corrupt.manifest.json"
    corrupt.write_text("{not valid json", encoding="utf-8")
    with pytest.raises(SealError, match="strict historical manifest load failed"):
        audit_data_root(tmp_path)


def test_audit_stream_and_market_filters_are_exclusive(tmp_path: Path) -> None:
    build_fixture(tmp_path)
    document = audit_data_root(tmp_path, market="spot")
    assert document["streams"] == []
    document = audit_data_root(tmp_path, stream="agg_trade")
    assert document["streams"] == []


def test_audit_intra_chunk_transitions_are_boundary_local(tmp_path: Path) -> None:
    """Two transitions in one manifest: A->B explicit, B->C unmarked.

    The explicit sequence_gap on A->B must never classify B->C.
    """
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        seal_chunk(
            layout,
            catalog,
            [
                usdm_envelope("conn-a1", 21),
                usdm_envelope("conn-b1", 22, ("sequence_gap",)),
                usdm_envelope("conn-b1", 23),
                usdm_envelope("conn-c1", 24),
            ],
        )
    document = audit_data_root(tmp_path)
    transitions = all_transitions(document)
    assert len(transitions) == 2
    by_connection = {item["old_connection_id"]: item for item in transitions}
    assert by_connection["conn-a1"]["kind"] == EXPLICIT_SEQUENCE_GAP
    assert by_connection["conn-a1"]["first_new_frame"]["connection_id"] == "conn-b1"
    assert by_connection["conn-b1"]["kind"] == UNMARKED_RECONNECT
    assert by_connection["conn-b1"]["first_new_frame"]["connection_id"] == "conn-c1"


def test_audit_single_frame_marked_connection_is_ambiguous(tmp_path: Path) -> None:
    """A marker on a single-frame connection cannot be attributed to its end."""
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        seal_chunk(
            layout,
            catalog,
            [
                usdm_envelope("conn-a5", 71),
                usdm_envelope("conn-b5", 72, ("sequence_gap",)),
                usdm_envelope("conn-c5", 73),
            ],
        )
    document = audit_data_root(tmp_path)
    transitions = all_transitions(document)
    by_connection = {item["old_connection_id"]: item for item in transitions}
    assert by_connection["conn-a5"]["kind"] == EXPLICIT_SEQUENCE_GAP
    assert by_connection["conn-b5"]["kind"] == UNKNOWN


def test_audit_inter_chunk_ignores_unrelated_old_manifest_gap(
    tmp_path: Path,
) -> None:
    """An earlier sequence_gap inside the old chunk does not mark old->new.

    The old chunk's manifest carries gap evidence (the sequence_gap on its
    first connection), but that evidence cannot be attributed to exactly the
    old->new inter-chunk boundary. Per the P2-C taxonomy the boundary is
    UNKNOWN (evidence exists but unattributable), never UNMARKED_RECONNECT
    (whose meaning is "no evidence exists") and never EXPLICIT.
    """
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        seal_chunk(
            layout,
            catalog,
            [
                usdm_envelope("conn-a2", 31, ("sequence_gap",)),
                usdm_envelope("conn-b2", 32),
            ],
        )
        seal_chunk(
            layout,
            catalog,
            [usdm_envelope("conn-c2", 33)],
        )
    document = audit_data_root(tmp_path)
    transitions = all_transitions(document)
    inter = [item for item in transitions if item["boundary_kind"] == "inter_chunk"]
    assert len(inter) == 1
    assert inter[0]["old_connection_id"] == "conn-b2"
    assert inter[0]["new_connection_id"] == "conn-c2"
    assert inter[0]["kind"] == UNKNOWN


def test_audit_inter_chunk_ignores_unrelated_new_manifest_gap(
    tmp_path: Path,
) -> None:
    """A later sequence_gap inside the new chunk does not mark old->new.

    The new chunk's manifest carries gap evidence (the sequence_gap on its
    later connection), but that evidence cannot be attributed to exactly the
    old->new inter-chunk boundary. Per the P2-C taxonomy the boundary is
    UNKNOWN (evidence exists but unattributable), never UNMARKED_RECONNECT.
    """
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        seal_chunk(
            layout,
            catalog,
            [usdm_envelope("conn-a3", 41)],
        )
        seal_chunk(
            layout,
            catalog,
            [
                usdm_envelope("conn-b3", 42),
                usdm_envelope("conn-c3", 43, ("sequence_gap",)),
            ],
        )
    document = audit_data_root(tmp_path)
    inter = [
        item for item in all_transitions(document)
        if item["boundary_kind"] == "inter_chunk"
    ]
    assert len(inter) == 1
    assert inter[0]["old_connection_id"] == "conn-a3"
    assert inter[0]["new_connection_id"] == "conn-b3"
    assert inter[0]["kind"] == UNKNOWN


def test_audit_blue_green_overlap_is_transition_local(tmp_path: Path) -> None:
    """Overlap at A->B never exempts an unmarked third transition B->C."""
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        seal_chunk(
            layout,
            catalog,
            [
                usdm_envelope("conn-a4", 51, (OVERLAP_FLAG, "deployment_id=d")),
                usdm_envelope("conn-b4", 52, (OVERLAP_FLAG, "deployment_id=d")),
                usdm_envelope("conn-c4", 53),
            ],
        )
    document = audit_data_root(tmp_path)
    transitions = all_transitions(document)
    by_connection = {item["old_connection_id"]: item for item in transitions}
    assert by_connection["conn-a4"]["kind"] == BLUE_GREEN_OVERLAP
    assert by_connection["conn-b4"]["kind"] == UNMARKED_RECONNECT


def test_audit_catalog_gap_matching_is_stream_specific(tmp_path: Path) -> None:
    """A boundary for stream A must never match stream B's gap interval."""
    build_fixture(tmp_path)
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        catalog.record_operational_event(
            event_id="stream-discontinuity-started:gap-b",
            event_type="STREAM_DISCONTINUITY_STARTED",
            occurred_at_utc_ns=1_000_000_003,
            evidence={
                "gap_id": "gap-b",
                "market": "um_perpetual",
                "symbol": "BTCUSDT",
                "stream": "agg_trade",
                "gap_started_at_utc_ns": 1_000_000_003,
                "original_connection_id": "conn-b",
            },
            symbol="BTCUSDT",
        )
        catalog.record_operational_event(
            event_id="stream-discontinuity-completed:gap-b",
            event_type="STREAM_DISCONTINUITY_COMPLETED",
            occurred_at_utc_ns=1_000_000_004,
            evidence={
                "gap_id": "gap-b",
                "market": "um_perpetual",
                "symbol": "BTCUSDT",
                "stream": "agg_trade",
                "gap_ended_at_utc_ns": 1_000_000_004,
                "new_connection_id": "conn-c",
            },
            symbol="BTCUSDT",
        )
    document = audit_data_root(tmp_path)
    unmarked = [
        transition
        for transition in all_transitions(document)
        if transition["kind"] == UNMARKED_RECONNECT
        and transition["old_connection_id"] == "conn-b"
        and transition["new_connection_id"] == "conn-c"
    ]
    assert len(unmarked) == 1
    assert unmarked[0]["catalog_gap_match"] == "UNMATCHED"


def test_audit_catalog_gap_identity_selects_correct_nearby_gap(tmp_path: Path) -> None:
    """Two nearby gaps for the same stream; exact identity selects the right one."""
    build_fixture(tmp_path)
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        for gap_id, old_conn, new_conn, start in (
            ("gap-x", "conn-b", "conn-c", 1_000_000_003),
            ("gap-y", "conn-d", "conn-e", 1_000_000_004),
        ):
            catalog.record_operational_event(
                event_id=f"stream-discontinuity-started:{gap_id}",
                event_type="STREAM_DISCONTINUITY_STARTED",
                occurred_at_utc_ns=start,
                evidence={
                    "gap_id": gap_id,
                    "market": "um_perpetual",
                    "symbol": "BTCUSDT",
                    "stream": "book_ticker",
                    "gap_started_at_utc_ns": start,
                    "original_connection_id": old_conn,
                },
                symbol="BTCUSDT",
            )
            catalog.record_operational_event(
                event_id=f"stream-discontinuity-completed:{gap_id}",
                event_type="STREAM_DISCONTINUITY_COMPLETED",
                occurred_at_utc_ns=start + 1,
                evidence={
                    "gap_id": gap_id,
                    "market": "um_perpetual",
                    "symbol": "BTCUSDT",
                    "stream": "book_ticker",
                    "gap_ended_at_utc_ns": start + 1,
                    "new_connection_id": new_conn,
                },
                symbol="BTCUSDT",
            )
    document = audit_data_root(tmp_path)
    transitions = all_transitions(document)
    by_pair = {
        (item["old_connection_id"], item["new_connection_id"]): item
        for item in transitions
    }
    b_to_c = by_pair[("conn-b", "conn-c")]
    assert b_to_c["catalog_gap_match"] == "MATCHED"
    assert b_to_c["catalog_identity_match"] is True
    d_to_e = by_pair[("conn-d", "conn-e")]
    assert d_to_e["catalog_gap_match"] == "MATCHED"
    assert d_to_e["catalog_identity_match"] is True


def test_audit_unmatched_catalog_counts_use_gap_id_identity(tmp_path: Path) -> None:
    """Orphan STARTED + unrelated orphan COMPLETED never cancel out."""
    build_fixture(tmp_path)
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        catalog.record_operational_event(
            event_id="stream-discontinuity-started:orphan-started",
            event_type="STREAM_DISCONTINUITY_STARTED",
            occurred_at_utc_ns=1_000_000_010,
            evidence={
                "gap_id": "orphan-started",
                "market": "um_perpetual",
                "symbol": "BTCUSDT",
                "stream": "book_ticker",
                "gap_started_at_utc_ns": 1_000_000_010,
            },
            symbol="BTCUSDT",
        )
        catalog.record_operational_event(
            event_id="stream-discontinuity-completed:orphan-completed",
            event_type="STREAM_DISCONTINUITY_COMPLETED",
            occurred_at_utc_ns=1_000_000_011,
            evidence={
                "gap_id": "orphan-completed",
                "market": "um_perpetual",
                "symbol": "BTCUSDT",
                "stream": "book_ticker",
                "gap_ended_at_utc_ns": 1_000_000_011,
            },
            symbol="BTCUSDT",
        )
    document = audit_data_root(tmp_path)
    assert document["catalog"]["discontinuity_started"] == 1
    assert document["catalog"]["discontinuity_completed"] == 1
    assert document["catalog"]["matched_pairs"] == 0
    assert document["catalog"]["unmatched_started"] == 1
    assert document["catalog"]["unmatched_completed"] == 1


def test_audit_archived_raw_with_ambiguous_manifest_is_unknown(tmp_path: Path) -> None:
    """Archived Raw + manifest gap evidence that cannot be boundary-attributed."""
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        seal_chunk(
            layout,
            catalog,
            [
                usdm_envelope("conn-m", 61, ("sequence_gap",)),
                usdm_envelope("conn-n", 62),
            ],
        )
    sealed = next((tmp_path / "data" / "sealed").glob("*.bmdr.zst"))
    sealed.unlink()
    document = audit_data_root(tmp_path)
    transitions = all_transitions(document)
    assert len(transitions) == 1
    assert transitions[0]["kind"] == UNKNOWN
    assert transitions[0]["frame_detail_unavailable"] is True


def test_audit_cutoff_filters_manifests_and_is_deterministic(tmp_path: Path) -> None:
    build_fixture(tmp_path)
    manifests_dir = tmp_path / "data" / "manifests"
    all_manifests = sorted(manifests_dir.glob("*.manifest.json"))
    created = [
        int(json.loads(path.read_text(encoding="utf-8"))["created_at_utc_ns"])
        for path in all_manifests
    ]
    cutoff = max(created) - 1
    document = audit_data_root(tmp_path, cutoff_utc_ns=cutoff)
    assert document["audit_cutoff_utc_ns"] == cutoff
    assert document["manifest_inventory_count"] == len(
        [value for value in created if value <= cutoff]
    )
    assert canonical(document) == canonical(
        audit_data_root(tmp_path, cutoff_utc_ns=cutoff)
    )


def test_audit_missing_directories_are_reported_not_created(tmp_path: Path) -> None:
    root = tmp_path / "incomplete-root"
    root.mkdir()
    document = audit_data_root(root)
    assert document["missing_inputs"]
    assert "data" in document["missing_inputs"]
    assert "data/manifests" in document["missing_inputs"]
    # No directory was created by the audit itself.
    assert sorted(entry.name for entry in root.iterdir()) == []


def test_audit_does_not_mutate_the_audited_tree(tmp_path: Path) -> None:
    build_fixture(tmp_path)

    def snapshot() -> dict[str, tuple[int, int, int]]:
        recorded: dict[str, tuple[int, int, int]] = {}
        for path in sorted(tmp_path.rglob("*")):
            if not path.is_file():
                continue
            recorded[str(path)] = (
                path.stat().st_size,
                int(path.stat().st_mtime),
                hash(path.read_bytes()),
            )
        return recorded

    before = snapshot()
    document = audit_data_root(tmp_path)
    assert document["summary"]["chunks_scanned"] == 4
    assert before == snapshot()


def test_audit_cli_rejects_output_inside_data_root(tmp_path: Path) -> None:
    build_fixture(tmp_path)
    result = subprocess.run(
        [
            sys.executable,
            str(AUDIT_TOOL),
            "--data-root",
            str(tmp_path),
            "--output",
            str(tmp_path / "result.json"),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert "resolves inside the audited data root" in result.stderr
    assert not (tmp_path / "result.json").exists()


def test_audit_cli_rejects_output_symlink_beneath_data_root(
    tmp_path: Path,
) -> None:
    build_fixture(tmp_path)
    external = tmp_path / "external-parent"
    external.mkdir()
    link = external / "result-link.json"
    link.symlink_to(tmp_path / "result.json")
    result = subprocess.run(
        [
            sys.executable,
            str(AUDIT_TOOL),
            "--data-root",
            str(tmp_path),
            "--output",
            str(link),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert "resolves inside the audited data root" in result.stderr
    assert not (tmp_path / "result.json").exists()


def test_audit_cli_allows_external_output_and_records_canonical_sha(
    tmp_path: Path,
) -> None:
    build_fixture(tmp_path)
    external_parent = tmp_path.parent / f"{tmp_path.name}-output"
    external_parent.mkdir()
    output = external_parent / "audit.json"
    result = subprocess.run(
        [
            sys.executable,
            str(AUDIT_TOOL),
            "--data-root",
            str(tmp_path),
            "--cutoff-utc-ns",
            str(2_000_000_000_000_000_000),
            "--output",
            str(output),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    artifact = json.loads(output.read_text(encoding="utf-8"))
    canonical_payload = artifact["canonical"]
    sha = artifact["canonical_sha256"]
    assert "generated_at_utc_ns" in artifact["execution"]
    assert "generated_at_utc_ns" not in canonical_payload
    assert sha == __import__("hashlib").sha256(
        canonical(canonical_payload).encode("utf-8")
    ).hexdigest()
    assert canonical_payload["audit_cutoff_utc_ns"] == 2_000_000_000_000_000_000
    assert canonical_payload["manifest_inventory_count"] == 4
    # Re-running with the same input set reproduces the exact canonical SHA.
    second_output = external_parent / "audit2.json"
    result = subprocess.run(
        [
            sys.executable,
            str(AUDIT_TOOL),
            "--data-root",
            str(tmp_path),
            "--cutoff-utc-ns",
            str(2_000_000_000_000_000_000),
            "--output",
            str(second_output),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    second = json.loads(second_output.read_text(encoding="utf-8"))
    assert second["canonical_sha256"] == sha


def test_audit_works_on_read_only_tree(tmp_path: Path) -> None:
    if os.geteuid() == 0:
        pytest.skip("chmod-based read-only fixture is unreliable as root")
    build_fixture(tmp_path)
    for directory in (tmp_path / "data", tmp_path / "data" / "manifests"):
        directory.chmod(stat.S_IRUSR | stat.S_IXUSR)
    try:
        document = audit_data_root(tmp_path)
        assert document["summary"]["chunks_scanned"] == 4
    finally:
        for directory in (tmp_path / "data", tmp_path / "data" / "manifests"):
            directory.chmod(stat.S_IRWXU)


def test_audit_archived_boundary_without_any_evidence_is_unmarked(
    tmp_path: Path,
) -> None:
    """Archived Raw + no gap evidence on either adjacent manifest: the
    connection change is unmarked, not merely unattributable."""
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        seal_chunk(layout, catalog, [usdm_envelope("conn-p", 81)])
        seal_chunk(layout, catalog, [usdm_envelope("conn-q", 82)])
    for sealed in (tmp_path / "data" / "sealed").glob("*.bmdr.zst"):
        sealed.unlink()
    document = audit_data_root(tmp_path)
    transitions = all_transitions(document)
    inter = [
        item for item in transitions if item["boundary_kind"] == "inter_chunk"
    ]
    assert len(inter) == 1
    assert inter[0]["old_connection_id"] == "conn-p"
    assert inter[0]["new_connection_id"] == "conn-q"
    assert inter[0]["kind"] == UNMARKED_RECONNECT
    assert inter[0]["frame_detail_unavailable"] is True


def record_gap_interval(
    catalog: Catalog,
    *,
    gap_id: str,
    market: str,
    stream: str,
    symbol: str,
    started_at_utc_ns: int,
    ended_at_utc_ns: int,
    original_connection_id: str | None = None,
    new_connection_id: str | None = None,
) -> None:
    evidence: dict[str, Any] = {
        "gap_id": gap_id,
        "market": market,
        "symbol": symbol,
        "stream": stream,
        "gap_started_at_utc_ns": started_at_utc_ns,
    }
    if original_connection_id is not None:
        evidence["original_connection_id"] = original_connection_id
    catalog.record_operational_event(
        event_id=f"stream-discontinuity-started:{gap_id}",
        event_type="STREAM_DISCONTINUITY_STARTED",
        occurred_at_utc_ns=started_at_utc_ns,
        evidence=evidence,
        symbol=symbol,
    )
    completed: dict[str, Any] = {
        **evidence,
        "gap_ended_at_utc_ns": ended_at_utc_ns,
    }
    if new_connection_id is not None:
        completed["new_connection_id"] = new_connection_id
    catalog.record_operational_event(
        event_id=f"stream-discontinuity-completed:{gap_id}",
        event_type="STREAM_DISCONTINUITY_COMPLETED",
        occurred_at_utc_ns=ended_at_utc_ns,
        evidence=completed,
        symbol=symbol,
    )


def two_chunk_boundary(root: Path, old_conn: str, new_conn: str) -> None:
    """Seal two adjacent single-connection chunks: old_conn -> new_conn."""
    layout = ensure_storage_layout(root)
    with Catalog(layout.catalog) as catalog:
        seal_chunk(layout, catalog, [usdm_envelope(old_conn, 101)])
        seal_chunk(layout, catalog, [usdm_envelope(new_conn, 102)])


def inter_transitions(document: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        transition
        for stream in document["streams"]
        for transition in stream["transitions"]
        if transition["boundary_kind"] == "inter_chunk"
    ]


def test_catalog_exact_pair_proves_explicit(tmp_path: Path) -> None:
    """TEST-201: Catalog A->B plus transition A->B is EXACT_PAIR and may
    classify the boundary EXPLICIT_SEQUENCE_GAP with matched gap provenance."""
    two_chunk_boundary(tmp_path, "conn-A", "conn-B")
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        record_gap_interval(
            catalog,
            gap_id="gap-exact",
            market="um_perpetual",
            stream="book_ticker",
            symbol="BTCUSDT",
            started_at_utc_ns=1_000_000_101,
            ended_at_utc_ns=1_000_000_102,
            original_connection_id="conn-A",
            new_connection_id="conn-B",
        )
    document = audit_data_root(tmp_path)
    transitions = inter_transitions(document)
    assert len(transitions) == 1
    transition = transitions[0]
    assert transition["kind"] == EXPLICIT_SEQUENCE_GAP
    assert transition["catalog_identity_match"] is True
    assert transition["catalog_identity_match_kind"] == "EXACT_PAIR"
    assert transition["catalog_matched_gap_id"] == "gap-exact"
    assert transition["catalog_gap_match"] == "MATCHED"


def test_catalog_partial_old_match_is_never_explicit(tmp_path: Path) -> None:
    """TEST-202: Catalog A->C, transition A->B: old matches, new mismatches.
    PARTIAL_OLD must classify UNKNOWN, never EXPLICIT."""
    two_chunk_boundary(tmp_path, "conn-A", "conn-B")
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        record_gap_interval(
            catalog,
            gap_id="gap-partial-old",
            market="um_perpetual",
            stream="book_ticker",
            symbol="BTCUSDT",
            started_at_utc_ns=1_000_000_101,
            ended_at_utc_ns=1_000_000_102,
            original_connection_id="conn-A",
            new_connection_id="conn-C",
        )
    document = audit_data_root(tmp_path)
    transition = inter_transitions(document)[0]
    assert transition["kind"] == UNKNOWN
    assert transition["catalog_identity_match"] is False
    assert transition["catalog_identity_match_kind"] == "PARTIAL_OLD"
    assert transition["catalog_matched_gap_id"] == "gap-partial-old"


def test_catalog_partial_new_match_is_never_explicit(tmp_path: Path) -> None:
    """TEST-203: Catalog C->B, transition A->B: new matches, old mismatches.
    PARTIAL_NEW must classify UNKNOWN, never EXPLICIT."""
    two_chunk_boundary(tmp_path, "conn-A", "conn-B")
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        record_gap_interval(
            catalog,
            gap_id="gap-partial-new",
            market="um_perpetual",
            stream="book_ticker",
            symbol="BTCUSDT",
            started_at_utc_ns=1_000_000_101,
            ended_at_utc_ns=1_000_000_102,
            original_connection_id="conn-C",
            new_connection_id="conn-B",
        )
    document = audit_data_root(tmp_path)
    transition = inter_transitions(document)[0]
    assert transition["kind"] == UNKNOWN
    assert transition["catalog_identity_match"] is False
    assert transition["catalog_identity_match_kind"] == "PARTIAL_NEW"


def test_catalog_unrelated_pair_has_no_identity_match(tmp_path: Path) -> None:
    """TEST-204: Catalog C->D vs transition A->B: no identity match at all."""
    two_chunk_boundary(tmp_path, "conn-A", "conn-B")
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        # The interval's time window is placed away from the boundary so
        # neither identity nor time overlap can match.
        record_gap_interval(
            catalog,
            gap_id="gap-unrelated",
            market="um_perpetual",
            stream="book_ticker",
            symbol="BTCUSDT",
            started_at_utc_ns=9_000_000_101,
            ended_at_utc_ns=9_000_000_102,
            original_connection_id="conn-C",
            new_connection_id="conn-D",
        )
    document = audit_data_root(tmp_path)
    transition = inter_transitions(document)[0]
    assert transition["kind"] == UNMARKED_RECONNECT
    assert transition["catalog_identity_match"] is False
    assert transition["catalog_identity_match_kind"] == "NONE"
    assert transition["catalog_matched_gap_id"] is None


def test_catalog_started_without_completed_new_is_not_exact(tmp_path: Path) -> None:
    """TEST-205: STARTED old=A, new connection absent: not an exact pair."""
    two_chunk_boundary(tmp_path, "conn-A", "conn-B")
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        record_gap_interval(
            catalog,
            gap_id="gap-pending",
            market="um_perpetual",
            stream="book_ticker",
            symbol="BTCUSDT",
            started_at_utc_ns=1_000_000_101,
            ended_at_utc_ns=1_000_000_102,
            original_connection_id="conn-A",
        )
    document = audit_data_root(tmp_path)
    transition = inter_transitions(document)[0]
    assert transition["catalog_identity_match"] is False
    assert transition["catalog_identity_match_kind"] == "PARTIAL_OLD"
    assert transition["kind"] == UNKNOWN


def test_catalog_exact_pair_wins_over_nearby_partial(tmp_path: Path) -> None:
    """TEST-206: exact A->B plus a nearby partial A->C; the exact candidate
    is selected and may prove EXPLICIT."""
    two_chunk_boundary(tmp_path, "conn-A", "conn-B")
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        record_gap_interval(
            catalog,
            gap_id="gap-nearby",
            market="um_perpetual",
            stream="book_ticker",
            symbol="BTCUSDT",
            started_at_utc_ns=1_000_000_100,
            ended_at_utc_ns=1_000_000_103,
            original_connection_id="conn-A",
            new_connection_id="conn-C",
        )
        record_gap_interval(
            catalog,
            gap_id="gap-exact",
            market="um_perpetual",
            stream="book_ticker",
            symbol="BTCUSDT",
            started_at_utc_ns=1_000_000_101,
            ended_at_utc_ns=1_000_000_102,
            original_connection_id="conn-A",
            new_connection_id="conn-B",
        )
    document = audit_data_root(tmp_path)
    transition = inter_transitions(document)[0]
    assert transition["kind"] == EXPLICIT_SEQUENCE_GAP
    assert transition["catalog_identity_match_kind"] == "EXACT_PAIR"
    assert transition["catalog_matched_gap_id"] == "gap-exact"


def test_catalog_two_exact_candidates_are_ambiguous(tmp_path: Path) -> None:
    """TEST-207: two exact A->B intervals: AMBIGUOUS, fail closed to UNKNOWN."""
    two_chunk_boundary(tmp_path, "conn-A", "conn-B")
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        record_gap_interval(
            catalog,
            gap_id="gap-exact-1",
            market="um_perpetual",
            stream="book_ticker",
            symbol="BTCUSDT",
            started_at_utc_ns=1_000_000_101,
            ended_at_utc_ns=1_000_000_102,
            original_connection_id="conn-A",
            new_connection_id="conn-B",
        )
        record_gap_interval(
            catalog,
            gap_id="gap-exact-2",
            market="um_perpetual",
            stream="book_ticker",
            symbol="BTCUSDT",
            started_at_utc_ns=1_000_000_101,
            ended_at_utc_ns=1_000_000_102,
            original_connection_id="conn-A",
            new_connection_id="conn-B",
        )
    document = audit_data_root(tmp_path)
    transition = inter_transitions(document)[0]
    assert transition["kind"] == UNKNOWN
    assert transition["catalog_gap_match"] == "AMBIGUOUS"
    assert transition["catalog_identity_match"] is False
    assert transition["catalog_identity_match_kind"] == "AMBIGUOUS"
    assert transition["catalog_matched_gap_id"] is None


def test_catalog_matching_is_market_stream_specific_exact(tmp_path: Path) -> None:
    """TEST-208: identical connection ids on a different stream never match."""
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        writer = RawChunkWriter(
            layout=layout,
            catalog=catalog,
            market="um_perpetual",
            symbol="BTCUSDT",
            stream="agg_trade",
            collector_instance_id="audit-test",
            collector_version="0.1.0+test",
            rotation=RotationPolicy(seconds=60),
            durability_interval_seconds=0,
        )
        from binance_market_data_recorder.binance.usdm.schema import (
            envelope_from_websocket_frame,
        )

        for index, connection_id in enumerate(("conn-A", "conn-B")):
            writer.append(
                envelope_from_websocket_frame(
                    raw_payload=book_ticker(200 + index),
                    stream=UsdMStream.AGG_TRADE,
                    connection_id=connection_id,
                    collector_instance_id="audit-test",
                    collector_version="0.1.0+test",
                    receive_time_utc_ns=1_000_000_200 + index,
                    receive_monotonic_ns=200 + index,
                )
            )
        writer.close()
        seal_partial(writer.path, layout=layout, catalog=catalog)
        record_gap_interval(
            catalog,
            gap_id="gap-other-stream",
            market="um_perpetual",
            stream="agg_trade",
            symbol="BTCUSDT",
            started_at_utc_ns=1_000_000_200,
            ended_at_utc_ns=1_000_000_201,
            original_connection_id="conn-A",
            new_connection_id="conn-B",
        )
        # The same connection pair on book_ticker must NOT match the
        # agg_trade interval.
        two_chunk_boundary(tmp_path, "conn-A", "conn-B")
    document = audit_data_root(tmp_path)
    agg = next(
        stream for stream in document["streams"]
        if stream["stream"] == "agg_trade"
    )
    # The agg_trade chunk has its own intra-chunk transition; its Catalog
    # interval matches it exactly, but never the book_ticker boundary that
    # reuses the same connection ids.
    assert len(agg["transitions"]) == 1
    agg_transition = agg["transitions"][0]
    assert agg_transition["catalog_identity_match_kind"] == "EXACT_PAIR"
    assert agg_transition["catalog_matched_gap_id"] == "gap-other-stream"
    book = next(
        stream for stream in document["streams"]
        if stream["stream"] == "book_ticker"
    )
    book_transitions = [
        transition for transition in book["transitions"]
        if transition["boundary_kind"] == "inter_chunk"
    ]
    assert len(book_transitions) == 1
    assert book_transitions[0]["catalog_identity_match_kind"] == "NONE"
    assert book_transitions[0]["catalog_gap_match"] == "UNMATCHED"


def test_multi_connection_old_chunk_with_reconnect_gap_is_unknown(
    tmp_path: Path,
) -> None:
    """TEST-501 (P2-C): multi-connection old chunk sealed with reconnect_gap
    whose exact transition location cannot be attributed to the current
    inter-chunk boundary classifies UNKNOWN, never UNMARKED_RECONNECT."""
    from binance_market_data_recorder.spool.seal import RECONNECT_GAP_FLAG

    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        seal_chunk(
            layout,
            catalog,
            [
                usdm_envelope("conn-a1", 111),
                usdm_envelope("conn-b1", 112),
            ],
            forced_flags=frozenset({RECONNECT_GAP_FLAG}),
        )
        seal_chunk(layout, catalog, [usdm_envelope("conn-c1", 113)])
    document = audit_data_root(tmp_path)
    inter = inter_transitions(document)
    b_to_c = [
        transition for transition in inter
        if transition["old_connection_id"] == "conn-b1"
    ]
    assert len(b_to_c) == 1
    assert b_to_c[0]["kind"] == UNKNOWN
    assert b_to_c[0]["old_manifest"]["gap"] is True
    assert b_to_c[0]["old_manifest"]["complete"] is False


def test_multi_connection_old_chunk_with_overlap_is_unknown(
    tmp_path: Path,
) -> None:
    """TEST-502 (P2-C): equivalent unattributable overlap evidence on the old
    manifest classifies UNKNOWN, never UNMARKED_RECONNECT."""
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        seal_chunk(
            layout,
            catalog,
            [
                usdm_envelope("conn-a2", 121, (OVERLAP_FLAG, "deployment_id=d")),
                usdm_envelope("conn-b2", 122, (OVERLAP_FLAG, "deployment_id=d")),
            ],
        )
        seal_chunk(layout, catalog, [usdm_envelope("conn-c2", 123)])
    document = audit_data_root(tmp_path)
    inter = inter_transitions(document)
    b_to_c = [
        transition for transition in inter
        if transition["old_connection_id"] == "conn-b2"
    ]
    assert len(b_to_c) == 1
    assert b_to_c[0]["kind"] == UNKNOWN
    assert "blue_green_overlap" in b_to_c[0]["old_manifest"]["capture_flags"]


def test_zero_record_marker_does_not_hide_logical_transition(tmp_path: Path) -> None:
    """TEST-1101 / IR-002: A -> empty marker -> B is one logical A -> B
    transition, and the intervening boundary manifest remains evidence."""
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        old = seal_chunk(layout, catalog, [usdm_envelope("conn-a", 201)])
        marker = seal_chunk(
            layout,
            catalog,
            [],
            forced_flags=frozenset({"reconnect_gap"}),
        )
        new = seal_chunk(layout, catalog, [usdm_envelope("conn-b", 202)])

    document = audit_data_root(tmp_path)
    transitions = inter_transitions(document)
    assert len(transitions) == 1
    transition = transitions[0]
    assert transition["old_chunk_id"] == old["chunk_id"]
    assert transition["new_chunk_id"] == new["chunk_id"]
    assert transition["old_connection_id"] == "conn-a"
    assert transition["new_connection_id"] == "conn-b"
    assert transition["kind"] == EXPLICIT_SEQUENCE_GAP
    assert [item["chunk_id"] for item in transition["intervening_manifests"]] == [
        marker["chunk_id"]
    ]
    assert document["summary"]["transitions_total"] == 1


def test_multiple_zero_record_chunks_produce_one_logical_transition(
    tmp_path: Path,
) -> None:
    """TEST-1102: A -> empty1 -> empty2 -> B has one denominator entry."""
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        seal_chunk(layout, catalog, [usdm_envelope("conn-a", 211)])
        first = seal_chunk(layout, catalog, [])
        second = seal_chunk(
            layout,
            catalog,
            [],
            forced_flags=frozenset({"reconnect_gap"}),
        )
        seal_chunk(layout, catalog, [usdm_envelope("conn-b", 212)])
    transitions = inter_transitions(audit_data_root(tmp_path))
    assert len(transitions) == 1
    assert transitions[0]["kind"] == EXPLICIT_SEQUENCE_GAP
    assert [
        item["chunk_id"] for item in transitions[0]["intervening_manifests"]
    ] == [first["chunk_id"], second["chunk_id"]]


def test_empty_chunk_with_unrelated_catalog_gap_is_not_explicit(
    tmp_path: Path,
) -> None:
    """TEST-1104: a nearby but different Catalog pair cannot prove A -> B."""
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        seal_chunk(layout, catalog, [usdm_envelope("conn-a", 221)])
        marker = seal_chunk(layout, catalog, [])
        seal_chunk(layout, catalog, [usdm_envelope("conn-b", 222)])
        record_gap_interval(
            catalog,
            gap_id="gap-unrelated-empty",
            market="um_perpetual",
            stream="book_ticker",
            symbol="BTCUSDT",
            original_connection_id="conn-x",
            new_connection_id="conn-y",
            started_at_utc_ns=1_000_000_000,
            ended_at_utc_ns=2_000_000_000,
        )
    transitions = inter_transitions(audit_data_root(tmp_path))
    assert len(transitions) == 1
    assert transitions[0]["kind"] != EXPLICIT_SEQUENCE_GAP
    assert transitions[0]["catalog_identity_match_kind"] != "EXACT_PAIR"
    assert transitions[0]["intervening_manifests"][0]["chunk_id"] == marker[
        "chunk_id"
    ]


def test_archived_zero_record_marker_manifest_remains_in_denominator(
    tmp_path: Path,
) -> None:
    """TEST-1105: missing marker Raw does not erase A -> B or its manifest
    boundary evidence."""
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        seal_chunk(layout, catalog, [usdm_envelope("conn-a", 231)])
        marker = seal_chunk(
            layout,
            catalog,
            [],
            forced_flags=frozenset({"reconnect_gap"}),
        )
        seal_chunk(layout, catalog, [usdm_envelope("conn-b", 232)])
    marker_artifact = layout.root / str(marker["relative_path"])
    marker_artifact.unlink()
    transitions = inter_transitions(audit_data_root(tmp_path))
    assert len(transitions) == 1
    assert transitions[0]["kind"] == EXPLICIT_SEQUENCE_GAP
    assert transitions[0]["frame_detail_unavailable"] is True
    assert transitions[0]["intervening_manifests"][0] == {
        "chunk_id": marker["chunk_id"],
        "record_count": 0,
        "gap": True,
        "complete": False,
        "capture_flags": ["reconnect_gap"],
        "frame_detail_unavailable": True,
    }


def test_zero_record_chunk_between_same_connection_invents_no_reconnect(
    tmp_path: Path,
) -> None:
    """TEST-1106: A -> empty -> A carries evidence but no connection change."""
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        seal_chunk(layout, catalog, [usdm_envelope("conn-a", 241)])
        seal_chunk(
            layout,
            catalog,
            [],
            forced_flags=frozenset({"reconnect_gap"}),
        )
        seal_chunk(layout, catalog, [usdm_envelope("conn-a", 242)])
    document = audit_data_root(tmp_path)
    assert inter_transitions(document) == []
    assert document["summary"]["transitions_total"] == 0
