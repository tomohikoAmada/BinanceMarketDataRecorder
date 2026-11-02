from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest

from binance_market_data_recorder.audit.reconnect_boundaries import (
    EXPLICIT_SEQUENCE_GAP,
    UNKNOWN,
    UNMARKED_RECONNECT,
    ChunkScan,
    FrameIdentity,
    _classify_inter_chunk,
)
from binance_market_data_recorder.binance.spot.websocket import SpotStreamCollector
from binance_market_data_recorder.binance.usdm.websocket import UsdMStreamCollector
from binance_market_data_recorder.service.acceptance_v5_raw import scan_raw
from binance_market_data_recorder.service.acceptance_v5_reconnect import advance_reconnect
from binance_market_data_recorder.spool.seal import (
    SealError,
    parse_strict_manifest_bytes,
    seal_partial,
)
from binance_market_data_recorder.spool.stream import StreamSpool
from binance_market_data_recorder.spool.writer import RawChunkWriter, RotationPolicy
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tests.factories import event
from tests.integration.test_spot_stream_collector import ScriptedWebSocket
from tests.integration.test_spot_stream_collector import make_stream as spot_stream
from tests.integration.test_usdm_stream_collector import BlockingSocket
from tests.integration.test_usdm_stream_collector import make_stream as usdm_stream


def make_test_spool(root: Path, catalog: Catalog, *, symbol: str = "BTCUSDT") -> StreamSpool:
    return StreamSpool(
        layout=ensure_storage_layout(root),
        catalog=catalog,
        market="spot",
        symbol=symbol,
        stream="diff_depth",
        collector_instance_id="test-instance",
        collector_version="0.1.0",
        queue_capacity=64,
        rotation=RotationPolicy(seconds=60),
        durability_interval_seconds=0,
        max_frame_bytes=1024 * 1024,
    )


def test_spool_startup_creates_zero_files_on_disk(tmp_path: Path) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        spool = make_test_spool(tmp_path, catalog)
        # Calling seal_capture_startup is a no-op that creates zero files on disk
        result = spool.seal_capture_startup()
        assert result == {}
        assert list(layout.active.glob("*.partial")) == []
        assert list(layout.sealed.glob("*.zst")) == []
        assert list(layout.manifests.glob("*.json")) == []


def test_spool_first_real_chunk_has_process_boundary_and_second_does_not(tmp_path: Path) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        spool = make_test_spool(tmp_path, catalog)

        # Enqueue first event and drain
        ev1 = event().model_copy(update={
            "market": "spot", "symbol": "BTCUSDT", "stream": "diff_depth",
            "connection_id": "conn-1", "receive_time_utc_ns": 1_000_000_000,
        })
        spool.enqueue(ev1)
        spool.drain_one()

        # Seal first chunk
        m1 = spool._seal_current()
        assert m1 is not None
        assert m1["record_count"] == 1
        assert m1["process_boundary_before_first_frame"] is True

        # Enqueue second event and drain
        ev2 = event().model_copy(update={
            "market": "spot", "symbol": "BTCUSDT", "stream": "diff_depth",
            "connection_id": "conn-1", "receive_time_utc_ns": 2_000_000_000,
        })
        spool.enqueue(ev2)
        spool.drain_one()

        # Seal second chunk
        m2 = spool._seal_current()
        assert m2 is not None
        assert m2["record_count"] == 1
        assert "process_boundary_before_first_frame" not in m2

        # Verify disk artifacts
        sealed = list(layout.sealed.glob("*.zst"))
        assert len(sealed) == 2
        for s in sealed:
            assert s.stat().st_size > 0
        assert list(layout.active.glob("*.partial")) == []


def test_parse_strict_manifest_validates_process_boundary_flag(tmp_path: Path) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        spool = make_test_spool(tmp_path, catalog)
        spool.enqueue(event())
        spool.drain_one()
        manifest = spool._seal_current()
        assert manifest is not None
        manifest_hex = UUID(str(manifest["chunk_id"])).hex
        manifest_path = layout.manifests / f"{manifest_hex}.manifest.json"
        raw_bytes = manifest_path.read_bytes()

        # Valid manifest parses cleanly
        parsed = parse_strict_manifest_bytes(
            raw_bytes, path=manifest_path, recorder_root=layout.root
        )
        assert parsed["process_boundary_before_first_frame"] is True

        # Invalid non-bool value raises SealError
        bad_doc = json.loads(raw_bytes)
        bad_doc["process_boundary_before_first_frame"] = "not-a-bool"
        with pytest.raises(SealError, match="invalid process_boundary_before_first_frame"):
            parse_strict_manifest_bytes(
                json.dumps(bad_doc).encode(), path=manifest_path, recorder_root=layout.root
            )

        # Unrecognized extra field raises SealError
        bad_doc2 = json.loads(raw_bytes)
        bad_doc2["unexpected_extra_field"] = True
        with pytest.raises(SealError, match="malformed fields"):
            parse_strict_manifest_bytes(
                json.dumps(bad_doc2).encode(), path=manifest_path, recorder_root=layout.root
            )


@pytest.mark.parametrize("old_flags", [(), ("sequence_gap",)])
def test_inter_chunk_classification_with_process_boundary(old_flags: tuple[str, ...]) -> None:
    old_scan = ChunkScan(manifest={"connection_ids": ["old-conn"]})
    new_scan = ChunkScan(manifest={
        "connection_ids": ["new-conn"],
        "process_boundary_before_first_frame": True,
    })
    last_old = FrameIdentity(
        chunk_id="old-chunk", frame_index=0, connection_id="old-conn",
        receive_time_utc_ns=100, capture_flags=old_flags, source_sequence={},
        payload_sha256="a" * 64, exchange_event_time_ns=None,
    )
    first_new = FrameIdentity(
        chunk_id="new-chunk", frame_index=0, connection_id="new-conn",
        receive_time_utc_ns=200, capture_flags=(), source_sequence={},
        payload_sha256="b" * 64, exchange_event_time_ns=None,
    )

    # With process_boundary_before_first_frame, inter-chunk change is EXPLICIT_SEQUENCE_GAP
    kind = _classify_inter_chunk(
        old_scan, new_scan, last_old, first_new,
        catalog_identity_kind="NONE", catalog_matched_gap_id=None,
        old_frames=[last_old], intervening_chunks=[],
    )
    assert kind == EXPLICIT_SEQUENCE_GAP

    # Without startup evidence, retain the existing unmarked/ambiguous distinction.
    unmarked_scan = ChunkScan(manifest={"connection_ids": ["new-conn"]})
    kind_unmarked = _classify_inter_chunk(
        old_scan, unmarked_scan, last_old, first_new,
        catalog_identity_kind="NONE", catalog_matched_gap_id=None,
        old_frames=[last_old], intervening_chunks=[],
    )
    assert kind_unmarked == (UNKNOWN if old_flags else UNMARKED_RECONNECT)


@pytest.mark.parametrize("old_flags", [(), ("sequence_gap",)])
def test_delayed_stream_reconnect_projection_without_empty_chunks(
    tmp_path: Path, old_flags: tuple[str, ...],
) -> None:
    layout = ensure_storage_layout(tmp_path)
    contexts: dict[str, Any] = {}
    now = 1_700_000_000_000_000_000
    t0 = now + 100

    with Catalog(layout.catalog) as catalog:
        def seal_chunk(
            connection: str, received: int, *, is_startup: bool = False
        ) -> dict[str, Any]:
            writer = RawChunkWriter(
                layout=layout, catalog=catalog, market="um_perpetual", symbol="ETHUSDT",
                stream="liquidation", collector_instance_id=connection, collector_version="test",
            )
            writer.append(event().model_copy(update={
                "market": "um_perpetual", "symbol": "ETHUSDT", "stream": "liquidation",
                "connection_id": connection, "collector_instance_id": connection,
                "source_sequence": {}, "receive_time_utc_ns": received,
                "capture_flags": old_flags if connection == "prior-process" else (),
            }))
            writer.close()
            return seal_partial(
                writer.path, layout=layout, catalog=catalog,
                process_boundary_before_first_frame=is_startup,
            )

        def advance(state: dict[str, Any], manifest: dict[str, Any]) -> Any:
            proof = {"local": scan_raw(layout.root, manifest["relative_path"], manifest)}
            return advance_reconnect(state, manifest, proof, discontinuity_rows=[],
                                     t0_utc_ns=t0, context_bound=42)

        # Prior process chunk
        old = seal_chunk("prior-process", now)
        assert advance(contexts, old)[0] == []

        # New process first chunk has process_boundary_before_first_frame = True (no 0-record file)
        new = seal_chunk("new-process", t0 + 1_000_000_000_000, is_startup=True)
        findings, boundary = advance(contexts, new)
        assert findings == []
        assert boundary["kind"] == "EXPLICIT_SEQUENCE_GAP"

        # Later unmarked connection in same process is caught
        later = seal_chunk("later-unmarked", t0 + 2_000_000_000_000, is_startup=False)
        assert advance(contexts, later)[0] == ["unmarked_reconnect"]


@pytest.mark.parametrize("market", ["spot", "um_perpetual"])
def test_collector_startup_creates_zero_empty_chunks(tmp_path: Path, market: str) -> None:
    async def exercise() -> None:
        stop = asyncio.Event()
        openings = 0

        @asynccontextmanager
        async def opener(_url: str) -> Any:
            nonlocal openings
            openings += 1
            # Before and when socket opens, zero manifests and zero partials exist
            manifests = list((tmp_path / "data/manifests").glob("*.json"))
            assert len(manifests) == 0
            partials = list((tmp_path / "data/active").glob("*.partial"))
            assert len(partials) == 0
            stop.set()
            yield (ScriptedWebSocket([], stop) if market == "spot" else BlockingSocket())

        collector: SpotStreamCollector | UsdMStreamCollector
        if market == "spot":
            collector, catalog = spot_stream(tmp_path, opener=opener, stop=stop)
        else:
            collector, catalog = usdm_stream(tmp_path, opener)
        try:
            await collector.run(stop)
            assert openings == 1
            assert collector._capture_startup_recorded
            # No files created because no frames arrived
            assert len(list(collector.spool.layout.manifests.glob("*.json"))) == 0
            assert len(list(collector.spool.layout.sealed.glob("*.zst"))) == 0
        finally:
            catalog.close()

    asyncio.run(exercise())
