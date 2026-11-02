from __future__ import annotations

import asyncio
import copy
import json
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, cast

import pytest

import binance_market_data_recorder.spool.seal as seal_module
from binance_market_data_recorder.binance.spot.websocket import SpotStreamCollector
from binance_market_data_recorder.binance.usdm.websocket import UsdMStreamCollector
from binance_market_data_recorder.service.acceptance_v5_raw import scan_raw
from binance_market_data_recorder.service.acceptance_v5_reconnect import advance_reconnect
from binance_market_data_recorder.spool.recovery import RecoveryConflictError, recover_storage
from binance_market_data_recorder.spool.seal import (
    FORCED_FLAGS_EVIDENCE_KEY,
    seal_partial,
    validate_sealed_artifact,
)
from binance_market_data_recorder.spool.stream import StreamSpool
from binance_market_data_recorder.spool.writer import RawChunkWriter, RotationPolicy
from binance_market_data_recorder.storage.catalog import Catalog, ChunkState
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tests.factories import event
from tests.integration.test_spot_stream_collector import ScriptedWebSocket, depth
from tests.integration.test_spot_stream_collector import make_stream as spot_stream
from tests.integration.test_usdm_stream_collector import (
    BlockingSocket,
    ScriptedSocket,
)
from tests.integration.test_usdm_stream_collector import depth as usdm_depth
from tests.integration.test_usdm_stream_collector import make_stream as usdm_stream


def make_spool(root: Path, catalog: Catalog, *, symbol: str = "ETHUSDT") -> StreamSpool:
    return StreamSpool(
        layout=ensure_storage_layout(root), catalog=catalog,
        market="um_perpetual", symbol=symbol, stream="liquidation",
        collector_instance_id="new-process", collector_version="test",
        queue_capacity=4, rotation=RotationPolicy(seconds=60),
        durability_interval_seconds=0, max_frame_bytes=1024 * 1024,
    )


@pytest.mark.parametrize("market", ["spot", "um_perpetual"])
def test_startup_creates_zero_empty_chunks_and_opens_cleanly(
    tmp_path: Path, market: str,
) -> None:
    async def exercise() -> None:
        stop = asyncio.Event()
        openings = 0

        @asynccontextmanager
        async def opener(_url: str) -> Any:
            nonlocal openings
            openings += 1
            # In M2.2 (updating ADR-0038), zero 0-record .zst chunks or manifests
            # are generated on startup.
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
            # Two explicit run calls on this same capture owner do not
            # create any 0-record files.
            await collector.run(stop)
            stop.clear()
            await collector.run(stop)
            assert openings == 2
            assert collector._capture_startup_recorded
            assert len(list(collector.spool.layout.manifests.glob("*.json"))) == 0
            assert len(list(collector.spool.layout.sealed.glob("*.zst"))) == 0
        finally:
            catalog.close()

    asyncio.run(exercise())


def test_idle_spool_rotation_creates_zero_files(tmp_path: Path) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        spool = make_spool(tmp_path, catalog)
        # Rotating an idle spool with 0 records does not create any files
        assert spool.drain_one() is False
        assert spool.close_and_seal() is None
        assert not list(layout.active.glob("*.partial"))
        assert not list(layout.sealed.glob("*.zst"))
        assert not list(layout.manifests.glob("*.json"))


@pytest.mark.parametrize("crash_at", ["before_manifest", "before_sealed"])
@pytest.mark.parametrize("retry", ["recovery", "direct"])
def test_startup_seal_crash_retains_process_boundary_flag(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, crash_at: str, retry: str,
) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        spool = make_spool(tmp_path, catalog)
        # Enqueue first event
        spool.enqueue(event().model_copy(update={
            "market": "um_perpetual", "symbol": "ETHUSDT", "stream": "liquidation",
        }))
        spool.drain_one()

        with monkeypatch.context() as context:
            if crash_at == "before_manifest":
                def fail_manifest(*_args: Any) -> Any:
                    raise OSError("injected startup crash")
                context.setattr(seal_module, "_atomic_json", fail_manifest)
            else:
                original = catalog.transition
                def fail_sealed(chunk_id: str, state: ChunkState, **kwargs: Any) -> None:
                    if state is ChunkState.SEALED:
                        raise OSError("injected startup crash")
                    original(chunk_id, state, **kwargs)
                context.setattr(catalog, "transition", fail_sealed)
            with pytest.raises(OSError, match="injected startup crash"):
                spool._seal_current()
        partial, = layout.active.glob("*.partial")
        if retry == "direct":
            seal_partial(partial, layout=layout, catalog=catalog)
        else:
            recover_storage(layout=layout, catalog=catalog)
        path, = layout.manifests.glob("*.json")
        marker = json.loads(path.read_bytes())
        assert marker["record_count"] == 1
        assert marker["process_boundary_before_first_frame"] is True
        validate_sealed_artifact(layout.root / marker["relative_path"], marker)
        assert catalog.state(marker["chunk_id"]) is ChunkState.SEALED
        assert not list(layout.active.glob("*.partial"))
        before = path.read_bytes()
        recover_storage(layout=layout, catalog=catalog)
        assert path.read_bytes() == before


def test_startup_crash_before_sealing_keeps_unfinished_header(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        spool = make_spool(tmp_path, catalog)
        spool.enqueue(event().model_copy(update={
            "market": "um_perpetual", "symbol": "ETHUSDT", "stream": "liquidation",
        }))
        spool.drain_one()
        with monkeypatch.context() as context:
            original = catalog.transition
            def fail_sealing(chunk_id: str, state: ChunkState, **kwargs: Any) -> None:
                if state is ChunkState.SEALING:
                    raise OSError("before durable flags")
                original(chunk_id, state, **kwargs)
            context.setattr(catalog, "transition", fail_sealing)
            with pytest.raises(OSError, match="before durable flags"):
                spool._seal_current()
        partial, = layout.active.glob("*.partial")
        recover_storage(layout=layout, catalog=catalog)
        assert partial.exists()
        assert not list(layout.manifests.glob("*.json"))
        assert len(catalog.chunks_in_states(ChunkState.ACTIVE)) == 1


@pytest.mark.parametrize("flags", [None, [], [""], [1], ["a", "a"], ["b", "a"]])
def test_malformed_durable_flags_keep_partial(
    tmp_path: Path, flags: Any,
) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        writer = RawChunkWriter(
            layout=layout, catalog=catalog, market="spot", symbol="BTCUSDT",
            stream="diff_depth", collector_instance_id="test", collector_version="test",
        )
        writer.close()
        chunk_id = str(writer.header.chunk_id)
        catalog.transition(chunk_id, ChunkState.SEALING, idempotency_key="bad-flags",
                           evidence={"verified_frames": 0, FORCED_FLAGS_EVIDENCE_KEY: flags})
        with pytest.raises(RecoveryConflictError, match="FORCED_FLAGS_MALFORMED"):
            recover_storage(layout=layout, catalog=catalog)
        assert writer.path.exists()
        assert catalog.state(chunk_id) is ChunkState.SEALING
        assert not list(layout.manifests.glob("*.json"))


def test_delayed_liquidation_uses_process_boundary_without_empty_chunks(tmp_path: Path) -> None:
    layout = ensure_storage_layout(tmp_path)
    contexts: dict[str, Any] = {}
    now = 1_700_000_000_000_000_000
    t0 = now + 100
    with Catalog(layout.catalog) as catalog:
        def seal_frame(
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

        old = seal_frame("prior-process", now)
        assert advance(contexts, old)[0] == []
        baseline = copy.deepcopy(contexts)

        # Without process_boundary_before_first_frame, unmarked connection change fails
        new_unmarked = seal_frame("new-process", t0 + 1_000_000_000_000, is_startup=False)
        assert advance(copy.deepcopy(baseline), new_unmarked)[0] == ["unmarked_reconnect"]

        # With process_boundary_before_first_frame (M2.2), no 0-record marker needed:
        # the inter-chunk connection change is proven as EXPLICIT_SEQUENCE_GAP
        new = seal_frame("new-process", t0 + 1_000_000_000_000, is_startup=True)
        findings, boundary = advance(contexts, new)
        assert findings == [] and boundary["kind"] == "EXPLICIT_SEQUENCE_GAP"

        # A later unmarked connection in the same process is still correctly flagged
        next_connection = seal_frame(
            "later-unmarked-connection", t0 + 2_000_000_000_000, is_startup=False
        )
        assert advance(contexts, next_connection)[0] == ["unmarked_reconnect"]

        # Fresh context sees first frame cleanly
        fresh: dict[str, Any] = {}
        assert advance(fresh, new)[1]["kind"] == "NO_CONNECTION_CHANGE"


@pytest.mark.parametrize("market", ["spot", "um_perpetual"])
def test_real_drop_before_first_payload_and_restored_pending_gap(
    tmp_path: Path, market: str,
) -> None:
    async def exercise() -> None:
        stop = asyncio.Event()
        attempts = 0
        @asynccontextmanager
        async def dropping(_url: str) -> Any:
            nonlocal attempts
            attempts += 1
            # The initial connection opens but drops before receiving data.
            payloads = [] if attempts == 1 else (
                [depth(1, 1)] if market == "spot" else [usdm_depth(1, 1, 0)]
            )
            yield (ScriptedWebSocket(payloads) if market == "spot"
                   else ScriptedSocket(payloads))
        first: SpotStreamCollector | UsdMStreamCollector
        if market == "spot":
            first, catalog = spot_stream(tmp_path, opener=dropping, stop=stop)
        else:
            first, catalog = usdm_stream(tmp_path, dropping)
        try:
            await asyncio.wait_for(first.run(stop), 3)
            assert attempts == 2
            pending, = catalog.unclosed_stream_discontinuities(
                market=market, symbol="BTCUSDT", stream="diff_depth",
            )
            gap_id = cast(dict[str, Any], pending["evidence"])["gap_id"]
            docs = [
                json.loads(path.read_bytes())
                for path in first.spool.layout.manifests.glob("*.json")
            ]
            # M2.2: Exactly 1 chunk (the actual captured network tail). NO 0-record startup marker!
            assert len(docs) == 1
            assert docs[0]["record_count"] == 1
            assert docs[0]["process_boundary_before_first_frame"] is True
            assert docs[0]["gap"] is True
        finally:
            catalog.close()

        @asynccontextmanager
        async def recovering(_url: str) -> Any:
            yield (ScriptedWebSocket([depth(1, 1)], stop) if market == "spot"
                   else ScriptedSocket([usdm_depth(1, 1, 0)], stop))
        second: SpotStreamCollector | UsdMStreamCollector
        if market == "spot":
            second, catalog = spot_stream(tmp_path, opener=recovering, stop=stop)
        else:
            second, catalog = usdm_stream(tmp_path, recovering)
        try:
            assert second._pending_gap is not None
            assert second._pending_gap["gap_id"] == gap_id
            await asyncio.wait_for(second.run(stop), 3)
            events = catalog.operational_events()
            assert [row["event_type"] for row in events] == [
                "STREAM_DISCONTINUITY_STARTED", "STREAM_DISCONTINUITY_COMPLETED",
            ]
            assert cast(dict[str, Any], events[1]["evidence"])["gap_id"] == gap_id
            assert not catalog.unclosed_stream_discontinuities(
                market=market, symbol="BTCUSDT", stream="diff_depth",
            )
        finally:
            catalog.close()
    asyncio.run(exercise())
