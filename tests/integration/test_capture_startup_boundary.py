from __future__ import annotations

import asyncio
import copy
import json
import threading
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
    RECONNECT_GAP_FLAG,
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
@pytest.mark.parametrize("failure", [False, True])
def test_startup_commits_before_socket_and_only_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, market: str, failure: bool,
) -> None:
    async def exercise() -> None:
        stop = asyncio.Event()
        openings = 0

        @asynccontextmanager
        async def opener(_url: str) -> Any:
            nonlocal openings
            openings += 1
            manifests = list((tmp_path / "data/manifests").glob("*.json"))
            assert len(manifests) == 1
            marker = json.loads(manifests[0].read_bytes())
            assert marker["record_count"] == 0
            assert marker["connection_ids"] == []
            assert marker["gap"] is True and marker["complete"] is False
            assert marker["market"] == market
            assert marker["collector_instance_ids"] == [collector.collector_instance_id]
            assert catalog.operational_events() == []
            stop.set()
            yield BlockingSocket()  # No payload is needed for the initial boundary.

        collector: SpotStreamCollector | UsdMStreamCollector
        if market == "spot":
            collector, catalog = spot_stream(tmp_path, opener=opener, stop=stop)
        else:
            collector, catalog = usdm_stream(tmp_path, opener)
        if failure:
            def fail_compression(*_args: Any) -> Any:
                raise OSError("startup seal failed")
            monkeypatch.setattr(seal_module, "_compress", fail_compression)
        try:
            if failure:
                with pytest.raises(OSError, match="startup seal failed"):
                    await collector.run(stop)
                assert openings == 0
                assert not collector._capture_startup_recorded
                assert catalog.operational_events() == []
            else:
                # Two explicit run calls on this same capture owner do not
                # add a marker for each network generation.
                await collector.run(stop)
                stop.clear()
                await collector.run(stop)
                assert openings == 2
                assert len(list(collector.spool.layout.manifests.glob("*.json"))) == 1
        finally:
            catalog.close()

    asyncio.run(exercise())


def test_startup_marker_does_not_pass_through_idle_rotation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        spool = make_spool(tmp_path, catalog)
        monkeypatch.setattr(RawChunkWriter, "should_rotate", lambda _writer: True)
        marker = spool.seal_capture_startup()
        assert marker["record_count"] == 0
        assert marker["capture_flags"] == [RECONNECT_GAP_FLAG]
        assert marker["gap"] is True and marker["complete"] is False
        assert not list(layout.active.glob("*.partial"))


@pytest.mark.parametrize("crash_at", ["before_manifest", "before_sealed"])
@pytest.mark.parametrize("retry", ["recovery", "direct"])
def test_startup_seal_crash_retains_flags(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, crash_at: str, retry: str,
) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        spool = make_spool(tmp_path, catalog)
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
                spool.seal_capture_startup()
        partial, = layout.active.glob("*.partial")
        if retry == "direct":
            seal_partial(partial, layout=layout, catalog=catalog)
        else:
            recover_storage(layout=layout, catalog=catalog)
        path, = layout.manifests.glob("*.json")
        marker = json.loads(path.read_bytes())
        assert marker["record_count"] == 0 and marker["connection_ids"] == []
        assert marker["gap"] is True and marker["complete"] is False
        assert marker["collector_instance_ids"] == ["new-process"]
        validate_sealed_artifact(layout.root / marker["relative_path"], marker)
        assert catalog.state(marker["chunk_id"]) is ChunkState.SEALED
        assert catalog.operational_events() == []
        assert not list(layout.active.glob("*.partial"))
        before = path.read_bytes()
        recover_storage(layout=layout, catalog=catalog)
        assert path.read_bytes() == before


def test_startup_crash_before_sealing_keeps_unfinished_header_and_never_opens(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        spool = make_spool(tmp_path, catalog)
        with monkeypatch.context() as context:
            original = catalog.transition
            def fail_sealing(chunk_id: str, state: ChunkState, **kwargs: Any) -> None:
                if state is ChunkState.SEALING:
                    raise OSError("before durable flags")
                original(chunk_id, state, **kwargs)
            context.setattr(catalog, "transition", fail_sealing)
            with pytest.raises(OSError, match="before durable flags"):
                spool.seal_capture_startup()
        partial, = layout.active.glob("*.partial")
        recover_storage(layout=layout, catalog=catalog)
        assert partial.exists()  # Existing REQ-108 retention, no false complete.
        assert not list(layout.manifests.glob("*.json"))
        assert len(catalog.chunks_in_states(ChunkState.ACTIVE)) == 1
        assert catalog.operational_events() == []
        # A later process can record its own boundary but does not silently
        # clean this incomplete attempt; zero-partials acceptance still blocks.
        marker = make_spool(tmp_path, catalog).seal_capture_startup()
        assert marker["gap"] is True
        assert partial.exists()


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


def test_delayed_liquidation_uses_only_its_startup_boundary(tmp_path: Path) -> None:
    layout = ensure_storage_layout(tmp_path)
    contexts: dict[str, Any] = {}
    now = 1_700_000_000_000_000_000
    t0 = now + 100
    with Catalog(layout.catalog) as catalog:
        def seal_frame(connection: str, received: int) -> dict[str, Any]:
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
            return seal_partial(writer.path, layout=layout, catalog=catalog)

        def advance(state: dict[str, Any], manifest: dict[str, Any]) -> Any:
            proof = {"local": scan_raw(layout.root, manifest["relative_path"], manifest)}
            return advance_reconnect(state, manifest, proof, discontinuity_rows=[],
                                     t0_utc_ns=t0, context_bound=42)

        old = seal_frame("prior-process", now)
        assert advance(contexts, old)[0] == []
        baseline = copy.deepcopy(contexts)
        marker = make_spool(tmp_path, catalog).seal_capture_startup()
        new = seal_frame("new-process", t0 + 1_000_000_000_000)
        assert advance(copy.deepcopy(baseline), new)[0] == ["unmarked_reconnect"]
        assert advance(contexts, marker)[1]["kind"] == "ZERO_RECORD_MARKER"
        findings, boundary = advance(contexts, new)
        assert findings == [] and boundary["kind"] == "EXPLICIT_SEQUENCE_GAP"
        next_connection = seal_frame("later-unmarked-connection", t0 + 2_000_000_000_000)
        assert advance(contexts, next_connection)[0] == ["unmarked_reconnect"]

        other_product = make_spool(tmp_path, catalog, symbol="BTCUSDT").seal_capture_startup()
        wrong_product = copy.deepcopy(baseline)
        assert advance(wrong_product, other_product)[0] == []
        assert advance(wrong_product, new)[0] == ["unmarked_reconnect"]
        fresh: dict[str, Any] = {}
        assert advance(fresh, marker)[0] == []
        assert advance(fresh, new)[1]["kind"] == "NO_CONNECTION_CHANGE"


@pytest.mark.parametrize("market", ["spot", "um_perpetual"])
def test_startup_cancellation_waits_for_owned_seal_without_opening_socket(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, market: str,
) -> None:
    entered = threading.Event()
    release = threading.Event()
    original = seal_module._compress
    def blocking_compress(*args: Any) -> Any:
        entered.set()
        assert release.wait(5)
        return original(*args)
    monkeypatch.setattr(seal_module, "_compress", blocking_compress)

    async def exercise() -> None:
        stop = asyncio.Event()
        @asynccontextmanager
        async def opener(_url: str) -> Any:
            raise AssertionError("cancelled startup opened a socket")
            yield  # pragma: no cover
        collector: SpotStreamCollector | UsdMStreamCollector
        if market == "spot":
            collector, catalog = spot_stream(tmp_path, opener=opener, stop=stop)
        else:
            collector, catalog = usdm_stream(tmp_path, opener)
        task = asyncio.create_task(collector.run(stop))
        try:
            assert await asyncio.to_thread(entered.wait, 5)
            task.cancel()
            await asyncio.sleep(0)
            assert not task.done()
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert collector._capture_startup_recorded
            marker_path, = collector.spool.layout.manifests.glob("*.json")
            assert json.loads(marker_path.read_bytes())["gap"] is True
            assert catalog.operational_events() == []
        finally:
            release.set()
            catalog.close()
    asyncio.run(exercise())


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
            assert len(docs) == 2  # Initial marker and actual captured network tail.
            assert sorted(doc["record_count"] for doc in docs) == [0, 1]
            assert all(doc["gap"] for doc in docs)
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
