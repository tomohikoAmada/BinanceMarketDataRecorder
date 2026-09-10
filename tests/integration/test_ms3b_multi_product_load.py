from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import pytest

from binance_market_data_recorder.domain.event import EventEnvelope, Market
from binance_market_data_recorder.spool.format import FRAME_PREFIX
from binance_market_data_recorder.spool.queue import IngressQueueFull
from binance_market_data_recorder.spool.recovery import recover_partials
from binance_market_data_recorder.spool.seal import (
    read_strict_manifest,
    seal_partial,
    validate_sealed_artifact,
)
from binance_market_data_recorder.spool.stream import StreamSpool
from binance_market_data_recorder.spool.writer import RawChunkWriter, RotationPolicy
from binance_market_data_recorder.storage.catalog import Catalog, ChunkState
from binance_market_data_recorder.storage.layout import (
    StorageLayout,
    ensure_storage_layout,
)

PROFILE_D_SYMBOLS = (
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "XRPUSDT",
    "DOGEUSDT",
    "SUIUSDT",
    "LINKUSDT",
)
CORE_STREAMS = ("diff_depth", "agg_trade", "book_ticker")
PROFILE_D_IDENTITIES = tuple(
    (market, symbol, stream)
    for market in ("spot", "um_perpetual")
    for symbol in PROFILE_D_SYMBOLS
    for stream in CORE_STREAMS
)


@dataclass(frozen=True, slots=True)
class _Identity:
    market: str
    symbol: str
    stream: str


def _event(identity: _Identity, ordinal: int) -> EventEnvelope:
    payload = json.dumps(
        {
            "market": identity.market,
            "symbol": identity.symbol,
            "stream": identity.stream,
            "ordinal": ordinal,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return EventEnvelope(
        market=cast(Market, identity.market),
        symbol=identity.symbol,
        stream=identity.stream,
        module=f"test.{identity.market}.ms3b",
        connection_id=f"connection-{identity.market}-{identity.symbol}",
        collector_instance_id=f"collector-{identity.market}-{identity.symbol}",
        collector_version="0.1.0+ms3b-test",
        receive_time_utc_ns=1_700_000_000_000_000_000 + ordinal,
        receive_monotonic_ns=5_000_000_000 + ordinal,
        exchange_event_time=1_700_000_000_000 + ordinal,
        source_sequence={"ordinal": ordinal},
        raw_payload=payload,
    )


def _spool(
    layout: StorageLayout,
    catalog: Catalog,
    identity: _Identity,
    *,
    queue_capacity: int = 128,
) -> StreamSpool:
    return StreamSpool(
        layout=layout,
        catalog=catalog,
        market=identity.market,
        symbol=identity.symbol,
        stream=identity.stream,
        collector_instance_id=f"collector-{identity.market}-{identity.symbol}",
        collector_version="0.1.0+ms3b-test",
        queue_capacity=queue_capacity,
        rotation=RotationPolicy(seconds=3_600.0, bytes=128 * 1024**2),
        durability_interval_seconds=0,
        max_frame_bytes=1024 * 1024,
    )


def test_profile_d_preserves_42_core_identities_and_4032_raw_events(
    tmp_path: Path,
) -> None:
    layout = ensure_storage_layout(tmp_path / "profile-d")
    maximum_queue_depth = 0
    with Catalog(layout.catalog) as catalog:
        for market, symbol, stream in PROFILE_D_IDENTITIES:
            identity = _Identity(market, symbol, stream)
            spool = _spool(layout, catalog, identity)
            for ordinal in range(1, 97):
                spool.enqueue(_event(identity, ordinal))
                maximum_queue_depth = max(maximum_queue_depth, spool.queue.depth)
                assert spool.queue.depth <= spool.queue.capacity
            assert spool.drain_all() == 96
            assert spool.close_and_seal() is not None

        manifests = [
            read_strict_manifest(path, recorder_root=layout.root)
            for path in sorted(layout.manifests.glob("*.manifest.json"))
        ]
        observed = {
            (str(item["market"]), str(item["symbol"]), str(item["stream"]))
            for item in manifests
        }
        expected = set(PROFILE_D_IDENTITIES)
        identity_mismatches = len(observed ^ expected)
        assert len(observed) == 42
        assert identity_mismatches == 0
        assert sum(cast(int, item["record_count"]) for item in manifests) == 4032
        assert maximum_queue_depth <= 128
        assert len(catalog.chunks_in_states(ChunkState.SEALED)) == 42

        for manifest in manifests:
            validate_sealed_artifact(
                layout.root / str(manifest["relative_path"]), manifest
            )
            assert manifest["record_count"] == 96
            assert manifest["complete"] is True
            assert manifest["gap"] is False

    assert len(list(layout.active.glob("*.partial"))) == 0
    assert len(list(layout.sealed.glob("*.partial"))) == 0
    assert len(list(layout.manifests.glob("*.partial"))) == 0


def test_ms3b_backpressure_is_explicit_and_product_local(tmp_path: Path) -> None:
    layout = ensure_storage_layout(tmp_path / "backpressure")
    affected = _Identity("spot", "ETHUSDT", "agg_trade")
    sibling = _Identity("um_perpetual", "ETHUSDT", "agg_trade")
    with Catalog(layout.catalog) as catalog:
        affected_spool = _spool(
            layout, catalog, affected, queue_capacity=1
        )
        affected_spool.enqueue(_event(affected, 1))
        with pytest.raises(IngressQueueFull, match="capacity 1 exhausted"):
            affected_spool.enqueue(_event(affected, 2))
        assert affected_spool.queue.depth == 1
        assert affected_spool.drain_all() == 1
        affected_manifest = affected_spool.close_and_seal()
        assert affected_manifest is not None

        sibling_spool = _spool(layout, catalog, sibling, queue_capacity=1)
        sibling_spool.enqueue(_event(sibling, 1))
        assert sibling_spool.drain_all() == 1
        sibling_manifest = sibling_spool.close_and_seal()
        assert sibling_manifest is not None

        assert (
            affected_manifest["market"],
            affected_manifest["symbol"],
            affected_manifest["stream"],
        ) == ("spot", "ETHUSDT", "agg_trade")
        assert (
            sibling_manifest["market"],
            sibling_manifest["symbol"],
            sibling_manifest["stream"],
        ) == ("um_perpetual", "ETHUSDT", "agg_trade")
        assert len(catalog.chunks_in_states(ChunkState.SEALED)) == 2


def test_ms3b_partial_recovery_is_product_local_and_raw_v1_unchanged(
    tmp_path: Path,
) -> None:
    layout = ensure_storage_layout(tmp_path / "recovery")
    recovered_identity = _Identity("um_perpetual", "SOLUSDT", "diff_depth")
    sibling_identity = _Identity("spot", "SOLUSDT", "diff_depth")
    with Catalog(layout.catalog) as catalog:
        sibling_writer = RawChunkWriter(
            layout=layout,
            catalog=catalog,
            market=sibling_identity.market,
            symbol=sibling_identity.symbol,
            stream=sibling_identity.stream,
            collector_instance_id="sibling",
            collector_version="0.1.0+ms3b-test",
            durability_interval_seconds=0,
        )
        sibling_writer.append(_event(sibling_identity, 1))
        sibling_writer.close()
        sibling_manifest = seal_partial(
            sibling_writer.path, layout=layout, catalog=catalog
        )
        sibling_manifest_path = layout.root / "data" / "manifests" / (
            f"{str(sibling_manifest['chunk_id']).replace('-', '')}.manifest.json"
        )
        sibling_manifest_bytes = sibling_manifest_path.read_bytes()

        recovered_writer = RawChunkWriter(
            layout=layout,
            catalog=catalog,
            market=recovered_identity.market,
            symbol=recovered_identity.symbol,
            stream=recovered_identity.stream,
            collector_instance_id="recovered",
            collector_version="0.1.0+ms3b-test",
            durability_interval_seconds=0,
        )
        recovered_writer.append(_event(recovered_identity, 1))
        recovered_writer.close()
        with recovered_writer.path.open("ab") as partial:
            partial.write(FRAME_PREFIX.pack(100, 0, 0, 0) + b"truncated")

        actions = recover_partials(layout=layout, catalog=catalog)
        assert len(actions) == 1
        assert actions[0].action == "tail_truncated"
        recovered_manifest = seal_partial(
            recovered_writer.path, layout=layout, catalog=catalog
        )
        assert recovered_manifest["recovered"] is True
        assert recovered_manifest["complete"] is False
        assert recovered_manifest["market"] == "um_perpetual"
        assert recovered_manifest["symbol"] == "SOLUSDT"
        assert recovered_manifest["stream"] == "diff_depth"
        assert sibling_manifest_path.read_bytes() == sibling_manifest_bytes
        assert len(catalog.chunks_in_states(ChunkState.SEALED)) == 2
