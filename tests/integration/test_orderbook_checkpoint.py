from __future__ import annotations

import json
from pathlib import Path

import pytest

from binance_market_data_recorder.orderbook.checkpoint import (
    CheckpointError,
    OrderBookCheckpointStore,
)
from binance_market_data_recorder.orderbook.model import BookSnapshot, DepthUpdate
from binance_market_data_recorder.orderbook.reconstructor import LocalBookReconstructor
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout


def depth(sequence: int) -> DepthUpdate:
    return DepthUpdate(
        market="spot",
        symbol="BTCUSDT",
        first_update_id=sequence,
        final_update_id=sequence,
        previous_final_update_id=None,
        bids=((str(90 + sequence % 10), str(sequence)),),
        asks=((str(110 + sequence % 10), str(sequence)),),
    )


def synchronized() -> LocalBookReconstructor:
    reconstructor = LocalBookReconstructor("spot")
    reconstructor.offer(DepthUpdate("spot", "BTCUSDT", 10, 11, None, (("99", "2"),), ()))
    reconstructor.synchronize(BookSnapshot("spot", "BTCUSDT", 10, (("98", "1"),), (("111", "1"),)))
    return reconstructor


def test_checkpoint_restore_and_origin_replay_converge(tmp_path: Path) -> None:
    origin = synchronized()
    for sequence in range(12, 30):
        origin.offer(depth(sequence))
    checkpoint_hash = origin.book.logical_hash()

    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        store = OrderBookCheckpointStore(layout, catalog)
        path = store.save(
            origin,
            collector_version="0.1.0+test",
            source_chunk_hashes=("a" * 64,),
            utc_clock_ns=lambda: 123,
        )
        assert not list(layout.checkpoints.glob("*.partial"))
        document = json.loads(path.read_text())
        row = catalog.orderbook_checkpoint(document["checkpoint_id"])
        assert row is not None
        assert row["book_hash"] == checkpoint_hash
        restored = store.restore(path)
        assert restored.book.logical_hash() == checkpoint_hash

        for sequence in range(30, 50):
            event = depth(sequence)
            origin.offer(event)
            restored.offer(event)
        assert restored.book.logical_hash() == origin.book.logical_hash()


def test_checkpoint_hash_corruption_is_rejected(tmp_path: Path) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        store = OrderBookCheckpointStore(layout, catalog)
        path = store.save(
            synchronized(),
            collector_version="test",
            source_chunk_hashes=("b" * 64,),
            utc_clock_ns=lambda: 1,
        )
        document = json.loads(path.read_text())
        document["book_hash"] = "0" * 64
        path.write_text(json.dumps(document))
        with pytest.raises(CheckpointError, match="document hash mismatch"):
            store.restore(path)


def test_unreliable_book_cannot_be_checkpointed(tmp_path: Path) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        store = OrderBookCheckpointStore(layout, catalog)
        with pytest.raises(CheckpointError, match="unreliable"):
            store.save(LocalBookReconstructor("spot"), collector_version="test")


def test_resynced_checkpoint_preserves_incomplete_gap_history(tmp_path: Path) -> None:
    reconstructor = synchronized()
    assert not reconstructor.offer(DepthUpdate("spot", "BTCUSDT", 13, 15, None, (), (), 13))
    reconstructor.synchronize(BookSnapshot("spot", "BTCUSDT", 14, (("99", "1"),), (("111", "1"),)))
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        store = OrderBookCheckpointStore(layout, catalog)
        path = store.save(
            reconstructor,
            collector_version="test",
            source_chunk_hashes=("c" * 64,),
        )
        restored = store.restore(path)
    interval = restored.unreliable_intervals[0]
    assert interval.ended_at_update_id == 15
    assert interval.complete is False
