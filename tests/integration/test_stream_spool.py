from __future__ import annotations

from pathlib import Path

from binance_market_data_recorder.spool.stream import StreamSpool
from binance_market_data_recorder.spool.writer import RotationPolicy
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tests.factories import event


def test_stream_spool_rotates_at_first_byte_threshold(tmp_path: Path) -> None:
    layout = ensure_storage_layout(tmp_path)
    payload = b"x" * (1024 * 1024)
    with Catalog(layout.catalog) as catalog:
        spool = StreamSpool(
            layout=layout,
            catalog=catalog,
            market="spot",
            symbol="BTCUSDT",
            stream="diff_depth",
            collector_instance_id="collector-1",
            collector_version="0.1.0+test",
            queue_capacity=2,
            rotation=RotationPolicy(bytes=1024 * 1024),
            durability_interval_seconds=0,
            max_frame_bytes=2 * 1024 * 1024,
        )
        spool.enqueue(event(1, payload=payload))
        spool.enqueue(event(2, payload=payload))
        assert spool.drain_all() == 2
        spool.close_and_seal()

    manifests = list(layout.manifests.glob("*.manifest.json"))
    assert len(manifests) == 2
