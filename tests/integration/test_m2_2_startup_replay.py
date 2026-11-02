from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from binance_market_data_recorder.domain.event import EventEnvelope, Market
from binance_market_data_recorder.normalize import Normalizer
from binance_market_data_recorder.normalize.raw import iter_source_records, load_source_chunks
from binance_market_data_recorder.replay import ManifestCatalog, ReplayQuery
from binance_market_data_recorder.spool.stream import StreamSpool
from binance_market_data_recorder.spool.writer import RotationPolicy
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tests.normalization_support import envelope, fixture, seal_events


@pytest.mark.parametrize("market", ["spot", "um_perpetual"])
def test_startup_manifests_normalize_and_replay_with_legacy_chunks(
    tmp_path: Path, market: Market,
) -> None:
    layout = ensure_storage_layout(tmp_path)
    payload = json.loads(fixture("spot" if market == "spot" else "usdm", "agg_trade.json"))

    def trade(ordinal: int, process: str) -> EventEnvelope:
        body = {**payload, "a": ordinal, "f": ordinal * 10, "l": ordinal * 10}
        return envelope(
            market=market, stream="agg_trade", raw_payload=json.dumps(body).encode(),
            ordinal=ordinal, collector_instance_id=process,
        ).model_copy(update={"connection_id": process})

    with Catalog(layout.catalog) as catalog:
        legacy = trade(1, "legacy")
        manifest = seal_events(layout=layout, catalog=catalog, events=[legacy])
        assert "process_boundary_before_first_frame" not in manifest
        expected = [legacy]
        for process in ("process-a", "process-b"):
            spool = StreamSpool(
                layout=layout, catalog=catalog, market=market, symbol="BTCUSDT",
                stream="agg_trade", collector_instance_id=process, collector_version="test",
                queue_capacity=4, rotation=RotationPolicy(),
                durability_interval_seconds=0, max_frame_bytes=1024 * 1024,
            )
            for first_chunk in (True, False):
                item = trade(len(expected) + 1, process)
                expected.append(item)
                spool.enqueue(item)
                spool.drain_all()
                sealed = spool.close_and_seal()
                assert sealed is not None and sealed["record_count"] == 1
                assert sealed.get("process_boundary_before_first_frame", False) is first_chunk
        chunks = load_source_chunks(layout=layout, catalog=catalog)
        raw = sorted(
            (record.envelope for chunk in chunks for record in iter_source_records(chunk)),
            key=lambda item: item.receive_time_utc_ns,
        )
        assert raw == expected
        sources = list(layout.sealed.iterdir()) + list(layout.manifests.iterdir())
        before = {path: path.read_bytes() for path in sources}
        result = Normalizer(layout=layout, catalog=catalog).run()
        assert result.build_id is not None
        dataset = ManifestCatalog(layout.root).open_build(result.build_id)
        events = list(dataset.replay(ReplayQuery(markets=(market,), streams=("agg_trade",))))
        assert [item.row["aggregate_trade_id"] for item in events] == [1, 2, 3, 4, 5]
        assert [item.row["connection_id"] for item in events] == [
            item.connection_id for item in expected
        ]
        assert [item.row["raw_payload_sha256"] for item in events] == [
            hashlib.sha256(item.raw_payload).hexdigest() for item in expected
        ]
        assert len(chunks) == 5 and dataset.summary.source_chunk_count == 5
        assert {path: path.read_bytes() for path in sources} == before
        assert not list(layout.active.iterdir())
