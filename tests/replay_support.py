from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from binance_market_data_recorder.normalize import Normalizer
from binance_market_data_recorder.orderbook.checkpoint import (
    OrderBookCheckpointStore,
)
from binance_market_data_recorder.orderbook.model import BookSnapshot, DepthUpdate
from binance_market_data_recorder.orderbook.reconstructor import (
    LocalBookReconstructor,
)
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import StorageLayout, ensure_storage_layout
from tests.normalization_support import BASE_NS, envelope, fixture, seal_events


@dataclass(frozen=True, slots=True)
class ReplayFixture:
    layout: StorageLayout
    build_id: str
    checkpoint_id: str


def _agg_trade_payload(aggregate_id: int) -> bytes:
    document = json.loads(fixture("spot", "agg_trade.json"))
    document["a"] = aggregate_id
    document["f"] = aggregate_id * 10
    document["l"] = aggregate_id * 10
    return json.dumps(document, sort_keys=True, separators=(",", ":")).encode()


def _depth_payload(first: int, final: int) -> bytes:
    document = json.loads(fixture("spot", "diff_depth.json"))
    document["U"] = first
    document["u"] = final
    return json.dumps(document, sort_keys=True, separators=(",", ":")).encode()


def _checkpoint_book() -> LocalBookReconstructor:
    book = LocalBookReconstructor("spot")
    book.offer(
        DepthUpdate(
            "spot",
            "BTCUSDT",
            157,
            160,
            None,
            (("1.0", "2.0"),),
            (("2.0", "3.0"),),
            BASE_NS,
        )
    )
    book.synchronize(
        BookSnapshot(
            "spot",
            "BTCUSDT",
            157,
            (("1.0", "1.0"),),
            (("2.0", "1.0"),),
        )
    )
    return book


def build_replay_fixture(root: Path) -> ReplayFixture:
    layout = ensure_storage_layout(root)
    with Catalog(layout.catalog) as catalog:
        manifests = []
        manifests.append(
            seal_events(
                layout=layout,
                catalog=catalog,
                events=[
                    envelope(
                        market="spot",
                        stream="agg_trade",
                        raw_payload=_agg_trade_payload(1),
                        ordinal=20,
                        source_sequence={"a": 1, "f": 10, "l": 10},
                        receive_time_utc_ns=BASE_NS + 20_000_000,
                        exchange_event_time=1_500,
                        exchange_trade_time=2_000,
                    )
                ],
            )
        )
        manifests.append(
            seal_events(
                layout=layout,
                catalog=catalog,
                events=[
                    envelope(
                        market="spot",
                        stream="agg_trade",
                        raw_payload=_agg_trade_payload(2),
                        ordinal=10,
                        source_sequence={"a": 2, "f": 20, "l": 20},
                        receive_time_utc_ns=BASE_NS + 10_000_000,
                        exchange_event_time=2_500,
                        exchange_trade_time=2_000,
                    )
                ],
            )
        )
        manifests.append(
            seal_events(
                layout=layout,
                catalog=catalog,
                events=[
                    envelope(
                        market="spot",
                        stream="agg_trade",
                        raw_payload=_agg_trade_payload(3),
                        ordinal=5,
                        source_sequence={"a": 3, "f": 30, "l": 30},
                        receive_time_utc_ns=BASE_NS + 5_000_000,
                        exchange_event_time=3_500,
                        exchange_trade_time=4_000,
                    )
                ],
            )
        )
        manifests.append(
            seal_events(
                layout=layout,
                catalog=catalog,
                events=[
                    envelope(
                        market="spot",
                        stream="book_ticker",
                        raw_payload=fixture("spot", "book_ticker.json"),
                        ordinal=30,
                        source_sequence={"u": 400900217},
                        receive_time_utc_ns=BASE_NS + 30_000_000,
                        exchange_event_time=None,
                    )
                ],
            )
        )
        first_depth = seal_events(
            layout=layout,
            catalog=catalog,
            events=[
                envelope(
                    market="spot",
                    stream="diff_depth",
                    raw_payload=_depth_payload(157, 160),
                    ordinal=40,
                    source_sequence={"U": 157, "u": 160},
                    receive_time_utc_ns=BASE_NS + 40_000_000,
                    exchange_event_time=4_000,
                )
            ],
        )
        manifests.append(first_depth)
        manifests.append(
            seal_events(
                layout=layout,
                catalog=catalog,
                events=[
                    envelope(
                        market="spot",
                        stream="diff_depth",
                        raw_payload=_depth_payload(161, 161),
                        ordinal=50,
                        source_sequence={"U": 161, "u": 161},
                        receive_time_utc_ns=BASE_NS + 50_000_000,
                        exchange_event_time=5_000,
                    )
                ],
            )
        )
        manifests.append(
            seal_events(
                layout=layout,
                catalog=catalog,
                events=[
                    envelope(
                        market="um_perpetual",
                        stream="diff_depth",
                        raw_payload=fixture("usdm", "diff_depth.json"),
                        ordinal=60,
                        source_sequence={"U": 157, "u": 160, "pu": 149},
                        flags=("sequence_gap",),
                        receive_time_utc_ns=BASE_NS + 60_000_000,
                        exchange_event_time=6_000,
                        exchange_transaction_time=6_100,
                    )
                ],
            )
        )
        checkpoint_path = OrderBookCheckpointStore(layout, catalog).save(
            _checkpoint_book(),
            collector_version="replay-fixture",
            source_chunk_hashes=(str(first_depth["uncompressed_sha256"]),),
            utc_clock_ns=lambda: BASE_NS + 45_000_000,
        )
        checkpoint_id = checkpoint_path.name.removesuffix(".orderbook.json")
        result = Normalizer(layout=layout, catalog=catalog).run()
    assert result.build_id is not None
    assert len(manifests) == 7
    return ReplayFixture(layout, result.build_id, checkpoint_id)
