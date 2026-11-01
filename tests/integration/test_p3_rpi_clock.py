"""RPI recovery must retain other still-active availability conditions."""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Iterator
from functools import partial
from pathlib import Path

import pytest

from binance_market_data_recorder.binance.public_rest import capture_public_response
from binance_market_data_recorder.binance.public_sources import product_sources
from binance_market_data_recorder.binance.public_websocket import (
    DepthSequenceObserver,
    envelope_for_group,
    stream_groups,
)
from binance_market_data_recorder.collector import coverage
from binance_market_data_recorder.collector.coverage import ProductCapture, SelectedAssetCapture
from binance_market_data_recorder.metrics.continuity import continuity_report
from binance_market_data_recorder.normalize.raw import iter_source_records, load_source_chunks
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tests.integration.test_p2_capture import Client
from tests.unit.test_p2_coverage import SMALL, Response, products
from tests.unit.test_p3_availability import BASE, flush, owner_at

SECOND = 1_000_000_000


class RpiFixture:
    def __init__(self, owner: SelectedAssetCapture) -> None:
        self.owner = owner
        self.now = BASE
        self.monotonic = 0
        self.last = 8
        self.raw_frames: list[bytes] = []
        owner.sources.clear()
        self.product = next(p for p in products() if p.key.market == "um_perpetual")
        sources = product_sources(self.product, SMALL)
        self.source = next(s for s in sources if s.stream == "rpi_depth")
        self.snapshot_source = next(s for s in sources if s.stream == "rpi_depth_snapshot")
        for source in (self.source, self.snapshot_source):
            owner.add_source(source)
        self.capture = ProductCapture(owner, self.product)
        owner.collectors[self.product.key] = self.capture
        self.state = owner.sources[self.source.key]
        self.state.connected = True
        self.key = "/".join(self.source.key)
        self.group = stream_groups((self.source,), 128)[0]
        self.sequence = DepthSequenceObserver()
        self.spool = owner.spool(self.source.market, self.source.symbol, self.group.name)

    def receive(self, *, gap: bool = False, received_at: int | None = None) -> None:
        previous = self.last + (4 if gap else 0)
        self.last = previous + 2
        self.monotonic += 1_000_000
        raw = json.dumps({"stream": self.group.wire_names[0], "data": {
            "s": self.product.key.symbol, "ps": self.product.pair, "st": 1,
            "e": "depthUpdate", "E": 1000, "T": 999,
            "U": previous + 1, "u": self.last, "pu": previous, "b": [], "a": [],
        }}).encode()
        envelope = self.sequence.observe(envelope_for_group(
            self.group, self.product, raw_payload=raw, connection_id="c",
            collector_instance_id="i", collector_version="v",
            receive_time_utc_ns=self.now if received_at is None else received_at,
            receive_monotonic_ns=self.monotonic,
        ))
        self.spool.enqueue(envelope)
        self.spool.drain_all()
        self.raw_frames.append(raw)
        self.capture.persisted(envelope)

    def snapshot(self) -> None:
        self.capture.snapshot_persisted(capture_public_response(
            self.snapshot_source,
            lambda s, p: Response({"lastUpdateId": self.last - 1, "bids": [], "asks": []}),
            collector_instance_id="i", collector_version="v", utc_clock_ns=lambda: self.now,
        ).envelope)

    def monitor(self, seconds: int) -> None:
        self.now = BASE + seconds * SECOND
        self.owner._observe_availability(self.now)

    def report(self, end: int) -> dict[str, object]:
        flush(self.owner.availability, self.owner.catalog)
        return continuity_report(  # type: ignore[no-any-return]
            self.owner.catalog, start=BASE, end=BASE + end * SECOND, source_ids=[self.key],
        )["sources"][0]


@pytest.fixture
def rpi(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[RpiFixture]:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        fixture = RpiFixture(owner_at(tmp_path, catalog))
        monkeypatch.setattr(time, "time_ns", lambda: fixture.now)
        fixture.receive()
        fixture.snapshot()
        assert fixture.capture.rpi.is_reliable
        yield fixture
        fixture.spool.close_and_seal()
        restored = [
            record.envelope
            for chunk in load_source_chunks(layout=layout, catalog=catalog)
            for record in iter_source_records(chunk)
        ]
        assert [envelope.raw_payload for envelope in restored] == fixture.raw_frames
        assert [envelope.receive_monotonic_ns for envelope in restored] == [
            (i + 1) * 1_000_000 for i in range(len(restored))
        ]


@pytest.mark.parametrize("callback", ["none", "frame", "snapshot"])
@pytest.mark.parametrize("sequence_break", [False, True])
def test_rpi_callbacks_preserve_entire_known_clock_interval(
    rpi: RpiFixture, callback: str, sequence_break: bool,
) -> None:
    for second in range(11):
        rpi.monitor(second)
    gap_id = None
    for second in range(10):
        rpi.monitor(second)  # UTC reverses ten seconds; monotonic receives still increase.
        health = rpi.owner.availability.sources[rpi.key]
        gap_id = gap_id or health.gap_id
        assert health.reason == "CLOCK_DISCONTINUITY"
        rpi.now += 1_000_000
        if second == 0 and sequence_break:
            rpi.receive(gap=True)
            assert not rpi.capture.rpi.is_reliable
            rpi.snapshot()
        if callback == "frame":
            rpi.receive()
        elif callback == "snapshot":
            rpi.snapshot()
        assert rpi.capture.rpi.is_reliable
        assert health.gap_id == gap_id
        assert health.unavailable_since == BASE
        assert health.reason == "CLOCK_DISCONTINUITY"
    rpi.now = BASE + 10 * SECOND
    if callback == "frame":
        rpi.receive()
    elif callback == "snapshot":
        rpi.snapshot()
    else:
        rpi.monitor(10)
    assert rpi.owner.availability.sources[rpi.key].unavailable_since is None
    for second in range(11, 22):
        rpi.monitor(second)
    report = rpi.report(20)
    assert report["unavailable_intervals"] == [(BASE, BASE + 10 * SECOND)]
    assert report["unavailable_or_unknown_ns"] == 10 * SECOND
    assert report["coverage"] == 0.5
    if sequence_break:
        rpi.spool.close_and_seal()
        assert any(
            "sequence_gap" in record.envelope.capture_flags
            for chunk in load_source_chunks(layout=rpi.owner.layout, catalog=rpi.owner.catalog)
            for record in iter_source_records(chunk)
        )


def test_rpi_received_before_clock_reversal_cannot_restore_current_clock(rpi: RpiFixture) -> None:
    for second in range(11):
        rpi.monitor(second)
    rpi.monitor(0)
    rpi.now += 1_000_000
    # This frame was received at the pre-reversal boundary and persisted later.
    rpi.receive(received_at=BASE + 10 * SECOND)
    health = rpi.owner.availability.sources[rpi.key]
    assert health.unavailable_since == BASE
    assert health.reason == "CLOCK_DISCONTINUITY"
    for second in range(1, 22):
        rpi.monitor(second)
    assert rpi.report(20)["unavailable_intervals"] == [(BASE, BASE + 10 * SECOND)]


@pytest.mark.parametrize("callback", ["frame", "snapshot"])
def test_rpi_bridge_does_not_clear_same_group_writer_stall(
    rpi: RpiFixture, callback: str,
) -> None:
    collector = rpi.owner.make_group(
        rpi.group, rpi.product, lambda e: None, lambda e: None,
    )
    collector._received_total = 2
    collector._persisted_total = 1
    collector._pending_since_utc_ns = collector._last_write_utc_ns = BASE
    rpi.monitor(0)
    rpi.now += 100_000_000
    rpi.receive(gap=True)
    assert not rpi.capture.rpi.is_reliable
    health = rpi.owner.availability.sources[rpi.key]
    identity = health.gap_id
    for second in range(1, 12):
        rpi.monitor(second)
    rpi.now += 1_000_000
    rpi.snapshot()
    if callback == "frame":
        rpi.receive()
    assert rpi.capture.rpi.is_reliable
    assert health.gap_id == identity
    assert health.unavailable_since == BASE + 100_000_000
    collector._persisted_total = 2
    collector._pending_since_utc_ns = None
    rpi.now = BASE + 12 * SECOND
    rpi.snapshot()
    assert health.unavailable_since is None
    for second in range(12, 23):
        rpi.monitor(second)
    assert rpi.report(20)["unavailable_intervals"] == [
        (BASE + 100_000_000, BASE + 12 * SECOND),
    ]


@pytest.mark.parametrize("blocker", ["disconnected", "malformed", "dormant"])
def test_rpi_snapshot_preserves_other_source_conditions(rpi: RpiFixture, blocker: str) -> None:
    rpi.monitor(0)
    rpi.now += SECOND
    rpi.owner.availability.failure(rpi.key, rpi.now, blocker)
    health = rpi.owner.availability.sources[rpi.key]
    identity = health.gap_id
    rpi.state.connected = blocker != "disconnected"
    rpi.state.error = "MalformedWebSocketPayload" if blocker == "malformed" else None
    rpi.state.dormant = blocker == "dormant"
    rpi.now += 1_000_000
    rpi.snapshot()
    assert health.gap_id == identity
    assert health.unavailable_since == BASE + SECOND
    rpi.state.connected, rpi.state.error, rpi.state.dormant = True, None, False
    rpi.now = BASE + 2 * SECOND
    rpi.snapshot()
    assert health.unavailable_since is None


def test_rest_success_also_retains_known_clock_uncertainty(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = BASE
    monkeypatch.setattr(time, "time_ns", lambda: now)
    monkeypatch.setattr(coverage, "capture_public_response", partial(
        capture_public_response, utc_clock_ns=lambda: now,
    ))

    async def scenario() -> None:
        nonlocal now
        layout = ensure_storage_layout(tmp_path)
        with Catalog(layout.catalog) as catalog:
            owner = owner_at(tmp_path, catalog)
            owner.sources.clear()
            product = next(p for p in products() if p.key.market == "um_perpetual")
            source = next(s for s in product_sources(product, SMALL) if s.stream == "open_interest")
            owner.add_source(source)
            owner.clients["usdm"] = Client({
                "symbol": product.key.symbol, "openInterest": "1", "time": 1000,
            })
            state = owner.sources[source.key]
            assert (await owner.poll_once(state)).valid
            for second in range(11):
                now = BASE + second * SECOND
                owner._observe_availability(now)
            for second in range(10):
                now = BASE + second * SECOND
                owner._observe_availability(now)
                now += 1_000_000
                assert (await owner.poll_once(state)).valid
                health = owner.availability.sources["/".join(source.key)]
                assert health.unavailable_since == BASE
                assert health.reason == "CLOCK_DISCONTINUITY"
            now = BASE + 10 * SECOND
            assert (await owner.poll_once(state)).valid
            assert owner.availability.sources["/".join(source.key)].unavailable_since is None
            for spool in owner.spools.values():
                spool.close_and_seal()

    asyncio.run(scenario())


def test_delivery_completion_ends_eligibility_without_clock_health_credit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = BASE
    monkeypatch.setattr(time, "time_ns", lambda: now)
    monkeypatch.setattr(coverage, "capture_public_response", partial(
        capture_public_response, utc_clock_ns=lambda: now,
    ))

    async def scenario() -> None:
        nonlocal now
        layout = ensure_storage_layout(tmp_path)
        with Catalog(layout.catalog) as catalog:
            owner = owner_at(tmp_path, catalog)
            owner.sources.clear()
            product = next(p for p in products() if p.key.market == "um_delivery")
            owner.directory.products[product.key] = product
            source = next(s for s in product_sources(product, SMALL) if s.family == "D31")
            owner.add_source(source)
            owner.clients["usdm"] = Client([
                {"deliveryTime": product.delivery_time_ms, "deliveryPrice": "1.0001"},
            ])
            state = owner.sources[source.key]
            state.first_due = state.last_success = BASE
            for second in range(11):
                now = BASE + second * SECOND
                owner._observe_availability(now)
            now = BASE
            owner._observe_availability(now)
            health = owner.availability.sources["/".join(source.key)]
            assert health.reason == "CLOCK_DISCONTINUITY"
            last_valid = health.last_valid
            now += 1_000_000
            assert (await owner.poll_once(state)).valid
            assert state.dormant and health.dormant
            assert health.last_valid == last_valid  # No clock-health credit from the response.
            flush(owner.availability, catalog)
            gaps = catalog.source_availability_intervals("/".join(source.key), BASE, BASE + SECOND)
            assert len(gaps) == 1
            assert gaps[0]["started_at_utc_ns"] == BASE
            assert gaps[0]["ended_at_utc_ns"] == now
            for spool in owner.spools.values():
                spool.close_and_seal()

    asyncio.run(scenario())
