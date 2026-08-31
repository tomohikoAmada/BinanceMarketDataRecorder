from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path
from typing import Any, ClassVar, cast

import pytest

from binance_market_data_recorder.binance.usdm.side_data_rest import (
    FIVE_MINUTE_KINDS,
    FIVE_MINUTE_PERIOD_MS,
    FIVE_MINUTE_RETENTION,
    PublicResponse,
    RestSideDataKind,
)
from binance_market_data_recorder.binance.usdm.side_data_schema import UsdMSideStream
from binance_market_data_recorder.collector.usdm_side_data import (
    RestSideDataPoller,
    SideDataStats,
    SideDataSupervisor,
    UsdMRestCooldown,
    UsdMSideDataSettings,
)
from binance_market_data_recorder.domain.event import EventEnvelope
from binance_market_data_recorder.storage.catalog import Catalog


def test_periodic_side_data_is_not_stale_before_its_next_poll_plus_grace() -> None:
    stats = SideDataStats(True, expected_interval_seconds=3600.0)
    stats.status = "RUNNING"
    stats.last_success_at_utc_ns = time.time_ns() - 1800 * 1_000_000_000
    assert stats.public_dict(degraded_after_seconds=900.0)["status"] == "RUNNING"

    stats.last_success_at_utc_ns = time.time_ns() - 4501 * 1_000_000_000
    assert stats.public_dict(degraded_after_seconds=900.0)["status"] == "STALE"


def test_each_side_data_kind_can_be_enabled_independently() -> None:
    settings = UsdMSideDataSettings(
        mark_price_enabled=False,
        liquidation_enabled=True,
        premium_index_enabled=False,
        funding_history_enabled=True,
        funding_info_enabled=False,
        open_interest_enabled=True,
        exchange_info_enabled=False,
    )
    assert not settings.stream_enabled(UsdMSideStream.MARK_PRICE)
    assert settings.stream_enabled(UsdMSideStream.LIQUIDATION)
    assert not settings.rest_enabled(RestSideDataKind.PREMIUM_INDEX)
    assert settings.rest_enabled(RestSideDataKind.FUNDING_HISTORY)
    assert not settings.rest_enabled(RestSideDataKind.FUNDING_INFO)
    assert settings.rest_enabled(RestSideDataKind.OPEN_INTEREST)
    assert not settings.rest_enabled(RestSideDataKind.EXCHANGE_INFO)


def test_polling_intervals_must_be_positive() -> None:
    with pytest.raises(ValueError, match="must be positive"):
        UsdMSideDataSettings(open_interest_interval_seconds=0)


def test_terminal_side_failure_does_not_set_shared_core_stop() -> None:
    class Failing:
        terminal_on_failure = False

        async def run(self, stop: asyncio.Event) -> None:
            raise RuntimeError("injected terminal side failure")

    class Healthy:
        terminal_on_failure = False

        def __init__(self) -> None:
            self.started = asyncio.Event()

        async def run(self, stop: asyncio.Event) -> None:
            self.started.set()
            await stop.wait()

    async def exercise() -> tuple[SideDataSupervisor, asyncio.Event]:
        stop = asyncio.Event()
        healthy = Healthy()
        stats = {"failing": SideDataStats(True), "healthy": SideDataStats(True)}
        supervisor = SideDataSupervisor(
            {"failing": Failing, "healthy": lambda: healthy},
            stats,
            logging.getLogger("test.side-supervisor"),
            retry_initial_seconds=0.001,
            retry_maximum_seconds=0.001,
        )
        task = asyncio.create_task(supervisor.run(stop))
        await asyncio.wait_for(healthy.started.wait(), timeout=1)
        for _ in range(100):
            if stats["failing"].attempts >= 2:
                break
            await asyncio.sleep(0.001)
        assert not stop.is_set()
        assert not task.done()
        stop.set()
        await asyncio.wait_for(task, timeout=1)
        return supervisor, stop

    supervisor, stop = asyncio.run(exercise())
    assert stop.is_set()
    assert isinstance(supervisor.failures["failing"], RuntimeError)
    assert supervisor.stats["failing"].failures >= 1
    assert supervisor.stats["failing"].attempts >= 2


class CursorModel:
    def __init__(self, value: dict[str, object]) -> None:
        self.value = value

    def to_dict(self) -> dict[str, object]:
        return self.value


class CursorResponse:
    status = 200
    headers: ClassVar[dict[str, object]] = {"X-MBX-USED-WEIGHT-1M": "1"}

    def __init__(self, values: list[dict[str, object]]) -> None:
        self.values = values

    def data(self) -> object:
        return [CursorModel(value) for value in self.values]


def _cursor_model(
    kind: RestSideDataKind, timestamp: int
) -> dict[str, object]:
    common: dict[str, object] = {"timestamp": timestamp}
    if kind is RestSideDataKind.OPEN_INTEREST_STATISTICS:
        return {
            **common,
            "symbol": "BTCUSDT",
            "sumOpenInterest": "1",
            "sumOpenInterestValue": "2",
        }
    if kind is RestSideDataKind.TAKER_BUY_SELL_VOLUME:
        return {
            **common,
            "buySellRatio": "1",
            "buyVol": "2",
            "sellVol": "2",
        }
    if kind in {
        RestSideDataKind.GLOBAL_LONG_SHORT_RATIO,
        RestSideDataKind.TOP_LONG_SHORT_ACCOUNT_RATIO,
        RestSideDataKind.TOP_LONG_SHORT_POSITION_RATIO,
    }:
        return {
            **common,
            "symbol": "BTCUSDT",
            "longShortRatio": "1",
            "longAccount": "0.5",
            "shortAccount": "0.5",
        }
    return {
        **common,
        "pair": "BTCUSDT",
        "contractType": "PERPETUAL",
        "indexPrice": "1",
        "futuresPrice": "1",
        "basis": "0",
        "basisRate": "0",
        "annualizedBasisRate": "",
    }


class CursorApi:
    def __init__(
        self,
        kind: RestSideDataKind,
        *,
        empty: bool = False,
        fail_once: bool = False,
    ) -> None:
        self.kind = kind
        self.empty = empty
        self.fail_once = fail_once
        self.calls: list[tuple[int, int, int]] = []

    def _call(self, *args: object) -> PublicResponse:
        limit = cast(int, args[-3])
        start = cast(int, args[-2])
        end = cast(int, args[-1])
        self.calls.append((start, end, limit))
        if self.fail_once:
            self.fail_once = False
            raise RuntimeError("injected request failure")
        response_start = (
            start - FIVE_MINUTE_PERIOD_MS
            if self.kind is RestSideDataKind.TAKER_BUY_SELL_VOLUME
            else start
        )
        timestamps = list(
            range(response_start, end + 1, FIVE_MINUTE_PERIOD_MS)
        )[:limit]
        return CursorResponse(
            [] if self.empty else [_cursor_model(self.kind, item) for item in timestamps]
        )

    open_interest_statistics = _call
    taker_buy_sell_volume = _call
    long_short_ratio = _call
    top_trader_long_short_ratio_accounts = _call
    top_trader_long_short_ratio_positions = _call
    basis = _call


class CursorSpool:
    def __init__(self, *, fail_sync: bool = False) -> None:
        self.envelopes: list[EventEnvelope] = []
        self.fail_sync = fail_sync
        self.closed = False

    def enqueue(self, envelope: EventEnvelope) -> None:
        self.envelopes.append(envelope)

    def drain_all(self) -> int:
        return len(self.envelopes)

    def sync(self) -> None:
        if self.fail_sync:
            raise OSError("injected fsync failure")

    def close_and_seal(self) -> None:
        self.closed = True


def _cursor_poller(
    *,
    kind: RestSideDataKind,
    tmp_path: Path,
    catalog: Catalog,
    api: CursorApi,
    spool: CursorSpool,
    now_ms: int,
    batch_limit: int = 2,
    batches: int = 2,
) -> RestSideDataPoller:
    return RestSideDataPoller(
        kind=kind,
        interval_seconds=300,
        spool=cast(Any, spool),
        stats=SideDataStats(True),
        collector_instance_id="cursor-test",
        collector_version="test",
        logger=logging.getLogger(f"test.cursor.{kind.value}"),
        catalog=catalog,
        rest_api=cast(Any, api),
        catchup_batch_limit=batch_limit,
        catchup_batches_per_attempt=batches,
        utc_clock_ns=lambda: now_ms * 1_000_000,
    )


@pytest.mark.parametrize("kind", sorted(FIVE_MINUTE_KINDS, key=str))
def test_each_five_minute_kind_uses_independent_durable_bounded_cursor(
    tmp_path: Path, kind: RestSideDataKind
) -> None:
    async def exercise() -> None:
        now_ms = 50 * 24 * 60 * 60 * 1000 + 1
        last_closed = (
            now_ms // FIVE_MINUTE_PERIOD_MS
        ) * FIVE_MINUTE_PERIOD_MS - FIVE_MINUTE_PERIOD_MS
        _, retention_ms = FIVE_MINUTE_RETENTION[kind]
        earliest = last_closed - retention_ms + 2 * FIVE_MINUTE_PERIOD_MS
        catalog = Catalog(tmp_path / f"{kind.value}.sqlite")
        api = CursorApi(kind)
        spool = CursorSpool()
        poller = _cursor_poller(
            kind=kind,
            tmp_path=tmp_path,
            catalog=catalog,
            api=api,
            spool=spool,
            now_ms=now_ms,
        )
        assert not await poller._catch_up_five_minute(asyncio.Event())
        api_limit = (
            3
            if kind is RestSideDataKind.TAKER_BUY_SELL_VOLUME
            else 2
        )
        assert api.calls == [
            (
                earliest,
                earliest
                + (
                    3
                    if kind is RestSideDataKind.TAKER_BUY_SELL_VOLUME
                    else 2
                )
                * FIVE_MINUTE_PERIOD_MS
                - 1,
                api_limit,
            ),
            (
                earliest + 2 * FIVE_MINUTE_PERIOD_MS,
                earliest
                + (
                    5
                    if kind is RestSideDataKind.TAKER_BUY_SELL_VOLUME
                    else 4
                )
                * FIVE_MINUTE_PERIOD_MS
                - 1,
                api_limit,
            ),
        ]
        cursor = catalog.side_data_cursor(kind.value)
        assert cursor is not None
        assert cursor["last_persisted_period_timestamp"] == (
            earliest + 3 * FIVE_MINUTE_PERIOD_MS
        )
        assert cursor["source_retention_window"] == FIVE_MINUTE_RETENTION[kind][0]
        assert len(spool.envelopes) == 2
        catalog.close()

    asyncio.run(exercise())


def test_five_minute_cursor_resumes_without_skipping_after_restart(
    tmp_path: Path,
) -> None:
    async def exercise() -> None:
        kind = RestSideDataKind.OPEN_INTEREST_STATISTICS
        now_ms = 50 * 24 * 60 * 60 * 1000 + 1
        catalog = Catalog(tmp_path / "resume.sqlite")
        first_api = CursorApi(kind)
        first = _cursor_poller(
            kind=kind,
            tmp_path=tmp_path,
            catalog=catalog,
            api=first_api,
            spool=CursorSpool(),
            now_ms=now_ms,
            batches=1,
        )
        await first._catch_up_five_minute(asyncio.Event())
        cursor = catalog.side_data_cursor(kind.value)
        assert cursor is not None
        expected_next = (
            cast(int, cursor["last_persisted_period_timestamp"])
            + FIVE_MINUTE_PERIOD_MS
        )
        second_api = CursorApi(kind)
        second = _cursor_poller(
            kind=kind,
            tmp_path=tmp_path,
            catalog=catalog,
            api=second_api,
            spool=CursorSpool(),
            now_ms=now_ms,
            batches=1,
        )
        await second._catch_up_five_minute(asyncio.Event())
        assert second_api.calls[0][0] == expected_next
        catalog.close()

    asyncio.run(exercise())


def test_cursor_does_not_advance_on_request_empty_or_fsync_failure(
    tmp_path: Path,
) -> None:
    async def exercise() -> None:
        kind = RestSideDataKind.BASIS
        now_ms = 50 * 24 * 60 * 60 * 1000 + 1
        request_catalog = Catalog(tmp_path / "request-failure.sqlite")
        request_api = CursorApi(kind, fail_once=True)
        failing_request = _cursor_poller(
            kind=kind,
            tmp_path=tmp_path,
            catalog=request_catalog,
            api=request_api,
            spool=CursorSpool(),
            now_ms=now_ms,
        )
        with pytest.raises(RuntimeError, match="request failure"):
            await failing_request._catch_up_five_minute(asyncio.Event())
        assert request_catalog.side_data_cursor(kind.value) is None
        await failing_request._catch_up_five_minute(asyncio.Event())
        assert request_api.calls[0][0] == request_api.calls[1][0]
        assert request_catalog.side_data_cursor(kind.value) is not None
        request_catalog.close()

        empty_catalog = Catalog(tmp_path / "empty.sqlite")
        empty_spool = CursorSpool()
        empty = _cursor_poller(
            kind=kind,
            tmp_path=tmp_path,
            catalog=empty_catalog,
            api=CursorApi(kind, empty=True),
            spool=empty_spool,
            now_ms=now_ms,
        )
        with pytest.raises(RuntimeError, match="EMPTY_RESPONSE"):
            await empty._catch_up_five_minute(asyncio.Event())
        assert len(empty_spool.envelopes) == 1
        assert empty_catalog.side_data_cursor(kind.value) is None
        assert len(
            empty_catalog.operational_events(event_type="SIDE_DATA_EMPTY_RESPONSE")
        ) == 1
        empty_catalog.close()

        fsync_catalog = Catalog(tmp_path / "fsync.sqlite")
        fsync_failure = _cursor_poller(
            kind=kind,
            tmp_path=tmp_path,
            catalog=fsync_catalog,
            api=CursorApi(kind),
            spool=CursorSpool(fail_sync=True),
            now_ms=now_ms,
        )
        with pytest.raises(OSError, match="fsync"):
            await fsync_failure._catch_up_five_minute(asyncio.Event())
        assert fsync_catalog.side_data_cursor(kind.value) is None
        fsync_catalog.close()

    asyncio.run(exercise())


def test_empty_five_minute_response_uses_bounded_retry_delay(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def exercise() -> list[float]:
        kind = RestSideDataKind.TAKER_BUY_SELL_VOLUME
        now_ms = 50 * 24 * 60 * 60 * 1000 + 1
        catalog = Catalog(tmp_path / "empty-retry.sqlite")
        api = CursorApi(kind, empty=True)
        poller = _cursor_poller(
            kind=kind,
            tmp_path=tmp_path,
            catalog=catalog,
            api=api,
            spool=CursorSpool(),
            now_ms=now_ms,
        )
        stop = asyncio.Event()
        wait_timeouts: list[float] = []

        async def immediate_timeout(awaitable: Any, timeout: float) -> None:
            wait_timeouts.append(timeout)
            awaitable.close()
            if len(wait_timeouts) >= 2:
                stop.set()
            raise TimeoutError

        monkeypatch.setattr(asyncio, "wait_for", immediate_timeout)
        await poller.run(stop)
        catalog.close()
        return wait_timeouts

    assert asyncio.run(exercise())[0] == 5.0


def test_usdm_rest_cooldown_deadlines_are_shared_and_only_extend() -> None:
    mono = [100.0]
    utc = [1_000_000_000_000]
    cooldown = UsdMRestCooldown(
        monotonic_clock=lambda: mono[0], utc_clock_ns=lambda: utc[0]
    )
    first = cooldown.install(status=429, retry_after_seconds=60)
    assert first[0] == 160.0
    assert first[1] == 1_060_000_000_000
    shorter = cooldown.install(status=429, retry_after_seconds=5)
    assert shorter[:2] == first[:2]
    ban = cooldown.install(status=418)
    assert ban[0] == 86_500.0
    assert ban[1] == 87_400_000_000_000


def test_usdm_rest_request_rechecks_cooldown_after_request_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def exercise() -> int:
        kind = RestSideDataKind.BASIS
        catalog = Catalog(tmp_path / "race.sqlite")
        spool = CursorSpool()
        mono = [10.0]
        cooldown = UsdMRestCooldown(monotonic_clock=lambda: mono[0])
        lock = asyncio.Lock()
        poller = _cursor_poller(
            kind=kind,
            tmp_path=tmp_path,
            catalog=catalog,
            api=CursorApi(kind),
            spool=spool,
            now_ms=50 * 24 * 60 * 60 * 1000 + 1,
        )
        poller.request_lock = lock
        poller.cooldown = cooldown
        calls = 0

        def transport(**_: object) -> object:
            nonlocal calls
            calls += 1
            return object()

        monkeypatch.setattr(
            "binance_market_data_recorder.collector.usdm_side_data.capture_rest_side_data",
            transport,
        )
        stop = asyncio.Event()
        poller._active_stop = stop
        await lock.acquire()
        request = asyncio.create_task(poller._request())
        await asyncio.sleep(0)
        cooldown.install(status=418, retry_after_seconds=20)
        lock.release()
        await asyncio.sleep(0)
        assert calls == 0
        mono[0] = 31.0
        cooldown._changed.set()
        await asyncio.wait_for(request, timeout=1)
        catalog.close()
        return calls

    assert asyncio.run(exercise()) == 1


def test_usdm_rest_cooldown_wait_is_stop_aware_for_ban_fallback() -> None:
    async def exercise() -> None:
        cooldown = UsdMRestCooldown(monotonic_clock=lambda: 0.0)
        cooldown.install(status=418)
        stop = asyncio.Event()
        waiter = asyncio.create_task(cooldown.wait(stop))
        await asyncio.sleep(0)
        stop.set()
        await asyncio.wait_for(waiter, timeout=0.2)

    asyncio.run(exercise())


def test_cursor_records_unrecoverable_retention_gap_before_catchup(
    tmp_path: Path,
) -> None:
    async def exercise() -> None:
        kind = RestSideDataKind.GLOBAL_LONG_SHORT_RATIO
        now_ms = 50 * 24 * 60 * 60 * 1000 + 1
        catalog = Catalog(tmp_path / "gap.sqlite")
        catalog.advance_side_data_cursor(
            kind=kind.value,
            last_persisted_period_timestamp=0,
            updated_at_utc_ns=1,
            source_retention_window="latest_30_days",
            retention_window_ms=FIVE_MINUTE_RETENTION[kind][1],
        )
        api = CursorApi(kind)
        poller = _cursor_poller(
            kind=kind,
            tmp_path=tmp_path,
            catalog=catalog,
            api=api,
            spool=CursorSpool(),
            now_ms=now_ms,
            batches=1,
        )
        await poller._catch_up_five_minute(asyncio.Event())
        gaps = catalog.operational_events(
            event_type="SIDE_DATA_UNRECOVERABLE_GAP"
        )
        assert len(gaps) == 1
        evidence = cast(dict[str, object], gaps[0]["evidence"])
        gap_end = evidence["gap_end_timestamp"]
        assert isinstance(gap_end, int)
        assert api.calls[0][0] == gap_end + FIVE_MINUTE_PERIOD_MS
        catalog.close()

    asyncio.run(exercise())
