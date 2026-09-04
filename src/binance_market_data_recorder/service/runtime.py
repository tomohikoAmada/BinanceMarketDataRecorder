"""Long-running native service assembly with graceful shutdown evidence."""

from __future__ import annotations

import asyncio
import logging
import os
import shutil
import signal
import sys
import threading
import time
from collections.abc import Callable, Mapping
from contextlib import suppress
from functools import partial
from pathlib import Path
from typing import Any, Protocol
from uuid import uuid4

from ..binance.spot.exchange_info import create_spot_exchange_info_api
from ..binance.spot.rest import PublicSpotRestApi
from ..binance.spot.websocket import SPOT_WEBSOCKET_BASE_URL, open_spot_websocket
from ..binance.usdm.rest import create_usdm_rest_api
from ..binance.usdm.side_data_rest import create_usdm_side_rest_api
from ..binance.usdm.websocket import USDM_WEBSOCKET_ROOT, open_usdm_websocket
from ..collector import (
    MarketCollectorSupervisor,
    SpotCollector,
    SpotCollectorSettings,
    UsdMCollector,
    UsdMCollectorSettings,
)
from ..collector.usdm_side_data import UsdMSideDataSettings
from ..config import RecorderConfig
from ..logging import log_event
from ..spool import recover_storage
from ..storage.capacity import VPS_PRODUCTION_V1, selected_capacity_profile
from ..storage.catalog import Catalog, stream_discontinuity_event_id
from ..storage.forecast import StorageForecaster
from ..storage.layout import StorageLayout, ensure_storage_layout
from ..supervisor.readiness import CORE_STREAMS, ReadinessSnapshot
from ..version import git_commit, package_version
from .deployment_identity import RuntimeDeploymentIdentity
from .lock import ServiceProcessLock
from .power import (
    CaffeinateAssertion,
    ClockDiscontinuityDetector,
    MacSleepObserver,
    NoopSleepObserver,
    SleepGap,
)
from .resources import current_rss_bytes, peak_rss_bytes
from .state import ServiceStateStore


class RuntimeCollector(Protocol):
    async def run(self, stop: asyncio.Event) -> None: ...

    def readiness_snapshot(self) -> ReadinessSnapshot: ...


class SleepObserver(Protocol):
    def start(self) -> None: ...

    def stop(self) -> None: ...


CollectorFactory = Callable[
    [RecorderConfig, logging.Logger, str, str],
    Mapping[str, RuntimeCollector],
]
SleepObserverFactory = Callable[[Callable[[str, int], None]], SleepObserver]


def _collector_factory(
    config: RecorderConfig,
    logger: logging.Logger,
    collector_version: str,
    service_instance_id: str,
) -> Mapping[str, RuntimeCollector]:
    proxy_policy = config.proxy_policy()
    spot_websocket_opener = partial(
        open_spot_websocket,
        proxy=proxy_policy.websocket_proxy(SPOT_WEBSOCKET_BASE_URL),
    )
    usdm_websocket_opener = partial(
        open_usdm_websocket,
        proxy=proxy_policy.websocket_proxy(USDM_WEBSOCKET_ROOT),
    )
    timeout_ms = 10_000
    return {
        "spot": SpotCollector(
            SpotCollectorSettings(
                data_root=config.data_root,
                collector_instance_id=f"{service_instance_id}-spot",
                collector_version=collector_version,
                queue_capacity=config.ingress_queue_capacity,
                receipt_queue_capacity=config.ingress_queue_capacity,
                rotation_seconds=config.rotation_seconds,
                rotation_bytes=config.rotation_bytes,
                durability_interval_seconds=config.durability_interval_seconds,
                max_frame_bytes=config.max_frame_bytes,
                exchange_info_enabled=config.spot_exchange_info_enabled,
                exchange_info_interval_seconds=(
                    config.spot_exchange_info_interval_seconds
                ),
                side_data_degraded_after_seconds=config.side_degraded_after_seconds,
            ),
            logger=logger,
            rest_api=PublicSpotRestApi(
                timeout_ms=timeout_ms,
                proxy_policy=proxy_policy,
            ),
            websocket_opener=spot_websocket_opener,
            exchange_info_api=create_spot_exchange_info_api(
                timeout_ms=timeout_ms,
                proxy_policy=proxy_policy,
            ),
        ),
        "um_perpetual": UsdMCollector(
            UsdMCollectorSettings(
                data_root=config.data_root,
                collector_instance_id=f"{service_instance_id}-um",
                collector_version=collector_version,
                queue_capacity=config.ingress_queue_capacity,
                receipt_queue_capacity=config.ingress_queue_capacity,
                rotation_seconds=config.rotation_seconds,
                rotation_bytes=config.rotation_bytes,
                durability_interval_seconds=config.durability_interval_seconds,
                max_frame_bytes=config.max_frame_bytes,
                side_data=UsdMSideDataSettings(
                    mark_price_enabled=config.side_mark_price_enabled,
                    liquidation_enabled=config.side_liquidation_enabled,
                    premium_index_enabled=config.side_premium_index_enabled,
                    funding_history_enabled=config.side_funding_history_enabled,
                    funding_info_enabled=config.side_funding_info_enabled,
                    open_interest_enabled=config.side_open_interest_enabled,
                    exchange_info_enabled=config.side_exchange_info_enabled,
                    premium_index_interval_seconds=config.side_premium_index_interval_seconds,
                    funding_history_interval_seconds=config.side_funding_history_interval_seconds,
                    funding_info_interval_seconds=config.side_funding_info_interval_seconds,
                    open_interest_interval_seconds=config.side_open_interest_interval_seconds,
                    exchange_info_interval_seconds=config.side_exchange_info_interval_seconds,
                    degraded_after_seconds=config.side_degraded_after_seconds,
                    open_interest_statistics_enabled=(
                        config.side_open_interest_statistics_enabled
                    ),
                    taker_buy_sell_volume_enabled=(
                        config.side_taker_buy_sell_volume_enabled
                    ),
                    global_long_short_ratio_enabled=(
                        config.side_global_long_short_ratio_enabled
                    ),
                    top_long_short_account_ratio_enabled=(
                        config.side_top_long_short_account_ratio_enabled
                    ),
                    top_long_short_position_ratio_enabled=(
                        config.side_top_long_short_position_ratio_enabled
                    ),
                    basis_enabled=config.side_basis_enabled,
                    open_interest_statistics_interval_seconds=(
                        config.side_open_interest_statistics_interval_seconds
                    ),
                    taker_buy_sell_volume_interval_seconds=(
                        config.side_taker_buy_sell_volume_interval_seconds
                    ),
                    global_long_short_ratio_interval_seconds=(
                        config.side_global_long_short_ratio_interval_seconds
                    ),
                    top_long_short_account_ratio_interval_seconds=(
                        config.side_top_long_short_account_ratio_interval_seconds
                    ),
                    top_long_short_position_ratio_interval_seconds=(
                        config.side_top_long_short_position_ratio_interval_seconds
                    ),
                    basis_interval_seconds=config.side_basis_interval_seconds,
                ),
            ),
            logger=logger,
            rest_api=create_usdm_rest_api(
                timeout_ms=timeout_ms,
                proxy_policy=proxy_policy,
            ),
            side_rest_api=create_usdm_side_rest_api(
                timeout_ms=timeout_ms,
                proxy_policy=proxy_policy,
            ),
            websocket_opener=usdm_websocket_opener,
        ),
    }


class ServiceRuntime:
    """Own process lock, recovery, collectors, state, sleep, and SIGTERM."""

    def __init__(
        self,
        *,
        config: RecorderConfig,
        logger: logging.Logger,
        authority_path: Path | None = None,
        collector_factory: CollectorFactory = _collector_factory,
        sleep_observer_factory: SleepObserverFactory | None = None,
        power_assertion: CaffeinateAssertion | None = None,
        utc_clock_ns: Callable[[], int] = time.time_ns,
        monotonic_clock_ns: Callable[[], int] = time.monotonic_ns,
        deployment_identity: RuntimeDeploymentIdentity | None = None,
        disk_usage: Callable[[Path], Any] = shutil.disk_usage,
        capacity_poll_seconds: float | None = None,
    ) -> None:
        self.config = config
        self.logger = logger
        self.authority_path = authority_path
        self.collector_factory = collector_factory
        self.sleep_observer_factory = sleep_observer_factory or (
            MacSleepObserver if sys.platform == "darwin" else NoopSleepObserver
        )
        self.utc_clock_ns = utc_clock_ns
        self.monotonic_clock_ns = monotonic_clock_ns
        self.deployment_identity = deployment_identity
        if config.capacity_profile == VPS_PRODUCTION_V1.profile_id and (
            deployment_identity is None
            or deployment_identity.capacity_profile_id != config.capacity_profile
        ):
            raise ValueError("VPS runtime requires validated deployment identity")
        self.disk_usage = disk_usage
        self.capacity_poll_seconds = (
            config.heartbeat_seconds
            if capacity_poll_seconds is None
            else capacity_poll_seconds
        )
        if self.capacity_poll_seconds <= 0:
            raise ValueError("capacity poll interval must be positive")
        self.layout: StorageLayout = ensure_storage_layout(config.data_root)
        self.state_store = ServiceStateStore(
            self.layout.state / "service_state.json"
        )
        self.process_lock = ServiceProcessLock(
            self.layout.state / "service.lock"
        )
        self.power_assertion = power_assertion or CaffeinateAssertion(
            enabled=config.prevent_sleep
        )
        self.service_instance_id = str(uuid4())
        commit = git_commit()
        self.collector_version = (
            f"{package_version()}+git.{commit}" if commit else package_version()
        )
        self.started_at_utc_ns: int | None = None
        self.shutdown_reason: str | None = None
        self._status = "STARTING"
        self._stop: asyncio.Event | None = None
        self._recovery_stop: threading.Event | None = None
        self._catalog: Catalog | None = None
        self._supervisor: MarketCollectorSupervisor | None = None
        self._collectors: Mapping[str, RuntimeCollector] = {}
        self._sleep_started_at_utc_ns: int | None = None
        self._last_sleep_gap: SleepGap | None = None
        self._detector = ClockDiscontinuityDetector(
            threshold_seconds=config.sleep_gap_threshold_seconds
        )
        self._recovery_action_count = 0
        self._catalog_open = False
        self._startup_recovery_complete = False
        self._capacity_evidence: dict[str, object] | None = None
        self._state_write_lock = asyncio.Lock()

    def request_stop(self, reason: str) -> None:
        if not reason:
            raise ValueError("shutdown reason must be non-empty")
        self.shutdown_reason = reason
        self._status = "STOPPING"
        if self._stop is not None:
            self._stop.set()
        if self._recovery_stop is not None:
            self._recovery_stop.set()
        log_event(
            self.logger,
            logging.INFO,
            "service_stop_requested",
            "graceful service stop requested",
            reason=reason,
        )

    def _observe_sleep_notification(self, event: str, occurred_at_utc_ns: int) -> None:
        if event == "will_sleep":
            self._sleep_started_at_utc_ns = occurred_at_utc_ns
            catalog = self._catalog
            if catalog is not None:
                catalog.record_operational_event(
                    event_id=f"system-sleep-begin:{self.service_instance_id}:{occurred_at_utc_ns}",
                    event_type="SYSTEM_SLEEP_BEGIN",
                    occurred_at_utc_ns=occurred_at_utc_ns,
                    evidence={
                        "service_instance_id": self.service_instance_id,
                        "source": "NSWorkspaceWillSleepNotification",
                    },
                )
            return
        if event != "did_wake":
            return
        started = self._sleep_started_at_utc_ns or occurred_at_utc_ns
        self._sleep_started_at_utc_ns = None
        self._record_sleep_gap(
            SleepGap(
                started_at_utc_ns=started,
                ended_at_utc_ns=occurred_at_utc_ns,
                duration_ns=max(0, occurred_at_utc_ns - started),
                source="NSWorkspace",
            )
        )

    def _record_sleep_gap(self, gap: SleepGap) -> None:
        previous = self._last_sleep_gap
        if (
            previous is not None
            and gap.started_at_utc_ns <= previous.ended_at_utc_ns
            and gap.ended_at_utc_ns >= previous.started_at_utc_ns
        ):
            return
        self._last_sleep_gap = gap
        catalog = self._catalog
        if catalog is not None:
            catalog.record_operational_event(
                event_id=(
                    f"system-sleep-gap:{self.service_instance_id}:"
                    f"{gap.started_at_utc_ns}:{gap.ended_at_utc_ns}"
                ),
                event_type="SYSTEM_SLEEP_GAP",
                occurred_at_utc_ns=gap.ended_at_utc_ns,
                evidence={
                    **gap.public_dict(),
                    "service_instance_id": self.service_instance_id,
                    "gap_marked": True,
                },
            )
        log_event(
            self.logger,
            logging.WARNING,
            "system_sleep_gap",
            "capture continuity is not assumed across system sleep",
            **gap.public_dict(),
        )

    def _market_state(self) -> dict[str, dict[str, object]]:
        output: dict[str, dict[str, object]] = {}
        failures = self._supervisor.failures if self._supervisor is not None else {}
        for name, collector in self._collectors.items():
            readiness = collector.readiness_snapshot()
            output[name] = {
                "status": "FAILED" if name in failures else (
                    "READY" if readiness.ready else "CONNECTING"
                ),
                "ready": readiness.ready,
                "collector_instance_id": readiness.collector_instance_id,
                "collector_version": readiness.collector_version,
                "connected_streams": sorted(readiness.connected_streams),
                "persisted_streams": sorted(readiness.persisted_streams),
                "snapshot_persisted": readiness.snapshot_persisted,
                "orderbook_synchronized": readiness.orderbook_synchronized,
                "last_receive_time_utc_ns": readiness.last_receive_time_utc_ns,
                "failure": (
                    type(failures[name]).__name__
                    if name in failures
                    else readiness.failure
                ),
            }
            side_status = getattr(collector, "side_data_status", None)
            if callable(side_status):
                output[name]["side_data"] = side_status()
        return output

    def _state_document(self) -> dict[str, object]:
        markets = self._market_state()
        ready_count = sum(bool(item["ready"]) for item in markets.values())
        if ready_count == len(markets) and markets:
            network_status = "ALL_MARKETS_READY"
        elif ready_count:
            network_status = "DEGRADED"
        else:
            network_status = "CONNECTING"
        side_items: list[dict[str, object]] = []
        for market in markets.values():
            side_data = market.get("side_data")
            if isinstance(side_data, dict):
                side_items.extend(
                    item for item in side_data.values() if isinstance(item, dict)
                )
        if any(
            item.get("enabled")
            and item.get("status") in {"RETRYING", "STALE"}
            for item in side_items
        ):
            network_status = "DEGRADED"
        heartbeat = self.utc_clock_ns()
        return {
            "schema_version": "service-state.v1",
            "status": self._status,
            "pid": os.getpid(),
            "service_instance_id": self.service_instance_id,
            "collector_version": self.collector_version,
            "started_at_utc_ns": self.started_at_utc_ns,
            "heartbeat_at_utc_ns": heartbeat,
            "heartbeat_interval_seconds": self.config.heartbeat_seconds,
            "network_connected": ready_count > 0,
            "network_status": network_status,
            **self.config.proxy_policy().status().public_dict(),
            "markets": markets,
            "shutdown_reason": self.shutdown_reason,
            "prevent_sleep_enabled": self.config.prevent_sleep,
            "power_assertion_active": self.power_assertion.active,
            "last_sleep_gap": (
                self._last_sleep_gap.public_dict()
                if self._last_sleep_gap is not None
                else None
            ),
            "recovery_action_count": self._recovery_action_count,
            "catalog_open": self._catalog_open,
            "startup_recovery_complete": self._startup_recovery_complete,
            "capacity_profile_id": self.config.capacity_profile,
            "capacity": self._capacity_evidence,
            "deployment_identity": (
                {
                    "identity_sha256": self.deployment_identity.identity_sha256,
                    "source_git_sha": self.deployment_identity.source_git_sha,
                    "wheel_sha256": self.deployment_identity.wheel_sha256,
                    "config_sha256": self.deployment_identity.config_sha256,
                    "systemd_unit_sha256": (
                        self.deployment_identity.systemd_unit_sha256
                    ),
                    "capacity_profile_id": (
                        self.deployment_identity.capacity_profile_id
                    ),
                }
                if self.deployment_identity is not None
                else None
            ),
            "runtime_metrics": {
                "process_cpu_seconds": time.process_time(),
                "current_rss_bytes": current_rss_bytes(),
                "peak_rss_bytes": peak_rss_bytes(),
            },
        }

    async def _write_state(self) -> None:
        async with self._state_write_lock:
            document = self._state_document()
            await asyncio.to_thread(self.state_store.write, document)

    async def _heartbeat(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            wall = self.utc_clock_ns()
            monotonic = self.monotonic_clock_ns()
            gap = self._detector.observe(wall, monotonic)
            if gap is not None:
                self._record_sleep_gap(gap)
            await self._write_state()
            await self._wait_for_heartbeat_interval(stop)

    async def _wait_for_heartbeat_interval(self, stop: asyncio.Event) -> None:
        with suppress(TimeoutError):
            await asyncio.wait_for(
                stop.wait(), timeout=self.config.heartbeat_seconds
            )

    def _observe_vps_capacity(self) -> dict[str, object]:
        profile = selected_capacity_profile(self.config.capacity_profile)
        if profile is None:
            raise RuntimeError("VPS capacity observation requires an explicit profile")
        if self._catalog is None:
            raise RuntimeError("VPS capacity observation requires an open Catalog")
        observed_at = self.utc_clock_ns()
        forecaster = StorageForecaster(
            catalog=self._catalog,
            data_root=self.config.data_root,
            utc_clock_ns=self.utc_clock_ns,
            disk_usage=self.disk_usage,
        )
        forecaster.observe_internal(observed_at_utc_ns=observed_at)
        forecast = forecaster.forecast(
            "internal",
            now_utc_ns=observed_at,
            capacity_profile=profile,
        )
        required = {
            "observed_at_utc_ns",
            "total_bytes",
            "free_bytes",
            "capacity_profile",
            "capacity_state",
            "hard_reserve_eta",
        }
        if not required <= set(forecast):
            raise RuntimeError("current VPS capacity evidence is incomplete")
        if forecast["capacity_profile"] != profile.profile_id:
            raise RuntimeError("current VPS capacity profile evidence mismatches")
        total = forecast["total_bytes"]
        free = forecast["free_bytes"]
        if (
            not isinstance(total, int)
            or isinstance(total, bool)
            or not isinstance(free, int)
            or isinstance(free, bool)
            or total <= 0
            or free < 0
            or free > total
        ):
            raise RuntimeError("current VPS capacity observation is invalid")
        evidence = {
            "observed_at_utc_ns": forecast["observed_at_utc_ns"],
            "total_bytes": total,
            "free_bytes": free,
            "capacity_profile": forecast["capacity_profile"],
            "capacity_state": forecast["capacity_state"],
            "hard_reserve_eta": forecast["hard_reserve_eta"],
            "actual_hard_reserve_reached": free <= profile.hard_reserve_bytes,
        }
        self._capacity_evidence = evidence
        return evidence

    async def _capacity_monitor(
        self, stop: asyncio.Event
    ) -> dict[str, object] | None:
        while not stop.is_set():
            try:
                await asyncio.wait_for(
                    stop.wait(), timeout=self.capacity_poll_seconds
                )
            except TimeoutError:
                evidence = await asyncio.to_thread(self._observe_vps_capacity)
                await self._write_state()
                if evidence["actual_hard_reserve_reached"] is True:
                    return evidence
        return None

    def _record_hard_reserve_stop(self, evidence: Mapping[str, object]) -> None:
        if self._catalog is None:
            raise RuntimeError("hard-reserve stop requires an open Catalog")
        observed_at = evidence.get("observed_at_utc_ns")
        if not isinstance(observed_at, int) or isinstance(observed_at, bool):
            raise RuntimeError("hard-reserve stop timestamp is invalid")
        existing_stops = self._catalog.operational_events(
            event_type="DISK_EMERGENCY_STOP"
        )
        if not existing_stops:
            inserted = self._catalog.record_operational_event(
                event_id=f"disk-emergency-stop:{self.service_instance_id}",
                event_type="DISK_EMERGENCY_STOP",
                occurred_at_utc_ns=observed_at,
                evidence={
                    "service_instance_id": self.service_instance_id,
                    "capacity_profile": self.config.capacity_profile,
                    "capacity_state": evidence.get("capacity_state"),
                    "free_bytes": evidence.get("free_bytes"),
                    "total_bytes": evidence.get("total_bytes"),
                    "hard_reserve_bytes": VPS_PRODUCTION_V1.hard_reserve_bytes,
                    "gap_start_at_utc_ns": observed_at,
                    "unarchived_raw_deleted": False,
                    "termination": "HARD_RESERVE_SAFETY_STOP",
                },
            )
            if not inserted:
                raise RuntimeError("hard-reserve stop event identity collision")
        for market in ("spot", "um_perpetual"):
            for stream in sorted(CORE_STREAMS):
                symbol = "BTCUSDT"
                if self._catalog.unclosed_stream_discontinuities(
                    market=market, symbol=symbol, stream=stream
                ):
                    continue
                gap_id = (
                    f"hard-reserve:{self.service_instance_id}:{market}:{stream}"
                )
                self._catalog.ensure_operational_event(
                    event_id=stream_discontinuity_event_id(
                        event_type="STREAM_DISCONTINUITY_STARTED",
                        market=market,
                        symbol=symbol,
                        stream=stream,
                        gap_id=gap_id,
                    ),
                    event_type="STREAM_DISCONTINUITY_STARTED",
                    occurred_at_utc_ns=observed_at,
                    evidence={
                        "gap_id": gap_id,
                        "market": market,
                        "symbol": symbol,
                        "stream": stream,
                        "reason": "session_restart",
                        "interval_classification": "UNRELIABLE",
                        "gap_started_at_utc_ns": observed_at,
                        "original_connection_id": (
                            f"hard-reserve-safety-stop:{self.service_instance_id}"
                        ),
                        "original_generation": 0,
                        "boundary_kind": "no_last_frame_available",
                        "boundary_frame_persisted": False,
                        "boundary_precision": (
                            "actual hard-reserve safety termination; no future "
                            "capture continuity is assumed"
                        ),
                    },
                    symbol=symbol,
                )

    def _install_signal_handlers(self, loop: asyncio.AbstractEventLoop) -> list[signal.Signals]:
        installed: list[signal.Signals] = []
        for selected in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(
                    selected,
                    self.request_stop,
                    selected.name,
                )
            except (NotImplementedError, RuntimeError):
                continue
            installed.append(selected)
        return installed

    async def run(self) -> None:
        self.process_lock.acquire()
        stop = asyncio.Event()
        recovery_stop = threading.Event()
        self._stop = stop
        self._recovery_stop = recovery_stop
        loop = asyncio.get_running_loop()
        installed_signals = self._install_signal_handlers(loop)

        def notify_sleep(event: str, occurred_at: int) -> None:
            loop.call_soon_threadsafe(
                self._observe_sleep_notification,
                event,
                occurred_at,
            )

        observer = self.sleep_observer_factory(
            notify_sleep
        )
        heartbeat_task: asyncio.Task[None] | None = None
        supervisor_task: asyncio.Task[None] | None = None
        capacity_task: asyncio.Task[dict[str, object] | None] | None = None
        failure: BaseException | None = None
        try:
            self.started_at_utc_ns = self.utc_clock_ns()
            self._catalog = Catalog(self.layout.catalog)
            self._catalog_open = True
            await self._write_state()
            heartbeat_task = asyncio.create_task(self._heartbeat(stop))
            await asyncio.sleep(0)
            if recovery_stop.is_set():
                return
            recovery_actions = await asyncio.to_thread(
                recover_storage,
                layout=self.layout,
                catalog=self._catalog,
                authority_path=self.authority_path,
                stop_requested=recovery_stop.is_set,
            )
            self._recovery_action_count = len(recovery_actions)
            if recovery_stop.is_set():
                return
            self._startup_recovery_complete = True
            if self.config.capacity_profile == VPS_PRODUCTION_V1.profile_id:
                capacity = await asyncio.to_thread(self._observe_vps_capacity)
                if recovery_stop.is_set():
                    return
                if capacity["actual_hard_reserve_reached"] is True:
                    self.shutdown_reason = "HARD_RESERVE_SAFETY_STOP"
                    self._status = "STOPPING"
                    self._record_hard_reserve_stop(capacity)
                    return
            self._collectors = self.collector_factory(
                self.config,
                self.logger,
                self.collector_version,
                self.service_instance_id,
            )
            def observe_terminal(name: str, exc: BaseException) -> None:
                if self._catalog is None:
                    return
                occurred_at = self.utc_clock_ns()
                self._catalog.record_operational_event(
                    event_id=(
                        f"core-market-terminal:{self.service_instance_id}:"
                        f"{name}:{occurred_at}"
                    ),
                    event_type="CORE_MARKET_TERMINAL_FAILURE",
                    occurred_at_utc_ns=occurred_at,
                    evidence={
                        "market": name,
                        "error_type": type(exc).__name__,
                        "restart_owner": (
                            "systemd" if sys.platform.startswith("linux") else "launchd"
                        ),
                    },
                )

            self._supervisor = MarketCollectorSupervisor(
                self._collectors, terminal_failure_observer=observe_terminal
            )
            self.power_assertion.start()
            observer.start()
            self._status = "RUNNING"
            self._catalog.record_operational_event(
                event_id=f"service-started:{self.service_instance_id}",
                event_type="SERVICE_STARTED",
                occurred_at_utc_ns=self.started_at_utc_ns,
                evidence={
                    "pid": os.getpid(),
                    "collector_version": self.collector_version,
                    "recovery_action_count": self._recovery_action_count,
                    "prevent_sleep": self.config.prevent_sleep,
                },
            )
            supervisor_task = asyncio.create_task(self._supervisor.run(stop))
            await asyncio.sleep(0)
            await self._write_state()
            if self.config.capacity_profile == VPS_PRODUCTION_V1.profile_id:
                capacity_task = asyncio.create_task(self._capacity_monitor(stop))
                done, _pending = await asyncio.wait(
                    {supervisor_task, capacity_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if supervisor_task in done:
                    await supervisor_task
                else:
                    runtime_capacity = capacity_task.result()
                    if runtime_capacity is not None:
                        self.request_stop("HARD_RESERVE_SAFETY_STOP")
                        await supervisor_task
                        self._record_hard_reserve_stop(runtime_capacity)
            else:
                await supervisor_task
        except BaseException as exc:
            failure = exc
            self._status = "FAILED"
            stop.set()
            if self._catalog is not None:
                occurred_at = self.utc_clock_ns()
                self._catalog.record_operational_event(
                    event_id=f"service-failed:{self.service_instance_id}:{occurred_at}",
                    event_type="SERVICE_FAILED",
                    occurred_at_utc_ns=occurred_at,
                    evidence={"error_type": type(exc).__name__},
                )
            with suppress(BaseException):
                await self._write_state()
            raise
        finally:
            stop.set()
            if heartbeat_task is not None:
                await asyncio.gather(heartbeat_task, return_exceptions=True)
            if capacity_task is not None:
                if not capacity_task.done():
                    capacity_task.cancel()
                await asyncio.gather(capacity_task, return_exceptions=True)
            if supervisor_task is not None and not supervisor_task.done():
                await asyncio.gather(supervisor_task, return_exceptions=True)
            try:
                observer.stop()
            except RuntimeError as exc:
                log_event(
                    self.logger,
                    logging.ERROR,
                    "sleep_observer_stop_failed",
                    "sleep observer cleanup failed",
                    error_type=type(exc).__name__,
                )
            self.power_assertion.stop()
            if failure is None:
                self._status = "STOPPED"
                if self._catalog is not None:
                    stopped_at = self.utc_clock_ns()
                    self._catalog.record_operational_event(
                        event_id=f"service-stopped:{self.service_instance_id}:{stopped_at}",
                        event_type="SERVICE_STOPPED",
                        occurred_at_utc_ns=stopped_at,
                        evidence={"reason": self.shutdown_reason or "completed"},
                    )
                self._catalog_open = False
                await self._write_state()
            if self._catalog is not None:
                self._catalog.close()
                self._catalog = None
            for selected in installed_signals:
                loop.remove_signal_handler(selected)
            self._stop = None
            self._recovery_stop = None
            self.process_lock.release()


async def run_service(
    config: RecorderConfig,
    *,
    logger: logging.Logger,
    authority_path: Path | None = None,
    deployment_identity: RuntimeDeploymentIdentity | None = None,
) -> None:
    await ServiceRuntime(
        config=config,
        logger=logger,
        authority_path=authority_path,
        deployment_identity=deployment_identity,
    ).run()
