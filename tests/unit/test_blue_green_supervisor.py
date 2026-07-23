from __future__ import annotations

import asyncio
from dataclasses import replace
from pathlib import Path

from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.supervisor import (
    BlueGreenSupervisor,
    DeploymentReason,
    ReadinessSnapshot,
    RunningCollector,
)


class FakeManagedCollector:
    def __init__(
        self,
        instance_id: str,
        version: str,
        *,
        initially_ready: bool = False,
        become_ready: bool = True,
        fail: bool = False,
        stop_delay_seconds: float = 0.0,
    ) -> None:
        self.instance_id = instance_id
        self.version = version
        self.become_ready = become_ready
        self.fail = fail
        self.stop_delay_seconds = stop_delay_seconds
        self.context: tuple[str | None, str | None, str | None] = (
            None,
            None,
            None,
        )
        self.observed_contexts: list[tuple[str | None, str | None, str | None]] = []
        streams = frozenset({"diff_depth", "agg_trade", "book_ticker"})
        self._snapshot = ReadinessSnapshot(
            market="spot",
            symbol="BTCUSDT",
            collector_instance_id=instance_id,
            collector_version=version,
            connected_streams=streams if initially_ready else frozenset(),
            persisted_streams=streams if initially_ready else frozenset(),
            snapshot_persisted=initially_ready,
            orderbook_synchronized=initially_ready,
            event_count=1 if initially_ready else 0,
            last_receive_time_utc_ns=1 if initially_ready else None,
            failure=None,
        )

    async def run(self, stop: asyncio.Event) -> None:
        ticks = 0
        while not stop.is_set():
            ticks += 1
            if self.fail and ticks == 2:
                raise RuntimeError("injected candidate failure")
            if self.become_ready and ticks >= 2:
                streams = frozenset({"diff_depth", "agg_trade", "book_ticker"})
                self._snapshot = replace(
                    self._snapshot,
                    connected_streams=streams,
                    persisted_streams=streams,
                    snapshot_persisted=True,
                    orderbook_synchronized=True,
                )
            if self._snapshot.ready:
                self._snapshot = replace(
                    self._snapshot,
                    event_count=self._snapshot.event_count + 1,
                    last_receive_time_utc_ns=(
                        (self._snapshot.last_receive_time_utc_ns or 0) + 1
                    ),
                )
                self.observed_contexts.append(self.context)
            await asyncio.sleep(0.001)
        if self.stop_delay_seconds:
            await asyncio.sleep(self.stop_delay_seconds)

    def readiness_snapshot(self) -> ReadinessSnapshot:
        return self._snapshot

    def set_handoff_context(
        self,
        *,
        deployment_id: str | None,
        role: str | None,
        reason: str | None,
    ) -> None:
        self.context = (deployment_id, role, reason)


async def _ready_active(supervisor: BlueGreenSupervisor) -> tuple[
    FakeManagedCollector, RunningCollector
]:
    active = FakeManagedCollector("old-instance", "1.0.0", initially_ready=True)
    running = supervisor.start(active)
    await asyncio.sleep(0.005)
    return active, running


def test_synchronized_candidate_overlaps_then_stops_old(tmp_path: Path) -> None:
    async def exercise() -> None:
        with Catalog(tmp_path / "catalog.sqlite") as catalog:
            supervisor = BlueGreenSupervisor(catalog=catalog, poll_seconds=0.001)
            old, running = await _ready_active(supervisor)
            candidate = FakeManagedCollector("new-instance", "2.0.0")
            result = await supervisor.deploy(
                active=running,
                candidate=candidate,
                readiness_timeout_seconds=0.2,
                overlap_timeout_seconds=0.2,
                shutdown_timeout_seconds=0.2,
            )
            assert result.state == "CUTOVER_COMPLETE"
            assert result.old_stopped is True
            assert result.candidate_running is True
            assert result.overlap_identifiable is True
            assert result.gap_opened is False
            assert any(context[1] == "active" for context in old.observed_contexts)
            assert any(
                context[1] == "candidate"
                for context in candidate.observed_contexts
            )
            deployment = catalog.deployment(result.deployment_id)
            assert deployment is not None
            assert deployment["state"] == "CUTOVER_COMPLETE"
            assert [event["to_state"] for event in catalog.deployment_events(
                result.deployment_id
            )] == [
                "CANDIDATE_STARTING",
                "CANDIDATE_READY",
                "OVERLAP_CONFIRMED",
                "CUTOVER_COMPLETE",
            ]
            assert result.promoted is not None
            result.promoted.stop.set()
            await result.promoted.task

    asyncio.run(exercise())


def test_failed_or_unready_candidate_never_stops_old(tmp_path: Path) -> None:
    async def exercise(*, fail: bool, become_ready: bool) -> None:
        catalog_path = tmp_path / f"{fail}-{become_ready}.sqlite"
        with Catalog(catalog_path) as catalog:
            supervisor = BlueGreenSupervisor(catalog=catalog, poll_seconds=0.001)
            _old, running = await _ready_active(supervisor)
            result = await supervisor.deploy(
                active=running,
                candidate=FakeManagedCollector(
                    "candidate",
                    "2.0.0",
                    fail=fail,
                    become_ready=become_ready,
                ),
                readiness_timeout_seconds=0.02,
                overlap_timeout_seconds=0.02,
                shutdown_timeout_seconds=0.1,
            )
            assert result.state == "ROLLED_BACK"
            assert result.old_stopped is False
            assert result.candidate_running is False
            assert not running.task.done()
            running.stop.set()
            await running.task

    asyncio.run(exercise(fail=True, become_ready=True))
    asyncio.run(exercise(fail=False, become_ready=False))


def test_rollback_and_connection_rotation_reuse_same_gate(tmp_path: Path) -> None:
    async def exercise() -> None:
        with Catalog(tmp_path / "catalog.sqlite") as catalog:
            supervisor = BlueGreenSupervisor(catalog=catalog, poll_seconds=0.001)
            _old, active = await _ready_active(supervisor)
            upgraded = await supervisor.deploy(
                active=active,
                candidate=FakeManagedCollector("v2", "2.0.0"),
                readiness_timeout_seconds=0.2,
                overlap_timeout_seconds=0.2,
                shutdown_timeout_seconds=0.2,
            )
            assert upgraded.promoted is not None
            rolled_back = await supervisor.deploy(
                active=upgraded.promoted,
                candidate=FakeManagedCollector("v1-rollback", "1.0.0"),
                reason=DeploymentReason.ROLLBACK,
                readiness_timeout_seconds=0.2,
                overlap_timeout_seconds=0.2,
                shutdown_timeout_seconds=0.2,
            )
            assert rolled_back.state == "CUTOVER_COMPLETE"
            assert rolled_back.reason == "ROLLBACK"
            assert rolled_back.promoted is not None
            rotated = await supervisor.rotate_after(
                active=rolled_back.promoted,
                candidate_factory=lambda _deployment_id: FakeManagedCollector(
                    "v1-rotation", "1.0.0"
                ),
                rotation_seconds=0.001,
                readiness_timeout_seconds=0.2,
                overlap_timeout_seconds=0.2,
                shutdown_timeout_seconds=0.2,
            )
            assert rotated.state == "CUTOVER_COMPLETE"
            assert rotated.reason == "CONNECTION_ROTATION"
            assert rotated.promoted is not None
            rotated.promoted.stop.set()
            await rotated.promoted.task

    asyncio.run(exercise())


def test_old_shutdown_timeout_keeps_ready_candidate_running(tmp_path: Path) -> None:
    async def exercise() -> None:
        with Catalog(tmp_path / "catalog.sqlite") as catalog:
            supervisor = BlueGreenSupervisor(catalog=catalog, poll_seconds=0.001)
            old = FakeManagedCollector(
                "old-instance",
                "1.0.0",
                initially_ready=True,
                stop_delay_seconds=0.05,
            )
            active = supervisor.start(old)
            await asyncio.sleep(0.005)
            result = await supervisor.deploy(
                active=active,
                candidate=FakeManagedCollector("candidate", "2.0.0"),
                readiness_timeout_seconds=0.2,
                overlap_timeout_seconds=0.2,
                shutdown_timeout_seconds=0.001,
            )
            assert result.state == "CUTOVER_COMPLETE"
            assert result.old_stopped is False
            assert result.candidate_running is True
            assert result.gap_opened is False
            assert result.error is not None
            assert result.promoted is not None
            result.promoted.stop.set()
            await result.promoted.task
            await active.task

    asyncio.run(exercise())
