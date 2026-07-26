"""经审计的 readiness-gated blue/green Collector 实例交接。

BlueGreenSupervisor 实现 ADR-0018:在活跃 Collector 继续运行的同时启动候选
Collector。交接通过 Catalog 审计状态进行:

1. CANDIDATE_STARTING:身份验证,创建 Catalog deployment 记录。
2. CANDIDATE_READY:候选者达到完全 readiness(3 条流已连接 + 已持久化 +
   snapshot 已同步 + orderbook 可靠)。
3. OVERLAP_CONFIRMED:活跃和候选者均至少产生了一个新的 post-readiness 事件,
   证明并发运行。
4. CUTOVER_COMPLETE:活跃被停止(通过 stop 事件 + drain 优雅停止),
   候选者成为唯一运行的 Collector。

若任何步骤失败,候选者被停止,deployment 标记为 ROLLED_BACK。
活跃 Collector 在 overlap 确认之前永不被停止。overlap 期间的 Raw 事件携带
capture flags(blue_green_overlap、deployment_id、instance_role、handoff_reason),
因此规范化可确定性地去重。

Deployment 状态以幂等键经 Catalog 持久化,因此交接期间崩溃可在重启时协调。
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol
from uuid import uuid4

from ..storage.catalog import Catalog, DeploymentState
from .readiness import ReadinessSnapshot


class DeploymentReason(StrEnum):
    UPGRADE = "UPGRADE"
    ROLLBACK = "ROLLBACK"
    CONNECTION_ROTATION = "CONNECTION_ROTATION"


class DeploymentError(RuntimeError):
    """A handoff request violates identity or active-instance safety."""


class ManagedCollector(Protocol):
    async def run(self, stop: asyncio.Event) -> None: ...

    def readiness_snapshot(self) -> ReadinessSnapshot: ...

    def set_handoff_context(
        self,
        *,
        deployment_id: str | None,
        role: str | None,
        reason: str | None,
    ) -> None: ...


@dataclass(slots=True)
class RunningCollector:
    instance: ManagedCollector
    stop: asyncio.Event
    task: asyncio.Task[None]


@dataclass(frozen=True, slots=True)
class DeploymentResult:
    deployment_id: str
    state: str
    reason: str
    active_instance_id: str
    candidate_instance_id: str
    old_stopped: bool
    candidate_running: bool
    overlap_identifiable: bool
    gap_opened: bool
    error: str | None
    promoted: RunningCollector | None

    def public_dict(self) -> dict[str, object]:
        return {
            "deployment_id": self.deployment_id,
            "state": self.state,
            "reason": self.reason,
            "active_instance_id": self.active_instance_id,
            "candidate_instance_id": self.candidate_instance_id,
            "old_stopped": self.old_stopped,
            "candidate_running": self.candidate_running,
            "overlap_identifiable": self.overlap_identifiable,
            "gap_opened": self.gap_opened,
            "error": self.error,
        }


class BlueGreenSupervisor:
    """Keep the active instance until candidate sync and overlap are proven."""

    def __init__(
        self,
        *,
        catalog: Catalog,
        utc_clock_ns: Callable[[], int] = time.time_ns,
        poll_seconds: float = 0.01,
    ) -> None:
        if poll_seconds <= 0:
            raise ValueError("deployment poll interval must be positive")
        self._catalog = catalog
        self._utc_clock_ns = utc_clock_ns
        self._poll_seconds = poll_seconds

    @staticmethod
    def start(instance: ManagedCollector) -> RunningCollector:
        stop = asyncio.Event()
        return RunningCollector(
            instance=instance,
            stop=stop,
            task=asyncio.create_task(instance.run(stop)),
        )

    async def deploy(
        self,
        *,
        active: RunningCollector,
        candidate: ManagedCollector,
        reason: DeploymentReason = DeploymentReason.UPGRADE,
        readiness_timeout_seconds: float = 60.0,
        overlap_timeout_seconds: float = 10.0,
        shutdown_timeout_seconds: float = 30.0,
        deployment_id: str | None = None,
    ) -> DeploymentResult:
        if min(
            readiness_timeout_seconds,
            overlap_timeout_seconds,
            shutdown_timeout_seconds,
        ) <= 0:
            raise ValueError("deployment timeouts must be positive")
        active_snapshot = active.instance.readiness_snapshot()
        candidate_snapshot = candidate.readiness_snapshot()
        self._validate_identity(active_snapshot, candidate_snapshot)
        if active.task.done() or not active_snapshot.ready:
            raise DeploymentError("active Collector is not running and ready")
        identifier = deployment_id or str(uuid4())
        started_at = self._utc_clock_ns()
        self._catalog.create_deployment(
            deployment_id=identifier,
            reason=reason.value,
            market=active_snapshot.market,
            symbol=active_snapshot.symbol,
            active_instance_id=active_snapshot.collector_instance_id,
            active_version=active_snapshot.collector_version,
            candidate_instance_id=candidate_snapshot.collector_instance_id,
            candidate_version=candidate_snapshot.collector_version,
            occurred_at_utc_ns=started_at,
            evidence={
                "active_readiness": active_snapshot.public_dict(),
                "candidate_readiness": candidate_snapshot.public_dict(),
            },
        )
        context = {
            "deployment_id": identifier,
            "reason": reason.value,
        }
        active.instance.set_handoff_context(
            deployment_id=identifier,
            role="active",
            reason=reason.value,
        )
        candidate.set_handoff_context(
            deployment_id=identifier,
            role="candidate",
            reason=reason.value,
        )
        candidate_running = self.start(candidate)
        ready, failure = await self._wait_candidate(
            deployment_id=identifier,
            active=active,
            candidate=candidate_running,
            timeout_seconds=readiness_timeout_seconds,
        )
        if not ready:
            return await self._rollback(
                deployment_id=identifier,
                reason=reason,
                active=active,
                candidate=candidate_running,
                active_snapshot=active_snapshot,
                candidate_snapshot=candidate_snapshot,
                error=failure or "candidate readiness timeout",
                gap_opened=failure == (
                    "active Collector stopped before candidate readiness"
                ),
            )
        ready_snapshot = candidate.readiness_snapshot()
        self._catalog.transition_deployment(
            identifier,
            DeploymentState.CANDIDATE_READY,
            idempotency_key=f"candidate-ready:{identifier}",
            evidence={"readiness": ready_snapshot.public_dict()},
            occurred_at_utc_ns=self._utc_clock_ns(),
        )
        overlap_active_baseline = active.instance.readiness_snapshot().event_count
        overlap_candidate_baseline = ready_snapshot.event_count
        overlap, failure = await self._wait_overlap(
            deployment_id=identifier,
            active=active,
            candidate=candidate_running,
            active_baseline=overlap_active_baseline,
            candidate_baseline=overlap_candidate_baseline,
            timeout_seconds=overlap_timeout_seconds,
        )
        if not overlap:
            return await self._rollback(
                deployment_id=identifier,
                reason=reason,
                active=active,
                candidate=candidate_running,
                active_snapshot=active_snapshot,
                candidate_snapshot=ready_snapshot,
                error=failure or "overlap evidence timeout",
                gap_opened=failure == "active Collector stopped during overlap",
            )
        active_overlap = active.instance.readiness_snapshot()
        candidate_overlap = candidate.readiness_snapshot()
        self._catalog.transition_deployment(
            identifier,
            DeploymentState.OVERLAP_CONFIRMED,
            idempotency_key=f"overlap-confirmed:{identifier}",
            evidence={
                **context,
                "active_baseline_event_count": overlap_active_baseline,
                "candidate_baseline_event_count": overlap_candidate_baseline,
                "active_event_count": active_overlap.event_count,
                "candidate_event_count": candidate_overlap.event_count,
                "active_instance_id": active_overlap.collector_instance_id,
                "candidate_instance_id": candidate_overlap.collector_instance_id,
                "capture_flags": _handoff_flags(identifier, "candidate", reason.value),
            },
            occurred_at_utc_ns=self._utc_clock_ns(),
        )
        active.stop.set()
        try:
            await asyncio.wait_for(
                asyncio.shield(active.task), timeout=shutdown_timeout_seconds
            )
        except TimeoutError:
            return self._complete_cutover(
                deployment_id=identifier,
                reason=reason,
                active=active,
                candidate=candidate_running,
                active_snapshot=active_overlap,
                candidate_snapshot=candidate_overlap,
                old_stopped=False,
                warning="old Collector shutdown timed out; stop remains requested",
            )
        except BaseException as exc:
            return self._complete_cutover(
                deployment_id=identifier,
                reason=reason,
                active=active,
                candidate=candidate_running,
                active_snapshot=active_overlap,
                candidate_snapshot=candidate_overlap,
                old_stopped=True,
                warning=f"old Collector exited with {type(exc).__name__}",
            )
        return self._complete_cutover(
            deployment_id=identifier,
            reason=reason,
            active=active,
            candidate=candidate_running,
            active_snapshot=active_overlap,
            candidate_snapshot=candidate_overlap,
            old_stopped=True,
            warning=None,
        )

    def _complete_cutover(
        self,
        *,
        deployment_id: str,
        reason: DeploymentReason,
        active: RunningCollector,
        candidate: RunningCollector,
        active_snapshot: ReadinessSnapshot,
        candidate_snapshot: ReadinessSnapshot,
        old_stopped: bool,
        warning: str | None,
    ) -> DeploymentResult:
        current_candidate = candidate.instance.readiness_snapshot()
        gap_opened = candidate.task.done() or not current_candidate.ready
        if gap_opened:
            self._record_gap(
                deployment_id,
                "CANDIDATE_LOST_READINESS_DURING_CUTOVER",
                {"warning": warning},
            )
        candidate.instance.set_handoff_context(
            deployment_id=None, role=None, reason=None
        )
        self._catalog.transition_deployment(
            deployment_id,
            DeploymentState.CUTOVER_COMPLETE,
            idempotency_key=f"cutover-complete:{deployment_id}",
            evidence={
                "old_stopped": old_stopped,
                "old_stop_requested": active.stop.is_set(),
                "candidate_still_ready": current_candidate.ready,
                "unmarked_gap": gap_opened,
                "warning": warning,
            },
            occurred_at_utc_ns=self._utc_clock_ns(),
            error=warning,
        )
        return DeploymentResult(
            deployment_id=deployment_id,
            state=DeploymentState.CUTOVER_COMPLETE.value,
            reason=reason.value,
            active_instance_id=active_snapshot.collector_instance_id,
            candidate_instance_id=candidate_snapshot.collector_instance_id,
            old_stopped=old_stopped,
            candidate_running=not candidate.task.done(),
            overlap_identifiable=True,
            gap_opened=gap_opened,
            error=warning,
            promoted=candidate,
        )

    async def rotate(
        self,
        *,
        active: RunningCollector,
        candidate_factory: Callable[[str], ManagedCollector],
        **timeouts: float,
    ) -> DeploymentResult:
        """Reuse the same readiness/overlap gate for proactive 24-hour rotation."""

        deployment_id = str(uuid4())
        return await self.deploy(
            active=active,
            candidate=candidate_factory(deployment_id),
            reason=DeploymentReason.CONNECTION_ROTATION,
            deployment_id=deployment_id,
            readiness_timeout_seconds=timeouts.get("readiness_timeout_seconds", 60.0),
            overlap_timeout_seconds=timeouts.get("overlap_timeout_seconds", 10.0),
            shutdown_timeout_seconds=timeouts.get("shutdown_timeout_seconds", 30.0),
        )

    async def rotate_after(
        self,
        *,
        active: RunningCollector,
        candidate_factory: Callable[[str], ManagedCollector],
        rotation_seconds: float = 23 * 60 * 60 + 40 * 60,
        **timeouts: float,
    ) -> DeploymentResult:
        """Schedule proactive handoff before Binance's documented 24-hour limit."""

        if not 0 < rotation_seconds < 24 * 60 * 60:
            raise ValueError("rotation must be scheduled before 24 hours")
        await asyncio.sleep(rotation_seconds)
        if active.task.done():
            raise DeploymentError("active Collector stopped before planned rotation")
        return await self.rotate(
            active=active,
            candidate_factory=candidate_factory,
            **timeouts,
        )

    async def _wait_candidate(
        self,
        *,
        deployment_id: str,
        active: RunningCollector,
        candidate: RunningCollector,
        timeout_seconds: float,
    ) -> tuple[bool, str | None]:
        deadline = asyncio.get_running_loop().time() + timeout_seconds
        while asyncio.get_running_loop().time() < deadline:
            if active.task.done():
                self._record_gap(
                    deployment_id,
                    "ACTIVE_FAILED_BEFORE_CANDIDATE_READY",
                    {},
                )
                return False, "active Collector stopped before candidate readiness"
            if candidate.task.done():
                return False, _task_failure(candidate.task)
            snapshot = candidate.instance.readiness_snapshot()
            if snapshot.failure is not None:
                return False, snapshot.failure
            if snapshot.ready:
                return True, None
            await asyncio.sleep(self._poll_seconds)
        return False, None

    async def _wait_overlap(
        self,
        *,
        deployment_id: str,
        active: RunningCollector,
        candidate: RunningCollector,
        active_baseline: int,
        candidate_baseline: int,
        timeout_seconds: float,
    ) -> tuple[bool, str | None]:
        deadline = asyncio.get_running_loop().time() + timeout_seconds
        while asyncio.get_running_loop().time() < deadline:
            if active.task.done():
                self._record_gap(
                    deployment_id,
                    "ACTIVE_FAILED_DURING_OVERLAP",
                    {},
                )
                return False, "active Collector stopped during overlap"
            if candidate.task.done():
                return False, _task_failure(candidate.task)
            old = active.instance.readiness_snapshot()
            new = candidate.instance.readiness_snapshot()
            if not new.ready:
                return False, new.failure or "candidate lost readiness"
            if (
                old.event_count > active_baseline
                and new.event_count > candidate_baseline
            ):
                return True, None
            await asyncio.sleep(self._poll_seconds)
        return False, None

    async def _rollback(
        self,
        *,
        deployment_id: str,
        reason: DeploymentReason,
        active: RunningCollector,
        candidate: RunningCollector,
        active_snapshot: ReadinessSnapshot,
        candidate_snapshot: ReadinessSnapshot,
        error: str,
        gap_opened: bool = False,
    ) -> DeploymentResult:
        candidate.stop.set()
        await asyncio.gather(candidate.task, return_exceptions=True)
        active.instance.set_handoff_context(
            deployment_id=None, role=None, reason=None
        )
        candidate.instance.set_handoff_context(
            deployment_id=None, role=None, reason=None
        )
        self._catalog.transition_deployment(
            deployment_id,
            DeploymentState.ROLLED_BACK,
            idempotency_key=f"deployment-rollback:{deployment_id}",
            evidence={
                "active_task_running": not active.task.done(),
                "candidate_stopped": candidate.task.done(),
                "gap_opened": gap_opened,
            },
            occurred_at_utc_ns=self._utc_clock_ns(),
            error=error,
        )
        return DeploymentResult(
            deployment_id=deployment_id,
            state=DeploymentState.ROLLED_BACK.value,
            reason=reason.value,
            active_instance_id=active_snapshot.collector_instance_id,
            candidate_instance_id=candidate_snapshot.collector_instance_id,
            old_stopped=active.task.done(),
            candidate_running=False,
            overlap_identifiable=False,
            gap_opened=gap_opened,
            error=error,
            promoted=None,
        )

    def _record_gap(
        self, deployment_id: str, event_type: str, evidence: dict[str, object]
    ) -> None:
        occurred_at = self._utc_clock_ns()
        self._catalog.record_operational_event(
            event_id=f"deployment-gap:{deployment_id}:{occurred_at}",
            event_type=event_type,
            occurred_at_utc_ns=occurred_at,
            evidence=evidence,
        )

    @staticmethod
    def _validate_identity(
        active: ReadinessSnapshot, candidate: ReadinessSnapshot
    ) -> None:
        if (
            active.market != candidate.market
            or active.symbol != candidate.symbol
        ):
            raise DeploymentError("active and candidate scopes differ")
        if active.collector_instance_id == candidate.collector_instance_id:
            raise DeploymentError("candidate must use a distinct instance ID")


def _handoff_flags(
    deployment_id: str, role: str, reason: str
) -> tuple[str, ...]:
    return (
        "blue_green_overlap",
        f"deployment_id={deployment_id}",
        f"instance_role={role}",
        f"handoff_reason={reason.lower()}",
    )


def _task_failure(task: asyncio.Task[None]) -> str:
    if task.cancelled():
        return "candidate Collector was cancelled"
    exception = task.exception()
    if exception is None:
        return "candidate Collector stopped before cutover"
    return f"candidate Collector failed: {type(exception).__name__}"
