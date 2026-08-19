"""有序、幂等的磁盘紧急操作,绝不静默删除 Raw。

DiskEmergencyCoordinator 应用 ADR-0016 紧急策略:
1. WARNING(空闲 <= 40%)和 CRITICAL(空闲 <= 15%)仅告警;无自动操作。
2. EMERGENCY(空闲 <= max(10 GiB, 5%))触发:暂停非核心工作,优先处理
   已验证归档(加速 LOCAL_DELETE_PENDING 完成)。
3. 若空闲字节 <= hard_reserve(max(5 GiB, 2% 容量, 2 * rotation_bytes)),
   密封活跃 Raw、停止 Collector、记录 DISK_EMERGENCY_STOP 并打开缺口。
   这是幂等的:若 Catalog 中已存在 DISK_EMERGENCY_STOP,仅重新执行
   SEAL_ACTIVE + STOP_COLLECTORS。

紧急策略绝不删除未归档的 Raw。hard reserve 确保在磁盘真正满之前有足够余量
完成优雅密封 + 停止。
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from .capacity import CapacityDecision, VpsCapacityState
from .catalog import Catalog
from .forecast import SpaceSeverity, hard_reserve_bytes, space_severity


@dataclass(frozen=True, slots=True)
class EmergencyActions:
    suspend_non_core: Callable[[], None]
    prioritize_verified_archive: Callable[[], None]
    seal_active: Callable[[], None]
    stop_collectors: Callable[[], None]
    open_gap: Callable[[int], None]


class DiskEmergencyCoordinator:
    def __init__(
        self,
        *,
        catalog: Catalog,
        actions: EmergencyActions,
        rotation_bytes: int,
    ) -> None:
        self.catalog = catalog
        self.actions = actions
        self.rotation_bytes = rotation_bytes

    def apply(
        self,
        *,
        total_bytes: int,
        free_bytes: int,
        observed_at_utc_ns: int,
        capacity_decision: CapacityDecision | None = None,
    ) -> dict[str, object]:
        if capacity_decision is not None:
            if (
                capacity_decision.total_bytes != total_bytes
                or capacity_decision.free_bytes != free_bytes
            ):
                raise ValueError("capacity decision does not match observation")
            return self.apply_capacity_decision(
                capacity_decision, observed_at_utc_ns=observed_at_utc_ns
            )
        severity = space_severity(total_bytes, free_bytes)
        hard_reserve = hard_reserve_bytes(
            total_bytes, rotation_bytes=self.rotation_bytes
        )
        result: dict[str, object] = {
            "severity": severity,
            "hard_reserve_bytes": hard_reserve,
            "actions": [],
            "collector_stop": False,
        }
        action_names: list[str] = []
        if severity is not SpaceSeverity.EMERGENCY:
            return result
        self.actions.suspend_non_core()
        action_names.append("SUSPEND_NON_CORE")
        self.actions.prioritize_verified_archive()
        action_names.append("PRIORITIZE_VERIFIED_ARCHIVE_DELETE")
        if free_bytes > hard_reserve:
            result["actions"] = action_names
            return result
        existing = self.catalog.operational_events(
            event_type="DISK_EMERGENCY_STOP"
        )
        if existing:
            self.actions.seal_active()
            action_names.append("SEAL_ACTIVE")
            self.actions.stop_collectors()
            action_names.append("STOP_COLLECTORS")
            action_names.append("ALREADY_STOPPED")
            result["actions"] = action_names
            result["collector_stop"] = True
            return result
        self.actions.seal_active()
        action_names.append("SEAL_ACTIVE")
        self.actions.stop_collectors()
        action_names.append("STOP_COLLECTORS")
        event_id = f"disk-emergency-stop:{observed_at_utc_ns}"
        inserted = self.catalog.record_operational_event(
            event_id=event_id,
            event_type="DISK_EMERGENCY_STOP",
            occurred_at_utc_ns=observed_at_utc_ns,
            evidence={
                "free_bytes": free_bytes,
                "hard_reserve_bytes": hard_reserve,
                "total_bytes": total_bytes,
            },
        )
        if not inserted:
            raise RuntimeError("disk emergency event identity collision")
        action_names.append("DISK_EMERGENCY_STOP")
        self.actions.open_gap(observed_at_utc_ns)
        action_names.append("OPEN_GAP")
        result["actions"] = action_names
        result["collector_stop"] = True
        result["event_id"] = event_id
        return result

    def apply_capacity_decision(
        self,
        decision: CapacityDecision,
        *,
        observed_at_utc_ns: int,
    ) -> dict[str, object]:
        """Apply an already-derived named-profile decision.

        Forecast windows are intentionally not recomputed here.  Only the
        decision's actual free-byte observation may authorize a hard stop.
        """

        result: dict[str, object] = {
            "capacity_profile": decision.profile_id,
            "capacity_state": decision.state,
            "hard_reserve_bytes": decision.profile.hard_reserve_bytes,
            "actions": [],
            "collector_stop": False,
        }
        action_names: list[str] = []
        if decision.state in {
            VpsCapacityState.NORMAL,
            VpsCapacityState.WARNING,
            VpsCapacityState.CRITICAL,
        }:
            return result
        self.actions.suspend_non_core()
        action_names.append("SUSPEND_NON_CORE")
        self.actions.prioritize_verified_archive()
        action_names.append("PRIORITIZE_VERIFIED_ARCHIVE_DELETE")
        if not decision.hard_stop_eligible:
            result["actions"] = action_names
            return result
        existing = self.catalog.operational_events(
            event_type="DISK_EMERGENCY_STOP"
        )
        self.actions.seal_active()
        action_names.append("SEAL_ACTIVE")
        self.actions.stop_collectors()
        action_names.append("STOP_COLLECTORS")
        result["collector_stop"] = True
        if existing:
            action_names.append("ALREADY_STOPPED")
            result["actions"] = action_names
            return result
        event_id = f"disk-emergency-stop:{observed_at_utc_ns}"
        inserted = self.catalog.record_operational_event(
            event_id=event_id,
            event_type="DISK_EMERGENCY_STOP",
            occurred_at_utc_ns=observed_at_utc_ns,
            evidence={
                "free_bytes": decision.free_bytes,
                "hard_reserve_bytes": decision.profile.hard_reserve_bytes,
                "total_bytes": decision.total_bytes,
            },
        )
        if not inserted:
            raise RuntimeError("disk emergency event identity collision")
        action_names.append("DISK_EMERGENCY_STOP")
        self.actions.open_gap(observed_at_utc_ns)
        action_names.append("OPEN_GAP")
        result["actions"] = action_names
        result["event_id"] = event_id
        return result
