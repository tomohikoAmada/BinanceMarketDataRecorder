"""Ordered, idempotent disk-emergency actions without silent Raw deletion."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

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
    ) -> dict[str, object]:
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
            result["actions"] = [*action_names, "ALREADY_STOPPED"]
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
