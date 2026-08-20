"""Pure named capacity policies for derived storage decisions."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType
from typing import Final

GIB: Final = 1024**3

WARNING_BYTES: Final = 18 * GIB
CRITICAL_BYTES: Final = 14 * GIB
EMERGENCY_BYTES: Final = 12 * GIB
HARD_RESERVE_BYTES: Final = 10 * GIB

ETA_7D_NS: Final = 604_800 * 1_000_000_000
ETA_72H_NS: Final = 259_200 * 1_000_000_000
ETA_24H_NS: Final = 86_400 * 1_000_000_000


class VpsCapacityState(StrEnum):
    NORMAL = "NORMAL"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"
    EMERGENCY = "EMERGENCY"
    HARD_RESERVE = "HARD_RESERVE"


_SEVERITY: Final = {
    VpsCapacityState.NORMAL: 0,
    VpsCapacityState.WARNING: 1,
    VpsCapacityState.CRITICAL: 2,
    VpsCapacityState.EMERGENCY: 3,
    VpsCapacityState.HARD_RESERVE: 4,
}

_ACTIONS: Final = {
    VpsCapacityState.NORMAL: ("CONTINUE_CAPTURE",),
    VpsCapacityState.WARNING: (
        "CONTINUE_CAPTURE",
        "ARCHIVE_SOON_RECOMMENDED",
    ),
    VpsCapacityState.CRITICAL: (
        "CONTINUE_INTEGRITY_CRITICAL_WORK",
        "ARCHIVE_STRONGLY_RECOMMENDED",
    ),
    VpsCapacityState.EMERGENCY: (
        "SUSPEND_NON_CORE",
        "PRIORITIZE_VERIFIED_ARCHIVE_DELETE",
    ),
    VpsCapacityState.HARD_RESERVE: (
        "SUSPEND_NON_CORE",
        "PRIORITIZE_VERIFIED_ARCHIVE_DELETE",
        "HARD_STOP_ELIGIBLE",
    ),
}


@dataclass(frozen=True, slots=True)
class CapacityProfile:
    """An explicitly selected, immutable capacity policy."""

    profile_id: str
    scope: str
    warning_bytes: int
    critical_bytes: int
    emergency_bytes: int
    hard_reserve_bytes: int

    def __post_init__(self) -> None:
        if not self.profile_id or not self.scope:
            raise ValueError("capacity profile identity is required")
        thresholds = (
            self.warning_bytes,
            self.critical_bytes,
            self.emergency_bytes,
            self.hard_reserve_bytes,
        )
        if any(value <= 0 for value in thresholds):
            raise ValueError("capacity profile thresholds must be positive")
        if not (
            self.warning_bytes
            > self.critical_bytes
            > self.emergency_bytes
            > self.hard_reserve_bytes
        ):
            raise ValueError("capacity profile thresholds must descend")

    def validate_scope(self, scope_id: str) -> None:
        if scope_id != self.scope:
            raise ValueError(
                f"capacity profile {self.profile_id!r} is not valid for {scope_id!r}"
            )

    def decide(
        self,
        *,
        scope_id: str,
        total_bytes: int,
        free_bytes: int,
        hard_reserve_eta: Mapping[str, object],
        now_utc_ns: int,
    ) -> CapacityDecision:
        return evaluate_capacity(
            profile=self,
            scope_id=scope_id,
            total_bytes=total_bytes,
            free_bytes=free_bytes,
            hard_reserve_eta=hard_reserve_eta,
            now_utc_ns=now_utc_ns,
        )


VPS_PRODUCTION_V1: Final = CapacityProfile(
    profile_id="vps-production-v1",
    scope="internal",
    warning_bytes=WARNING_BYTES,
    critical_bytes=CRITICAL_BYTES,
    emergency_bytes=EMERGENCY_BYTES,
    hard_reserve_bytes=HARD_RESERVE_BYTES,
)


def selected_capacity_profile(profile_id: str | None) -> CapacityProfile | None:
    """Resolve only an already validated explicit configuration value."""

    if profile_id is None:
        return None
    if profile_id == VPS_PRODUCTION_V1.profile_id:
        return VPS_PRODUCTION_V1
    raise ValueError(f"unknown capacity profile: {profile_id!r}")


@dataclass(frozen=True, slots=True)
class CapacityDecision:
    """A pure result of applying one selected capacity profile."""

    profile: CapacityProfile
    scope_id: str
    total_bytes: int
    free_bytes: int
    absolute_state: VpsCapacityState
    eta_state: VpsCapacityState
    state: VpsCapacityState
    hard_reserve_eta: Mapping[str, object]
    now_utc_ns: int

    @property
    def profile_id(self) -> str:
        return self.profile.profile_id

    @property
    def hard_stop_eligible(self) -> bool:
        return self.free_bytes <= self.profile.hard_reserve_bytes

    @property
    def recommended_actions(self) -> tuple[str, ...]:
        return _ACTIONS[self.state]

    @property
    def thresholds(self) -> Mapping[str, int]:
        return MappingProxyType(
            {
                "warning": self.profile.warning_bytes,
                "critical": self.profile.critical_bytes,
                "emergency": self.profile.emergency_bytes,
                "hard_reserve": self.profile.hard_reserve_bytes,
            }
        )


def _validate_observation(total_bytes: int, free_bytes: int) -> None:
    if (
        isinstance(total_bytes, bool)
        or isinstance(free_bytes, bool)
        or total_bytes <= 0
        or free_bytes < 0
        or free_bytes > total_bytes
    ):
        raise ValueError("invalid capacity observation")


def classify_absolute_state(
    profile: CapacityProfile, *, total_bytes: int, free_bytes: int
) -> VpsCapacityState:
    """Classify absolute free bytes; filesystem capacity never scales limits."""

    _validate_observation(total_bytes, free_bytes)
    if free_bytes <= profile.hard_reserve_bytes:
        return VpsCapacityState.HARD_RESERVE
    if free_bytes <= profile.emergency_bytes:
        return VpsCapacityState.EMERGENCY
    if free_bytes <= profile.critical_bytes:
        return VpsCapacityState.CRITICAL
    if free_bytes <= profile.warning_bytes:
        return VpsCapacityState.WARNING
    return VpsCapacityState.NORMAL


def classify_eta_state(
    eta: Mapping[str, object], *, now_utc_ns: int
) -> VpsCapacityState:
    """Classify existing integer-nanosecond ETA evidence for the hard reserve."""

    if not isinstance(eta, Mapping):
        raise ValueError("malformed capacity ETA evidence")
    if isinstance(now_utc_ns, bool) or not isinstance(now_utc_ns, int):
        raise ValueError("invalid ETA reference time")
    status = eta.get("status")
    if not isinstance(status, str):
        raise ValueError("malformed capacity ETA status")
    if status == "REACHED":
        return VpsCapacityState.HARD_RESERVE
    if status == "INSUFFICIENT_DATA":
        return VpsCapacityState.WARNING
    if status in {"NOT_APPROACHING", "BEYOND_SUPPORTED_RANGE"}:
        return VpsCapacityState.NORMAL
    if status != "FORECAST":
        raise ValueError(f"unknown capacity ETA status: {status!r}")
    eta_utc_ns = eta.get("utc_ns")
    if isinstance(eta_utc_ns, bool) or not isinstance(eta_utc_ns, int):
        raise ValueError("malformed forecast ETA timestamp")
    remaining_ns = eta_utc_ns - now_utc_ns
    if remaining_ns <= ETA_24H_NS:
        return VpsCapacityState.EMERGENCY
    if remaining_ns <= ETA_72H_NS:
        return VpsCapacityState.CRITICAL
    if remaining_ns <= ETA_7D_NS:
        return VpsCapacityState.WARNING
    return VpsCapacityState.NORMAL


def evaluate_capacity(
    *,
    profile: CapacityProfile,
    scope_id: str,
    total_bytes: int,
    free_bytes: int,
    hard_reserve_eta: Mapping[str, object],
    now_utc_ns: int,
) -> CapacityDecision:
    """Evaluate one explicit profile without inspecting or mutating the system."""

    profile.validate_scope(scope_id)
    _validate_observation(total_bytes, free_bytes)
    absolute_state = classify_absolute_state(
        profile, total_bytes=total_bytes, free_bytes=free_bytes
    )
    eta_state = classify_eta_state(hard_reserve_eta, now_utc_ns=now_utc_ns)
    state = max((absolute_state, eta_state), key=_SEVERITY.__getitem__)
    return CapacityDecision(
        profile=profile,
        scope_id=scope_id,
        total_bytes=total_bytes,
        free_bytes=free_bytes,
        absolute_state=absolute_state,
        eta_state=eta_state,
        state=state,
        hard_reserve_eta=MappingProxyType(dict(hard_reserve_eta)),
        now_utc_ns=now_utc_ns,
    )


__all__ = [
    "CRITICAL_BYTES",
    "EMERGENCY_BYTES",
    "ETA_7D_NS",
    "ETA_24H_NS",
    "ETA_72H_NS",
    "HARD_RESERVE_BYTES",
    "VPS_PRODUCTION_V1",
    "WARNING_BYTES",
    "CapacityDecision",
    "CapacityProfile",
    "VpsCapacityState",
    "classify_absolute_state",
    "classify_eta_state",
    "evaluate_capacity",
    "selected_capacity_profile",
]
