"""Recovery-first, exact-identity readiness for the VPS systemd service."""

from __future__ import annotations

import os
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..storage.capacity import HARD_RESERVE_BYTES, VPS_PRODUCTION_V1
from ..storage.catalog import Catalog
from ..supervisor.readiness import CORE_STREAMS
from .deployment_identity import (
    DeploymentIdentity,
    DeploymentIdentityError,
    deployment_identity_path,
    verify_identity_files,
    verify_vps_identity_permissions,
)
from .state import ServiceStateError, ServiceStateStore
from .systemd import SystemdError, SystemdManager

READINESS_TIMEOUT_SECONDS = 300.0
MAX_PROCESS_ENVIRONMENT_BYTES = 1024 * 1024
PROXY_ENVIRONMENT_NAMES = frozenset(
    {
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "NO_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
        "no_proxy",
    }
)


@dataclass(frozen=True, slots=True)
class DeploymentReadinessResult:
    state: str
    reasons: tuple[str, ...]
    evidence: Mapping[str, object]

    def public_dict(self) -> dict[str, object]:
        return {
            "schema_version": "deployment-readiness.v1",
            "state": self.state,
            "reasons": list(self.reasons),
            "evidence": dict(self.evidence),
        }


def _process_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _catalog_ready(path: Path) -> bool:
    if not path.is_file():
        return False
    with Catalog(path, read_only=True) as catalog:
        return catalog.integrity_check() == ("ok",)


def _integer(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _read_process_environment(pid: int) -> dict[str, str]:
    path = Path("/proc") / str(pid) / "environ"
    try:
        with path.open("rb") as handle:
            raw = handle.read(MAX_PROCESS_ENVIRONMENT_BYTES + 1)
    except OSError as exc:
        raise DeploymentIdentityError(
            "cannot read the effective service process environment"
        ) from exc
    if len(raw) > MAX_PROCESS_ENVIRONMENT_BYTES:
        raise DeploymentIdentityError("service process environment exceeds safety bound")
    environment: dict[str, str] = {}
    for entry in raw.split(b"\0"):
        if not entry:
            continue
        name_bytes, separator, value_bytes = entry.partition(b"=")
        if not separator or not name_bytes:
            raise DeploymentIdentityError("service process environment is malformed")
        name = os.fsdecode(name_bytes)
        if name in environment:
            raise DeploymentIdentityError(
                "service process environment contains duplicate names"
            )
        environment[name] = os.fsdecode(value_bytes)
    return environment


def verify_direct_process_environment(
    environment: Mapping[str, str],
) -> dict[str, object]:
    active_proxy = sorted(
        name for name in PROXY_ENVIRONMENT_NAMES if environment.get(name, "")
    )
    if active_proxy:
        raise DeploymentIdentityError(
            "effective service process proxy environment is active: "
            + ", ".join(active_proxy)
        )
    active_operational = sorted(
        name
        for name, value in environment.items()
        if name.startswith("BINANCE_MARKET_RECORDER_") and value
    )
    if active_operational:
        raise DeploymentIdentityError(
            "effective service process operational environment is active: "
            + ", ".join(active_operational)
        )
    return {
        "routing_variables": sorted(PROXY_ENVIRONMENT_NAMES),
        "explicitly_empty": sorted(
            name
            for name in PROXY_ENVIRONMENT_NAMES
            if name in environment and environment[name] == ""
        ),
        "active_proxy_variables": [],
        "active_recorder_variables": [],
        "direct_network_environment": True,
    }


def _verify_vps_identity(identity: DeploymentIdentity) -> Mapping[str, object]:
    evidence = dict(
        verify_identity_files(
            identity,
            expected_config_path=Path(identity.config_path),
            expected_profile_id=VPS_PRODUCTION_V1.profile_id,
            require_root_controlled=True,
        )
    )
    evidence["identity_permissions"] = verify_vps_identity_permissions(
        deployment_identity_path(Path(identity.config_path)),
        expected_group=str(identity.systemd_effective.get("group", "")),
    )
    return evidence


class VpsReadinessEvaluator:
    def __init__(
        self,
        *,
        data_root: Path,
        identity: DeploymentIdentity,
        systemd_manager: SystemdManager,
        utc_clock_ns: Callable[[], int] = time.time_ns,
        process_alive: Callable[[int], bool] = _process_alive,
        catalog_ready: Callable[[Path], bool] = _catalog_ready,
        identity_verifier: Callable[[DeploymentIdentity], Mapping[str, object]] | None = None,
        process_environment: Callable[[int], Mapping[str, str]] = (
            _read_process_environment
        ),
    ) -> None:
        self.data_root = data_root
        self.identity = identity
        self.systemd_manager = systemd_manager
        self.utc_clock_ns = utc_clock_ns
        self.process_alive = process_alive
        self.catalog_ready = catalog_ready
        self.identity_verifier = identity_verifier or _verify_vps_identity
        self.process_environment = process_environment

    def _result(
        self, state: str, reasons: list[str], evidence: Mapping[str, object]
    ) -> DeploymentReadinessResult:
        return DeploymentReadinessResult(state, tuple(reasons), evidence)

    def evaluate(self) -> DeploymentReadinessResult:
        evidence: dict[str, object] = {}
        try:
            identity_evidence = dict(self.identity_verifier(self.identity))
            install_contract = self.systemd_manager.verify_install_contract()
            effective = self.systemd_manager.verify_effective_properties(
                expected=dict(self.identity.systemd_effective)
            )
            systemd = self.systemd_manager.runtime_properties()
        except (DeploymentIdentityError, OSError, SystemdError, ValueError) as exc:
            return self._result(
                "FAILED",
                [f"deployment_evidence_invalid:{type(exc).__name__}:{exc}"],
                evidence,
            )
        evidence["identity"] = identity_evidence
        evidence["install_contract"] = install_contract
        evidence["systemd_effective"] = effective
        evidence["systemd_runtime"] = systemd
        active_state = systemd.get("active_state")
        if active_state == "failed":
            return self._result("FAILED", ["systemd_service_failed"], evidence)
        if active_state != "active":
            return self._result("NOT_READY", ["systemd_service_not_active"], evidence)
        main_pid = _integer(systemd.get("main_pid"))
        if main_pid is None or main_pid <= 0:
            return self._result("FAILED", ["systemd_main_pid_invalid"], evidence)
        try:
            process_environment = verify_direct_process_environment(
                self.process_environment(main_pid)
            )
        except (DeploymentIdentityError, OSError, ValueError) as exc:
            return self._result(
                "FAILED",
                [f"process_environment_invalid:{type(exc).__name__}:{exc}"],
                evidence,
            )
        evidence["process_environment"] = process_environment
        try:
            service_state = ServiceStateStore(
                self.data_root / "state" / "service_state.json"
            ).read()
        except ServiceStateError as exc:
            return self._result(
                "FAILED", [f"service_state_invalid:{exc}"], evidence
            )
        if service_state is None:
            return self._result("NOT_READY", ["service_state_not_published"], evidence)
        evidence["service_state"] = service_state
        state_pid = _integer(service_state.get("pid"))
        if state_pid != main_pid:
            return self._result("FAILED", ["main_pid_service_state_mismatch"], evidence)
        heartbeat = _integer(service_state.get("heartbeat_at_utc_ns"))
        interval = service_state.get("heartbeat_interval_seconds")
        if (
            heartbeat is None
            or not isinstance(interval, (int, float))
            or isinstance(interval, bool)
            or interval <= 0
        ):
            return self._result("FAILED", ["service_heartbeat_invalid"], evidence)
        now = self.utc_clock_ns()
        heartbeat_age = now - heartbeat
        maximum_age = int(max(30.0, float(interval) * 3) * 1_000_000_000)
        if heartbeat_age < -5_000_000_000:
            return self._result("FAILED", ["service_heartbeat_in_future"], evidence)
        if not self.process_alive(state_pid):
            return self._result("FAILED", ["service_state_pid_dead"], evidence)
        if heartbeat_age > maximum_age:
            return self._result("FAILED", ["service_heartbeat_stale"], evidence)
        observed_status = service_state.get("status")
        if observed_status == "FAILED":
            return self._result("FAILED", ["runtime_reported_failed"], evidence)
        if observed_status != "RUNNING":
            return self._result(
                "NOT_READY", [f"runtime_status_{str(observed_status).casefold()}"], evidence
            )
        claimed_identity = service_state.get("deployment_identity")
        expected_identity = {
            "identity_sha256": self.identity.identity_sha256,
            "source_git_sha": self.identity.source_git_sha,
            "wheel_sha256": self.identity.wheel_sha256,
            "config_sha256": self.identity.config_sha256,
            "systemd_unit_sha256": self.identity.systemd_unit_sha256,
            "capacity_profile_id": self.identity.capacity_profile_id,
        }
        if claimed_identity != expected_identity:
            return self._result("FAILED", ["runtime_deployment_identity_mismatch"], evidence)
        if service_state.get("capacity_profile_id") != VPS_PRODUCTION_V1.profile_id:
            return self._result("FAILED", ["runtime_capacity_profile_mismatch"], evidence)
        if service_state.get("catalog_open") is not True:
            return self._result("FAILED", ["catalog_not_open"], evidence)
        if service_state.get("startup_recovery_complete") is not True:
            return self._result("NOT_READY", ["startup_recovery_incomplete"], evidence)
        try:
            catalog_is_ready = self.catalog_ready(
                self.data_root / "state" / "catalog.sqlite"
            )
        except (OSError, RuntimeError, ValueError) as exc:
            return self._result(
                "FAILED", [f"catalog_validation_failed:{type(exc).__name__}"], evidence
            )
        if not catalog_is_ready:
            return self._result("FAILED", ["catalog_validation_failed"], evidence)
        markets = service_state.get("markets")
        if not isinstance(markets, dict):
            return self._result("FAILED", ["market_readiness_absent"], evidence)
        for market_name in ("spot", "um_perpetual"):
            market = markets.get(market_name)
            if not isinstance(market, dict):
                return self._result(
                    "NOT_READY", [f"{market_name}_readiness_absent"], evidence
                )
            if market.get("failure") is not None:
                return self._result("FAILED", [f"{market_name}_failed"], evidence)
            connected = market.get("connected_streams")
            persisted = market.get("persisted_streams")
            if (
                market.get("ready") is not True
                or not isinstance(connected, list)
                or not isinstance(persisted, list)
                or not set(connected) >= CORE_STREAMS
                or not set(persisted) >= CORE_STREAMS
                or market.get("snapshot_persisted") is not True
                or market.get("orderbook_synchronized") is not True
            ):
                return self._result(
                    "NOT_READY", [f"{market_name}_core_not_ready"], evidence
                )
        capacity = service_state.get("capacity")
        if not isinstance(capacity, dict):
            return self._result("FAILED", ["capacity_observation_unavailable"], evidence)
        capacity_observed = _integer(capacity.get("observed_at_utc_ns"))
        total_bytes = _integer(capacity.get("total_bytes"))
        free_bytes = _integer(capacity.get("free_bytes"))
        if (
            capacity_observed is None
            or total_bytes is None
            or free_bytes is None
            or total_bytes <= 0
            or free_bytes < 0
            or free_bytes > total_bytes
        ):
            return self._result("FAILED", ["capacity_observation_invalid"], evidence)
        if now - capacity_observed > maximum_age or capacity_observed - now > 5_000_000_000:
            return self._result("FAILED", ["capacity_observation_stale"], evidence)
        if capacity.get("capacity_profile") != VPS_PRODUCTION_V1.profile_id:
            return self._result("FAILED", ["capacity_evaluation_profile_mismatch"], evidence)
        capacity_state = capacity.get("capacity_state")
        if capacity_state not in {
            "NORMAL",
            "WARNING",
            "CRITICAL",
            "EMERGENCY",
            "HARD_RESERVE",
        }:
            return self._result("FAILED", ["capacity_evaluation_invalid"], evidence)
        if free_bytes <= HARD_RESERVE_BYTES or capacity_state == "HARD_RESERVE":
            return self._result("NOT_READY", ["hard_reserve_safety_stop"], evidence)
        evidence["capacity_state"] = capacity_state
        evidence["free_bytes"] = free_bytes
        return self._result("READY", [], evidence)


def wait_for_readiness(
    evaluator: VpsReadinessEvaluator,
    *,
    timeout_seconds: float = READINESS_TIMEOUT_SECONDS,
    poll_seconds: float = 1.0,
    monotonic_clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], Any] = time.sleep,
) -> DeploymentReadinessResult:
    if timeout_seconds <= 0 or poll_seconds <= 0:
        raise ValueError("readiness timing must be positive")
    deadline = monotonic_clock() + timeout_seconds
    last = evaluator.evaluate()
    while last.state == "NOT_READY" and monotonic_clock() < deadline:
        sleep(min(poll_seconds, max(0.0, deadline - monotonic_clock())))
        last = evaluator.evaluate()
    if last.state == "NOT_READY":
        return DeploymentReadinessResult(
            "FAILED",
            (*last.reasons, "readiness_deadline_expired"),
            last.evidence,
        )
    return last


__all__ = [
    "PROXY_ENVIRONMENT_NAMES",
    "READINESS_TIMEOUT_SECONDS",
    "DeploymentReadinessResult",
    "VpsReadinessEvaluator",
    "verify_direct_process_environment",
    "wait_for_readiness",
]
