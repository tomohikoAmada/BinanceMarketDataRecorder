"""Repository-owned, read-only M22.9 acceptance evidence observer.

This module is intentionally an observer, not a supervisor.  Its only writes
are immutable, hash-chained JSON records below the operator-selected evidence
root.  Recorder production state is opened read-only and is never repaired,
rotated, or sampled through a writable API.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import time
from collections.abc import Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol, cast
from uuid import uuid4

from ..audit.reconnect_boundaries import audit_data_root
from ..spool.seal import SealError
from ..storage.capacity import VpsCapacityState, evaluate_capacity, selected_capacity_profile
from ..storage.catalog import Catalog, CatalogStateError, ChunkState, RemoteArchiveState
from ..storage.layout import fsync_directory
from .deployment_identity import (
    DeploymentIdentity,
    DeploymentIdentityError,
    enforce_vps_paths,
    load_deployment_identity,
    verify_identity_files,
    verify_vps_identity_permissions,
)
from .readiness import VpsReadinessEvaluator
from .state import ServiceStateError, ServiceStateStore
from .systemd import SystemdError, SystemdManager

SCHEMA_VERSION = "m22.9-acceptance-evidence.v1"
STAGE_NAMES = ("2h", "12h", "24h", "72h", "168h")
STAGE_DURATION_NS = {
    "2h": 7_200_000_000_000,
    "12h": 43_200_000_000_000,
    "24h": 86_400_000_000_000,
    "72h": 259_200_000_000_000,
    "168h": 604_800_000_000_000,
}
SAMPLE_INTERVAL_NS = 300 * 1_000_000_000
MAX_EVIDENCE_GAP_NS = 600 * 1_000_000_000
RESULTS = frozenset({"PASS_CANDIDATE", "FAIL", "INCOMPLETE", "REVIEW_REQUIRED"})
_HEX64 = frozenset("0123456789abcdef")
_COMMON_FIELDS = frozenset(
    {
        "schema_version",
        "evidence_kind",
        "stage",
        "run_id",
        "observed_at_utc_ns",
        "observed_at_boottime_ns",
        "boot_id",
        "deployment_identity_sha256",
        "source_git_sha",
        "wheel_sha256",
        "config_sha256",
        "systemd_unit_sha256",
        "capacity_profile_id",
        "prior_stage_evidence_sha256",
        "stage_start_evidence_sha256",
        "previous_sample_sha256",
        "systemd_process_incarnation",
        "service_instance_id",
        "readiness",
        "catalog_integrity",
        "capacity",
        "discontinuity_summary",
        "reconnect_summary",
        "manifest_inventory",
        "observer_status",
        "blocking_findings",
        "result",
    }
)
_EXTRA_FIELDS = frozenset(
    {"identity", "identity_static_verification", "elapsed_boottime_ns", "required_duration_ns"}
)


def _integer(value: object, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise AcceptanceError(f"{field} is not an integer")
    return value


class AcceptanceError(RuntimeError):
    """Acceptance evidence is absent, unsafe, or fails closed."""


class Clock(Protocol):
    def utc_ns(self) -> int: ...

    def boottime_ns(self) -> int: ...

    def boot_id(self) -> str: ...


class LinuxClock:
    def utc_ns(self) -> int:
        return time.time_ns()

    def boottime_ns(self) -> int:
        if not hasattr(time, "CLOCK_BOOTTIME"):
            raise AcceptanceError("Linux CLOCK_BOOTTIME is unavailable")
        return time.clock_gettime_ns(time.CLOCK_BOOTTIME)

    def boot_id(self) -> str:
        try:
            value = Path("/proc/sys/kernel/random/boot_id").read_text(encoding="ascii").strip()
        except OSError as exc:
            raise AcceptanceError("cannot read Linux boot_id") from exc
        if not value:
            raise AcceptanceError("Linux boot_id is empty")
        return value


def canonical_json(document: Mapping[str, object]) -> bytes:
    """Encode authoritative evidence as compact sorted JSON plus one newline."""

    return (
        json.dumps(dict(document), sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n"
    ).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _safe_evidence_root(root: Path, data_root: Path) -> Path:
    candidate = root.expanduser().resolve(strict=False)
    production = {
        data_root.resolve(strict=False),
        Path("/var/lib/binance-market-data-recorder").resolve(),
        Path("/opt/binance-market-data-recorder").resolve(),
        Path("/etc/binance-market-data-recorder").resolve(),
    }
    for forbidden in production:
        try:
            candidate.relative_to(forbidden)
        except ValueError:
            continue
        raise AcceptanceError("evidence root is inside Recorder production state")
    return candidate


def _publish(root: Path, filename: str, document: Mapping[str, object]) -> tuple[Path, str]:
    if not filename or Path(filename).name != filename or filename.startswith("."):
        raise AcceptanceError("evidence filename is unsafe")
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    path = root / filename
    body = canonical_json(document)
    if path.exists():
        raise AcceptanceError(f"evidence filename collision: {path.name}")
    temporary = root / f".{filename}.{uuid4().hex}.partial"
    descriptor = -1
    try:
        descriptor = os.open(
            temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o600
        )
        view = memoryview(body)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise OSError("evidence write made no progress")
            view = view[written:]
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        if path.exists():
            raise AcceptanceError(f"evidence filename collision: {path.name}")
        # A hard-link publication is no-clobber even if another actor creates
        # the destination between the explicit existence check and publication.
        os.link(temporary, path)
        temporary.unlink()
        fsync_directory(root)
        if path.read_bytes() != body:
            raise AcceptanceError("published evidence readback mismatch")
    except FileExistsError as exc:
        raise AcceptanceError(f"evidence filename collision: {path.name}") from exc
    except OSError as exc:
        raise AcceptanceError(f"cannot publish evidence: {type(exc).__name__}") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        with suppress(OSError):
            temporary.unlink()
    return path, sha256_bytes(body)


def _read_published(path: Path) -> tuple[dict[str, object], str]:
    try:
        body = path.read_bytes()
        value: Any = json.loads(body)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AcceptanceError(f"invalid published evidence: {path}") from exc
    if not isinstance(value, dict) or canonical_json(value) != body:
        raise AcceptanceError(f"evidence is not canonical JSON: {path}")
    if value.get("schema_version") != SCHEMA_VERSION:
        raise AcceptanceError("unsupported acceptance evidence schema")
    if not set(value) >= _COMMON_FIELDS or set(value) - (_COMMON_FIELDS | _EXTRA_FIELDS):
        raise AcceptanceError("acceptance evidence fields are not exact")
    for field_name in ("observed_at_utc_ns", "observed_at_boottime_ns"):
        _integer(value.get(field_name), field_name)
    if value.get("result") not in RESULTS:
        raise AcceptanceError("acceptance evidence result is invalid")
    return value, sha256_bytes(body)


def _identity_fields(identity: DeploymentIdentity) -> dict[str, object]:
    return {
        "deployment_identity_sha256": identity.identity_sha256,
        "source_git_sha": identity.source_git_sha,
        "wheel_sha256": identity.wheel_sha256,
        "config_sha256": identity.config_sha256,
        "systemd_unit_sha256": identity.systemd_unit_sha256,
        "capacity_profile_id": identity.capacity_profile_id,
    }


def _empty_common(
    *,
    kind: str,
    stage: str,
    run_id: str,
    identity: DeploymentIdentity,
    now_utc: int,
    now_boot: int,
    boot_id: str,
) -> dict[str, object]:
    return {
        "schema_version": SCHEMA_VERSION,
        "evidence_kind": kind,
        "stage": stage,
        "run_id": run_id,
        "observed_at_utc_ns": now_utc,
        "observed_at_boottime_ns": now_boot,
        "boot_id": boot_id,
        **_identity_fields(identity),
        "prior_stage_evidence_sha256": None,
        "stage_start_evidence_sha256": None,
        "previous_sample_sha256": None,
        "systemd_process_incarnation": None,
        "service_instance_id": None,
        "readiness": {},
        "catalog_integrity": {},
        "capacity": {},
        "discontinuity_summary": {},
        "reconnect_summary": {},
        "manifest_inventory": {},
        "observer_status": "OBSERVED",
        "blocking_findings": [],
        "result": "INCOMPLETE",
    }


def _manager_for(
    identity: DeploymentIdentity, *, manager: SystemdManager | None = None
) -> SystemdManager:
    if manager is not None:
        return manager
    return SystemdManager(
        data_root=Path(str(identity.systemd_effective["working_directory"])),
        config_file=Path(identity.config_path),
        user=str(identity.systemd_effective.get("user", "")),
        group=str(identity.systemd_effective.get("group", "")),
        python_executable=Path("/opt/binance-market-data-recorder/venv/bin/python"),
        capacity_profile_id=identity.capacity_profile_id,
    )


def _validate_stage(stage: str) -> None:
    if stage not in STAGE_NAMES:
        raise AcceptanceError(f"unsupported duration stage: {stage}")


def create_identity_evidence(
    *,
    config_file: Path,
    expected_source_git_sha: str,
    evidence_root: Path,
    data_root: Path,
    identity: DeploymentIdentity | None = None,
    manager: SystemdManager | None = None,
) -> tuple[Path, str, dict[str, object]]:
    """Perform existing static deployment verification and publish identity."""

    if len(expected_source_git_sha) != 40 or any(
        char not in _HEX64 for char in expected_source_git_sha.lower()
    ):
        raise AcceptanceError("expected source Git SHA must be 40 hexadecimal characters")
    root = _safe_evidence_root(evidence_root, data_root)
    selected = identity or load_deployment_identity(
        config_file.with_name("deployment-identity.json")
    )
    if selected.source_git_sha != expected_source_git_sha.lower():
        raise AcceptanceError("deployed source Git identity does not match expectation")
    try:
        enforce_vps_paths(selected)
        permissions = verify_vps_identity_permissions(
            config_file.with_name("deployment-identity.json"),
            expected_group=str(selected.systemd_effective.get("group", "")),
        )
        files = verify_identity_files(
            selected,
            expected_config_path=config_file,
            expected_profile_id=selected.capacity_profile_id,
            require_root_controlled=True,
        )
        systemd = _manager_for(selected, manager=manager)
        install = systemd.verify_install_contract()
        effective = systemd.verify_effective_properties(expected=dict(selected.systemd_effective))
    except (DeploymentIdentityError, OSError, SystemdError, ValueError) as exc:
        raise AcceptanceError(str(exc)) from exc
    now = LinuxClock()
    document = _empty_common(
        kind="identity-result",
        stage="identity",
        run_id=uuid4().hex,
        identity=selected,
        now_utc=now.utc_ns(),
        now_boot=now.boottime_ns(),
        boot_id=now.boot_id(),
    )
    document.update(
        {
            "observer_status": "COMPLETE",
            "identity": selected.document(),
            "identity_static_verification": {
                "files": files,
                "permissions": permissions,
                "install_contract": install,
                "systemd_effective": effective,
            },
            "result": "PASS_CANDIDATE",
        }
    )
    path, digest = _publish(root, "identity-result.json", document)
    return path, digest, document


def _same_identity(document: Mapping[str, object], identity: DeploymentIdentity) -> bool:
    return (
        document.get("deployment_identity_sha256") == identity.identity_sha256
        and document.get("source_git_sha") == identity.source_git_sha
        and document.get("wheel_sha256") == identity.wheel_sha256
        and document.get("config_sha256") == identity.config_sha256
        and document.get("systemd_unit_sha256") == identity.systemd_unit_sha256
        and document.get("capacity_profile_id") == identity.capacity_profile_id
    )


def read_identity_evidence(
    path: Path, identity: DeploymentIdentity
) -> tuple[dict[str, object], str]:
    document, digest = _read_published(path)
    if document.get("evidence_kind") != "identity-result" or document.get("stage") != "identity":
        raise AcceptanceError("identity evidence kind is invalid")
    if document.get("result") != "PASS_CANDIDATE" or not _same_identity(document, identity):
        raise AcceptanceError("identity evidence is not eligible for this artifact")
    return document, digest


def create_readiness_evidence(
    *,
    identity_evidence_path: Path,
    identity: DeploymentIdentity,
    manager: SystemdManager,
    evaluator: VpsReadinessEvaluator,
    evidence_root: Path,
    data_root: Path,
) -> tuple[Path, str, dict[str, object]]:
    root = _safe_evidence_root(evidence_root, data_root)
    _identity_doc, prior = read_identity_evidence(identity_evidence_path, identity)
    result = evaluator.evaluate()
    now = LinuxClock()
    document = _empty_common(
        kind="readiness-result",
        stage="readiness",
        run_id=uuid4().hex,
        identity=identity,
        now_utc=now.utc_ns(),
        now_boot=now.boottime_ns(),
        boot_id=now.boot_id(),
    )
    document.update(
        {
            "prior_stage_evidence_sha256": prior,
            "readiness": result.public_dict(),
            "observer_status": "COMPLETE",
            "result": "PASS_CANDIDATE" if result.state == "READY" else "INCOMPLETE",
        }
    )
    # Force the installed read-only process-incarnation API to be part of the
    # readiness record even though the existing evaluator owns the readiness
    # definition itself.
    document["systemd_process_incarnation"] = manager.process_incarnation()
    path, digest = _publish(root, "readiness-result.json", document)
    return path, digest, document


def _state_instance(data_root: Path) -> tuple[dict[str, object], str]:
    state = ServiceStateStore(data_root / "state" / "service_state.json").read()
    if state is None:
        raise AcceptanceError("service state is absent")
    instance = state.get("service_instance_id")
    if not isinstance(instance, str) or not instance:
        raise AcceptanceError("service_instance_id is absent")
    return state, instance


def _capacity(
    data_root: Path, now_utc_ns: int, disk_usage: Callable[[Path], Any]
) -> dict[str, object]:
    profile = selected_capacity_profile("vps-production-v1")
    if profile is None:
        raise AcceptanceError("VPS capacity profile is unavailable")
    try:
        usage = disk_usage(data_root)
        total, free = int(usage.total), int(usage.free)
        decision = evaluate_capacity(
            profile=profile,
            scope_id="internal",
            total_bytes=total,
            free_bytes=free,
            hard_reserve_eta={"status": "INSUFFICIENT_DATA"},
            now_utc_ns=now_utc_ns,
        )
    except (AttributeError, TypeError, ValueError) as exc:
        raise AcceptanceError("current disk observation is invalid") from exc
    return {
        "profile_id": profile.profile_id,
        "total_bytes": total,
        "free_bytes": free,
        "state": decision.state.value,
        "recommended_actions": list(decision.recommended_actions),
        "hard_reserve_bytes": profile.hard_reserve_bytes,
    }


@dataclass
class AcceptanceObserver:
    stage: str
    run_id: str
    data_root: Path
    evidence_root: Path
    identity: DeploymentIdentity
    prior_stage_sha256: str
    manager: SystemdManager
    evaluator: VpsReadinessEvaluator
    clock: Clock = field(default_factory=LinuxClock)
    disk_usage: Callable[[Path], Any] = shutil.disk_usage
    identity_verifier: Callable[..., Mapping[str, object]] = verify_identity_files
    t0_utc_ns: int | None = None
    t0_boottime_ns: int | None = None
    t0_boot_id: str | None = None
    frozen_process: dict[str, object] | None = None
    frozen_service_instance_id: str | None = None
    stage_start_sha256: str | None = None
    last_sample_sha256: str | None = None
    last_sample_utc_ns: int | None = None
    last_sample_boottime_ns: int | None = None
    initial_manifest_members: dict[str, str] = field(default_factory=dict)
    ever_blocking_findings: set[str] = field(default_factory=set)

    def __post_init__(self) -> None:
        _validate_stage(self.stage)
        self.evidence_root = _safe_evidence_root(self.evidence_root, self.data_root)

    def _catalog_evidence(self, t0: int) -> tuple[dict[str, object], list[str]]:
        path = self.data_root / "state" / "catalog.sqlite"
        if not path.is_file():
            raise AcceptanceError("Catalog is unavailable")
        with Catalog(path, read_only=True) as catalog:
            integrity = catalog.integrity_check()
            if integrity != ("ok",):
                raise AcceptanceError("Catalog integrity check failed")
            malformed = catalog.malformed_discontinuity_events()
            degraded = catalog.degraded_closed_discontinuity_pairs()
            unclosed = catalog.unclosed_stream_discontinuities_by_stream()
            closed = catalog.closed_stream_discontinuity_intervals_by_stream()
            events = catalog.operational_events()
            terminal = [
                str(event.get("event_type"))
                for event in events
                if _integer(event.get("occurred_at_utc_ns", 0), "event timestamp") >= t0
                and str(event.get("event_type"))
                in {"SERVICE_FAILED", "SERVICE_STOPPED", "CORE_MARKET_TERMINAL_FAILURE"}
            ]
            discontinuity = {
                "malformed_events": malformed,
                "degraded_pairs": degraded,
                "unclosed": {
                    f"{market}:{stream}": items for (market, stream), items in unclosed.items()
                },
                "terminal_events": terminal,
                "timing": [
                    {
                        "gap_id": interval.get("gap_id"),
                        "timing": (
                            "PRE_WINDOW_TO_IN_WINDOW"
                            if _integer(interval.get("started_at_utc_ns"), "gap start") < t0
                            and _integer(interval.get("ended_at_utc_ns"), "gap end") >= t0
                            else (
                                "CROSS_TARGET"
                                if _integer(interval.get("started_at_utc_ns"), "gap start")
                                < t0 + STAGE_DURATION_NS[self.stage]
                                <= _integer(interval.get("ended_at_utc_ns"), "gap end")
                                else "IN_WINDOW_COMPLETE"
                            )
                        ),
                        "open_at_target": False,
                    }
                    for intervals in closed.values()
                    for interval in intervals
                ],
                "open_at_target": any(
                    isinstance(event.get("evidence"), dict)
                    and _integer(
                        cast(dict[str, object], event["evidence"]).get("gap_started_at_utc_ns"),
                        "gap start",
                    )
                    <= t0 + STAGE_DURATION_NS[self.stage]
                    for events in unclosed.values()
                    for event in events
                ),
            }
        findings: list[str] = []
        if malformed or degraded:
            findings.append("malformed_discontinuity_authority")
        if terminal:
            findings.append("terminal_service_or_core_failure")
        if unclosed:
            findings.append("unresolved_discontinuity")
        return {"integrity_check": list(integrity), "discontinuity": discontinuity}, findings

    def _raw_evidence(self) -> tuple[dict[str, object], list[str]]:
        try:
            audit = audit_data_root(self.data_root)
        except (OSError, SealError, CatalogStateError, ValueError, RuntimeError) as exc:
            raise AcceptanceError(f"strict Raw/manifest audit failed: {exc}") from exc
        summary = audit.get("summary")
        if not isinstance(summary, dict):
            raise AcceptanceError("Raw audit summary is malformed")
        findings: list[str] = []
        if _integer(summary.get("unmarked_reconnect", 0), "unmarked reconnect count"):
            findings.append("UNMARKED_RECONNECT")
        if _integer(summary.get("unknown", 0), "unknown reconnect count"):
            findings.append("UNKNOWN_RECONNECT_BOUNDARY")
        catalog_findings = audit.get("catalog_findings", [])
        if not isinstance(catalog_findings, list):
            raise AcceptanceError("Raw audit Catalog findings are malformed")
        findings.extend(str(item) for item in catalog_findings)
        inventory = audit.get("manifest_inventory", {})
        members = inventory.get("members", []) if isinstance(inventory, dict) else []
        member_paths = {
            str(item.get("path"))
            for item in members
            if isinstance(item, dict) and isinstance(item.get("path"), str)
        }
        loss: list[dict[str, object]] = []
        catalog_path = self.data_root / "state" / "catalog.sqlite"
        with Catalog(catalog_path, read_only=True) as catalog:

            def classify_absence(chunk_id: str) -> str:
                _chunk, local, remote = catalog.source_lifecycle_snapshot(chunk_id)
                if local is not None and local.get("state") == "LOCAL_DELETED":
                    return "AUTHORIZED_LOCAL_DELETE"
                if (
                    remote is not None
                    and remote.get("state") == RemoteArchiveState.REMOTE_DELETED.value
                ):
                    return "AUTHORIZED_REMOTE_DELETE"
                if remote is not None or local is not None:
                    return "UNKNOWN"
                return "UNEXPLAINED_ABSENCE"

            absences = inventory.get("artifact_absences", []) if isinstance(inventory, dict) else []
            for absence in absences:
                if not isinstance(absence, dict):
                    continue
                chunk_id = str(absence.get("chunk_id", ""))
                classification = classify_absence(chunk_id)
                loss.append({**absence, "classification": classification})
                if classification == "UNEXPLAINED_ABSENCE":
                    findings.append("unexplained_raw_absence")
                elif classification == "UNKNOWN":
                    findings.append("unknown_raw_absence")
                if absence.get("has_sequence_gap_marker") == "true":
                    findings.append("missing_first_new_raw_proof")
            for row in catalog.chunks_in_states(*tuple(ChunkState)):
                manifest_path = row.get("manifest_path")
                if not isinstance(manifest_path, str) or manifest_path in member_paths:
                    continue
                classification = classify_absence(str(row.get("chunk_id", "")))
                loss.append(
                    {
                        "chunk_id": row.get("chunk_id"),
                        "manifest_path": manifest_path,
                        "classification": classification,
                    }
                )
                if classification == "UNEXPLAINED_ABSENCE":
                    findings.append("unexplained_raw_absence")
                elif classification == "UNKNOWN":
                    findings.append("unknown_raw_absence")
        return {"audit": audit, "inventory": inventory, "raw_loss": loss}, findings

    def _observation(self) -> tuple[dict[str, object], list[str]]:
        if (
            self.t0_utc_ns is None
            or self.t0_boottime_ns is None
            or self.t0_boot_id is None
            or self.frozen_process is None
            or self.frozen_service_instance_id is None
        ):
            raise AcceptanceError("stage T0 is not initialized")
        now_utc = self.clock.utc_ns()
        now_boot = self.clock.boottime_ns()
        boot_id = self.clock.boot_id()
        document = _empty_common(
            kind="stage-sample",
            stage=self.stage,
            run_id=self.run_id,
            identity=self.identity,
            now_utc=now_utc,
            now_boot=now_boot,
            boot_id=boot_id,
        )
        findings: list[str] = []
        if boot_id != self.t0_boot_id:
            findings.append("boot_id_changed")
        wall_floor = (
            self.last_sample_utc_ns if self.last_sample_utc_ns is not None else self.t0_utc_ns
        )
        if wall_floor is not None and now_utc < wall_floor:
            findings.append("unsafe_wall_clock_backward")
        evidence_floor = (
            self.last_sample_boottime_ns
            if self.last_sample_boottime_ns is not None
            else self.t0_boottime_ns
        )
        if evidence_floor is not None and now_boot - evidence_floor > MAX_EVIDENCE_GAP_NS:
            findings.append("acceptance_observation_gap")
        if evidence_floor is not None and now_boot < evidence_floor:
            findings.append("boottime_non_monotonic")
        try:
            current_process = self.manager.process_incarnation()
        except SystemdError as exc:
            findings.append(f"systemd_process_incarnation_invalid:{exc}")
            current_process = {}
        if current_process != self.frozen_process:
            findings.append("process_incarnation_changed")
        try:
            if self.identity_verifier is verify_identity_files:
                self.identity_verifier(
                    self.identity,
                    expected_config_path=Path(self.identity.config_path),
                    expected_profile_id=self.identity.capacity_profile_id,
                    require_root_controlled=True,
                )
            else:
                self.identity_verifier(self.identity)
        except (DeploymentIdentityError, OSError, ValueError) as exc:
            findings.append(f"artifact_identity_changed:{type(exc).__name__}")
        try:
            state, instance = _state_instance(self.data_root)
        except (ServiceStateError, AcceptanceError) as exc:
            findings.append(f"service_state_invalid:{type(exc).__name__}")
            state, instance = {}, ""
        if instance != self.frozen_service_instance_id:
            findings.append("service_instance_id_changed")
        expected_runtime_identity = {
            "identity_sha256": self.identity.identity_sha256,
            "source_git_sha": self.identity.source_git_sha,
            "wheel_sha256": self.identity.wheel_sha256,
            "config_sha256": self.identity.config_sha256,
            "systemd_unit_sha256": self.identity.systemd_unit_sha256,
            "capacity_profile_id": self.identity.capacity_profile_id,
        }
        if state.get("deployment_identity") != expected_runtime_identity:
            findings.append("runtime_deployment_identity_mismatch")
        readiness = self.evaluator.evaluate()
        if readiness.state == "FAILED":
            findings.append("readiness_failed")
        elif readiness.state != "READY":
            findings.append("readiness_not_ready")
        try:
            catalog, catalog_findings = self._catalog_evidence(self.t0_utc_ns)
            raw, raw_findings = self._raw_evidence()
            findings.extend(catalog_findings)
            findings.extend(raw_findings)
        except AcceptanceError as exc:
            findings.append(str(exc))
            catalog, raw = {}, {}
        capacity = _capacity(self.data_root, now_utc, self.disk_usage)
        if (
            _integer(capacity["free_bytes"], "free capacity")
            <= _integer(capacity["hard_reserve_bytes"], "hard reserve")
            or capacity["state"] == VpsCapacityState.HARD_RESERVE.value
        ):
            findings.append("hard_reserve_violation")
        if (
            current_process.get("active_state") != "active"
            or current_process.get("sub_state") != "running"
            or current_process.get("result") != "success"
        ):
            findings.append("service_process_not_running")
        current_inventory = raw.get("inventory", {}) if isinstance(raw, dict) else {}
        current_members = (
            current_inventory.get("members", []) if isinstance(current_inventory, dict) else []
        )
        current_member_map = {
            str(item.get("path")): str(item.get("sha256"))
            for item in current_members
            if isinstance(item, dict) and isinstance(item.get("path"), str)
        }
        if any(
            current_member_map.get(path) != digest
            for path, digest in self.initial_manifest_members.items()
        ):
            findings.append("manifest_byte_mutation_or_loss")
        self.ever_blocking_findings.update(findings)
        all_findings = sorted(self.ever_blocking_findings)
        document.update(
            {
                "stage_start_evidence_sha256": self.stage_start_sha256,
                "previous_sample_sha256": self.last_sample_sha256,
                "systemd_process_incarnation": current_process,
                "service_instance_id": instance,
                "readiness": readiness.public_dict(),
                "catalog_integrity": catalog,
                "discontinuity_summary": catalog.get("discontinuity", {})
                if isinstance(catalog, dict)
                else {},
                "capacity": capacity,
                "reconnect_summary": raw,
                "manifest_inventory": raw.get("inventory", {}) if isinstance(raw, dict) else {},
                "blocking_findings": all_findings,
                "observer_status": "COMPLETE",
                "result": "FAIL"
                if any(
                    item in findings
                    for item in (
                        "process_incarnation_changed",
                        "boot_id_changed",
                        "hard_reserve_violation",
                        "readiness_failed",
                        "UNMARKED_RECONNECT",
                        "terminal_service_or_core_failure",
                        "unexplained_raw_absence",
                        "manifest_byte_mutation_or_loss",
                        "runtime_deployment_identity_mismatch",
                        "service_instance_id_changed",
                        "service_process_not_running",
                    )
                )
                or any(item.startswith("catalog_manifest_disagreement") for item in all_findings)
                else ("INCOMPLETE" if all_findings else "PASS_CANDIDATE"),
            }
        )
        if all_findings == ["readiness_not_ready"]:
            document["result"] = "REVIEW_REQUIRED"
        return document, findings

    def start(self) -> tuple[Path, str, dict[str, object]]:
        if self.t0_boottime_ns is not None:
            raise AcceptanceError("stage T0 is already initialized")
        self.t0_utc_ns = self.clock.utc_ns()
        self.t0_boottime_ns = self.clock.boottime_ns()
        self.t0_boot_id = self.clock.boot_id()
        self.frozen_process = self.manager.process_incarnation()
        _state, self.frozen_service_instance_id = _state_instance(self.data_root)
        self.run_id = self.run_id or uuid4().hex
        document, _ = self._observation()
        document["evidence_kind"] = "stage-start"
        document["stage_start_evidence_sha256"] = None
        path, digest = _publish(self.evidence_root, "stage-start.json", document)
        self.stage_start_sha256 = digest
        inventory = document.get("manifest_inventory")
        if isinstance(inventory, dict):
            members = inventory.get("members", [])
            self.initial_manifest_members = {
                str(item.get("path")): str(item.get("sha256"))
                for item in members
                if isinstance(item, dict) and isinstance(item.get("path"), str)
            }
        # The published T0 record cannot be rewritten.  The hash is bound by
        # all later samples and the final record; its own null field is part of
        # the immutable T0 representation.
        return path, digest, document

    def sample(self) -> tuple[Path, str, dict[str, object]]:
        document, _findings = self._observation()
        ordinal = len(list(self.evidence_root.glob("sample-*.json")))
        filename = f"sample-{ordinal:08d}.json"
        path, digest = _publish(self.evidence_root, filename, document)
        self.last_sample_sha256 = digest
        self.last_sample_utc_ns = _integer(document["observed_at_utc_ns"], "sample UTC timestamp")
        self.last_sample_boottime_ns = _integer(
            document["observed_at_boottime_ns"], "sample BOOTTIME timestamp"
        )
        return path, digest, document

    def finalize(self) -> tuple[Path, str, dict[str, object]]:
        _path, last, sample = self.sample()
        if (
            self.stage_start_sha256 is None
            or self.t0_boottime_ns is None
            or self.t0_boot_id is None
        ):
            raise AcceptanceError("cannot finalize stage without T0")
        elapsed = (
            _integer(sample["observed_at_boottime_ns"], "sample BOOTTIME timestamp")
            - self.t0_boottime_ns
        )
        if sample["boot_id"] != self.t0_boot_id or elapsed < STAGE_DURATION_NS[self.stage]:
            result = "INCOMPLETE"
        else:
            result = str(sample["result"])
        final = dict(sample)
        final.update(
            {
                "evidence_kind": "stage-final",
                "stage_start_evidence_sha256": self.stage_start_sha256,
                "previous_sample_sha256": last,
                "prior_stage_evidence_sha256": self.prior_stage_sha256,
                "elapsed_boottime_ns": elapsed,
                "required_duration_ns": STAGE_DURATION_NS[self.stage],
                "result": result,
                "observer_status": "FINALIZED",
            }
        )
        path, digest = _publish(self.evidence_root, "stage-final.json", final)
        return path, digest, final


def verify_prior_stage(
    path: Path, identity: DeploymentIdentity, stage: str
) -> tuple[dict[str, object], str]:
    document, digest = _read_published(path)
    expected_kind = "readiness-result" if stage == "2h" else "stage-final"
    if document.get("evidence_kind") != expected_kind or document.get("result") != "PASS_CANDIDATE":
        raise AcceptanceError("prior evidence is not an eligible predecessor")
    if not _same_identity(document, identity):
        raise AcceptanceError("prior-stage deployment identity mismatch")
    if expected_kind == "stage-final":
        previous_stage = document.get("stage")
        if not isinstance(previous_stage, str) or STAGE_NAMES.index(
            previous_stage
        ) + 1 != STAGE_NAMES.index(stage):
            raise AcceptanceError("prior-stage order is invalid")
    return document, digest


def resume_observer(
    stage_root: Path,
    *,
    data_root: Path,
    identity: DeploymentIdentity,
    manager: SystemdManager,
    evaluator: VpsReadinessEvaluator,
    clock: Clock | None = None,
    disk_usage: Callable[[Path], Any] = shutil.disk_usage,
) -> AcceptanceObserver:
    selected_clock = LinuxClock() if clock is None else clock
    start, start_sha = _read_published(stage_root / "stage-start.json")
    if (
        start.get("evidence_kind") != "stage-start"
        or start.get("stage") not in STAGE_NAMES
        or not _same_identity(start, identity)
    ):
        raise AcceptanceError("stage-start evidence is invalid")
    stage = str(start["stage"])
    start_process = start.get("systemd_process_incarnation")
    if not isinstance(start_process, dict):
        raise AcceptanceError("stage-start process-incarnation evidence is malformed")
    start_inventory = start.get("manifest_inventory")
    start_members = start_inventory.get("members", []) if isinstance(start_inventory, dict) else []
    if not isinstance(start_members, list):
        raise AcceptanceError("stage-start manifest inventory is malformed")
    start_findings = start.get("blocking_findings")
    if not isinstance(start_findings, list):
        raise AcceptanceError("stage-start findings are malformed")
    observer = AcceptanceObserver(
        stage=stage,
        run_id=str(start["run_id"]),
        data_root=data_root,
        evidence_root=stage_root,
        identity=identity,
        prior_stage_sha256=str(start.get("prior_stage_evidence_sha256") or ""),
        manager=manager,
        evaluator=evaluator,
        clock=selected_clock,
        disk_usage=disk_usage,
        t0_utc_ns=_integer(start["observed_at_utc_ns"], "stage-start UTC timestamp"),
        t0_boottime_ns=_integer(start["observed_at_boottime_ns"], "stage-start BOOTTIME timestamp"),
        t0_boot_id=str(start["boot_id"]),
        frozen_process=start_process,
        frozen_service_instance_id=str(start.get("service_instance_id") or ""),
        stage_start_sha256=start_sha,
        initial_manifest_members={
            str(item.get("path")): str(item.get("sha256"))
            for item in start_members
            if isinstance(item, dict) and isinstance(item.get("path"), str)
        },
        ever_blocking_findings={str(item) for item in start_findings},
    )
    samples = sorted(stage_root.glob("sample-*.json"))
    if samples:
        previous_sha: str | None = None
        sample: dict[str, object] = {}
        sample_sha = ""
        for sample_path in samples:
            sample, sample_sha = _read_published(sample_path)
            if sample.get("previous_sample_sha256") != previous_sha:
                raise AcceptanceError("sample hash chain is invalid")
            if sample.get("run_id") != observer.run_id or not _same_identity(sample, identity):
                raise AcceptanceError("sample identity chain is invalid")
            findings = sample.get("blocking_findings")
            if isinstance(findings, list):
                observer.ever_blocking_findings.update(str(item) for item in findings)
            previous_sha = sample_sha
        observer.last_sample_sha256 = sample_sha
        observer.last_sample_utc_ns = _integer(sample["observed_at_utc_ns"], "sample UTC timestamp")
        observer.last_sample_boottime_ns = _integer(
            sample["observed_at_boottime_ns"], "sample BOOTTIME timestamp"
        )
    if (stage_root / "stage-final.json").exists():
        raise AcceptanceError("stage is already finalized")
    return observer


__all__ = [
    "MAX_EVIDENCE_GAP_NS",
    "SAMPLE_INTERVAL_NS",
    "STAGE_DURATION_NS",
    "STAGE_NAMES",
    "AcceptanceError",
    "AcceptanceObserver",
    "Clock",
    "LinuxClock",
    "canonical_json",
    "create_identity_evidence",
    "create_readiness_evidence",
    "read_identity_evidence",
    "resume_observer",
    "sha256_bytes",
    "verify_prior_stage",
]
