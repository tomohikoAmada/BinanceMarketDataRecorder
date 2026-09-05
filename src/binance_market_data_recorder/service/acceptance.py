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

from ..audit.reconnect_boundaries import (
    UNKNOWN,
    UNMARKED_RECONNECT,
    incremental_audit_data_root,
    strict_manifest_inventory,
    validate_incremental_continuation,
)
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


def _digest(value: object, field: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(char not in _HEX64 for char in value)
    ):
        raise AcceptanceError(f"{field} is not a lowercase SHA-256 digest")
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
    if (
        document.get("result") != "PASS_CANDIDATE"
        or not _same_identity(document, identity)
        or document.get("identity") != identity.document()
    ):
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


def _manifest_members_from_inventory(inventory: object) -> dict[str, str]:
    if not isinstance(inventory, dict):
        raise AcceptanceError("Raw audit manifest inventory is malformed")
    members = inventory.get("members")
    if not isinstance(members, list):
        raise AcceptanceError("Raw audit manifest inventory is malformed")
    result: dict[str, str] = {}
    for member in members:
        if (
            not isinstance(member, dict)
            or not isinstance(member.get("path"), str)
            or not isinstance(member.get("chunk_id"), str)
            or member["path"] in result
        ):
            raise AcceptanceError("Raw audit manifest inventory member is malformed")
        result[str(member["path"])] = _digest(
            member.get("sha256"), "Raw manifest inventory digest"
        )
    return result


def _manifest_members_from_evidence(value: object) -> dict[str, str]:
    if not isinstance(value, dict):
        raise AcceptanceError("stage baseline manifest membership is malformed")
    result: dict[str, str] = {}
    for path, digest in value.items():
        if not isinstance(path, str) or path in result:
            raise AcceptanceError("stage baseline manifest membership is malformed")
        result[path] = _digest(digest, "stage baseline manifest digest")
    return result


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
    reconnect_continuation: dict[str, object] | None = None
    t0_manifest_members: dict[str, str] | None = None
    next_sample_ordinal: int = 0
    ever_blocking_findings: set[str] = field(default_factory=set)

    def __post_init__(self) -> None:
        _validate_stage(self.stage)
        _digest(self.prior_stage_sha256, "prior-stage evidence digest")
        self.evidence_root = _safe_evidence_root(self.evidence_root, self.data_root)

    def _freeze_manifest_membership(self) -> dict[str, str]:
        try:
            _chunks, inventory = strict_manifest_inventory(self.data_root, deep_scan=False)
        except (OSError, SealError, CatalogStateError, ValueError, RuntimeError) as exc:
            raise AcceptanceError(f"strict Raw/manifest baseline inventory failed: {exc}") from exc
        return _manifest_members_from_inventory(inventory)

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
            baseline_history: list[dict[str, object]] = []
            current_intervals: list[dict[str, object]] = []
            for intervals in closed.values():
                for interval in intervals:
                    started_at = _integer(interval.get("started_at_utc_ns"), "gap start")
                    ended_at = _integer(interval.get("ended_at_utc_ns"), "gap end")
                    item = {
                        "market": interval.get("market"),
                        "symbol": interval.get("symbol"),
                        "stream": interval.get("stream"),
                        "gap_id": interval.get("gap_id"),
                        "started_at_utc_ns": started_at,
                        "ended_at_utc_ns": ended_at,
                    }
                    if ended_at < t0:
                        baseline_history.append(item)
                    else:
                        item["timing"] = (
                            "CROSSES_T0" if started_at < t0 <= ended_at else "CURRENT_STAGE"
                        )
                        current_intervals.append(item)
            open_intervals: list[dict[str, object]] = []
            for events_for_stream in unclosed.values():
                for event in events_for_stream:
                    evidence = event.get("evidence")
                    if not isinstance(evidence, dict):
                        continue
                    started_at = _integer(evidence.get("gap_started_at_utc_ns"), "gap start")
                    open_intervals.append(
                        {
                            "market": evidence.get("market"),
                            "symbol": evidence.get("symbol"),
                            "stream": evidence.get("stream"),
                            "gap_id": evidence.get("gap_id"),
                            "started_at_utc_ns": started_at,
                            "timing": "OPEN_AT_T0" if started_at <= t0 else "OPENED_IN_STAGE",
                        }
                    )
            discontinuity = {
                "malformed_events": malformed,
                "degraded_pairs": degraded,
                "unclosed": {
                    f"{market}:{symbol}:{stream}": items
                    for (market, symbol, stream), items in unclosed.items()
                },
                "terminal_events": terminal,
                "baseline_history": baseline_history,
                "current_intervals": current_intervals,
                "open_intervals": open_intervals,
                "open_at_t0": any(item["timing"] == "OPEN_AT_T0" for item in open_intervals),
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
            audit = incremental_audit_data_root(
                self.data_root,
                continuation=self.reconnect_continuation,
            )
        except (OSError, SealError, CatalogStateError, ValueError, RuntimeError) as exc:
            raise AcceptanceError(f"strict Raw/manifest audit failed: {exc}") from exc
        continuation = audit.get("continuation")
        if not isinstance(continuation, dict):
            raise AcceptanceError("Raw audit continuation is malformed")
        continuation_members = continuation.get("manifest_members")
        if not isinstance(continuation_members, dict):
            raise AcceptanceError("Raw audit manifest membership is malformed")
        current_members = {
            str(path): _digest(digest, "Raw manifest member digest")
            for path, digest in continuation_members.items()
            if isinstance(path, str)
        }
        if len(current_members) != len(continuation_members):
            raise AcceptanceError("Raw audit manifest membership is malformed")
        if self.t0_manifest_members is None:
            raise AcceptanceError("baseline manifest membership is not frozen")
        self.reconnect_continuation = continuation
        summary = audit.get("summary")
        if not isinstance(summary, dict):
            raise AcceptanceError("Raw audit summary is malformed")
        findings: list[str] = []
        transitions = [
            item
            for stream_item in cast(list[object], audit.get("streams", []))
            if isinstance(stream_item, dict)
            for item in cast(list[object], stream_item.get("transitions", []))
            if isinstance(item, dict)
        ]
        inventory = audit.get("manifest_inventory")
        inventory_members = _manifest_members_from_inventory(inventory)
        members_by_chunk: dict[str, tuple[str, str]] = {}
        raw_inventory_members = inventory.get("members") if isinstance(inventory, dict) else None
        if not isinstance(raw_inventory_members, list):
            raise AcceptanceError("Raw audit manifest inventory is malformed")
        for member in raw_inventory_members:
            if not isinstance(member, dict) or not isinstance(member.get("chunk_id"), str):
                raise AcceptanceError("Raw audit manifest inventory member is malformed")
            chunk_id = str(member["chunk_id"])
            if chunk_id in members_by_chunk:
                raise AcceptanceError("Raw audit manifest inventory has duplicate chunk IDs")
            members_by_chunk[chunk_id] = (
                str(member["path"]),
                inventory_members[str(member["path"])],
            )

        def baseline_bound(transition: dict[str, object]) -> bool:
            required_chunk_ids: list[str] = []
            for field_name in ("old_chunk_id", "new_chunk_id"):
                chunk_id = transition.get(field_name)
                if not isinstance(chunk_id, str):
                    return False
                if chunk_id not in required_chunk_ids:
                    required_chunk_ids.append(chunk_id)
            intervening = transition.get("intervening_manifests")
            if intervening is not None:
                if not isinstance(intervening, list):
                    return False
                for manifest in intervening:
                    if not isinstance(manifest, dict) or not isinstance(
                        manifest.get("chunk_id"), str
                    ):
                        return False
                    chunk_id = str(manifest["chunk_id"])
                    if chunk_id not in required_chunk_ids:
                        required_chunk_ids.append(chunk_id)
            for chunk_id in required_chunk_ids:
                member = members_by_chunk.get(chunk_id)
                if member is None:
                    return False
                path, digest = member
                if self.t0_manifest_members is None or self.t0_manifest_members.get(path) != digest:
                    return False
            return (
                _integer(transition.get("occurred_at_utc_ns"), "boundary timestamp")
                < cast(int, self.t0_utc_ns)
            )

        baseline_history = [item for item in transitions if baseline_bound(item)]
        current_transitions = [item for item in transitions if item not in baseline_history]
        if any(item.get("kind") == UNMARKED_RECONNECT for item in current_transitions):
            findings.append("UNMARKED_RECONNECT")
        if any(item.get("kind") == UNKNOWN for item in current_transitions):
            findings.append("UNKNOWN_RECONNECT_BOUNDARY")
        integrity_findings = audit.get("integrity_findings")
        if not isinstance(integrity_findings, list):
            raise AcceptanceError("Raw audit integrity findings are malformed")
        if integrity_findings:
            findings.append("manifest_byte_mutation_or_loss")
        catalog_findings = audit.get("catalog_findings")
        if not isinstance(catalog_findings, list):
            raise AcceptanceError("Raw audit Catalog findings are malformed")
        findings.extend(str(item) for item in catalog_findings)
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
        return {
            "audit": audit,
            "inventory": inventory,
            "raw_loss": loss,
            "baseline_history": baseline_history,
            "current_transitions": current_transitions,
            "continuation": continuation,
            "baseline_manifest_members": dict(self.t0_manifest_members or {}),
        }, findings

    def _observation(
        self,
        *,
        observed_at_utc_ns: int | None = None,
        observed_at_boottime_ns: int | None = None,
        observed_boot_id: str | None = None,
    ) -> tuple[dict[str, object], list[str]]:
        if (
            self.t0_utc_ns is None
            or self.t0_boottime_ns is None
            or self.t0_boot_id is None
            or self.frozen_process is None
            or self.frozen_service_instance_id is None
        ):
            raise AcceptanceError("stage T0 is not initialized")
        now_utc = self.clock.utc_ns() if observed_at_utc_ns is None else observed_at_utc_ns
        now_boot = (
            self.clock.boottime_ns()
            if observed_at_boottime_ns is None
            else observed_at_boottime_ns
        )
        boot_id = self.clock.boot_id() if observed_boot_id is None else observed_boot_id
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
        self.ever_blocking_findings.update(findings)
        all_findings = sorted(self.ever_blocking_findings)
        soft_findings = {
            "acceptance_observation_gap",
            "readiness_not_ready",
            "unsafe_wall_clock_backward",
        }
        fatal = any(item not in soft_findings for item in all_findings)
        document.update(
            {
                "prior_stage_evidence_sha256": self.prior_stage_sha256,
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
                if fatal
                else ("INCOMPLETE" if all_findings else "PASS_CANDIDATE"),
            }
        )
        if all_findings == ["readiness_not_ready"]:
            document["result"] = "REVIEW_REQUIRED"
        return document, findings

    def start(self) -> tuple[Path, str, dict[str, object]]:
        if self.t0_boottime_ns is not None:
            raise AcceptanceError("stage T0 is already initialized")
        self.t0_manifest_members = self._freeze_manifest_membership()
        t0_utc_ns = self.clock.utc_ns()
        t0_boottime_ns = self.clock.boottime_ns()
        t0_boot_id = self.clock.boot_id()
        self.t0_utc_ns = t0_utc_ns
        self.t0_boottime_ns = t0_boottime_ns
        self.t0_boot_id = t0_boot_id
        self.frozen_process = self.manager.process_incarnation()
        _state, self.frozen_service_instance_id = _state_instance(self.data_root)
        self.run_id = self.run_id or uuid4().hex
        document, _ = self._observation(
            observed_at_utc_ns=t0_utc_ns,
            observed_at_boottime_ns=t0_boottime_ns,
            observed_boot_id=t0_boot_id,
        )
        document["evidence_kind"] = "stage-start"
        document["stage_start_evidence_sha256"] = None
        path, digest = _publish(self.evidence_root, "stage-start.json", document)
        self.stage_start_sha256 = digest
        # The published T0 record cannot be rewritten.  The hash is bound by
        # all later samples and the final record; its own null field is part of
        # the immutable T0 representation.
        return path, digest, document

    def sample(self) -> tuple[Path, str, dict[str, object]]:
        document, _findings = self._observation()
        filename = f"sample-{self.next_sample_ordinal:08d}.json"
        path, digest = _publish(self.evidence_root, filename, document)
        self.next_sample_ordinal += 1
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


def _chain_identity(
    document: Mapping[str, object],
    *,
    identity: DeploymentIdentity,
    stage: str,
    run_id: str,
) -> None:
    if (
        document.get("stage") != stage
        or document.get("run_id") != run_id
        or not _same_identity(document, identity)
    ):
        raise AcceptanceError("stage evidence identity chain is invalid")


def _continuation_from(document: Mapping[str, object]) -> dict[str, object]:
    reconnect = document.get("reconnect_summary")
    continuation = reconnect.get("continuation") if isinstance(reconnect, dict) else None
    if not isinstance(continuation, dict):
        raise AcceptanceError("reconnect continuation evidence is absent")
    try:
        validate_incremental_continuation(continuation)
    except SealError as exc:
        raise AcceptanceError("reconnect continuation evidence is invalid") from exc
    members = cast(dict[str, str], continuation["manifest_members"])
    inventory = document.get("manifest_inventory")
    inventory_members = inventory.get("members") if isinstance(inventory, dict) else None
    if not isinstance(inventory_members, list):
        raise AcceptanceError("manifest inventory evidence is malformed")
    observed_members: dict[str, str] = {}
    for item in inventory_members:
        if (
            not isinstance(item, dict)
            or not isinstance(item.get("path"), str)
            or not isinstance(item.get("sha256"), str)
            or not isinstance(item.get("chunk_id"), str)
            or str(item["path"]) in observed_members
        ):
            raise AcceptanceError("manifest inventory member is malformed")
        observed_members[str(item["path"])] = _digest(
            item["sha256"], "manifest inventory digest"
        )
    if observed_members != members:
        raise AcceptanceError("reconnect continuation does not bind manifest inventory")
    return continuation


def _sample_chain(
    stage_root: Path,
    *,
    start: Mapping[str, object],
    start_sha: str,
    identity: DeploymentIdentity,
    require_eligible: bool,
) -> tuple[list[dict[str, object]], str | None, dict[str, object]]:
    stage = str(start["stage"])
    run_id = str(start["run_id"])
    expected_previous: str | None = None
    samples: list[dict[str, object]] = []
    last_continuation = _continuation_from(start)
    previous_members = cast(dict[str, str], last_continuation["manifest_members"])
    baseline_members = _manifest_members_from_evidence(
        cast(dict[str, object], start.get("reconnect_summary", {})).get(
            "baseline_manifest_members"
        )
    )
    start_findings = start.get("blocking_findings")
    if not isinstance(start_findings, list) or any(
        not isinstance(item, str) for item in start_findings
    ):
        raise AcceptanceError("stage-start blocking findings are malformed")
    known_findings = set(start_findings)
    previous_boottime = _integer(
        start.get("observed_at_boottime_ns"), "stage-start BOOTTIME timestamp"
    )
    paths = sorted(stage_root.glob("sample-*.json"))
    for ordinal, sample_path in enumerate(paths):
        if sample_path.name != f"sample-{ordinal:08d}.json":
            raise AcceptanceError("sample ordinals are missing, duplicated, or malformed")
        sample, sample_sha = _read_published(sample_path)
        if sample.get("evidence_kind") != "stage-sample":
            raise AcceptanceError("sample evidence kind is invalid")
        _chain_identity(sample, identity=identity, stage=stage, run_id=run_id)
        if sample.get("stage_start_evidence_sha256") != start_sha:
            raise AcceptanceError("sample stage-start digest is invalid")
        if sample.get("prior_stage_evidence_sha256") != start.get(
            "prior_stage_evidence_sha256"
        ):
            raise AcceptanceError("sample predecessor digest is invalid")
        if sample.get("previous_sample_sha256") != expected_previous:
            raise AcceptanceError("sample hash chain is invalid")
        sample_reconnect = sample.get("reconnect_summary")
        sample_baseline = _manifest_members_from_evidence(
            cast(dict[str, object], sample_reconnect).get("baseline_manifest_members")
            if isinstance(sample_reconnect, dict)
            else None
        )
        if sample_baseline != baseline_members:
            raise AcceptanceError("sample baseline manifest membership changed")
        if (
            sample.get("boot_id") != start.get("boot_id")
            or sample.get("systemd_process_incarnation")
            != start.get("systemd_process_incarnation")
            or sample.get("service_instance_id") != start.get("service_instance_id")
        ):
            raise AcceptanceError("sample process/service authority is mixed")
        boottime = _integer(
            sample.get("observed_at_boottime_ns"), "sample BOOTTIME timestamp"
        )
        if boottime < previous_boottime:
            raise AcceptanceError("sample BOOTTIME chain is non-monotonic")
        if boottime - previous_boottime > MAX_EVIDENCE_GAP_NS:
            raise AcceptanceError("sample observation chain has an excessive gap")
        previous_boottime = boottime
        findings = sample.get("blocking_findings")
        if not isinstance(findings, list) or any(
            not isinstance(item, str) for item in findings
        ):
            raise AcceptanceError("sample blocking findings are malformed")
        sample_findings = set(findings)
        if not known_findings <= sample_findings:
            raise AcceptanceError("sample blocking findings are not monotonic")
        known_findings = sample_findings
        if require_eligible and findings:
            raise AcceptanceError("completed stage contains ineligible sample findings")
        if sample.get("observer_status") != "COMPLETE":
            raise AcceptanceError("sample observer status is invalid")
        if require_eligible and sample.get("result") != "PASS_CANDIDATE":
            raise AcceptanceError("completed stage contains an ineligible sample result")
        last_continuation = _continuation_from(sample)
        current_members = cast(dict[str, str], last_continuation["manifest_members"])
        lost_manifest_authority = any(
            current_members.get(path) != digest
            for path, digest in previous_members.items()
        )
        if (
            lost_manifest_authority
            and "manifest_byte_mutation_or_loss" not in sample_findings
        ):
            raise AcceptanceError("sample continuation lost manifest authority")
        previous_members = current_members
        samples.append(sample)
        expected_previous = sample_sha
    return samples, expected_previous, last_continuation


def verify_completed_stage(
    stage_root: Path,
    identity: DeploymentIdentity,
    *,
    expected_stage: str | None = None,
) -> tuple[dict[str, object], str]:
    """Verify the complete immutable authority for one duration stage."""

    start, start_sha = _read_published(stage_root / "stage-start.json")
    stage = start.get("stage")
    run_id = start.get("run_id")
    if (
        start.get("evidence_kind") != "stage-start"
        or not isinstance(stage, str)
        or stage not in STAGE_NAMES
        or (expected_stage is not None and stage != expected_stage)
        or not isinstance(run_id, str)
        or not run_id
    ):
        raise AcceptanceError("stage-start evidence is invalid")
    _chain_identity(start, identity=identity, stage=stage, run_id=run_id)
    prior_digest = _digest(
        start.get("prior_stage_evidence_sha256"), "stage-start predecessor digest"
    )
    if (
        start.get("stage_start_evidence_sha256") is not None
        or start.get("previous_sample_sha256") is not None
        or start.get("result") != "PASS_CANDIDATE"
        or start.get("blocking_findings") != []
        or not isinstance(start.get("systemd_process_incarnation"), dict)
        or not isinstance(start.get("service_instance_id"), str)
        or not start.get("service_instance_id")
    ):
        raise AcceptanceError("stage-start is not eligible")
    _continuation_from(start)
    samples, last_sample_sha, _continuation = _sample_chain(
        stage_root,
        start=start,
        start_sha=start_sha,
        identity=identity,
        require_eligible=True,
    )
    if not samples or last_sample_sha is None:
        raise AcceptanceError("completed stage has no canonical samples")
    final_path = stage_root / "stage-final.json"
    final, final_sha = _read_published(final_path)
    if final.get("evidence_kind") != "stage-final":
        raise AcceptanceError("stage-final evidence kind is invalid")
    _chain_identity(final, identity=identity, stage=stage, run_id=run_id)
    if (
        final.get("stage_start_evidence_sha256") != start_sha
        or final.get("previous_sample_sha256") != last_sample_sha
        or final.get("prior_stage_evidence_sha256") != prior_digest
        or final.get("boot_id") != start.get("boot_id")
        or final.get("systemd_process_incarnation")
        != start.get("systemd_process_incarnation")
        or final.get("service_instance_id") != start.get("service_instance_id")
        or final.get("blocking_findings") != []
        or final.get("result") != "PASS_CANDIDATE"
        or final.get("observer_status") != "FINALIZED"
    ):
        raise AcceptanceError("stage-final is not an eligible chain terminus")
    required = _integer(final.get("required_duration_ns"), "required duration")
    elapsed = _integer(final.get("elapsed_boottime_ns"), "elapsed duration")
    expected_required = STAGE_DURATION_NS[stage]
    expected_elapsed = _integer(
        final.get("observed_at_boottime_ns"), "final BOOTTIME timestamp"
    ) - _integer(start.get("observed_at_boottime_ns"), "stage-start BOOTTIME timestamp")
    if required != expected_required or elapsed != expected_elapsed or elapsed < required:
        raise AcceptanceError("stage duration authority is invalid")
    last_sample = samples[-1]
    expected_final = dict(last_sample)
    expected_final.update(
        {
            "evidence_kind": "stage-final",
            "stage_start_evidence_sha256": start_sha,
            "previous_sample_sha256": last_sample_sha,
            "prior_stage_evidence_sha256": prior_digest,
            "elapsed_boottime_ns": elapsed,
            "required_duration_ns": required,
            "result": "PASS_CANDIDATE",
            "observer_status": "FINALIZED",
        }
    )
    if final != expected_final:
        raise AcceptanceError("stage-final does not reference the actual last sample")
    _resolve_stage_predecessor(
        stage_root,
        identity=identity,
        stage=stage,
        prior_digest=prior_digest,
    )
    return final, final_sha


def _verify_readiness_predecessor(
    path: Path,
    identity: DeploymentIdentity,
    *,
    expected_digest: str | None = None,
) -> tuple[dict[str, object], str]:
    if path.name != "readiness-result.json":
        raise AcceptanceError("readiness predecessor must be canonical readiness-result.json")
    document, digest = _read_published(path)
    if expected_digest is not None and digest != expected_digest:
        raise AcceptanceError("readiness predecessor digest does not match")
    readiness = document.get("readiness")
    reasons = readiness.get("reasons") if isinstance(readiness, dict) else None
    if (
        document.get("evidence_kind") != "readiness-result"
        or document.get("stage") != "readiness"
        or document.get("result") != "PASS_CANDIDATE"
        or not _same_identity(document, identity)
        or not isinstance(readiness, dict)
        or readiness.get("schema_version") != "deployment-readiness.v1"
        or readiness.get("state") != "READY"
        or reasons != []
        or not isinstance(readiness.get("evidence"), dict)
    ):
        raise AcceptanceError("readiness is not an actual READY predecessor")
    prior_identity_digest = _digest(
        document.get("prior_stage_evidence_sha256"),
        "readiness identity predecessor digest",
    )
    identity_path = path.parent / "identity-result.json"
    _identity_document, identity_digest = read_identity_evidence(identity_path, identity)
    if identity_digest != prior_identity_digest:
        raise AcceptanceError("readiness identity predecessor digest does not match")
    return document, digest


def _resolve_stage_predecessor(
    stage_root: Path,
    *,
    identity: DeploymentIdentity,
    stage: str,
    prior_digest: str,
) -> tuple[dict[str, object], str]:
    """Resolve the actual predecessor beneath this operator evidence root."""

    if stage == "2h":
        return _verify_readiness_predecessor(
            stage_root.parent / "readiness-result.json",
            identity,
            expected_digest=prior_digest,
        )

    previous_stage = STAGE_NAMES[STAGE_NAMES.index(stage) - 1]
    evidence_root = stage_root.parent
    candidates = sorted(evidence_root.glob(f"{previous_stage}-*/stage-final.json"))
    matches: list[tuple[dict[str, object], str]] = []
    for candidate in candidates:
        _document, digest = _read_published(candidate)
        if digest != prior_digest:
            continue
        verified, verified_digest = verify_completed_stage(
            candidate.parent,
            identity,
            expected_stage=previous_stage,
        )
        if verified_digest != digest:
            raise AcceptanceError("duration predecessor digest changed during verification")
        matches.append((verified, verified_digest))
    if len(matches) != 1:
        raise AcceptanceError("duration predecessor authority is absent or ambiguous")
    return matches[0]


def verify_prior_stage(
    path: Path, identity: DeploymentIdentity, stage: str
) -> tuple[dict[str, object], str]:
    _validate_stage(stage)
    if stage == "2h":
        return _verify_readiness_predecessor(path, identity)
    previous_stage = STAGE_NAMES[STAGE_NAMES.index(stage) - 1]
    if path.name != "stage-final.json":
        raise AcceptanceError("duration predecessor must be canonical stage-final.json")
    return verify_completed_stage(path.parent, identity, expected_stage=previous_stage)


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
    if (stage_root / "stage-final.json").exists():
        raise AcceptanceError("stage is already finalized")
    start, start_sha = _read_published(stage_root / "stage-start.json")
    if (
        start.get("evidence_kind") != "stage-start"
        or start.get("stage") not in STAGE_NAMES
        or not _same_identity(start, identity)
        or start.get("stage_start_evidence_sha256") is not None
        or start.get("previous_sample_sha256") is not None
        or start.get("result") != "PASS_CANDIDATE"
    ):
        raise AcceptanceError("stage-start evidence is invalid")
    stage = str(start["stage"])
    run_id = start.get("run_id")
    if not isinstance(run_id, str) or not run_id:
        raise AcceptanceError("stage-start run_id is invalid")
    _chain_identity(start, identity=identity, stage=stage, run_id=run_id)
    prior_digest = _digest(
        start.get("prior_stage_evidence_sha256"), "stage-start predecessor digest"
    )
    baseline_members = _manifest_members_from_evidence(
        cast(dict[str, object], start.get("reconnect_summary", {})).get(
            "baseline_manifest_members"
        )
    )
    start_process = start.get("systemd_process_incarnation")
    if not isinstance(start_process, dict):
        raise AcceptanceError("stage-start process-incarnation evidence is malformed")
    start_findings = start.get("blocking_findings")
    if not isinstance(start_findings, list):
        raise AcceptanceError("stage-start findings are malformed")
    samples, last_sample_sha, continuation = _sample_chain(
        stage_root,
        start=start,
        start_sha=start_sha,
        identity=identity,
        require_eligible=False,
    )
    if selected_clock.boot_id() != start.get("boot_id"):
        raise AcceptanceError("resume boot identity changed")
    if selected_clock.boottime_ns() < _integer(
        start.get("observed_at_boottime_ns"), "stage-start BOOTTIME timestamp"
    ):
        raise AcceptanceError("resume BOOTTIME is before stage T0")
    try:
        if manager.process_incarnation() != start_process:
            raise AcceptanceError("resume process incarnation changed")
    except SystemdError as exc:
        raise AcceptanceError("resume process incarnation is unavailable") from exc
    _current_state, current_instance = _state_instance(data_root)
    if current_instance != start.get("service_instance_id"):
        raise AcceptanceError("resume service instance changed")
    observer = AcceptanceObserver(
        stage=stage,
        run_id=run_id,
        data_root=data_root,
        evidence_root=stage_root,
        identity=identity,
        prior_stage_sha256=prior_digest,
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
        t0_manifest_members=baseline_members,
        ever_blocking_findings={str(item) for item in start_findings},
        reconnect_continuation=continuation,
        next_sample_ordinal=len(samples),
    )
    if samples:
        sample = samples[-1]
        for existing_sample in samples:
            findings = existing_sample.get("blocking_findings")
            if isinstance(findings, list):
                observer.ever_blocking_findings.update(str(item) for item in findings)
        observer.last_sample_sha256 = last_sample_sha
        observer.last_sample_utc_ns = _integer(sample["observed_at_utc_ns"], "sample UTC timestamp")
        observer.last_sample_boottime_ns = _integer(
            sample["observed_at_boottime_ns"], "sample BOOTTIME timestamp"
        )
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
    "verify_completed_stage",
    "verify_prior_stage",
]
