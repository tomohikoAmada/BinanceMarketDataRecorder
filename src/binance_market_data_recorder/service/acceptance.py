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
    INCREMENTAL_SCHEMA_VERSION,
    UNKNOWN,
    UNMARKED_RECONNECT,
    ArchiveRootResolver,
    incremental_audit_data_root,
    strict_manifest_inventory,
    validate_incremental_continuation,
)
from ..spool.seal import SealError
from ..storage.capacity import VpsCapacityState, evaluate_capacity, selected_capacity_profile
from ..storage.catalog import Catalog, CatalogStateError
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

# ``SCHEMA_VERSION`` is retained as the V3 authority for callers that were
# written before the V4 implementation.  New production evidence is created
# explicitly with ``V4_SCHEMA_VERSION``; moving this alias would silently
# reinterpret historical V3 records.
SCHEMA_VERSION = "m22.9-acceptance-evidence.v3"
V3_SCHEMA_VERSION = SCHEMA_VERSION
PREVIOUS_SCHEMA_VERSION = "m22.9-acceptance-evidence.v2"
LEGACY_SCHEMA_VERSION = "m22.9-acceptance-evidence.v1"
V4_SCHEMA_VERSION = "m22.9-acceptance-evidence.v4"
CURRENT_SCHEMA_VERSION = V4_SCHEMA_VERSION
V4_DEADLINE_NS = 900 * 1_000_000_000
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

_V2_STAGE_CORE_FIELDS = frozenset(
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
        "blocking_findings",
        "new_finding_details",
        "observer_status",
        "result",
    }
)
_V2_STAGE_START_FIELDS = _V2_STAGE_CORE_FIELDS | frozenset(
    {
        "manifest_baseline",
        "manifest_transition",
        "raw_absence_baseline",
        "reconnect_baseline",
        "reconnect_transition",
        "catalog_baseline",
        "catalog_transition",
    }
)
_V2_STAGE_SAMPLE_FIELDS = _V2_STAGE_CORE_FIELDS | frozenset(
    {
        "sample_ordinal",
        "manifest_transition",
        "raw_absence_transition",
        "reconnect_transition",
        "catalog_transition",
    }
)
_V2_STAGE_FINAL_FIELDS = _V2_STAGE_CORE_FIELDS | frozenset(
    {
        "elapsed_boottime_ns",
        "required_duration_ns",
        "last_sample_ordinal",
        "last_sample_sha256",
        "eligible_for_next_stage",
    }
)

# V2 and V3 deliberately have the same bounded physical field layout.  They
# remain separate authorities because their acceptance semantics differ.
_V3_STAGE_CORE_FIELDS = frozenset(_V2_STAGE_CORE_FIELDS)
_V3_STAGE_START_FIELDS = frozenset(_V2_STAGE_START_FIELDS)
_V3_STAGE_SAMPLE_FIELDS = frozenset(_V2_STAGE_SAMPLE_FIELDS)
_V3_STAGE_FINAL_FIELDS = frozenset(_V2_STAGE_FINAL_FIELDS)

# V4 keeps the bounded V3 physical layout and adds one compact, independently
# reconstructible readiness-episode projection.  The verifier does not trust
# this projection: it rebuilds it from the immutable readiness and BOOTTIME
# fields in every sample and compares the result with the published value.
_V4_EPISODE_FIELD = "readiness_recovery_episode"
_V4_COMMON_FIELDS = _COMMON_FIELDS | frozenset({_V4_EPISODE_FIELD})
_V4_STAGE_CORE_FIELDS = _V3_STAGE_CORE_FIELDS | frozenset({_V4_EPISODE_FIELD})
_V4_STAGE_START_FIELDS = _V3_STAGE_START_FIELDS | frozenset({_V4_EPISODE_FIELD})
_V4_STAGE_SAMPLE_FIELDS = _V3_STAGE_SAMPLE_FIELDS | frozenset({_V4_EPISODE_FIELD})
_V4_STAGE_FINAL_FIELDS = _V3_STAGE_FINAL_FIELDS | frozenset({_V4_EPISODE_FIELD})
_V4_READINESS_BLOCKERS = frozenset(
    {
        "readiness_failed",
        "readiness_not_ready",
        "readiness_recovery_deadline_exceeded",
    }
)
_V4_SOFT_FINDINGS = frozenset(
    {"acceptance_observation_gap", "unsafe_wall_clock_backward"}
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
    schema_version = value.get("schema_version")
    if schema_version not in {
        LEGACY_SCHEMA_VERSION,
        PREVIOUS_SCHEMA_VERSION,
        SCHEMA_VERSION,
        V4_SCHEMA_VERSION,
    }:
        raise AcceptanceError("unsupported acceptance evidence schema")
    if schema_version == LEGACY_SCHEMA_VERSION:
        if not set(value) >= _COMMON_FIELDS or set(value) - (_COMMON_FIELDS | _EXTRA_FIELDS):
            raise AcceptanceError("acceptance evidence fields are not exact")
    else:
        kind = value.get("evidence_kind")
        if kind in {"identity-result", "readiness-result"}:
            required = _V4_COMMON_FIELDS if schema_version == V4_SCHEMA_VERSION else _COMMON_FIELDS
            allowed = required | _EXTRA_FIELDS
            if not set(value) >= required or set(value) - allowed:
                raise AcceptanceError("acceptance evidence fields are not exact")
        elif kind == "stage-start":
            expected = (
                _V2_STAGE_START_FIELDS
                if schema_version == PREVIOUS_SCHEMA_VERSION
                else (
                    _V4_STAGE_START_FIELDS
                    if schema_version == V4_SCHEMA_VERSION
                    else _V3_STAGE_START_FIELDS
                )
            )
            if set(value) != expected:
                raise AcceptanceError("stage-start evidence fields are not exact")
        elif kind == "stage-sample":
            expected = (
                _V2_STAGE_SAMPLE_FIELDS
                if schema_version == PREVIOUS_SCHEMA_VERSION
                else (
                    _V4_STAGE_SAMPLE_FIELDS
                    if schema_version == V4_SCHEMA_VERSION
                    else _V3_STAGE_SAMPLE_FIELDS
                )
            )
            if set(value) != expected:
                raise AcceptanceError("stage-sample evidence fields are not exact")
        elif kind == "stage-final":
            expected = (
                _V2_STAGE_FINAL_FIELDS
                if schema_version == PREVIOUS_SCHEMA_VERSION
                else (
                    _V4_STAGE_FINAL_FIELDS
                    if schema_version == V4_SCHEMA_VERSION
                    else _V3_STAGE_FINAL_FIELDS
                )
            )
            if set(value) != expected:
                raise AcceptanceError("stage-final evidence fields are not exact")
        else:
            raise AcceptanceError("acceptance evidence kind is invalid")
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
    schema_version: str = SCHEMA_VERSION,
) -> dict[str, object]:
    document: dict[str, object] = {
        "schema_version": schema_version,
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
    if schema_version == V4_SCHEMA_VERSION:
        document[_V4_EPISODE_FIELD] = _empty_v4_episode()
    return document


def _empty_v2_stage(
    *,
    kind: str,
    stage: str,
    run_id: str,
    identity: DeploymentIdentity,
    now_utc: int,
    now_boot: int,
    boot_id: str,
) -> dict[str, object]:
    """Return only the bounded common fields used by a historical v2 stage."""

    return {
        "schema_version": PREVIOUS_SCHEMA_VERSION,
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
        "blocking_findings": [],
        "new_finding_details": {},
        "observer_status": "OBSERVED",
        "result": "INCOMPLETE",
    }


def _empty_v3_stage(
    *,
    kind: str,
    stage: str,
    run_id: str,
    identity: DeploymentIdentity,
    now_utc: int,
    now_boot: int,
    boot_id: str,
) -> dict[str, object]:
    """Return only the bounded common fields used by a v3 stage record."""

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
        "blocking_findings": [],
        "new_finding_details": {},
        "observer_status": "OBSERVED",
        "result": "INCOMPLETE",
    }


def _empty_v4_episode() -> dict[str, object]:
    """Return the canonical empty V4 bounded-recovery projection."""

    return {
        "state": "NONE",
        "episode_start_utc_ns": None,
        "episode_start_boottime_ns": None,
        "episode_close_utc_ns": None,
        "episode_close_boottime_ns": None,
        "last_product_key": None,
        "last_reason": None,
        "age_ns": None,
        "deadline_ns": V4_DEADLINE_NS,
    }


def _empty_v4_stage(
    *,
    kind: str,
    stage: str,
    run_id: str,
    identity: DeploymentIdentity,
    now_utc: int,
    now_boot: int,
    boot_id: str,
) -> dict[str, object]:
    """Return the bounded common fields used by a V4 stage record."""

    document = _empty_v3_stage(
        kind=kind,
        stage=stage,
        run_id=run_id,
        identity=identity,
        now_utc=now_utc,
        now_boot=now_boot,
        boot_id=boot_id,
    )
    document["schema_version"] = V4_SCHEMA_VERSION
    document[_V4_EPISODE_FIELD] = _empty_v4_episode()
    return document


@dataclass
class _V4EpisodeState:
    """Constant-space state for one global bounded readiness episode."""

    state: str = "NONE"
    start_utc_ns: int | None = None
    start_boottime_ns: int | None = None
    close_utc_ns: int | None = None
    close_boottime_ns: int | None = None
    last_product_key: str | None = None
    last_reason: str | None = None

    def public(self, *, observed_boottime_ns: int) -> dict[str, object]:
        if self.state == "NONE":
            return _empty_v4_episode()
        if self.start_boottime_ns is None or self.start_utc_ns is None:
            raise AcceptanceError("V4 readiness episode start is incomplete")
        age_boottime_ns = (
            (self.close_boottime_ns or observed_boottime_ns) - self.start_boottime_ns
        )
        return {
            "state": self.state,
            "episode_start_utc_ns": self.start_utc_ns,
            "episode_start_boottime_ns": self.start_boottime_ns,
            "episode_close_utc_ns": self.close_utc_ns,
            "episode_close_boottime_ns": self.close_boottime_ns,
            "last_product_key": self.last_product_key,
            "last_reason": self.last_reason,
            "age_ns": age_boottime_ns,
            "deadline_ns": V4_DEADLINE_NS,
        }


def _v4_recoverable_reason(
    evaluator: object, readiness: Mapping[str, object]
) -> tuple[str, str] | None:
    """Return the configured ProductKey/reason pair for the sole V4 class."""

    if readiness.get("state") != "NOT_READY":
        return None
    reasons = readiness.get("reasons")
    if not isinstance(reasons, list) or len(reasons) != 1:
        return None
    reason = reasons[0]
    if not isinstance(reason, str) or not reason.endswith("_core_not_ready"):
        return None
    prefix = reason[: -len("_core_not_ready")]
    if prefix.count(":") != 1:
        return None
    market, symbol = prefix.split(":", 1)
    if not market or not symbol:
        return None
    expected_products = getattr(evaluator, "expected_products", None)
    if not isinstance(expected_products, (set, frozenset, tuple, list)):
        return None
    configured = {
        (
            (
                product[0]
                if isinstance(product, tuple) and len(product) == 2
                else getattr(product, "market", None)
            ),
            (
                product[1]
                if isinstance(product, tuple) and len(product) == 2
                else getattr(product, "symbol", None)
            ),
        )
        for product in expected_products
    }
    if (market, symbol) not in configured:
        return None
    return prefix, reason


def _v4_episode_observe(
    episode: _V4EpisodeState,
    *,
    evaluator: object,
    readiness: Mapping[str, object],
    observed_at_utc_ns: int,
    observed_at_boottime_ns: int,
    allow_start: bool,
) -> list[str]:
    """Advance the global V4 episode and return readiness findings.

    The caller owns the sticky finding set.  This function only tracks the
    bounded episode itself and never turns a valid intermediate recovery into
    a permanent blocker.
    """

    state = readiness.get("state")
    if state == "READY":
        if episode.state == "ACTIVE":
            if episode.start_boottime_ns is None:
                raise AcceptanceError("V4 active episode has no BOOTTIME start")
            age = observed_at_boottime_ns - episode.start_boottime_ns
            if age > V4_DEADLINE_NS:
                episode.state = "DEADLINE_EXCEEDED"
                return ["readiness_recovery_deadline_exceeded"]
            episode.state = "RECOVERED"
            episode.close_utc_ns = observed_at_utc_ns
            episode.close_boottime_ns = observed_at_boottime_ns
        return []
    if state == "FAILED":
        return ["readiness_failed"]

    recoverable = _v4_recoverable_reason(evaluator, readiness)
    if recoverable is None:
        return ["readiness_not_ready"]
    product_key, reason = recoverable
    if episode.state == "DEADLINE_EXCEEDED":
        return ["readiness_recovery_deadline_exceeded"]
    if episode.state in {"NONE", "RECOVERED"}:
        if not allow_start:
            return ["readiness_not_ready"]
        episode.state = "ACTIVE"
        episode.start_utc_ns = observed_at_utc_ns
        episode.start_boottime_ns = observed_at_boottime_ns
        episode.close_utc_ns = None
        episode.close_boottime_ns = None
    elif episode.state != "ACTIVE":
        raise AcceptanceError("V4 readiness episode state is invalid")
    episode.last_product_key = product_key
    episode.last_reason = reason
    if episode.start_boottime_ns is None:
        raise AcceptanceError("V4 readiness episode has no BOOTTIME start")
    if observed_at_boottime_ns - episode.start_boottime_ns > V4_DEADLINE_NS:
        episode.state = "DEADLINE_EXCEEDED"
        return ["readiness_recovery_deadline_exceeded"]
    return []


def _validate_v4_episode_document(
    value: object,
    *,
    observed_boottime_ns: int,
) -> dict[str, object]:
    if not isinstance(value, dict):
        raise AcceptanceError("V4 readiness episode is malformed")
    expected_keys = set(_empty_v4_episode())
    if set(value) != expected_keys:
        raise AcceptanceError("V4 readiness episode fields are not exact")
    state = value.get("state")
    if state not in {"NONE", "ACTIVE", "RECOVERED", "DEADLINE_EXCEEDED"}:
        raise AcceptanceError("V4 readiness episode state is invalid")
    if _integer(value.get("deadline_ns"), "V4 readiness deadline") != V4_DEADLINE_NS:
        raise AcceptanceError("V4 readiness deadline authority is invalid")
    for field_name in (
        "episode_start_utc_ns",
        "episode_start_boottime_ns",
        "episode_close_utc_ns",
        "episode_close_boottime_ns",
        "age_ns",
    ):
        field_value = value.get(field_name)
        if field_value is not None:
            _integer(field_value, f"V4 {field_name}")
    for field_name in ("last_product_key", "last_reason"):
        field_value = value.get(field_name)
        if field_value is not None and not isinstance(field_value, str):
            raise AcceptanceError(f"V4 {field_name} is invalid")
    if state == "NONE":
        if value != _empty_v4_episode():
            raise AcceptanceError("empty V4 readiness episode is not canonical")
        return dict(value)
    start_boot = _integer(value.get("episode_start_boottime_ns"), "V4 episode start BOOTTIME")
    start_utc = _integer(value.get("episode_start_utc_ns"), "V4 episode start UTC")
    last_key = value.get("last_product_key")
    last_reason = value.get("last_reason")
    if not isinstance(last_key, str) or not isinstance(last_reason, str):
        raise AcceptanceError("V4 readiness episode reason authority is incomplete")
    close_boot = value.get("episode_close_boottime_ns")
    close_utc = value.get("episode_close_utc_ns")
    if state == "RECOVERED":
        if not isinstance(close_boot, int) or isinstance(close_boot, bool):
            raise AcceptanceError("recovered V4 episode has no close BOOTTIME")
        if not isinstance(close_utc, int) or isinstance(close_utc, bool):
            raise AcceptanceError("recovered V4 episode has no close UTC")
        age_expected = close_boot - start_boot
        if close_boot > observed_boottime_ns:
            raise AcceptanceError("recovered V4 episode closes after its sample")
    else:
        if close_boot is not None or close_utc is not None:
            raise AcceptanceError("active V4 episode has a close timestamp")
        age_expected = observed_boottime_ns - start_boot
    if age_expected < 0 or value.get("age_ns") != age_expected:
        raise AcceptanceError("V4 readiness episode age is invalid")
    if start_utc < 0 or start_boot < 0:
        raise AcceptanceError("V4 readiness episode timestamp is invalid")
    return dict(value)


def _manifest_aggregate(records: Mapping[str, Mapping[str, object]]) -> str:
    canonical = "".join(
        f"{path}\t{record['sha256']}\n"
        for path, record in sorted(records.items())
    ).encode()
    return sha256_bytes(canonical)


def _key_aggregate(keys: set[str]) -> str:
    return sha256_bytes("".join(f"{key}\n" for key in sorted(keys)).encode())


def _manifest_records_from_inventory(inventory: object) -> dict[str, dict[str, object]]:
    if not isinstance(inventory, dict):
        raise AcceptanceError("Raw audit manifest inventory is malformed")
    members = inventory.get("members")
    if not isinstance(members, list):
        raise AcceptanceError("Raw audit manifest inventory is malformed")
    records: dict[str, dict[str, object]] = {}
    for member in members:
        if (
            not isinstance(member, dict)
            or not isinstance(member.get("path"), str)
            or not isinstance(member.get("chunk_id"), str)
            or member.get("path") in records
        ):
            raise AcceptanceError("Raw audit manifest inventory member is malformed")
        path = str(member["path"])
        records[path] = {
            "path": path,
            "sha256": _digest(member.get("sha256"), "Raw manifest inventory digest"),
            "chunk_id": str(member["chunk_id"]),
        }
    declared = inventory.get("sha256")
    if declared is not None and _digest(
        declared, "Raw manifest inventory aggregate"
    ) != _manifest_aggregate(records):
        raise AcceptanceError("Raw audit manifest inventory aggregate is invalid")
    count = inventory.get("count")
    if count is not None and _integer(count, "Raw manifest inventory count") != len(records):
        raise AcceptanceError("Raw audit manifest inventory count is invalid")
    return records


def _manifest_records_from_list(value: object, field: str) -> dict[str, dict[str, object]]:
    if not isinstance(value, list):
        raise AcceptanceError(f"{field} is malformed")
    records: dict[str, dict[str, object]] = {}
    for member in value:
        if (
            not isinstance(member, dict)
            or not isinstance(member.get("path"), str)
            or not isinstance(member.get("chunk_id"), str)
            or member["path"] in records
        ):
            raise AcceptanceError(f"{field} member is malformed")
        path = str(member["path"])
        records[path] = {
            "path": path,
            "sha256": _digest(member.get("sha256"), f"{field} digest"),
            "chunk_id": str(member["chunk_id"]),
        }
    return records


def _manifest_baseline_from_evidence(value: object) -> dict[str, dict[str, object]]:
    if not isinstance(value, dict):
        raise AcceptanceError("stage baseline manifest membership is malformed")
    members = value.get("members")
    if not isinstance(members, list):
        raise AcceptanceError("stage baseline manifest membership is malformed")
    records: dict[str, dict[str, object]] = {}
    for member in members:
        if (
            not isinstance(member, dict)
            or not isinstance(member.get("path"), str)
            or not isinstance(member.get("chunk_id"), str)
            or member["path"] in records
        ):
            raise AcceptanceError("stage baseline manifest member is malformed")
        path = str(member["path"])
        records[path] = {
            "path": path,
            "sha256": _digest(member.get("sha256"), "stage baseline manifest digest"),
            "chunk_id": str(member["chunk_id"]),
        }
    count = _integer(value.get("count"), "stage baseline manifest count")
    aggregate = _digest(value.get("aggregate_sha256"), "stage baseline manifest aggregate")
    if count != len(records) or aggregate != _manifest_aggregate(records):
        raise AcceptanceError("stage baseline manifest authority is invalid")
    return records


def _absence_key(member: Mapping[str, object]) -> tuple[str, str]:
    chunk_id = member.get("chunk_id")
    manifest_path = member.get("manifest_path")
    if not isinstance(chunk_id, str) or not chunk_id:
        raise AcceptanceError("Raw absence chunk identity is malformed")
    if not isinstance(manifest_path, str) or not manifest_path:
        raise AcceptanceError("Raw absence manifest identity is malformed")
    return chunk_id, manifest_path


def _catalog_key(item: Mapping[str, object]) -> tuple[str, str, str, str]:
    values = tuple(item.get(name) for name in ("market", "symbol", "stream", "gap_id"))
    if any(not isinstance(value, str) or not value for value in values):
        raise AcceptanceError("Catalog discontinuity identity is malformed")
    return cast(tuple[str, str, str, str], values)


def _catalog_open_sort_key(item: Mapping[str, object]) -> tuple[str, str, str, str, int, str]:
    market, symbol, stream, gap_id = _catalog_key(item)
    return (
        market,
        symbol,
        stream,
        gap_id,
        _integer(item.get("started_at_utc_ns"), "Catalog open start timestamp"),
        str(item.get("timing")),
    )


def _first_catalog_open_detail(
    open_state: Mapping[tuple[str, str, str, str], Mapping[str, object]],
) -> dict[str, object] | None:
    if not open_state:
        return None
    first = min(open_state.values(), key=_catalog_open_sort_key)
    # Keep the complete canonical lifecycle projection available.  This is
    # intentionally not reduced to a source label: the same detail is derived
    # by the streaming verifier from the immutable transition chain.
    return dict(first)


def _absence_members(value: object) -> dict[tuple[str, str], dict[str, object]]:
    if not isinstance(value, list):
        raise AcceptanceError("Raw absence evidence is malformed")
    result: dict[tuple[str, str], dict[str, object]] = {}
    for item in value:
        if not isinstance(item, dict):
            raise AcceptanceError("Raw absence evidence member is malformed")
        key = _absence_key(item)
        if key in result:
            raise AcceptanceError("Raw absence evidence contains duplicate identities")
        result[key] = {str(name): member for name, member in item.items()}
    return result


def _absence_aggregate(members: Mapping[tuple[str, str], Mapping[str, object]]) -> str:
    ordered = [dict(members[key]) for key in sorted(members)]
    body = json.dumps(ordered, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    return sha256_bytes(body)


def _raw_absence_baseline_from_evidence(value: object) -> dict[tuple[str, str], dict[str, object]]:
    if not isinstance(value, dict):
        raise AcceptanceError("Raw absence baseline is malformed")
    members = _absence_members(value.get("members"))
    count = _integer(value.get("count"), "Raw absence baseline count")
    aggregate = _digest(value.get("aggregate_sha256"), "Raw absence baseline aggregate")
    if count != len(members) or aggregate != _absence_aggregate(members):
        raise AcceptanceError("Raw absence baseline authority is invalid")
    return members


def _raw_absence_transition(
    previous: Mapping[tuple[str, str], Mapping[str, object]],
    current: Mapping[tuple[str, str], Mapping[str, object]],
) -> dict[str, object]:
    added = [dict(current[key]) for key in sorted(set(current) - set(previous))]
    reclassified = [
        {"previous": dict(previous[key]), "current": dict(current[key])}
        for key in sorted(set(previous) & set(current))
        if dict(previous[key]) != dict(current[key])
    ]
    resolved = [dict(previous[key]) for key in sorted(set(previous) - set(current))]
    return {
        "previous_count": len(previous),
        "previous_aggregate_sha256": _absence_aggregate(previous),
        "current_count": len(current),
        "current_aggregate_sha256": _absence_aggregate(current),
        "added": added,
        "reclassified": reclassified,
        "resolved": resolved,
    }


def _apply_raw_absence_transition(
    previous: Mapping[tuple[str, str], Mapping[str, object]],
    transition: object,
) -> dict[tuple[str, str], dict[str, object]]:
    if not isinstance(transition, dict):
        raise AcceptanceError("Raw absence transition is malformed")
    if _integer(transition.get("previous_count"), "Raw absence previous count") != len(previous):
        raise AcceptanceError("Raw absence transition previous count is invalid")
    if _digest(
        transition.get("previous_aggregate_sha256"), "Raw absence previous aggregate"
    ) != _absence_aggregate(previous):
        raise AcceptanceError("Raw absence transition predecessor is invalid")
    result = {key: dict(value) for key, value in previous.items()}
    added = _absence_members(transition.get("added"))
    for key, value in added.items():
        if key in result:
            raise AcceptanceError("Raw absence transition adds an existing identity")
        result[key] = value
    reclassified = transition.get("reclassified")
    if not isinstance(reclassified, list):
        raise AcceptanceError("Raw absence reclassification is malformed")
    for item in reclassified:
        if not isinstance(item, dict):
            raise AcceptanceError("Raw absence reclassification is malformed")
        old = item.get("previous")
        new = item.get("current")
        if not isinstance(old, dict) or not isinstance(new, dict):
            raise AcceptanceError("Raw absence reclassification is malformed")
        old_key = _absence_key(old)
        new_key = _absence_key(new)
        if old_key != new_key or old_key not in result or result[old_key] != old:
            raise AcceptanceError("Raw absence reclassification predecessor is invalid")
        result[old_key] = {str(name): value for name, value in new.items()}
    resolved = _absence_members(transition.get("resolved"))
    for key, value in resolved.items():
        if key not in result or result[key] != value:
            raise AcceptanceError("Raw absence resolution predecessor is invalid")
        del result[key]
    if _integer(transition.get("current_count"), "Raw absence current count") != len(result):
        raise AcceptanceError("Raw absence transition current count is invalid")
    if _digest(
        transition.get("current_aggregate_sha256"), "Raw absence current aggregate"
    ) != _absence_aggregate(result):
        raise AcceptanceError("Raw absence transition current aggregate is invalid")
    return result


@dataclass
class _LastSampleMetadata:
    ordinal: int
    sha256: str
    utc_ns: int
    boottime_ns: int
    result: str
    findings: frozenset[str]


@dataclass
class _V2ChainState:
    manifest_records: dict[str, dict[str, object]]
    raw_absences: dict[tuple[str, str], dict[str, object]]
    deferred_manifest_paths: set[str]
    continuation_streams: dict[str, object]
    reconnect_transition_keys: set[str]
    catalog_open: dict[tuple[str, str, str, str], dict[str, object]]
    catalog_interval_keys: set[tuple[str, str, str, str]]
    terminal_event_keys: set[str]
    known_findings: set[str]
    finding_detail_keys: set[str]
    catalog_current_interval_keys: set[tuple[str, str, str, str]] = field(
        default_factory=set
    )
    stage_start_utc_ns: int | None = None
    last_observed_utc_ns: int | None = None
    last: _LastSampleMetadata | None = None
    invalid_manifest_authority: bool = False
    readiness_episode: _V4EpisodeState | None = None


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
    schema_version: str = SCHEMA_VERSION,
) -> tuple[Path, str, dict[str, object]]:
    """Perform existing static deployment verification and publish identity."""

    if schema_version not in {SCHEMA_VERSION, V4_SCHEMA_VERSION}:
        raise AcceptanceError("unsupported generated acceptance schema")
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
        schema_version=schema_version,
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
    path: Path,
    identity: DeploymentIdentity,
    *,
    expected_schema: str | None = None,
) -> tuple[dict[str, object], str]:
    document, digest = _read_published(path)
    if expected_schema is not None and document.get("schema_version") != expected_schema:
        raise AcceptanceError("identity evidence schema cannot authorize this artifact")
    if document.get("evidence_kind") != "identity-result" or document.get("stage") != "identity":
        raise AcceptanceError("identity evidence kind is invalid")
    if (
        document.get("result") != "PASS_CANDIDATE"
        or not _same_identity(document, identity)
        or document.get("identity") != identity.document()
    ):
        raise AcceptanceError("identity evidence is not eligible for this artifact")
    if expected_schema == V4_SCHEMA_VERSION:
        observed_boot = _integer(
            document.get("observed_at_boottime_ns"), "identity BOOTTIME timestamp"
        )
        if _validate_v4_episode_document(
            document.get(_V4_EPISODE_FIELD), observed_boottime_ns=observed_boot
        ) != _empty_v4_episode():
            raise AcceptanceError("V4 identity evidence has a non-empty episode")
    return document, digest


def create_readiness_evidence(
    *,
    identity_evidence_path: Path,
    identity: DeploymentIdentity,
    manager: SystemdManager,
    evaluator: VpsReadinessEvaluator,
    evidence_root: Path,
    data_root: Path,
    schema_version: str = SCHEMA_VERSION,
) -> tuple[Path, str, dict[str, object]]:
    if schema_version not in {SCHEMA_VERSION, V4_SCHEMA_VERSION}:
        raise AcceptanceError("unsupported generated acceptance schema")
    root = _safe_evidence_root(evidence_root, data_root)
    _identity_doc, prior = read_identity_evidence(
        identity_evidence_path,
        identity,
        expected_schema=schema_version,
    )
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
        schema_version=schema_version,
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
    archive_root_resolver: ArchiveRootResolver | None = None
    t0_manifest_members: dict[str, str] | None = None
    t0_manifest_records: dict[str, dict[str, object]] | None = None
    t0_manifest_aggregate_sha256: str | None = None
    published_manifest_records: dict[str, dict[str, object]] | None = None
    published_raw_absences: dict[tuple[str, str], dict[str, object]] | None = None
    published_deferred_manifest_paths: set[str] = field(default_factory=set)
    published_reconnect_transition_keys: set[str] = field(default_factory=set)
    published_reconnect_streams: dict[str, object] = field(default_factory=dict)
    published_catalog_open: dict[tuple[str, str, str, str], dict[str, object]] = field(
        default_factory=dict
    )
    published_catalog_interval_keys: set[tuple[str, str, str, str]] = field(
        default_factory=set
    )
    published_terminal_event_keys: set[str] = field(default_factory=set)
    finding_details: dict[str, object] = field(default_factory=dict)
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
        records = _manifest_records_from_inventory(inventory)
        self.t0_manifest_records = records
        self.t0_manifest_aggregate_sha256 = _manifest_aggregate(records)
        return {path: str(record["sha256"]) for path, record in records.items()}

    def _catalog_evidence(
        self, t0: int, *, as_of_utc_ns: int
    ) -> tuple[dict[str, object], list[str], dict[str, object]]:
        path = self.data_root / "state" / "catalog.sqlite"
        if not path.is_file():
            raise AcceptanceError("Catalog is unavailable")
        with Catalog(path, read_only=True) as catalog:
            integrity = catalog.integrity_check()
            if integrity != ("ok",):
                raise AcceptanceError("Catalog integrity check failed")
            authority = catalog.discontinuity_authority_snapshot(
                as_of_utc_ns=as_of_utc_ns
            )
            malformed = cast(list[dict[str, object]], authority["malformed_events"])
            degraded = cast(list[dict[str, object]], authority["degraded_pairs"])
            unclosed = cast(
                dict[tuple[str, str, str], list[dict[str, object]]],
                authority["unclosed"],
            )
            closed = cast(
                dict[tuple[str, str, str], list[dict[str, object]]],
                authority["closed"],
            )
            events = cast(list[dict[str, object]], authority["operational_events"])
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
        detail_candidates: dict[str, object] = {}
        non_monotonic = sorted(
            (
                {
                    field: interval[field]
                    for field in (
                        "market",
                        "symbol",
                        "stream",
                        "gap_id",
                        "started_at_utc_ns",
                        "ended_at_utc_ns",
                    )
                }
                for interval in current_intervals
                if _integer(interval.get("ended_at_utc_ns"), "gap end")
                <= _integer(interval.get("started_at_utc_ns"), "gap start")
            ),
            key=lambda item: (
                str(item["market"]),
                str(item["symbol"]),
                str(item["stream"]),
                str(item["gap_id"]),
                _integer(item["started_at_utc_ns"], "gap start"),
                _integer(item["ended_at_utc_ns"], "gap end"),
            ),
        )
        if non_monotonic:
            findings.append("unsafe_wall_clock_backward")
            detail_candidates["unsafe_wall_clock_backward"] = non_monotonic[0]
        return (
            {"integrity_check": list(integrity), "discontinuity": discontinuity},
            findings,
            detail_candidates,
        )

    def _manifest_transition(
        self,
        current_records: dict[str, dict[str, object]],
        continuation_members: dict[str, str],
    ) -> dict[str, object]:
        previous = self.published_manifest_records
        if previous is None:
            previous = dict(current_records)
        anomalies: list[dict[str, object]] = []
        for path, previous_record in sorted(previous.items()):
            current_record = current_records.get(path)
            if current_record is None:
                anomalies.append(
                    {
                        "path": path,
                        "kind": "missing",
                        "expected_sha256": previous_record["sha256"],
                        "observed_sha256": None,
                    }
                )
                continue
            if (
                current_record.get("sha256") != previous_record.get("sha256")
                or current_record.get("chunk_id") != previous_record.get("chunk_id")
            ):
                anomalies.append(
                    {
                        "path": path,
                        "kind": "mutated",
                        "expected_sha256": previous_record["sha256"],
                        "observed_sha256": current_record["sha256"],
                    }
                )
        added = [
            dict(current_records[path])
            for path in sorted(set(current_records) - set(previous))
        ]
        deferred = [
            dict(current_records[path])
            for path in sorted(set(current_records) - set(continuation_members))
        ]
        if anomalies:
            transition: dict[str, object] = {
                "state": "ANOMALY",
                "previous_count": len(previous),
                "previous_aggregate_sha256": _manifest_aggregate(previous),
                "current_count": len(current_records),
                "current_aggregate_sha256": _manifest_aggregate(current_records),
                "added_members": [],
                "deferred_members": [],
                "anomalies": anomalies,
            }
        else:
            transition = {
                "state": "NORMAL",
                "previous_count": len(previous),
                "previous_aggregate_sha256": _manifest_aggregate(previous),
                "current_count": len(current_records),
                "current_aggregate_sha256": _manifest_aggregate(current_records),
                "added_members": added,
                "deferred_members": deferred,
                "anomalies": [],
            }
        self.published_manifest_records = current_records
        self.published_deferred_manifest_paths = {
            str(member["path"]) for member in deferred
        }
        return transition

    @staticmethod
    def _transition_key(transition: Mapping[str, object]) -> str:
        return sha256_bytes(canonical_json(transition))

    def _catalog_projection(
        self, catalog: Mapping[str, object], *, stage_start: bool
    ) -> tuple[dict[str, object], dict[str, object], list[str], dict[str, object]]:
        discontinuity = catalog.get("discontinuity")
        if not isinstance(discontinuity, dict):
            raise AcceptanceError("Catalog discontinuity evidence is malformed")
        current_intervals = discontinuity.get("current_intervals")
        open_intervals = discontinuity.get("open_intervals")
        terminal_events = discontinuity.get("terminal_events")
        if (
            not isinstance(current_intervals, list)
            or any(not isinstance(item, dict) for item in current_intervals)
            or not isinstance(open_intervals, list)
            or any(not isinstance(item, dict) for item in open_intervals)
            or not isinstance(terminal_events, list)
            or any(not isinstance(item, str) for item in terminal_events)
        ):
            raise AcceptanceError("Catalog discontinuity evidence is malformed")
        current_open: dict[tuple[str, str, str, str], dict[str, object]] = {}
        for item in open_intervals:
            key = _catalog_key(item)
            if key in current_open:
                raise AcceptanceError("Catalog open discontinuity identities are duplicated")
            current_open[key] = dict(item)
        current_closed: dict[tuple[str, str, str, str], dict[str, object]] = {}
        for item in current_intervals:
            key = _catalog_key(item)
            if key in current_closed:
                raise AcceptanceError("Catalog discontinuity identities are duplicated")
            current_closed[key] = dict(item)
        started = [
            dict(current_open[key])
            for key in sorted(current_open)
            if current_open[key].get("timing") == "OPENED_IN_STAGE"
            and key not in self.published_catalog_open
        ]
        completed = [
            dict(current_closed[key])
            for key in sorted(current_closed)
            if (
                current_closed[key].get("timing") == "CURRENT_STAGE"
                or (
                    current_closed[key].get("timing") == "CROSSES_T0"
                    and key in self.published_catalog_open
                )
            )
            and key not in self.published_catalog_interval_keys
        ]
        completed_keys = {_catalog_key(item) for item in completed}
        disappeared = set(self.published_catalog_open) - set(current_open)
        if not disappeared <= completed_keys:
            raise AcceptanceError(
                "Catalog open identity disappeared without completion authority"
            )
        new_terminal = [
            event for event in terminal_events if event not in self.published_terminal_event_keys
        ]
        transition: dict[str, object] = {
            "started": started,
            "completed": completed,
            "current_open": [dict(current_open[key]) for key in sorted(current_open)],
            "terminal_events": new_terminal,
            "summary": {
                "open_count": len(current_open),
                "current_interval_count": len(current_closed),
            },
        }
        self.published_catalog_open = current_open
        self.published_catalog_interval_keys.update(current_closed)
        self.published_terminal_event_keys.update(terminal_events)
        baseline: dict[str, object] = {}
        if stage_start:
            baseline = {
                "malformed_events": discontinuity.get("malformed_events", []),
                "degraded_pairs": discontinuity.get("degraded_pairs", []),
                "pre_t0_closed": discontinuity.get("baseline_history", []),
                "crossing_at_t0": [
                    dict(item)
                    for item in current_intervals
                    if item.get("timing") == "CROSSES_T0"
                ],
                "open_at_t0": [
                    dict(item)
                    for item in open_intervals
                    if item.get("timing") == "OPEN_AT_T0"
                ],
                "terminal_events": list(terminal_events),
            }
        details: dict[str, object] = {}
        if open_intervals:
            details["unresolved_discontinuity"] = dict(open_intervals[0])
        if discontinuity.get("malformed_events"):
            details["malformed_discontinuity_authority"] = discontinuity.get(
                "malformed_events"
            )
        if discontinuity.get("degraded_pairs"):
            details["degraded_discontinuity_authority"] = discontinuity.get(
                "degraded_pairs"
            )
        return baseline, transition, [str(item) for item in terminal_events], details

    def _raw_evidence(self) -> tuple[dict[str, object], list[str]]:
        try:
            audit = incremental_audit_data_root(
                self.data_root,
                continuation=self.reconnect_continuation,
                archive_root_resolver=self.archive_root_resolver,
            )
        except (OSError, SealError, CatalogStateError, ValueError, RuntimeError) as exc:
            raise AcceptanceError(f"strict Raw/manifest audit failed: {exc}") from exc
        continuation = audit.get("continuation")
        if not isinstance(continuation, dict):
            raise AcceptanceError("Raw audit continuation is malformed")
        continuation_members = continuation.get("manifest_members")
        if not isinstance(continuation_members, dict):
            raise AcceptanceError("Raw audit manifest membership is malformed")
        current_continuation_members = {
            str(path): _digest(digest, "Raw manifest member digest")
            for path, digest in continuation_members.items()
            if isinstance(path, str)
        }
        if len(current_continuation_members) != len(continuation_members):
            raise AcceptanceError("Raw audit manifest membership is malformed")
        if self.t0_manifest_members is None:
            raise AcceptanceError("baseline manifest membership is not frozen")
        self.reconnect_continuation = continuation
        if not isinstance(audit.get("summary"), dict):
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
        current_records = _manifest_records_from_inventory(inventory)
        members_by_chunk: dict[str, tuple[str, str]] = {}
        for path, record in current_records.items():
            chunk_id = str(record["chunk_id"])
            if chunk_id in members_by_chunk:
                raise AcceptanceError("Raw audit manifest inventory has duplicate chunk IDs")
            members_by_chunk[chunk_id] = (path, str(record["sha256"]))

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
        loss = audit.get("raw_loss")
        if not isinstance(loss, list) or any(not isinstance(item, dict) for item in loss):
            raise AcceptanceError("Raw audit loss classification is malformed")
        for item in loss:
            classification = item.get("classification")
            if classification == "UNEXPLAINED_ABSENCE":
                findings.append("unexplained_raw_absence")
            elif classification == "UNKNOWN":
                findings.append("unknown_raw_absence")
        current_absences = _absence_members(loss)
        previous_absences = self.published_raw_absences
        absence_transition = (
            None
            if previous_absences is None
            else _raw_absence_transition(previous_absences, current_absences)
        )
        self.published_raw_absences = current_absences
        transition = self._manifest_transition(
            current_records, current_continuation_members
        )
        if integrity_findings or transition.get("state") == "ANOMALY":
            findings.append("manifest_byte_mutation_or_loss")
        streams = continuation.get("streams")
        if not isinstance(streams, dict):
            raise AcceptanceError("Raw audit continuation streams are malformed")
        added_transitions: list[dict[str, object]] = []
        for item in current_transitions:
            key = self._transition_key(item)
            if key not in self.published_reconnect_transition_keys:
                added_transitions.append(dict(item))
                self.published_reconnect_transition_keys.add(key)
        reconnect_transition = {
            "added": added_transitions,
            "stage_transition_count": len(self.published_reconnect_transition_keys),
            "stage_transition_aggregate_sha256": _key_aggregate(
                self.published_reconnect_transition_keys
            ),
            "continuation": {
                "schema_version": continuation.get("schema_version"),
                "streams": streams,
            },
        }
        self.published_reconnect_streams = {
            str(name): value for name, value in streams.items()
        }
        details: dict[str, object] = {}
        if transition.get("anomalies"):
            details["manifest_byte_mutation_or_loss"] = transition["anomalies"]
        if any(item.get("kind") == UNMARKED_RECONNECT for item in current_transitions):
            details["UNMARKED_RECONNECT"] = next(
                item
                for item in current_transitions
                if item.get("kind") == UNMARKED_RECONNECT
            )
        if any(item.get("kind") == UNKNOWN for item in current_transitions):
            details["UNKNOWN_RECONNECT_BOUNDARY"] = next(
                item for item in current_transitions if item.get("kind") == UNKNOWN
            )
        unknown_absence = next(
            (
                item
                for item in loss
                if isinstance(item, dict)
                and item.get("classification") in {"UNEXPLAINED_ABSENCE", "UNKNOWN"}
            ),
            None,
        )
        if unknown_absence is not None:
            details["unexplained_raw_absence"] = unknown_absence
            details["unknown_raw_absence"] = unknown_absence
        return {
            "manifest_records": current_records,
            "manifest_transition": transition,
            "raw_absences": current_absences,
            "raw_absence_transition": absence_transition,
            "baseline_history": baseline_history,
            "current_transitions": current_transitions,
            "reconnect_baseline": {
                "pre_t0_history": baseline_history,
                "continuation": {
                    "schema_version": continuation.get("schema_version"),
                    "streams": streams,
                },
            },
            "reconnect_transition": reconnect_transition,
            "continuation_members": current_continuation_members,
            "details": details,
        }, findings

    def _fallback_raw_projection(self, *, stage_start: bool) -> dict[str, object]:
        """Keep a failed observation publishable without inventing live state."""

        records = dict(self.published_manifest_records or self.t0_manifest_records or {})
        continuation_members = {
            path: str(record["sha256"])
            for path, record in records.items()
            if path not in self.published_deferred_manifest_paths
        }
        transition = self._manifest_transition(records, continuation_members)
        absences = dict(self.published_raw_absences or {})
        continuation = self.reconnect_continuation
        streams: dict[str, object] = dict(self.published_reconnect_streams)
        if isinstance(continuation, dict) and isinstance(continuation.get("streams"), dict):
            streams = {
                str(name): value
                for name, value in cast(
                    dict[str, object], continuation["streams"]
                ).items()
            }
        return {
            "manifest_records": records,
            "manifest_transition": transition,
            "raw_absences": absences,
            "raw_absence_transition": (
                None if stage_start else _raw_absence_transition(absences, absences)
            ),
            "baseline_history": [],
            "current_transitions": [],
            "reconnect_baseline": {
                "pre_t0_history": [],
                "continuation": {
                    "schema_version": INCREMENTAL_SCHEMA_VERSION,
                    "streams": streams,
                },
            },
            "reconnect_transition": {
                "added": [],
                "stage_transition_count": len(self.published_reconnect_transition_keys),
                "stage_transition_aggregate_sha256": _key_aggregate(
                    self.published_reconnect_transition_keys
                ),
                "continuation": {
                    "schema_version": INCREMENTAL_SCHEMA_VERSION,
                    "streams": streams,
                },
            },
            "continuation_members": continuation_members,
            "details": {},
        }

    def _stage_document(
        self,
        *,
        kind: str,
        now_utc: int,
        now_boot: int,
        boot_id: str,
    ) -> dict[str, object]:
        return _empty_v3_stage(
            kind=kind,
            stage=self.stage,
            run_id=self.run_id,
            identity=self.identity,
            now_utc=now_utc,
            now_boot=now_boot,
            boot_id=boot_id,
        )

    def _readiness_findings(
        self,
        readiness: object,
        *,
        observed_at_utc_ns: int,
        observed_at_boottime_ns: int,
        stage_start: bool,
    ) -> list[str]:
        state = getattr(readiness, "state", None)
        if state == "FAILED":
            return ["readiness_failed"]
        if state != "READY":
            return ["readiness_not_ready"]
        return []

    def _soft_findings(self) -> set[str]:
        return {
            "acceptance_observation_gap",
            "readiness_not_ready",
            "unsafe_wall_clock_backward",
        }

    def _additional_stage_fields(self, *, observed_boottime_ns: int) -> dict[str, object]:
        return {}

    def _additional_final_findings(self, sample: Mapping[str, object]) -> set[str]:
        return set()

    def _final_result(
        self,
        *,
        sample: Mapping[str, object],
        elapsed: int,
        terminal_open_detail: dict[str, object] | None,
    ) -> str:
        if sample["boot_id"] != self.t0_boot_id or elapsed < STAGE_DURATION_NS[self.stage]:
            return "INCOMPLETE"
        if terminal_open_detail is not None:
            return "FAIL"
        return str(sample["result"])

    def _additional_final_details(
        self,
        *,
        sample: Mapping[str, object],
        terminal_open_detail: dict[str, object] | None,
    ) -> dict[str, object]:
        return (
            {"unresolved_discontinuity": terminal_open_detail}
            if terminal_open_detail is not None
            else {}
        )

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
        stage_start = self.stage_start_sha256 is None
        document = self._stage_document(
            kind="stage-sample",
            now_utc=now_utc,
            now_boot=now_boot,
            boot_id=boot_id,
        )
        findings: list[str] = []
        detail_candidates: dict[str, object] = {}
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
        findings.extend(
            self._readiness_findings(
                readiness,
                observed_at_utc_ns=now_utc,
                observed_at_boottime_ns=now_boot,
                stage_start=stage_start,
            )
        )
        try:
            catalog, catalog_findings, catalog_finding_details = self._catalog_evidence(
                self.t0_utc_ns,
                as_of_utc_ns=now_utc,
            )
            raw, raw_findings = self._raw_evidence()
            (
                catalog_baseline,
                catalog_transition,
                _terminal_events,
                catalog_details,
            ) = self._catalog_projection(catalog, stage_start=stage_start)
            findings.extend(catalog_findings)
            findings.extend(raw_findings)
            detail_candidates.update(catalog_finding_details)
            detail_candidates.update(catalog_details)
            raw_details = raw.get("details")
            if isinstance(raw_details, dict):
                detail_candidates.update(raw_details)
        except AcceptanceError as exc:
            findings.append(str(exc))
            catalog, raw = {}, self._fallback_raw_projection(stage_start=stage_start)
            catalog_baseline = {}
            catalog_transition = {
                "started": [],
                "completed": [],
                "current_open": [
                    dict(self.published_catalog_open[key])
                    for key in sorted(self.published_catalog_open)
                ],
                "terminal_events": [],
                "summary": {
                    "open_count": len(self.published_catalog_open),
                    "current_interval_count": len(self.published_catalog_interval_keys),
                },
            }
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
        previous_findings = set(self.ever_blocking_findings)
        self.ever_blocking_findings.update(findings)
        all_findings = sorted(self.ever_blocking_findings)
        new_finding_details: dict[str, object] = {}
        for finding in sorted(set(findings) - previous_findings):
            detail = detail_candidates.get(finding)
            if detail is None:
                detail = {"observed_at_utc_ns": now_utc}
            self.finding_details[finding] = detail
            new_finding_details[finding] = detail
        soft_findings = self._soft_findings()
        fatal = any(item not in soft_findings for item in all_findings)
        manifest_transition = raw.get("manifest_transition", {})
        reconnect_transition = raw.get("reconnect_transition", {})
        document.update(
            {
                "prior_stage_evidence_sha256": self.prior_stage_sha256,
                "stage_start_evidence_sha256": self.stage_start_sha256,
                "previous_sample_sha256": self.last_sample_sha256,
                "systemd_process_incarnation": current_process,
                "service_instance_id": instance,
                "readiness": readiness.public_dict(),
                "catalog_integrity": (
                    {"integrity_check": catalog.get("integrity_check", [])}
                    if isinstance(catalog, dict)
                    else {}
                ),
                "capacity": capacity,
                "blocking_findings": all_findings,
                "new_finding_details": new_finding_details,
                "observer_status": "COMPLETE",
                "result": "FAIL"
                if fatal
                else ("INCOMPLETE" if all_findings else "PASS_CANDIDATE"),
            }
        )
        document.update(self._additional_stage_fields(observed_boottime_ns=now_boot))
        if stage_start:
            if self.t0_manifest_records is None or self.t0_manifest_aggregate_sha256 is None:
                raise AcceptanceError("manifest baseline is not frozen")
            raw_absences = raw.get("raw_absences", {})
            if not isinstance(raw_absences, dict):
                raise AcceptanceError("Raw absence baseline is malformed")
            absence_members = _absence_members(
                [dict(raw_absences[key]) for key in sorted(raw_absences)]
            )
            document.update(
                {
                    "manifest_baseline": {
                        "count": len(self.t0_manifest_records),
                        "aggregate_sha256": self.t0_manifest_aggregate_sha256,
                        "members": [
                            dict(self.t0_manifest_records[path])
                            for path in sorted(self.t0_manifest_records)
                        ],
                    },
                    "manifest_transition": manifest_transition,
                    "raw_absence_baseline": {
                        "count": len(absence_members),
                        "aggregate_sha256": _absence_aggregate(absence_members),
                        "members": [
                            dict(absence_members[key]) for key in sorted(absence_members)
                        ],
                    },
                    "reconnect_baseline": raw.get("reconnect_baseline", {}),
                    "reconnect_transition": reconnect_transition,
                    "catalog_baseline": catalog_baseline,
                    "catalog_transition": catalog_transition,
                }
            )
        else:
            raw_absence_transition = raw.get("raw_absence_transition")
            if not isinstance(raw_absence_transition, dict):
                raw_absence_transition = _raw_absence_transition(
                    self.published_raw_absences or {}, self.published_raw_absences or {}
                )
            document.update(
                {
                    "sample_ordinal": self.next_sample_ordinal,
                    "manifest_transition": manifest_transition,
                    "raw_absence_transition": raw_absence_transition,
                    "reconnect_transition": reconnect_transition,
                    "catalog_transition": catalog_transition,
                }
            )
        if all_findings == ["readiness_not_ready"] and "readiness_not_ready" in soft_findings:
            document["result"] = "REVIEW_REQUIRED"
        return document, findings

    def start(self) -> tuple[Path, str, dict[str, object]]:
        if self.t0_boottime_ns is not None:
            raise AcceptanceError("stage T0 is already initialized")
        self.t0_manifest_members = self._freeze_manifest_membership()
        self.published_manifest_records = dict(self.t0_manifest_records or {})
        self.published_raw_absences = None
        self.published_deferred_manifest_paths = set()
        self.published_reconnect_transition_keys = set()
        self.published_reconnect_streams = {}
        self.published_catalog_open = {}
        self.published_catalog_interval_keys = set()
        self.published_terminal_event_keys = set()
        self.finding_details = {}
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
        if self.stage_start_sha256 is None:
            raise AcceptanceError("stage T0 evidence is not published")
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
        terminal_open_detail = _first_catalog_open_detail(self.published_catalog_open)
        final_findings = set(self.ever_blocking_findings)
        if terminal_open_detail is not None:
            final_findings.add("unresolved_discontinuity")
        final_findings.update(self._additional_final_findings(sample))
        result = self._final_result(
            sample=sample,
            elapsed=elapsed,
            terminal_open_detail=terminal_open_detail,
        )
        final = self._stage_document(
            kind="stage-final",
            now_utc=_integer(sample["observed_at_utc_ns"], "sample UTC timestamp"),
            now_boot=_integer(sample["observed_at_boottime_ns"], "sample BOOTTIME timestamp"),
            boot_id=str(sample["boot_id"]),
        )
        final.update(
            {
                "stage_start_evidence_sha256": self.stage_start_sha256,
                "previous_sample_sha256": last,
                "prior_stage_evidence_sha256": self.prior_stage_sha256,
                "systemd_process_incarnation": sample["systemd_process_incarnation"],
                "service_instance_id": sample["service_instance_id"],
                "blocking_findings": sorted(final_findings),
                "new_finding_details": self._additional_final_details(
                    sample=sample, terminal_open_detail=terminal_open_detail
                ),
                "result": result,
                "observer_status": "FINALIZED",
                "elapsed_boottime_ns": elapsed,
                "required_duration_ns": STAGE_DURATION_NS[self.stage],
                "last_sample_ordinal": self.next_sample_ordinal - 1,
                "last_sample_sha256": last,
                "eligible_for_next_stage": (
                    result == "PASS_CANDIDATE" and not final_findings
                ),
            }
        )
        for field_name in ("readiness", "catalog_integrity", "capacity"):
            final[field_name] = sample[field_name]
        final.update(
            self._additional_stage_fields(
                observed_boottime_ns=_integer(
                    sample["observed_at_boottime_ns"], "sample BOOTTIME timestamp"
                )
            )
        )
        path, digest = _publish(self.evidence_root, "stage-final.json", final)
        return path, digest, final


@dataclass
class V4AcceptanceObserver(AcceptanceObserver):
    """V4 observer with a global, constant-space readiness episode."""

    readiness_episode: _V4EpisodeState = field(default_factory=_V4EpisodeState)

    def _stage_document(
        self,
        *,
        kind: str,
        now_utc: int,
        now_boot: int,
        boot_id: str,
    ) -> dict[str, object]:
        return _empty_v4_stage(
            kind=kind,
            stage=self.stage,
            run_id=self.run_id,
            identity=self.identity,
            now_utc=now_utc,
            now_boot=now_boot,
            boot_id=boot_id,
        )

    def _readiness_findings(
        self,
        readiness: object,
        *,
        observed_at_utc_ns: int,
        observed_at_boottime_ns: int,
        stage_start: bool,
    ) -> list[str]:
        public = getattr(readiness, "public_dict", None)
        if not callable(public):
            raise AcceptanceError("readiness result cannot produce public evidence")
        value = public()
        if not isinstance(value, dict):
            raise AcceptanceError("readiness public evidence is malformed")
        return _v4_episode_observe(
            self.readiness_episode,
            evaluator=self.evaluator,
            readiness=value,
            observed_at_utc_ns=observed_at_utc_ns,
            observed_at_boottime_ns=observed_at_boottime_ns,
            allow_start=not stage_start,
        )

    def _soft_findings(self) -> set[str]:
        return {"acceptance_observation_gap", "unsafe_wall_clock_backward"}

    def _additional_stage_fields(self, *, observed_boottime_ns: int) -> dict[str, object]:
        return {
            _V4_EPISODE_FIELD: self.readiness_episode.public(
                observed_boottime_ns=observed_boottime_ns
            )
        }

    def _additional_final_findings(self, sample: Mapping[str, object]) -> set[str]:
        readiness = sample.get("readiness")
        if isinstance(readiness, dict) and readiness.get("state") != "READY":
            return {"readiness_not_ready"}
        return set()

    def _final_result(
        self,
        *,
        sample: Mapping[str, object],
        elapsed: int,
        terminal_open_detail: dict[str, object] | None,
    ) -> str:
        readiness = sample.get("readiness")
        if not isinstance(readiness, dict):
            return "FAIL"
        if readiness.get("state") != "READY":
            return "FAIL"
        if sample["boot_id"] != self.t0_boot_id or elapsed < STAGE_DURATION_NS[self.stage]:
            return "INCOMPLETE"
        if terminal_open_detail is not None:
            return "FAIL"
        return str(sample["result"])

    def _additional_final_details(
        self,
        *,
        sample: Mapping[str, object],
        terminal_open_detail: dict[str, object] | None,
    ) -> dict[str, object]:
        details = super()._additional_final_details(
            sample=sample, terminal_open_detail=terminal_open_detail
        )
        readiness = sample.get("readiness")
        if isinstance(readiness, dict) and readiness.get("state") != "READY":
            details["readiness_not_ready"] = {
                "observed_at_utc_ns": sample.get("observed_at_utc_ns")
            }
        return details


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
    if any(observed_members.get(path) != digest for path, digest in members.items()):
        raise AcceptanceError("reconnect continuation does not bind manifest inventory")
    return continuation


def _apply_manifest_transition(
    previous: Mapping[str, Mapping[str, object]],
    transition: object,
    *,
    blocking_findings: set[str] | frozenset[str] | None = None,
) -> tuple[dict[str, dict[str, object]], set[str], bool]:
    if not isinstance(transition, dict):
        raise AcceptanceError("manifest transition is malformed")
    state = transition.get("state")
    previous_records = {path: dict(record) for path, record in previous.items()}
    if _integer(transition.get("previous_count"), "manifest previous count") != len(previous):
        raise AcceptanceError("manifest transition previous count is invalid")
    if _digest(
        transition.get("previous_aggregate_sha256"), "manifest previous aggregate"
    ) != _manifest_aggregate(previous):
        raise AcceptanceError("manifest transition predecessor is invalid")
    if state == "ANOMALY":
        anomalies = transition.get("anomalies")
        if not isinstance(anomalies, list) or not anomalies:
            raise AcceptanceError("manifest anomaly transition is malformed")
        if (
            blocking_findings is None
            or "manifest_byte_mutation_or_loss" not in blocking_findings
        ):
            raise AcceptanceError(
                "manifest anomaly is not bound to manifest_byte_mutation_or_loss"
            )
        _integer(transition.get("current_count"), "manifest anomaly current count")
        _digest(
            transition.get("current_aggregate_sha256"),
            "manifest anomaly current aggregate",
        )
        if transition.get("added_members") != [] or transition.get("deferred_members") != []:
            raise AcceptanceError("manifest anomaly transition has legal additions")
        return previous_records, set(), True
    if state != "NORMAL":
        raise AcceptanceError("manifest transition state is invalid")
    added = _manifest_records_from_list(transition.get("added_members"), "manifest additions")
    result = dict(previous_records)
    for path, member in added.items():
        if path in result:
            raise AcceptanceError("manifest transition adds an existing member")
        result[path] = member
    deferred = _manifest_records_from_list(
        transition.get("deferred_members"), "manifest deferred members"
    )
    if any(path not in result or result[path] != member for path, member in deferred.items()):
        raise AcceptanceError("manifest deferred member is not current")
    if _integer(transition.get("current_count"), "manifest current count") != len(result):
        raise AcceptanceError("manifest transition current count is invalid")
    if _digest(
        transition.get("current_aggregate_sha256"), "manifest current aggregate"
    ) != _manifest_aggregate(result):
        raise AcceptanceError("manifest transition current aggregate is invalid")
    if transition.get("anomalies") != []:
        raise AcceptanceError("normal manifest transition contains anomalies")
    return result, set(deferred), False


def _continuation_projection(
    transition: object,
    manifest_records: Mapping[str, Mapping[str, object]],
    deferred_paths: set[str],
) -> dict[str, object]:
    if not isinstance(transition, dict):
        raise AcceptanceError("reconnect transition is malformed")
    continuation = transition.get("continuation")
    if not isinstance(continuation, dict):
        raise AcceptanceError("reconnect continuation projection is malformed")
    if continuation.get("schema_version") != INCREMENTAL_SCHEMA_VERSION:
        raise AcceptanceError("reconnect continuation projection schema is invalid")
    streams = continuation.get("streams")
    if not isinstance(streams, dict):
        raise AcceptanceError("reconnect continuation projection streams are malformed")
    members = {
        path: str(record["sha256"])
        for path, record in manifest_records.items()
        if path not in deferred_paths
    }
    reconstructed: dict[str, object] = {
        "schema_version": INCREMENTAL_SCHEMA_VERSION,
        "manifest_members": members,
        "streams": streams,
    }
    try:
        validate_incremental_continuation(reconstructed)
    except SealError as exc:
        raise AcceptanceError("reconnect continuation evidence is invalid") from exc
    return reconstructed


def _apply_reconnect_transition(
    state: _V2ChainState, transition: object
) -> None:
    if not isinstance(transition, dict):
        raise AcceptanceError("reconnect transition is malformed")
    added = transition.get("added")
    if not isinstance(added, list) or any(not isinstance(item, dict) for item in added):
        raise AcceptanceError("reconnect transition additions are malformed")
    for item in added:
        key = sha256_bytes(canonical_json(cast(Mapping[str, object], item)))
        if key in state.reconnect_transition_keys:
            raise AcceptanceError("reconnect transition is duplicated")
        state.reconnect_transition_keys.add(key)
    count = _integer(transition.get("stage_transition_count"), "reconnect transition count")
    if count != len(state.reconnect_transition_keys):
        raise AcceptanceError("reconnect transition count is invalid")
    if _digest(
        transition.get("stage_transition_aggregate_sha256"),
        "reconnect transition aggregate",
    ) != _key_aggregate(state.reconnect_transition_keys):
        raise AcceptanceError("reconnect transition aggregate is invalid")
    continuation = _continuation_projection(
        transition, state.manifest_records, state.deferred_manifest_paths
    )
    state.continuation_streams = cast(dict[str, object], continuation["streams"])


def _catalog_interval_timestamps(
    item: Mapping[str, object], field: str
) -> tuple[int, int]:
    return (
        _integer(item.get("started_at_utc_ns"), f"{field} start timestamp"),
        _integer(item.get("ended_at_utc_ns"), f"{field} end timestamp"),
    )


def _require_non_monotonic_catalog_blocker(
    started_at: int,
    ended_at: int,
    blocking_findings: set[str] | frozenset[str] | None,
    *,
    context: str,
) -> None:
    if ended_at <= started_at and (
        blocking_findings is None
        or "unsafe_wall_clock_backward" not in blocking_findings
    ):
        raise AcceptanceError(
            f"{context} non-monotonic completion is not bound to "
            "unsafe_wall_clock_backward"
        )


def _apply_catalog_transition(
    state: _V2ChainState,
    transition: object,
    *,
    current_observed_at_utc_ns: int | None = None,
    blocking_findings: set[str] | frozenset[str] | None = None,
) -> None:
    if not isinstance(transition, dict):
        raise AcceptanceError("Catalog transition is malformed")
    started = transition.get("started")
    completed = transition.get("completed")
    current_open = transition.get("current_open")
    terminal_events = transition.get("terminal_events")
    if (
        not isinstance(started, list)
        or any(not isinstance(item, dict) for item in started)
        or not isinstance(completed, list)
        or any(not isinstance(item, dict) for item in completed)
        or not isinstance(current_open, list)
        or any(not isinstance(item, dict) for item in current_open)
        or not isinstance(terminal_events, list)
        or any(not isinstance(item, str) for item in terminal_events)
    ):
        raise AcceptanceError("Catalog transition is malformed")

    previous_open = state.catalog_open
    open_state: dict[tuple[str, str, str, str], dict[str, object]] = {}
    for item in current_open:
        key = _catalog_key(item)
        if key in open_state:
            raise AcceptanceError("Catalog transition repeats an open identity")
        if item.get("timing") not in {"OPEN_AT_T0", "OPENED_IN_STAGE"}:
            raise AcceptanceError("Catalog transition open timing is invalid")
        started_at = _integer(item.get("started_at_utc_ns"), "Catalog open start timestamp")
        if state.stage_start_utc_ns is not None:
            if item.get("timing") == "OPEN_AT_T0" and started_at > state.stage_start_utc_ns:
                raise AcceptanceError("Catalog transition T0-open timing is invalid")
            if item.get("timing") == "OPENED_IN_STAGE" and started_at <= state.stage_start_utc_ns:
                raise AcceptanceError("Catalog transition stage-open timing is invalid")
        open_state[key] = dict(item)

    started_state: dict[tuple[str, str, str, str], dict[str, object]] = {}
    for item in started:
        key = _catalog_key(item)
        if key in started_state:
            raise AcceptanceError("Catalog transition repeats a start identity")
        if item.get("timing") != "OPENED_IN_STAGE" or key not in open_state:
            raise AcceptanceError("Catalog transition start is invalid")
        if key in previous_open:
            raise AcceptanceError("Catalog transition starts an already-open identity")
        if dict(item) != open_state[key]:
            raise AcceptanceError("Catalog transition start does not match current open")
        started_state[key] = dict(item)

    expected_started = set(open_state) - set(previous_open)
    if set(started_state) != expected_started:
        raise AcceptanceError("Catalog transition does not explain new open identities")
    for key in set(open_state) & set(previous_open):
        if open_state[key] != previous_open[key]:
            raise AcceptanceError("Catalog transition changes an open identity")

    completed_state: dict[tuple[str, str, str, str], dict[str, object]] = {}
    for item in completed:
        key = _catalog_key(item)
        if key in completed_state:
            raise AcceptanceError("Catalog transition repeats a completion identity")
        timing = item.get("timing")
        if timing not in {"CURRENT_STAGE", "CROSSES_T0"}:
            raise AcceptanceError("Catalog transition completion timing is invalid")
        started_at, ended_at = _catalog_interval_timestamps(item, "Catalog completion")
        if key in open_state:
            raise AcceptanceError("Catalog transition leaves a completed identity open")
        if key in state.catalog_interval_keys:
            raise AcceptanceError("Catalog transition completion is invalid")
        _require_non_monotonic_catalog_blocker(
            started_at,
            ended_at,
            blocking_findings,
            context="Catalog transition",
        )
        previous = previous_open.get(key)
        current_boundary = (
            current_observed_at_utc_ns
            if current_observed_at_utc_ns is not None
            else state.last_observed_utc_ns
        )
        if current_boundary is None or ended_at > current_boundary:
            raise AcceptanceError(
                "Catalog transition completion is after the current observation"
            )
        if previous is None:
            if timing != "CURRENT_STAGE":
                raise AcceptanceError("Catalog transition has an orphan crossing completion")
            previous_boundary = state.last_observed_utc_ns
            normal_window_invalid = (
                ended_at > started_at
                and (
                    previous_boundary is None
                    or started_at < previous_boundary
                )
            )
            non_monotonic_window_invalid = (
                ended_at <= started_at and started_at > current_boundary
            )
            if normal_window_invalid or non_monotonic_window_invalid:
                raise AcceptanceError(
                    "Catalog transition orphan completion is not between observations"
                )
        else:
            previous_started_at = _integer(
                previous.get("started_at_utc_ns"), "Catalog open start timestamp"
            )
            if started_at != previous_started_at:
                raise AcceptanceError("Catalog completion does not close the exact open identity")
            previous_timing = previous.get("timing")
            if previous_timing == "OPENED_IN_STAGE" and timing != "CURRENT_STAGE":
                raise AcceptanceError("Catalog transition has an invalid open completion timing")
            if previous_timing == "OPEN_AT_T0":
                stage_start = state.stage_start_utc_ns
                if (
                    stage_start is not None
                    and previous_started_at < stage_start
                    and timing != "CROSSES_T0"
                ):
                    raise AcceptanceError("Catalog transition has an invalid T0 crossing timing")
            if previous_timing not in {"OPEN_AT_T0", "OPENED_IN_STAGE"}:
                raise AcceptanceError("Catalog transition previous open timing is invalid")
        completed_state[key] = dict(item)

    disappeared = set(previous_open) - set(open_state)
    if not disappeared <= set(completed_state):
        raise AcceptanceError("Catalog open identity disappeared without completion authority")

    for key in completed_state:
        state.catalog_interval_keys.add(key)
        state.catalog_current_interval_keys.add(key)
    for event in terminal_events:
        if event in state.terminal_event_keys:
            raise AcceptanceError("Catalog terminal event is duplicated")
        state.terminal_event_keys.add(event)
    summary = transition.get("summary")
    if not isinstance(summary, dict):
        raise AcceptanceError("Catalog transition summary is malformed")
    if _integer(summary.get("open_count"), "Catalog open count") != len(open_state):
        raise AcceptanceError("Catalog transition open count is invalid")
    if _integer(
        summary.get("current_interval_count"), "Catalog current interval count"
    ) != len(state.catalog_current_interval_keys):
        raise AcceptanceError("Catalog current interval count is invalid")
    state.catalog_open = open_state
    if current_observed_at_utc_ns is not None:
        state.last_observed_utc_ns = current_observed_at_utc_ns


def _catalog_baseline_state(
    baseline: object,
    *,
    stage_start_utc_ns: int,
    blocking_findings: set[str] | frozenset[str] | None = None,
) -> tuple[
    dict[tuple[str, str, str, str], dict[str, object]],
    set[tuple[str, str, str, str]],
    set[tuple[str, str, str, str]],
    set[str],
]:
    if not isinstance(baseline, dict):
        raise AcceptanceError("Catalog baseline is malformed")
    open_items = baseline.get("open_at_t0")
    crossing = baseline.get("crossing_at_t0")
    closed = baseline.get("pre_t0_closed")
    terminal = baseline.get("terminal_events")
    if (
        not isinstance(open_items, list)
        or any(not isinstance(item, dict) for item in open_items)
        or not isinstance(crossing, list)
        or any(not isinstance(item, dict) for item in crossing)
        or not isinstance(closed, list)
        or any(not isinstance(item, dict) for item in closed)
        or not isinstance(terminal, list)
        or any(not isinstance(item, str) for item in terminal)
    ):
        raise AcceptanceError("Catalog baseline is malformed")
    open_state: dict[tuple[str, str, str, str], dict[str, object]] = {}
    for item in open_items:
        key = _catalog_key(item)
        if key in open_state:
            raise AcceptanceError("Catalog baseline repeats an open identity")
        if item.get("timing") != "OPEN_AT_T0":
            raise AcceptanceError("Catalog baseline open timing is invalid")
        started_at = _integer(item.get("started_at_utc_ns"), "Catalog baseline open timestamp")
        if started_at > stage_start_utc_ns:
            raise AcceptanceError("Catalog baseline OPEN_AT_T0 starts after stage T0")
        open_state[key] = dict(item)
    interval_keys: set[tuple[str, str, str, str]] = set()
    current_interval_keys: set[tuple[str, str, str, str]] = set()
    for item in crossing:
        key = _catalog_key(item)
        if item.get("timing") != "CROSSES_T0":
            raise AcceptanceError("Catalog baseline crossing timing is invalid")
        started_at, ended_at = _catalog_interval_timestamps(item, "Catalog baseline crossing")
        if not started_at < stage_start_utc_ns <= ended_at:
            raise AcceptanceError("Catalog baseline crossing is not between stage T0 boundaries")
        _require_non_monotonic_catalog_blocker(
            started_at,
            ended_at,
            blocking_findings,
            context="Catalog baseline",
        )
        if key in interval_keys:
            raise AcceptanceError("Catalog baseline repeats an interval identity")
        interval_keys.add(key)
        current_interval_keys.add(key)
    for item in closed:
        key = _catalog_key(item)
        if key in interval_keys:
            raise AcceptanceError("Catalog baseline repeats an interval identity")
        _started_at, ended_at = _catalog_interval_timestamps(
            item, "Catalog baseline closed interval"
        )
        if ended_at >= stage_start_utc_ns:
            raise AcceptanceError(
                "Catalog baseline pre_t0_closed interval is not before stage T0"
            )
        interval_keys.add(key)
    if set(open_state) & interval_keys:
        raise AcceptanceError("Catalog baseline identity is both open and closed")
    if len(set(terminal)) != len(terminal):
        raise AcceptanceError("Catalog baseline repeats a terminal event")
    return open_state, interval_keys, current_interval_keys, set(terminal)


def _v2_chain_state_from_start(
    start: Mapping[str, object], *, require_open_finding: bool = True
) -> _V2ChainState:
    start_utc_ns = _integer(start.get("observed_at_utc_ns"), "stage-start UTC timestamp")
    manifest_records = _manifest_baseline_from_evidence(start.get("manifest_baseline"))
    raw_absences = _raw_absence_baseline_from_evidence(start.get("raw_absence_baseline"))
    reconnect_baseline = start.get("reconnect_baseline")
    if not isinstance(reconnect_baseline, dict):
        raise AcceptanceError("reconnect baseline is malformed")
    continuation = reconnect_baseline.get("continuation")
    if not isinstance(continuation, dict):
        raise AcceptanceError("reconnect baseline continuation is malformed")
    streams = continuation.get("streams")
    if not isinstance(streams, dict):
        raise AcceptanceError("reconnect baseline streams are malformed")
    start_findings = start.get("blocking_findings")
    if not isinstance(start_findings, list) or any(
        not isinstance(item, str) for item in start_findings
    ):
        raise AcceptanceError("stage-start blocking findings are malformed")
    if start_findings != sorted(set(start_findings)):
        raise AcceptanceError("stage-start blocking findings are not canonical")
    details = start.get("new_finding_details")
    if not isinstance(details, dict) or any(
        not isinstance(key, str) for key in details
    ):
        raise AcceptanceError("stage-start finding details are malformed")
    if set(details) != set(start_findings):
        raise AcceptanceError("stage-start finding details are incomplete")
    open_state, interval_keys, current_interval_keys, terminal_keys = _catalog_baseline_state(
        start.get("catalog_baseline"),
        stage_start_utc_ns=start_utc_ns,
        blocking_findings=set(start_findings),
    )
    state = _V2ChainState(
        manifest_records=manifest_records,
        raw_absences=raw_absences,
        deferred_manifest_paths=set(),
        continuation_streams={str(name): value for name, value in streams.items()},
        reconnect_transition_keys=set(),
        catalog_open=open_state,
        catalog_interval_keys=interval_keys,
        # Stage-start catalog_transition carries the same terminal authority
        # summarized by catalog_baseline.  Seed it only through that
        # transition so the first observation is not mistaken for a duplicate.
        terminal_event_keys=set(),
        known_findings=set(start_findings),
        finding_detail_keys=set(details),
        catalog_current_interval_keys=current_interval_keys,
        stage_start_utc_ns=start_utc_ns,
        last_observed_utc_ns=start_utc_ns,
    )
    state.manifest_records, state.deferred_manifest_paths, state.invalid_manifest_authority = (
        _apply_manifest_transition(
            state.manifest_records,
            start.get("manifest_transition"),
            blocking_findings=set(start_findings),
        )
    )
    _apply_reconnect_transition(state, start.get("reconnect_transition"))
    _apply_catalog_transition(
        state,
        start.get("catalog_transition"),
        current_observed_at_utc_ns=start_utc_ns,
        blocking_findings=set(start_findings),
    )
    if not set(terminal_keys) <= state.terminal_event_keys:
        raise AcceptanceError("Catalog baseline terminal authority is not published")
    if (
        require_open_finding
        and state.catalog_open
        and "unresolved_discontinuity" not in state.known_findings
    ):
        raise AcceptanceError("Catalog open state is not bound to unresolved_discontinuity")
    if state.terminal_event_keys and "terminal_service_or_core_failure" not in state.known_findings:
        raise AcceptanceError(
            "Catalog terminal authority is not bound to terminal_service_or_core_failure"
        )
    return state


def _v3_chain_state_from_start(start: Mapping[str, object]) -> _V2ChainState:
    """Reconstruct v3 T0 state without v2's intermediate OPEN blocker rule."""

    return _v2_chain_state_from_start(start, require_open_finding=False)


def _v4_chain_state_from_start(start: Mapping[str, object]) -> _V2ChainState:
    """Reconstruct V4 T0 state and independently validate its empty episode."""

    state = _v3_chain_state_from_start(start)
    observed_utc = _integer(start.get("observed_at_utc_ns"), "stage-start UTC timestamp")
    observed_boot = _integer(
        start.get("observed_at_boottime_ns"), "stage-start BOOTTIME timestamp"
    )
    readiness = start.get("readiness")
    if not isinstance(readiness, dict):
        raise AcceptanceError("V4 stage-start readiness is malformed")
    reasons = readiness.get("reasons")
    if (
        readiness.get("schema_version") != "deployment-readiness.v1"
        or readiness.get("state") not in {"READY", "NOT_READY", "FAILED"}
        or not isinstance(reasons, list)
        or any(not isinstance(item, str) for item in reasons)
        or not isinstance(readiness.get("evidence"), dict)
    ):
        raise AcceptanceError("V4 stage-start readiness authority is malformed")
    episode = _V4EpisodeState()
    expected_findings = _v4_episode_observe(
        episode,
        evaluator=_V4VerifierEvaluator(start),
        readiness=readiness,
        observed_at_utc_ns=observed_utc,
        observed_at_boottime_ns=observed_boot,
        allow_start=False,
    )
    if expected_findings:
        # An ineligible stage-start is rejected by the completed-stage and
        # resume gates, but keeping this check here prevents a forged eligible
        # start from smuggling a pre-T0 NOT_READY into a V4 chain.
        raise AcceptanceError("V4 stage-start readiness cannot establish T0")
    expected_episode = episode.public(observed_boottime_ns=observed_boot)
    published_episode = _validate_v4_episode_document(
        start.get(_V4_EPISODE_FIELD), observed_boottime_ns=observed_boot
    )
    if published_episode != expected_episode:
        raise AcceptanceError("V4 stage-start readiness episode is not reconstructible")
    state.readiness_episode = episode
    return state


class _V4VerifierEvaluator:
    """Minimal evaluator view used by the streaming verifier's classifier."""

    def __init__(self, start: Mapping[str, object]) -> None:
        readiness = start.get("readiness")
        evidence = readiness.get("evidence") if isinstance(readiness, dict) else None
        service_state = evidence.get("service_state") if isinstance(evidence, dict) else None
        products = service_state.get("products") if isinstance(service_state, dict) else None
        configured: set[tuple[str, str]] = set()
        if isinstance(products, dict):
            for market, symbols in products.items():
                if isinstance(market, str) and isinstance(symbols, dict):
                    configured.update(
                        (market, symbol)
                        for symbol in symbols
                        if isinstance(symbol, str)
                    )
        self.expected_products = frozenset(configured)


def _sample_chain_v2(
    stage_root: Path,
    *,
    start: Mapping[str, object],
    start_sha: str,
    identity: DeploymentIdentity,
    require_eligible: bool,
) -> _V2ChainState:
    stage = str(start["stage"])
    run_id = str(start["run_id"])
    state = _v2_chain_state_from_start(start)
    expected_previous: str | None = None
    previous_boottime = _integer(
        start.get("observed_at_boottime_ns"), "stage-start BOOTTIME timestamp"
    )
    paths = sorted(stage_root.glob("sample-*.json"))
    for ordinal, sample_path in enumerate(paths):
        if sample_path.name != f"sample-{ordinal:08d}.json":
            raise AcceptanceError("sample ordinals are missing, duplicated, or malformed")
        sample, sample_sha = _read_published(sample_path)
        if sample.get("schema_version") != PREVIOUS_SCHEMA_VERSION:
            raise AcceptanceError("sample schema cannot be mixed into a v2 chain")
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
        if _integer(sample.get("sample_ordinal"), "sample ordinal") != ordinal:
            raise AcceptanceError("sample ordinal is invalid")
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
        observed_utc = _integer(sample.get("observed_at_utc_ns"), "sample UTC timestamp")
        findings = sample.get("blocking_findings")
        if not isinstance(findings, list) or any(
            not isinstance(item, str) for item in findings
        ):
            raise AcceptanceError("sample blocking findings are malformed")
        if findings != sorted(set(findings)):
            raise AcceptanceError("sample blocking findings are not canonical")
        sample_findings = set(findings)
        new_details = sample.get("new_finding_details")
        if not isinstance(new_details, dict):
            raise AcceptanceError("sample finding details are malformed")
        for key in new_details:
            if not isinstance(key, str) or key not in sample_findings:
                raise AcceptanceError("sample finding detail is not a blocker")
            if key in state.finding_detail_keys:
                raise AcceptanceError("sample finding detail is repeated")
        if sample.get("observer_status") != "COMPLETE":
            raise AcceptanceError("sample observer status is invalid")

        manifest_transition = sample.get("manifest_transition")
        if state.invalid_manifest_authority:
            if (
                isinstance(manifest_transition, dict)
                and manifest_transition.get("state") == "ANOMALY"
                and "manifest_byte_mutation_or_loss" not in sample_findings
            ):
                raise AcceptanceError(
                    "manifest anomaly is not bound to manifest_byte_mutation_or_loss"
                )
        else:
            state.manifest_records, state.deferred_manifest_paths, anomaly = (
                _apply_manifest_transition(
                    state.manifest_records,
                    manifest_transition,
                    blocking_findings=sample_findings,
                )
            )
            state.invalid_manifest_authority = anomaly
        state.raw_absences = _apply_raw_absence_transition(
            state.raw_absences, sample.get("raw_absence_transition")
        )
        _apply_reconnect_transition(state, sample.get("reconnect_transition"))
        _apply_catalog_transition(
            state,
            sample.get("catalog_transition"),
            current_observed_at_utc_ns=observed_utc,
            blocking_findings=sample_findings,
        )
        if state.catalog_open and "unresolved_discontinuity" not in sample_findings:
            raise AcceptanceError("Catalog open state is not bound to unresolved_discontinuity")
        if state.terminal_event_keys and "terminal_service_or_core_failure" not in sample_findings:
            raise AcceptanceError(
                "Catalog terminal authority is not bound to terminal_service_or_core_failure"
            )
        if not state.known_findings <= sample_findings:
            raise AcceptanceError("sample blocking findings are not monotonic")
        expected_new_findings = sample_findings - state.known_findings
        if set(new_details) != expected_new_findings:
            raise AcceptanceError("sample finding details are incomplete")
        state.finding_detail_keys.update(str(key) for key in new_details)
        state.known_findings = sample_findings
        if state.invalid_manifest_authority and require_eligible:
            raise AcceptanceError("completed stage contains invalid manifest authority")
        if require_eligible and findings:
            raise AcceptanceError("completed stage contains ineligible sample findings")
        if require_eligible and sample.get("result") != "PASS_CANDIDATE":
            raise AcceptanceError("completed stage contains an ineligible sample result")
        state.last = _LastSampleMetadata(
            ordinal=ordinal,
            sha256=sample_sha,
            utc_ns=observed_utc,
            boottime_ns=boottime,
            result=str(sample.get("result")),
            findings=frozenset(sample_findings),
        )
        expected_previous = sample_sha
    return state


def _sample_chain_v3(
    stage_root: Path,
    *,
    start: Mapping[str, object],
    start_sha: str,
    identity: DeploymentIdentity,
    require_eligible: bool,
    schema_version: str = SCHEMA_VERSION,
) -> _V2ChainState:
    """Stream a V3 or V4 chain with transient OPEN treated as causal state."""

    stage = str(start["stage"])
    run_id = str(start["run_id"])
    state = (
        _v4_chain_state_from_start(start)
        if schema_version == V4_SCHEMA_VERSION
        else _v3_chain_state_from_start(start)
    )
    v4_evaluator = _V4VerifierEvaluator(start) if schema_version == V4_SCHEMA_VERSION else None
    expected_previous: str | None = None
    previous_boottime = _integer(
        start.get("observed_at_boottime_ns"), "stage-start BOOTTIME timestamp"
    )
    paths = sorted(stage_root.glob("sample-*.json"))
    for ordinal, sample_path in enumerate(paths):
        if sample_path.name != f"sample-{ordinal:08d}.json":
            raise AcceptanceError("sample ordinals are missing, duplicated, or malformed")
        sample, sample_sha = _read_published(sample_path)
        if sample.get("schema_version") != schema_version:
            if schema_version == SCHEMA_VERSION:
                raise AcceptanceError("sample schema cannot be mixed into a v3 chain")
            raise AcceptanceError("sample schema cannot be mixed into a v4 chain")
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
        if _integer(sample.get("sample_ordinal"), "sample ordinal") != ordinal:
            raise AcceptanceError("sample ordinal is invalid")
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
        observed_utc = _integer(sample.get("observed_at_utc_ns"), "sample UTC timestamp")
        findings = sample.get("blocking_findings")
        if not isinstance(findings, list) or any(
            not isinstance(item, str) for item in findings
        ):
            raise AcceptanceError("sample blocking findings are malformed")
        if findings != sorted(set(findings)):
            raise AcceptanceError("sample blocking findings are not canonical")
        sample_findings = set(findings)
        if schema_version == V4_SCHEMA_VERSION:
            if state.readiness_episode is None or v4_evaluator is None:
                raise AcceptanceError("V4 readiness episode state is unavailable")
            readiness = sample.get("readiness")
            if not isinstance(readiness, dict):
                raise AcceptanceError("V4 sample readiness is malformed")
            if (
                readiness.get("schema_version") != "deployment-readiness.v1"
                or readiness.get("state") not in {"READY", "NOT_READY", "FAILED"}
                or not isinstance(readiness.get("reasons"), list)
                or any(not isinstance(item, str) for item in readiness["reasons"])
                or not isinstance(readiness.get("evidence"), dict)
            ):
                raise AcceptanceError("V4 sample readiness authority is malformed")
            expected_readiness_findings = _v4_episode_observe(
                state.readiness_episode,
                evaluator=v4_evaluator,
                readiness=readiness,
                observed_at_utc_ns=observed_utc,
                observed_at_boottime_ns=boottime,
                allow_start=True,
            )
            published_episode = _validate_v4_episode_document(
                sample.get(_V4_EPISODE_FIELD), observed_boottime_ns=boottime
            )
            expected_episode = state.readiness_episode.public(
                observed_boottime_ns=boottime
            )
            if published_episode != expected_episode:
                raise AcceptanceError("V4 readiness episode cannot be reconstructed")
            historical_readiness_blockers = (
                state.known_findings & _V4_READINESS_BLOCKERS
            )
            allowed_readiness_blockers = historical_readiness_blockers | set(
                expected_readiness_findings
            )
            actual_readiness_blockers = sample_findings & _V4_READINESS_BLOCKERS
            unexpected_readiness_blockers = (
                actual_readiness_blockers - allowed_readiness_blockers
            )
            if unexpected_readiness_blockers:
                raise AcceptanceError("V4 readiness blocker is unsupported")
            missing_readiness_blockers = (
                allowed_readiness_blockers - actual_readiness_blockers
            )
            if missing_readiness_blockers:
                raise AcceptanceError("V4 readiness blocker is missing")
            expected_sample_result = (
                "FAIL"
                if any(item not in _V4_SOFT_FINDINGS for item in sample_findings)
                else ("INCOMPLETE" if sample_findings else "PASS_CANDIDATE")
            )
            if sample.get("result") != expected_sample_result:
                raise AcceptanceError("V4 sample result is not bound to reconstructed findings")
        if "unresolved_discontinuity" in sample_findings:
            raise AcceptanceError(
                "v3 sample contains terminal-only unresolved_discontinuity"
            )
        new_details = sample.get("new_finding_details")
        if not isinstance(new_details, dict):
            raise AcceptanceError("sample finding details are malformed")
        for key in new_details:
            if not isinstance(key, str) or key not in sample_findings:
                raise AcceptanceError("sample finding detail is not a blocker")
            if key in state.finding_detail_keys:
                raise AcceptanceError("sample finding detail is repeated")
        if sample.get("observer_status") != "COMPLETE":
            raise AcceptanceError("sample observer status is invalid")

        manifest_transition = sample.get("manifest_transition")
        if state.invalid_manifest_authority:
            if (
                isinstance(manifest_transition, dict)
                and manifest_transition.get("state") == "ANOMALY"
                and "manifest_byte_mutation_or_loss" not in sample_findings
            ):
                raise AcceptanceError(
                    "manifest anomaly is not bound to manifest_byte_mutation_or_loss"
                )
        else:
            state.manifest_records, state.deferred_manifest_paths, anomaly = (
                _apply_manifest_transition(
                    state.manifest_records,
                    manifest_transition,
                    blocking_findings=sample_findings,
                )
            )
            state.invalid_manifest_authority = anomaly
        state.raw_absences = _apply_raw_absence_transition(
            state.raw_absences, sample.get("raw_absence_transition")
        )
        _apply_reconnect_transition(state, sample.get("reconnect_transition"))
        _apply_catalog_transition(
            state,
            sample.get("catalog_transition"),
            current_observed_at_utc_ns=observed_utc,
            blocking_findings=sample_findings,
        )
        if state.terminal_event_keys and "terminal_service_or_core_failure" not in sample_findings:
            raise AcceptanceError(
                "Catalog terminal authority is not bound to terminal_service_or_core_failure"
            )
        if not state.known_findings <= sample_findings:
            raise AcceptanceError("sample blocking findings are not monotonic")
        expected_new_findings = sample_findings - state.known_findings
        if set(new_details) != expected_new_findings:
            raise AcceptanceError("sample finding details are incomplete")
        state.finding_detail_keys.update(str(key) for key in new_details)
        state.known_findings = sample_findings
        if state.invalid_manifest_authority and require_eligible:
            raise AcceptanceError("completed stage contains invalid manifest authority")
        if require_eligible and findings:
            raise AcceptanceError("completed stage contains ineligible sample findings")
        if require_eligible and sample.get("result") != "PASS_CANDIDATE":
            raise AcceptanceError("completed stage contains an ineligible sample result")
        state.last = _LastSampleMetadata(
            ordinal=ordinal,
            sha256=sample_sha,
            utc_ns=observed_utc,
            boottime_ns=boottime,
            result=str(sample.get("result")),
            findings=frozenset(sample_findings),
        )
        expected_previous = sample_sha
    return state


def _sample_chain(
    stage_root: Path,
    *,
    start: Mapping[str, object],
    start_sha: str,
    identity: DeploymentIdentity,
    require_eligible: bool,
) -> _V2ChainState:
    """Dispatch only after the document's explicit semantic generation."""

    schema_version = start.get("schema_version")
    if schema_version == PREVIOUS_SCHEMA_VERSION:
        return _sample_chain_v2(
            stage_root,
            start=start,
            start_sha=start_sha,
            identity=identity,
            require_eligible=require_eligible,
        )
    if schema_version == SCHEMA_VERSION:
        return _sample_chain_v3(
            stage_root,
            start=start,
            start_sha=start_sha,
            identity=identity,
            require_eligible=require_eligible,
        )
    if schema_version == V4_SCHEMA_VERSION:
        return _sample_chain_v3(
            stage_root,
            start=start,
            start_sha=start_sha,
            identity=identity,
            require_eligible=require_eligible,
            schema_version=V4_SCHEMA_VERSION,
        )
    raise AcceptanceError("unsupported stage chain schema")


def _sample_chain_v1(
    stage_root: Path,
    *,
    start: Mapping[str, object],
    start_sha: str,
    identity: DeploymentIdentity,
    require_eligible: bool,
) -> tuple[dict[str, object] | None, str | None, dict[str, object], int]:
    """Stream the immutable v1 chain without changing its historical meaning."""

    stage = str(start["stage"])
    run_id = str(start["run_id"])
    expected_previous: str | None = None
    last_sample: dict[str, object] | None = None
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
        if sample.get("schema_version") != LEGACY_SCHEMA_VERSION:
            raise AcceptanceError("legacy sample schema is invalid")
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
            current_members.get(path) != digest for path, digest in previous_members.items()
        )
        if lost_manifest_authority and "manifest_byte_mutation_or_loss" not in sample_findings:
            raise AcceptanceError("sample continuation lost manifest authority")
        previous_members = current_members
        last_sample = sample
        expected_previous = sample_sha
    return last_sample, expected_previous, last_continuation, len(paths)


def _verify_completed_stage_v2(
    stage_root: Path,
    identity: DeploymentIdentity,
    *,
    start: Mapping[str, object],
    start_sha: str,
    expected_stage: str | None,
) -> tuple[dict[str, object], str]:
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
    state = _sample_chain_v2(
        stage_root,
        start=start,
        start_sha=start_sha,
        identity=identity,
        require_eligible=True,
    )
    if state.last is None:
        raise AcceptanceError("completed stage has no canonical samples")
    final, final_sha = _read_published(stage_root / "stage-final.json")
    if final.get("schema_version") != PREVIOUS_SCHEMA_VERSION:
        raise AcceptanceError("non-v2 final cannot terminate a v2 stage")
    if final.get("evidence_kind") != "stage-final":
        raise AcceptanceError("stage-final evidence kind is invalid")
    _chain_identity(final, identity=identity, stage=stage, run_id=run_id)
    last = state.last
    if (
        final.get("stage_start_evidence_sha256") != start_sha
        or final.get("previous_sample_sha256") != last.sha256
        or final.get("last_sample_sha256") != last.sha256
        or final.get("last_sample_ordinal") != last.ordinal
        or final.get("eligible_for_next_stage") is not True
        or final.get("prior_stage_evidence_sha256") != prior_digest
        or final.get("boot_id") != start.get("boot_id")
        or final.get("systemd_process_incarnation")
        != start.get("systemd_process_incarnation")
        or final.get("service_instance_id") != start.get("service_instance_id")
        or final.get("observed_at_utc_ns") != last.utc_ns
        or final.get("observed_at_boottime_ns") != last.boottime_ns
        or final.get("blocking_findings") != []
        or final.get("new_finding_details") != {}
        or final.get("result") != "PASS_CANDIDATE"
        or final.get("observer_status") != "FINALIZED"
    ):
        raise AcceptanceError("stage-final is not an eligible chain terminus")
    required = _integer(final.get("required_duration_ns"), "required duration")
    elapsed = _integer(final.get("elapsed_boottime_ns"), "elapsed duration")
    expected_required = STAGE_DURATION_NS[stage]
    expected_elapsed = last.boottime_ns - _integer(
        start.get("observed_at_boottime_ns"), "stage-start BOOTTIME timestamp"
    )
    if required != expected_required or elapsed != expected_elapsed or elapsed < required:
        raise AcceptanceError("stage duration authority is invalid")
    _resolve_stage_predecessor(
        stage_root,
        identity=identity,
        stage=stage,
        prior_digest=prior_digest,
        required_schema=PREVIOUS_SCHEMA_VERSION,
    )
    return final, final_sha


def _verify_completed_stage_v3(
    stage_root: Path,
    identity: DeploymentIdentity,
    *,
    start: Mapping[str, object],
    start_sha: str,
    expected_stage: str | None,
) -> tuple[dict[str, object], str]:
    """Verify a v3 chain with terminal-only unresolved Catalog semantics."""

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
    state = _sample_chain_v3(
        stage_root,
        start=start,
        start_sha=start_sha,
        identity=identity,
        require_eligible=True,
    )
    if state.last is None:
        raise AcceptanceError("completed stage has no canonical samples")
    final, final_sha = _read_published(stage_root / "stage-final.json")
    if final.get("schema_version") != SCHEMA_VERSION:
        raise AcceptanceError("non-v3 final cannot terminate a v3 stage")
    if final.get("evidence_kind") != "stage-final":
        raise AcceptanceError("stage-final evidence kind is invalid")
    _chain_identity(final, identity=identity, stage=stage, run_id=run_id)

    last = state.last
    terminal_detail = _first_catalog_open_detail(state.catalog_open)
    expected_findings = set(state.known_findings)
    expected_details: dict[str, object] = {}
    if terminal_detail is not None:
        expected_findings.add("unresolved_discontinuity")
        expected_details["unresolved_discontinuity"] = terminal_detail
    required = _integer(final.get("required_duration_ns"), "required duration")
    elapsed = _integer(final.get("elapsed_boottime_ns"), "elapsed duration")
    expected_required = STAGE_DURATION_NS[stage]
    expected_elapsed = last.boottime_ns - _integer(
        start.get("observed_at_boottime_ns"), "stage-start BOOTTIME timestamp"
    )
    if required != expected_required or elapsed != expected_elapsed or elapsed < required:
        raise AcceptanceError("stage duration authority is invalid")
    expected_result = (
        "INCOMPLETE"
        if elapsed < required
        else ("FAIL" if terminal_detail is not None else "PASS_CANDIDATE")
    )
    expected_eligible = expected_result == "PASS_CANDIDATE" and not expected_findings
    if (
        final.get("stage_start_evidence_sha256") != start_sha
        or final.get("previous_sample_sha256") != last.sha256
        or final.get("last_sample_sha256") != last.sha256
        or final.get("last_sample_ordinal") != last.ordinal
        or final.get("prior_stage_evidence_sha256") != prior_digest
        or final.get("boot_id") != start.get("boot_id")
        or final.get("systemd_process_incarnation")
        != start.get("systemd_process_incarnation")
        or final.get("service_instance_id") != start.get("service_instance_id")
        or final.get("observed_at_utc_ns") != last.utc_ns
        or final.get("observed_at_boottime_ns") != last.boottime_ns
        or final.get("blocking_findings") != sorted(expected_findings)
        or final.get("new_finding_details") != expected_details
        or final.get("result") != expected_result
        or final.get("eligible_for_next_stage") is not expected_eligible
        or final.get("observer_status") != "FINALIZED"
    ):
        raise AcceptanceError(
            "stage-final does not match reconstructed v3 terminal state/terminus"
        )
    _resolve_stage_predecessor(
        stage_root,
        identity=identity,
        stage=stage,
        prior_digest=prior_digest,
        required_schema=SCHEMA_VERSION,
    )
    if not expected_eligible:
        raise AcceptanceError("stage-final is not an eligible v3 chain terminus")
    return final, final_sha


def _verify_completed_stage_v4(
    stage_root: Path,
    identity: DeploymentIdentity,
    *,
    start: Mapping[str, object],
    start_sha: str,
    expected_stage: str | None,
) -> tuple[dict[str, object], str]:
    """Verify V4 by reconstructing readiness recovery from the sample stream."""

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
    state = _sample_chain_v3(
        stage_root,
        start=start,
        start_sha=start_sha,
        identity=identity,
        require_eligible=False,
        schema_version=V4_SCHEMA_VERSION,
    )
    if state.last is None or state.readiness_episode is None:
        raise AcceptanceError("completed V4 stage has no canonical samples")
    final, final_sha = _read_published(stage_root / "stage-final.json")
    if final.get("schema_version") != V4_SCHEMA_VERSION:
        raise AcceptanceError("non-v4 final cannot terminate a v4 stage")
    if final.get("evidence_kind") != "stage-final":
        raise AcceptanceError("stage-final evidence kind is invalid")
    _chain_identity(final, identity=identity, stage=stage, run_id=run_id)
    last = state.last
    last_sample, _last_sample_sha = _read_published(
        stage_root / f"sample-{last.ordinal:08d}.json"
    )
    expected_findings = set(state.known_findings)
    expected_details: dict[str, object] = {}
    terminal_detail = _first_catalog_open_detail(state.catalog_open)
    readiness = last_sample.get("readiness")
    if not isinstance(readiness, dict):
        raise AcceptanceError("V4 terminal sample readiness is malformed")
    if readiness.get("state") != "READY":
        expected_findings.add("readiness_not_ready")
        expected_details["readiness_not_ready"] = {
            "observed_at_utc_ns": last.utc_ns
        }
    if terminal_detail is not None:
        expected_findings.add("unresolved_discontinuity")
        expected_details["unresolved_discontinuity"] = terminal_detail
    required = _integer(final.get("required_duration_ns"), "required duration")
    elapsed = _integer(final.get("elapsed_boottime_ns"), "elapsed duration")
    expected_required = STAGE_DURATION_NS[stage]
    expected_elapsed = last.boottime_ns - _integer(
        start.get("observed_at_boottime_ns"), "stage-start BOOTTIME timestamp"
    )
    if required != expected_required or elapsed != expected_elapsed:
        raise AcceptanceError("stage duration authority is invalid")
    if readiness.get("state") != "READY" or terminal_detail is not None:
        expected_result = "FAIL"
    elif elapsed < required:
        expected_result = "INCOMPLETE"
    elif expected_findings - _V4_SOFT_FINDINGS:
        expected_result = "FAIL"
    elif expected_findings:
        expected_result = "INCOMPLETE"
    else:
        expected_result = "PASS_CANDIDATE"
    expected_eligible = expected_result == "PASS_CANDIDATE" and not expected_findings
    expected_episode = state.readiness_episode.public(observed_boottime_ns=last.boottime_ns)
    _validate_v4_episode_document(
        final.get(_V4_EPISODE_FIELD), observed_boottime_ns=last.boottime_ns
    )
    if (
        final.get("stage_start_evidence_sha256") != start_sha
        or final.get("previous_sample_sha256") != last.sha256
        or final.get("last_sample_sha256") != last.sha256
        or final.get("last_sample_ordinal") != last.ordinal
        or final.get("prior_stage_evidence_sha256") != prior_digest
        or final.get("boot_id") != start.get("boot_id")
        or final.get("systemd_process_incarnation")
        != start.get("systemd_process_incarnation")
        or final.get("service_instance_id") != start.get("service_instance_id")
        or final.get("observed_at_utc_ns") != last.utc_ns
        or final.get("observed_at_boottime_ns") != last.boottime_ns
        or final.get("blocking_findings") != sorted(expected_findings)
        or final.get("new_finding_details") != expected_details
        or final.get("result") != expected_result
        or final.get("eligible_for_next_stage") is not expected_eligible
        or final.get("observer_status") != "FINALIZED"
        or final.get(_V4_EPISODE_FIELD) != expected_episode
        or final.get("readiness") != last_sample.get("readiness")
        or final.get("catalog_integrity") != last_sample.get("catalog_integrity")
        or final.get("capacity") != last_sample.get("capacity")
    ):
        raise AcceptanceError(
            "stage-final does not match reconstructed v4 terminal state/terminus"
        )
    _resolve_stage_predecessor(
        stage_root,
        identity=identity,
        stage=stage,
        prior_digest=prior_digest,
        required_schema=V4_SCHEMA_VERSION,
    )
    if not expected_eligible:
        raise AcceptanceError("stage-final is not an eligible v4 chain terminus")
    return final, final_sha


def _verify_completed_stage_v1(
    stage_root: Path,
    identity: DeploymentIdentity,
    *,
    start: Mapping[str, object],
    start_sha: str,
    expected_stage: str | None,
) -> tuple[dict[str, object], str]:
    """Read historical v1 chains without rewriting or converting them."""

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
    last_sample, last_sample_sha, _continuation, _sample_count = _sample_chain_v1(
        stage_root,
        start=start,
        start_sha=start_sha,
        identity=identity,
        require_eligible=True,
    )
    if last_sample is None or last_sample_sha is None:
        raise AcceptanceError("completed stage has no canonical samples")
    final, final_sha = _read_published(stage_root / "stage-final.json")
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
        required_schema=LEGACY_SCHEMA_VERSION,
    )
    return final, final_sha


def verify_completed_stage(
    stage_root: Path,
    identity: DeploymentIdentity,
    *,
    expected_stage: str | None = None,
) -> tuple[dict[str, object], str]:
    """Dispatch to the immutable verifier for the document's schema generation."""

    start, start_sha = _read_published(stage_root / "stage-start.json")
    if start.get("schema_version") == LEGACY_SCHEMA_VERSION:
        return _verify_completed_stage_v1(
            stage_root,
            identity,
            start=start,
            start_sha=start_sha,
            expected_stage=expected_stage,
        )
    if start.get("schema_version") == PREVIOUS_SCHEMA_VERSION:
        return _verify_completed_stage_v2(
            stage_root,
            identity,
            start=start,
            start_sha=start_sha,
            expected_stage=expected_stage,
        )
    if start.get("schema_version") == SCHEMA_VERSION:
        return _verify_completed_stage_v3(
            stage_root,
            identity,
            start=start,
            start_sha=start_sha,
            expected_stage=expected_stage,
        )
    if start.get("schema_version") == V4_SCHEMA_VERSION:
        return _verify_completed_stage_v4(
            stage_root,
            identity,
            start=start,
            start_sha=start_sha,
            expected_stage=expected_stage,
        )
    raise AcceptanceError("unsupported completed-stage schema")


def _verify_readiness_predecessor(
    path: Path,
    identity: DeploymentIdentity,
    *,
    expected_digest: str | None = None,
    expected_schema: str | None = None,
) -> tuple[dict[str, object], str]:
    if path.name != "readiness-result.json":
        raise AcceptanceError("readiness predecessor must be canonical readiness-result.json")
    document, digest = _read_published(path)
    if expected_digest is not None and digest != expected_digest:
        raise AcceptanceError("readiness predecessor digest does not match")
    if expected_schema is not None and document.get("schema_version") != expected_schema:
        raise AcceptanceError("readiness predecessor schema cannot authorize this stage")
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
    if expected_schema == V4_SCHEMA_VERSION:
        observed_boot = _integer(
            document.get("observed_at_boottime_ns"), "readiness BOOTTIME timestamp"
        )
        if _validate_v4_episode_document(
            document.get(_V4_EPISODE_FIELD), observed_boottime_ns=observed_boot
        ) != _empty_v4_episode():
            raise AcceptanceError("V4 readiness predecessor has a non-empty episode")
    prior_identity_digest = _digest(
        document.get("prior_stage_evidence_sha256"),
        "readiness identity predecessor digest",
    )
    identity_path = path.parent / "identity-result.json"
    _identity_document, identity_digest = read_identity_evidence(
        identity_path,
        identity,
        expected_schema=expected_schema,
    )
    if identity_digest != prior_identity_digest:
        raise AcceptanceError("readiness identity predecessor digest does not match")
    return document, digest


def _resolve_stage_predecessor(
    stage_root: Path,
    *,
    identity: DeploymentIdentity,
    stage: str,
    prior_digest: str,
    required_schema: str | None = None,
) -> tuple[dict[str, object], str]:
    """Resolve the actual predecessor beneath this operator evidence root."""

    if stage == "2h":
        return _verify_readiness_predecessor(
            stage_root.parent / "readiness-result.json",
            identity,
            expected_digest=prior_digest,
            expected_schema=required_schema,
        )

    previous_stage = STAGE_NAMES[STAGE_NAMES.index(stage) - 1]
    evidence_root = stage_root.parent
    candidates = sorted(evidence_root.glob(f"{previous_stage}-*/stage-final.json"))
    matches: list[tuple[dict[str, object], str]] = []
    for candidate in candidates:
        _document, digest = _read_published(candidate)
        if digest != prior_digest:
            continue
        if required_schema is not None and _document.get("schema_version") != required_schema:
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
    path: Path,
    identity: DeploymentIdentity,
    stage: str,
    *,
    schema_version: str = SCHEMA_VERSION,
) -> tuple[dict[str, object], str]:
    _validate_stage(stage)
    if schema_version not in {SCHEMA_VERSION, V4_SCHEMA_VERSION}:
        raise AcceptanceError("unsupported current acceptance schema")
    if stage == "2h":
        return _verify_readiness_predecessor(
            path, identity, expected_schema=schema_version
        )
    previous_stage = STAGE_NAMES[STAGE_NAMES.index(stage) - 1]
    if path.name != "stage-final.json":
        raise AcceptanceError("duration predecessor must be canonical stage-final.json")
    predecessor, _predecessor_sha = _read_published(path)
    if predecessor.get("schema_version") != schema_version:
        raise AcceptanceError("mixed-schema predecessor cannot authorize this stage")
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
    archive_root_resolver: ArchiveRootResolver | None = None,
    schema_version: str = SCHEMA_VERSION,
    identity_verifier: Callable[..., Mapping[str, object]] = verify_identity_files,
) -> AcceptanceObserver:
    selected_clock = LinuxClock() if clock is None else clock
    if schema_version not in {SCHEMA_VERSION, V4_SCHEMA_VERSION}:
        raise AcceptanceError("unsupported resume acceptance schema")
    if (stage_root / "stage-final.json").exists():
        raise AcceptanceError("stage is already finalized")
    start, start_sha = _read_published(stage_root / "stage-start.json")
    start_manifest_transition = start.get("manifest_transition")
    if (
        isinstance(start_manifest_transition, dict)
        and start_manifest_transition.get("state") == "ANOMALY"
    ):
        start_findings = start.get("blocking_findings")
        if not isinstance(start_findings, list) or (
            "manifest_byte_mutation_or_loss" not in start_findings
        ):
            raise AcceptanceError(
                "manifest anomaly is not bound to manifest_byte_mutation_or_loss"
            )
        raise AcceptanceError(
            "manifest authority is invalid; stage cannot be deterministically resumed"
        )
    if (
        start.get("schema_version") != schema_version
        or start.get("evidence_kind") != "stage-start"
        or start.get("stage") not in STAGE_NAMES
        or not _same_identity(start, identity)
        or start.get("stage_start_evidence_sha256") is not None
        or start.get("previous_sample_sha256") is not None
    ):
        if start.get("schema_version") == LEGACY_SCHEMA_VERSION:
            if schema_version == SCHEMA_VERSION:
                raise AcceptanceError("v1 failed stage cannot resume as v2/v3")
            raise AcceptanceError("v1 failed stage cannot resume as v4")
        if start.get("schema_version") == PREVIOUS_SCHEMA_VERSION:
            if schema_version == SCHEMA_VERSION:
                raise AcceptanceError("v2 failed stage cannot resume as v3")
            raise AcceptanceError("v2 failed stage cannot resume as v4")
        if schema_version == V4_SCHEMA_VERSION and start.get("schema_version") == SCHEMA_VERSION:
            raise AcceptanceError("v3 observer cannot resume as v4")
        raise AcceptanceError("stage-start evidence is invalid")
    stage = str(start["stage"])
    run_id = start.get("run_id")
    if not isinstance(run_id, str) or not run_id:
        raise AcceptanceError("stage-start run_id is invalid")
    _chain_identity(start, identity=identity, stage=stage, run_id=run_id)
    prior_digest = _digest(
        start.get("prior_stage_evidence_sha256"), "stage-start predecessor digest"
    )
    start_process = start.get("systemd_process_incarnation")
    if not isinstance(start_process, dict):
        raise AcceptanceError("stage-start process-incarnation evidence is malformed")
    state = _sample_chain_v3(
        stage_root,
        start=start,
        start_sha=start_sha,
        identity=identity,
        require_eligible=False,
        schema_version=schema_version,
    )
    if state.invalid_manifest_authority:
        raise AcceptanceError(
            "manifest authority is invalid; stage cannot be deterministically resumed"
        )
    start_findings = start.get("blocking_findings")
    if not isinstance(start_findings, list) or any(
        not isinstance(item, str) for item in start_findings
    ):
        raise AcceptanceError("stage-start blocking findings are malformed")
    resumable_start = (
        start_findings == []
        and start.get("result") == "PASS_CANDIDATE"
    ) or (
        start_findings == ["unsafe_wall_clock_backward"]
        and start.get("result") == "INCOMPLETE"
    )
    if not resumable_start:
        raise AcceptanceError("stage-start is not resumable")
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
    manifest_records = _manifest_baseline_from_evidence(start.get("manifest_baseline"))
    t0_manifest_members = {
        path: str(record["sha256"]) for path, record in manifest_records.items()
    }
    continuation: dict[str, object] = {
        "schema_version": INCREMENTAL_SCHEMA_VERSION,
        "manifest_members": {
            path: str(record["sha256"])
            for path, record in state.manifest_records.items()
            if path not in state.deferred_manifest_paths
        },
        "streams": state.continuation_streams,
    }
    try:
        validate_incremental_continuation(continuation)
    except SealError as exc:
        raise AcceptanceError("resume reconnect continuation is invalid") from exc
    observer_type: type[AcceptanceObserver] = (
        V4AcceptanceObserver if schema_version == V4_SCHEMA_VERSION else AcceptanceObserver
    )
    observer_kwargs: dict[str, object] = {
        "stage": stage,
        "run_id": run_id,
        "data_root": data_root,
        "evidence_root": stage_root,
        "identity": identity,
        "prior_stage_sha256": prior_digest,
        "manager": manager,
        "evaluator": evaluator,
        "clock": selected_clock,
        "disk_usage": disk_usage,
        "identity_verifier": identity_verifier,
        "archive_root_resolver": archive_root_resolver,
        "t0_utc_ns": _integer(start["observed_at_utc_ns"], "stage-start UTC timestamp"),
        "t0_boottime_ns": _integer(
            start["observed_at_boottime_ns"], "stage-start BOOTTIME timestamp"
        ),
        "t0_boot_id": str(start["boot_id"]),
        "frozen_process": start_process,
        "frozen_service_instance_id": str(start.get("service_instance_id") or ""),
        "stage_start_sha256": start_sha,
        "t0_manifest_members": t0_manifest_members,
        "t0_manifest_records": manifest_records,
        "t0_manifest_aggregate_sha256": _manifest_aggregate(manifest_records),
        "published_manifest_records": state.manifest_records,
        "published_raw_absences": state.raw_absences,
        "published_deferred_manifest_paths": state.deferred_manifest_paths,
        "published_reconnect_transition_keys": state.reconnect_transition_keys,
        "published_reconnect_streams": state.continuation_streams,
        "published_catalog_open": state.catalog_open,
        "published_catalog_interval_keys": state.catalog_interval_keys,
        "published_terminal_event_keys": state.terminal_event_keys,
        "ever_blocking_findings": state.known_findings,
        "reconnect_continuation": continuation,
        "next_sample_ordinal": 0 if state.last is None else state.last.ordinal + 1,
        "last_sample_sha256": None if state.last is None else state.last.sha256,
        "last_sample_utc_ns": None if state.last is None else state.last.utc_ns,
        "last_sample_boottime_ns": None if state.last is None else state.last.boottime_ns,
    }
    if schema_version == V4_SCHEMA_VERSION:
        if state.readiness_episode is None:
            raise AcceptanceError("V4 resume readiness episode is absent")
        observer_kwargs["readiness_episode"] = state.readiness_episode
    observer = observer_type(
        **cast(Any, observer_kwargs),
    )
    return observer


__all__ = [
    "CURRENT_SCHEMA_VERSION",
    "LEGACY_SCHEMA_VERSION",
    "MAX_EVIDENCE_GAP_NS",
    "PREVIOUS_SCHEMA_VERSION",
    "SAMPLE_INTERVAL_NS",
    "SCHEMA_VERSION",
    "STAGE_DURATION_NS",
    "STAGE_NAMES",
    "V3_SCHEMA_VERSION",
    "V4_DEADLINE_NS",
    "V4_SCHEMA_VERSION",
    "AcceptanceError",
    "AcceptanceObserver",
    "Clock",
    "LinuxClock",
    "V4AcceptanceObserver",
    "canonical_json",
    "create_identity_evidence",
    "create_readiness_evidence",
    "read_identity_evidence",
    "resume_observer",
    "sha256_bytes",
    "verify_completed_stage",
    "verify_prior_stage",
]
