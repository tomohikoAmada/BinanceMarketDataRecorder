"""ADR-0034 bounded online producer and independent online replay.

Timed observation terminates at stage-target. Full qualification is a separate
quiescent command; finalize() here only publishes that immutable online target.
"""

from __future__ import annotations

import copy
import os
import shutil
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

from .acceptance import (
    MAX_EVIDENCE_GAP_NS,
    SAMPLE_INTERVAL_NS,
    STAGE_DURATION_NS,
    AcceptanceError,
    Clock,
    LinuxClock,
    _capacity,
    _identity_fields,
    _safe_evidence_root,
    _state_instance,
    _v4_episode_observe,
    _V4EpisodeState,
    canonical_json,
    sha256_bytes,
)
from .acceptance_v5_corpus import catalog_available
from .acceptance_v5_delta import (
    BOUNDED_BATCH_POLICY,
    DELTA_WORK_BUDGET_NS,
    FAMILIES,
    DependencyPending,
    _companions,
    delta_limits,
    empty_continuation,
    replay_delta,
)
from .acceptance_v5_io import (
    MAX_CANONICAL_BYTES_PER_SHARD,
    V5_SCHEMA_VERSION,
    read_document,
)
from .acceptance_v5_io import (
    publish as _publish,
)
from .acceptance_v5_raw import CancellableExecutor, RawAuthorityPending, cancellable_unit
from .deployment_identity import DeploymentIdentity, verify_identity_files
from .readiness import VpsReadinessEvaluator
from .systemd import SystemdManager

# A publication bound, not a cadence/readiness policy relaxation. Excess delta
# remains pending, with its cursor unchanged. This avoids a giant online JSON.
ONLINE_DOCUMENT_MAX_BYTES = 8 * MAX_CANONICAL_BYTES_PER_SHARD


def configured_products(evaluator: object) -> list[list[str]]:
    products = getattr(evaluator, "expected_products", ())
    return sorted(
        [
            [str(product[0]), str(product[1])]
            if isinstance(product, tuple)
            else [str(product.market), str(product.symbol)]
            for product in products
        ]
    )


def runtime_identity(identity: DeploymentIdentity) -> dict[str, object]:
    return {
        "identity_sha256": identity.identity_sha256,
        "source_git_sha": identity.source_git_sha,
        "wheel_sha256": identity.wheel_sha256,
        "config_sha256": identity.config_sha256,
        "systemd_unit_sha256": identity.systemd_unit_sha256,
        "capacity_profile_id": identity.capacity_profile_id,
    }


def online_result(findings: list[str], *, elapsed: int, stage: str) -> str:
    failures = set(findings) - {
        "acceptance_observation_gap",
        "target_delta_pending",
        "duration_short",
    }
    if failures:
        return "FAIL"
    if findings or elapsed < STAGE_DURATION_NS[stage]:
        return "INCOMPLETE"
    return "PASS_CANDIDATE"


def predecessor_reference(path: Path, identity: DeploymentIdentity, stage: str) -> dict[str, Any]:
    document, digest = read_document(path)
    if document.get("deployment_identity") != identity.document():
        raise AcceptanceError("V5 predecessor deployment identity differs")
    expected_kind = "baseline-audit-root" if stage == "2h" else "stage-final"
    if document.get("evidence_kind") != expected_kind or document.get("result") != "PASS_CANDIDATE":
        raise AcceptanceError("V5 predecessor is not eligible")
    if stage != "2h":
        stages = list(STAGE_DURATION_NS)
        if (
            document.get("stage") != stages[stages.index(stage) - 1]
            or document.get("eligible_for_next_stage") is not True
        ):
            raise AcceptanceError("V5 predecessor stage is invalid")
        root_path = path.parent / "terminal-audit" / "audit-root.json"
        audit, audit_sha = read_document(root_path)
        if audit_sha != document.get("terminal_audit_sha256"):
            raise AcceptanceError("V5 predecessor audit root binding differs")
    else:
        root_path, audit, audit_sha = path, document, digest
    if (
        audit.get("result") != "PASS_CANDIDATE"
        or audit.get("deployment_identity") != identity.document()
    ):
        raise AcceptanceError("V5 predecessor full audit did not pass")
    return {
        "path": str(path.resolve()),
        "sha256": digest,
        "audit_root_path": str(root_path.resolve()),
        "audit_root_sha256": audit_sha,
        "cursor_tuple": audit["catalog_authority"]["high_water"],
        "continuation_seed": audit["continuation_seed"],
        "configured_products": audit["configured_products"],
    }


def _replay_live(
    start: Mapping[str, Any],
    sample: Mapping[str, Any],
    identity: DeploymentIdentity,
    episode: _V4EpisodeState,
    previous: Mapping[str, Any] | None,
    *,
    allow_start: bool,
) -> list[str]:
    findings: set[str] = set(previous["blocking_findings"] if previous else [])
    if sample.get("deployment_identity") != identity.document():
        raise AcceptanceError("online identity authority differs")
    if sample["boot_id"] != start["boot_id"]:
        findings.add("boot_id_changed")
    if sample["systemd_process_incarnation"] != start["systemd_process_incarnation"]:
        findings.add("process_incarnation_changed")
    if sample["service_instance_id"] != start["service_instance_id"]:
        findings.add("service_instance_id_changed")
    if sample["runtime_deployment_identity"] != runtime_identity(identity):
        findings.add("runtime_deployment_identity_mismatch")
    if sample["identity_verification"]["valid"] is not True:
        findings.add("artifact_identity_changed")
    floor = previous or start
    if sample["observed_at_utc_ns"] < floor["observed_at_utc_ns"]:
        findings.add("unsafe_wall_clock_backward")
    difference = sample["observed_at_boottime_ns"] - floor["observed_at_boottime_ns"]
    if difference < 0:
        findings.add("boottime_non_monotonic")
    if difference > MAX_EVIDENCE_GAP_NS:
        findings.add("acceptance_observation_gap")
    evaluator = SimpleNamespace(
        expected_products=[tuple(item) for item in start["configured_products"]]
    )
    findings.update(
        _v4_episode_observe(
            episode,
            evaluator=evaluator,
            readiness=sample["readiness"],
            observed_at_utc_ns=sample["observed_at_utc_ns"],
            observed_at_boottime_ns=sample["observed_at_boottime_ns"],
            allow_start=allow_start,
        )
    )
    capacity = sample["capacity"]
    expected_capacity = _capacity(
        Path(start["data_root"]),
        sample["observed_at_utc_ns"],
        lambda _path: SimpleNamespace(total=capacity["total_bytes"], free=capacity["free_bytes"]),
    )
    if capacity != expected_capacity:
        raise AcceptanceError("V5 capacity authority is not independently reconstructible")
    if capacity["profile_id"] != identity.capacity_profile_id:
        findings.add("capacity_profile_identity_mismatch")
    if capacity["free_bytes"] <= capacity["hard_reserve_bytes"]:
        findings.add("hard_reserve_reached")
    if not sample["catalog_available"]:
        findings.add("catalog_unavailable")
    if len(sample["open_chunks"]) > start["open_chunk_bound"]:
        findings.add("open_chunk_topology_exceeded")
    if (
        sample["snapshot_status"] == "complete"
        and sample["continuation"]["processed"]["archive"] == sample["high_water"]["archive"]
        and sample["continuation"]["archive_counts"] != sample["archive_backlog"]
    ):
        findings.add("archive_aggregate_disagreement")
    findings.update(sample["continuation"]["integrity_findings"])
    return sorted(findings)


@dataclass
class V5AcceptanceObserver:
    stage: str
    run_id: str
    data_root: Path
    evidence_root: Path
    identity: DeploymentIdentity
    predecessor_path: Path
    manager: SystemdManager
    evaluator: VpsReadinessEvaluator
    clock: Clock = field(default_factory=LinuxClock)
    disk_usage: Callable[[Path], Any] = shutil.disk_usage
    identity_verifier: Callable[..., Mapping[str, object]] = verify_identity_files
    archive_root_resolver: Callable[[], Mapping[str, Path]] = field(default=lambda: {})
    archive_target_resolver: Callable[[Mapping[str, dict[str, Any]]], Mapping[str, Path]] | None = (
        None
    )
    snapshot_unit: Callable[..., dict[str, Any] | None] = cancellable_unit
    raw_unit: Callable[..., dict[str, Any] | None] = cancellable_unit
    t0_utc_ns: int | None = None
    t0_boottime_ns: int | None = None
    t0_boot_id: str | None = None
    stage_start_sha256: str | None = None
    last_sample_sha256: str | None = None
    last_sample_boottime_ns: int | None = None
    next_sample_ordinal: int = 0
    start_document: dict[str, Any] | None = None
    last_document: dict[str, Any] | None = None
    continuation: dict[str, Any] | None = None
    episode: _V4EpisodeState = field(default_factory=_V4EpisodeState)
    snapshot_status: str = "complete"
    archive_backlog: dict[str, int] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.stage not in STAGE_DURATION_NS:
            raise AcceptanceError("invalid V5 stage")
        self.evidence_root = _safe_evidence_root(self.evidence_root, self.data_root)
        if str(self.data_root.resolve()) != self.identity.systemd_effective.get(
            "working_directory"
        ):
            raise AcceptanceError("V5 data root differs from deployment service authority")
        # Avoid the deployment evaluator's hidden O(history) integrity_check.
        if isinstance(self.evaluator, VpsReadinessEvaluator):
            self.evaluator.catalog_ready = catalog_available

    def _live(self, now_utc: int, now_boot: int) -> dict[str, Any]:
        state, service_instance = _state_instance(self.data_root)
        verification: dict[str, Any] = {"valid": True, "details": {}}
        try:
            if self.identity_verifier is verify_identity_files:
                verification["details"] = dict(
                    self.identity_verifier(
                        self.identity,
                        expected_config_path=Path(self.identity.config_path),
                        expected_profile_id=self.identity.capacity_profile_id,
                        require_root_controlled=True,
                    )
                )
            else:
                verification["details"] = dict(self.identity_verifier(self.identity))
        except (OSError, ValueError, RuntimeError) as exc:
            verification = {"valid": False, "error": type(exc).__name__}
        return {
            "schema_version": V5_SCHEMA_VERSION,
            "stage": self.stage,
            "run_id": self.run_id,
            "deployment_identity": self.identity.document(),
            **_identity_fields(self.identity),
            "observed_at_utc_ns": now_utc,
            "observed_at_boottime_ns": now_boot,
            "boot_id": self.clock.boot_id(),
            "systemd_process_incarnation": self.manager.process_incarnation(),
            "service_instance_id": service_instance,
            "runtime_deployment_identity": state.get("deployment_identity"),
            "identity_verification": verification,
            "readiness": self.evaluator.evaluate().public_dict(),
            "capacity": _capacity(self.data_root, now_utc, self.disk_usage),
            "catalog_available": True,
        }

    def _delta(self, started: int) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
        if self.continuation is None or self.start_document is None:
            raise AcceptanceError("online continuation is not initialized")
        captured = self.snapshot_unit(
            {
                "operation": "catalog-snapshot",
                "catalog_path": str(self.data_root / "state" / "catalog.sqlite"),
                "processed": self.continuation["processed"],
                "open_chunk_bound": self.start_document["open_chunk_bound"],
                "max_pages_per_family": delta_limits(self.start_document)[0],
            },
            remaining_ns=DELTA_WORK_BUDGET_NS - (self.clock.boottime_ns() - started),
            time_ns=self.clock.boottime_ns,
        )
        self.snapshot_status = "complete" if captured is not None else "budget_pending"
        if captured is None:
            return (
                dict(self.continuation["observed_high_water"]),
                {family: [] for family in FAMILIES},
                self.continuation.get("open_chunks", []),
            )
        candidates = captured["candidates"]
        high_water = captured["high_water"]
        target_authority = captured["targets"]
        open_chunks = captured["open_chunks"]
        self.archive_backlog = captured["archive_backlog"]
        # No SQLite transaction spans filesystem/Raw work or a later archive commit.
        pages: dict[str, list[dict[str, Any]]] = {family: [] for family in FAMILIES}
        roots = (
            self.archive_target_resolver(target_authority)
            if self.archive_target_resolver
            else (self.archive_root_resolver() if target_authority else {})
        )
        used_bytes = 0
        byte_limit = self.start_document.get("delta_policy", {}).get(
            "max_canonical_delta_bytes", ONLINE_DOCUMENT_MAX_BYTES // 2
        )
        for family in FAMILIES:
            for selected in candidates[family]:
                entry = {**selected, "unit": None, "status": "acknowledged"}
                row, companions = selected["row"], selected["companions"]
                remaining = DELTA_WORK_BUDGET_NS - (self.clock.boottime_ns() - started)
                if remaining <= 0:
                    entry["status"] = "budget_pending"
                else:
                    try:
                        _companions(family, row, companions, high_water)
                    except DependencyPending:
                        entry["status"] = "pending"
                if (
                    entry["status"] == "acknowledged"
                    and family == "chunk"
                    and row["to_state"] == "SEALED"
                ):
                    chunk = companions["chunk"]
                    archive = companions.get("archive")
                    archive_root = (
                        roots.get(archive["transaction"]["storage_id"]) if archive else None
                    )
                    task = {
                        "operation": "new-manifest",
                        "data_root": str(self.data_root),
                        "manifest_path": chunk["manifest_path"],
                        "chunk": chunk,
                        "archive": archive,
                        "archive_root": str(archive_root) if archive_root else None,
                    }
                    remaining = DELTA_WORK_BUDGET_NS - (self.clock.boottime_ns() - started)
                    try:
                        result = self.raw_unit(
                            task, remaining_ns=remaining, time_ns=self.clock.boottime_ns
                        )
                    except RawAuthorityPending:
                        result = None
                        entry["status"] = "pending"
                    if result is None:
                        if entry["status"] != "pending":
                            entry["status"] = "budget_pending"
                    else:
                        entry["unit"] = result
                size = len(canonical_json(entry))
                if used_bytes + size > byte_limit:
                    if "delta_policy" in self.start_document:
                        # Frozen high-waters keep omitted work pending without
                        # admitting an over-budget descriptor or skipping it.
                        break
                    entry = {**selected, "unit": None, "status": "budget_pending"}
                pages[family].append(entry)
                used_bytes += len(canonical_json(entry))
                if entry["status"] != "acknowledged":
                    break
        return high_water, pages, open_chunks

    def _observe(self, *, starting: bool) -> dict[str, Any]:
        if self.start_document is None or self.continuation is None:
            raise AcceptanceError("stage initialization is missing")
        if starting:
            document = dict(self.start_document)
            now_utc, now_boot = document["observed_at_utc_ns"], document["observed_at_boottime_ns"]
        else:
            now_utc, now_boot = self.clock.utc_ns(), self.clock.boottime_ns()
            document = self._live(now_utc, now_boot)
        executor = CancellableExecutor()
        original_raw, original_snapshot = self.raw_unit, self.snapshot_unit
        if original_raw is cancellable_unit:
            self.raw_unit = executor
        if original_snapshot is cancellable_unit:
            self.snapshot_unit = executor
        try:
            high_water, pages, open_chunks = self._delta(now_boot)
        finally:
            executor.close()
            self.raw_unit, self.snapshot_unit = original_raw, original_snapshot
        document.update(
            {
                "evidence_kind": "stage-start" if starting else "stage-sample",
                "stage_start_sha256": self.stage_start_sha256,
                "previous_sample_sha256": self.last_sample_sha256,
                "sample_ordinal": None if starting else self.next_sample_ordinal,
                "high_water": high_water,
                "delta_pages": pages,
                "open_chunks": open_chunks,
                "archive_backlog": self.archive_backlog,
                "snapshot_status": self.snapshot_status,
            }
        )
        if "delta_policy" in self.start_document:
            document["delta_policy"] = dict(self.start_document["delta_policy"])
        continuation = replay_delta(
            self.continuation, high_water, pages, t0_utc_ns=self.t0_utc_ns,
            row_cap=delta_limits(self.start_document)[1],
        )
        continuation["open_chunks"] = open_chunks
        document["continuation"] = continuation
        document["continuation_sha256"] = sha256_bytes(canonical_json(continuation))
        document["delta_pending"] = self.snapshot_status != "complete" or any(
            continuation["processed"][family] != high_water[family] for family in FAMILIES
        )
        document["blocking_findings"] = _replay_live(
            self.start_document,
            document,
            self.identity,
            self.episode,
            self.last_document,
            allow_start=not starting,
        )
        document["readiness_episode"] = self.episode.public(observed_boottime_ns=now_boot)
        if len(canonical_json(document)) > ONLINE_DOCUMENT_MAX_BYTES:
            raise AcceptanceError("online evidence exceeds bounded publication size")
        self.continuation = continuation
        return document

    def start(self) -> tuple[Path, str, dict[str, Any]]:
        if (
            self.stage_start_sha256 is not None
            or (self.evidence_root / "stage-start.json").exists()
        ):
            raise AcceptanceError("V5 T0 already exists")
        predecessor = predecessor_reference(self.predecessor_path, self.identity, self.stage)
        if predecessor["configured_products"] != configured_products(self.evaluator):
            raise AcceptanceError("V5 predecessor product configuration differs")
        self.continuation = empty_continuation(
            predecessor["cursor_tuple"], products=configured_products(self.evaluator)
        )
        self.continuation["reconnect"] = predecessor["continuation_seed"].get("reconnect", {})
        self.continuation["archive_counts"] = predecessor["continuation_seed"].get(
            "archive_counts", self.continuation["archive_counts"]
        )
        self.continuation["open_discontinuities"] = predecessor["continuation_seed"].get(
            "open_discontinuities", {}
        )
        self.run_id = self.run_id or uuid4().hex
        initial = self._live(self.clock.utc_ns(), self.clock.boottime_ns())
        self.t0_utc_ns, self.t0_boottime_ns, self.t0_boot_id = (
            initial["observed_at_utc_ns"],
            initial["observed_at_boottime_ns"],
            initial["boot_id"],
        )
        initial.update(
            {
                "data_root": str(self.data_root.resolve()),
                "configured_products": configured_products(self.evaluator),
                "open_chunk_bound": max(1, len(configured_products(self.evaluator))) * 32,
                "predecessor": predecessor,
                "t0_utc_ns": self.t0_utc_ns,
                "t0_boottime_ns": self.t0_boottime_ns,
                "t0_boot_id": self.t0_boot_id,
                "required_duration_ns": STAGE_DURATION_NS[self.stage],
                "sample_interval_ns": SAMPLE_INTERVAL_NS,
                "max_evidence_gap_ns": MAX_EVIDENCE_GAP_NS,
                "delta_work_budget_ns": DELTA_WORK_BUDGET_NS,
                "delta_policy": dict(BOUNDED_BATCH_POLICY),
            }
        )
        self.start_document = initial
        # The initial delta is timed from this T0; it is never an all-history audit.
        document = {**initial, **self._observe(starting=True)}
        path, digest = _publish(self.evidence_root, "stage-start.json", document)
        self.start_document = document
        self.stage_start_sha256 = digest
        self.last_document = document
        return path, digest, document

    def sample(self) -> tuple[Path, str, dict[str, Any]]:
        if self.stage_start_sha256 is None or (self.evidence_root / "stage-target.json").exists():
            raise AcceptanceError("sample requires an open V5 timed stage")
        document = self._observe(starting=False)
        path, digest = _publish(
            self.evidence_root, f"sample-{self.next_sample_ordinal:08d}.json", document
        )
        self.last_sample_sha256 = digest
        self.last_sample_boottime_ns = document["observed_at_boottime_ns"]
        self.last_document = document
        self.next_sample_ordinal += 1
        return path, digest, document

    def finalize(self) -> tuple[Path, str, dict[str, Any]]:
        if self.t0_boottime_ns is None or self.start_document is None:
            raise AcceptanceError("target requires T0")
        if self.clock.boottime_ns() - self.t0_boottime_ns < STAGE_DURATION_NS[self.stage]:
            raise AcceptanceError("V5 required online duration not reached")
        if (
            self.last_sample_boottime_ns is not None
            and self.last_document is not None
            and self.last_sample_sha256 is not None
            and self.last_sample_boottime_ns - self.t0_boottime_ns >= STAGE_DURATION_NS[self.stage]
        ):
            sample_sha, sample = self.last_sample_sha256, self.last_document
        else:
            _path, sample_sha, sample = self.sample()
        findings = set(sample["blocking_findings"])
        if sample["delta_pending"] or sample["continuation"]["pending_causal_references"]:
            findings.add("target_delta_pending")
        if sample["readiness"]["state"] != "READY":
            findings.add("terminal_readiness_not_ready")
        if sample["continuation"]["open_discontinuities"]:
            findings.add("unresolved_discontinuity")
        elapsed = sample["observed_at_boottime_ns"] - self.t0_boottime_ns
        target = {
            "schema_version": V5_SCHEMA_VERSION,
            "evidence_kind": "stage-target",
            "stage": self.stage,
            "run_id": self.run_id,
            "deployment_identity": self.identity.document(),
            "stage_start_sha256": self.stage_start_sha256,
            "last_sample_sha256": sample_sha,
            "last_sample_ordinal": self.next_sample_ordinal - 1,
            "t0_utc_ns": self.t0_utc_ns,
            "t0_boottime_ns": self.t0_boottime_ns,
            "required_duration_ns": STAGE_DURATION_NS[self.stage],
            "elapsed_boottime_ns": elapsed,
            "terminal_utc_ns": sample["observed_at_utc_ns"],
            "terminal_boottime_ns": sample["observed_at_boottime_ns"],
            "boot_id": sample["boot_id"],
            "systemd_process_incarnation": sample["systemd_process_incarnation"],
            "service_instance_id": sample["service_instance_id"],
            "terminal_readiness": sample["readiness"],
            "cursor_tuple": sample["continuation"]["processed"],
            "target_high_water": sample["high_water"],
            "continuation_sha256": sample["continuation_sha256"],
            "predecessor": self.start_document["predecessor"],
            "blocking_findings": sorted(findings),
            "result": online_result(sorted(findings), elapsed=elapsed, stage=self.stage),
            "eligible_for_next_stage": False,
        }
        path, digest = _publish(self.evidence_root, "stage-target.json", target)
        return path, digest, target


def online_samples(root: Path) -> Iterator[tuple[dict[str, Any], str]]:
    ordinal = 0
    while (root / f"sample-{ordinal:08d}.json").exists():
        document, digest = read_document(
            root / f"sample-{ordinal:08d}.json", limit=ONLINE_DOCUMENT_MAX_BYTES
        )
        yield document, digest
        ordinal += 1
    # Missing/extra/reordered filenames cannot disappear from the authority.
    seen = 0
    with os.scandir(root) as entries:
        for entry in entries:
            if entry.name.startswith("sample-"):
                seen += 1
    if seen != ordinal:
        raise AcceptanceError("missing or extra online sample")


@dataclass
class OnlineReplay:
    start: dict[str, Any]
    start_sha256: str
    last: dict[str, Any]
    last_sha256: str | None
    sample_count: int
    continuation: dict[str, Any]
    episode: _V4EpisodeState
    target: dict[str, Any] | None = None
    target_sha256: str | None = None


def replay_online(
    root: Path,
    identity: DeploymentIdentity,
    *,
    require_target: bool,
    row_observer: Callable[[str, Mapping[str, Any]], None] | None = None,
    sample_observer: Callable[[Mapping[str, Any]], None] | None = None,
) -> OnlineReplay:
    """Independent streaming replay; only one bounded sample remains in RAM."""
    start, start_sha = read_document(root / "stage-start.json", limit=ONLINE_DOCUMENT_MAX_BYTES)
    stage = start.get("stage")
    if stage not in STAGE_DURATION_NS or start.get("evidence_kind") != "stage-start":
        raise AcceptanceError("invalid V5 stage-start")
    _max_pages, row_cap = delta_limits(start)
    if start.get("data_root") != identity.systemd_effective.get("working_directory"):
        raise AcceptanceError("V5 stage data root differs from deployment authority")
    if (
        start.get("required_duration_ns") != STAGE_DURATION_NS[stage]
        or start.get("sample_interval_ns") != SAMPLE_INTERVAL_NS
        or start.get("max_evidence_gap_ns") != MAX_EVIDENCE_GAP_NS
        or start.get("delta_work_budget_ns") != DELTA_WORK_BUDGET_NS
        or start.get("open_chunk_bound") != max(1, len(start["configured_products"])) * 32
    ):
        raise AcceptanceError("V5 frozen policy authority differs")
    if (
        start.get("t0_utc_ns") != start.get("observed_at_utc_ns")
        or start.get("t0_boottime_ns") != start.get("observed_at_boottime_ns")
        or start.get("t0_boot_id") != start.get("boot_id")
        or start.get("stage_start_sha256") is not None
        or start.get("previous_sample_sha256") is not None
        or start.get("sample_ordinal") is not None
    ):
        raise AcceptanceError("V5 stage-start/T0 authority differs")
    predecessor = predecessor_reference(Path(start["predecessor"]["path"]), identity, stage)
    if predecessor != start["predecessor"]:
        raise AcceptanceError("V5 predecessor reference changed")
    if predecessor["configured_products"] != start["configured_products"]:
        raise AcceptanceError("V5 predecessor product configuration differs")
    continuation = empty_continuation(
        predecessor["cursor_tuple"], products=start["configured_products"]
    )
    continuation["reconnect"] = copy.deepcopy(predecessor["continuation_seed"].get("reconnect", {}))
    continuation["archive_counts"] = copy.deepcopy(
        predecessor["continuation_seed"].get("archive_counts", continuation["archive_counts"])
    )
    continuation["open_discontinuities"] = copy.deepcopy(
        predecessor["continuation_seed"].get("open_discontinuities", {})
    )
    episode = _V4EpisodeState()
    last, last_sha, count = start, None, 0

    def check(document: dict[str, Any], *, starting: bool, ordinal: int | None) -> None:
        nonlocal continuation
        if document.get("schema_version") != V5_SCHEMA_VERSION or (
            document.get("stage") != stage or document.get("run_id") != start["run_id"]
        ):
            raise AcceptanceError("mixed V5 stage identity")
        if document.get("delta_policy") != start.get("delta_policy"):
            raise AcceptanceError("V5 delta policy changed after T0")
        expected_continuation = replay_delta(
            continuation,
            document["high_water"],
            document["delta_pages"],
            t0_utc_ns=start["t0_utc_ns"],
            row_cap=row_cap,
        )
        if "delta_policy" in start and sum(
            len(canonical_json(entry))
            for page in document["delta_pages"].values()
            for entry in page
        ) > start["delta_policy"]["max_canonical_delta_bytes"]:
            raise AcceptanceError("V5 delta byte cap exceeded")
        expected_continuation["open_chunks"] = document["open_chunks"]
        if document.get("snapshot_status") not in {"complete", "budget_pending"}:
            raise AcceptanceError("invalid online snapshot status")
        if row_observer:
            for family in FAMILIES:
                for entry in document["delta_pages"][family]:
                    if entry["status"] == "acknowledged":
                        row_observer(family, entry)
        if document.get("continuation") != expected_continuation or (
            document.get("continuation_sha256")
            != sha256_bytes(canonical_json(expected_continuation))
        ):
            raise AcceptanceError("V5 continuation is not independently reconstructible")
        pending = document["snapshot_status"] != "complete" or any(
            expected_continuation["processed"][family] != document["high_water"][family]
            for family in FAMILIES
        )
        if document.get("delta_pending") != pending or document.get("sample_ordinal") != ordinal:
            raise AcceptanceError("V5 pending/ordinal summary differs")
        findings = _replay_live(
            start, document, identity, episode, None if starting else last, allow_start=not starting
        )
        if document.get("blocking_findings") != findings or document.get("readiness_episode") != (
            episode.public(observed_boottime_ns=document["observed_at_boottime_ns"])
        ):
            raise AcceptanceError("V5 findings/readiness episode differ from reconstruction")
        continuation = expected_continuation
        if sample_observer:
            sample_observer(document)

    check(start, starting=True, ordinal=None)
    for sample, digest in online_samples(root):
        if sample.get("evidence_kind") != "stage-sample" or (
            sample.get("stage_start_sha256") != start_sha
            or sample.get("previous_sample_sha256") != last_sha
        ):
            raise AcceptanceError("V5 sample hash chain differs")
        check(sample, starting=False, ordinal=count)
        last, last_sha, count = sample, digest, count + 1
    replay = OnlineReplay(start, start_sha, last, last_sha, count, continuation, episode)
    target_path = root / "stage-target.json"
    if target_path.exists():
        target, target_sha = read_document(target_path)
        if count == 0:
            raise AcceptanceError("V5 target has no terminal sample")
        expected = derive_target(start, start_sha, last, str(last_sha), count - 1, identity)
        if target != expected:
            raise AcceptanceError("V5 target is not independently reconstructible")
        replay.target, replay.target_sha256 = target, target_sha
    elif require_target:
        raise AcceptanceError("V5 timed stage has no immutable target")
    return replay


def derive_target(
    start: Mapping[str, Any],
    start_sha: str,
    sample: Mapping[str, Any],
    sample_sha: str,
    ordinal: int,
    identity: DeploymentIdentity,
) -> dict[str, Any]:
    findings = set(sample["blocking_findings"])
    if sample["delta_pending"] or sample["continuation"]["pending_causal_references"]:
        findings.add("target_delta_pending")
    if sample["readiness"]["state"] != "READY":
        findings.add("terminal_readiness_not_ready")
    if sample["continuation"]["open_discontinuities"]:
        findings.add("unresolved_discontinuity")
    elapsed = sample["observed_at_boottime_ns"] - start["t0_boottime_ns"]
    stage = str(start["stage"])
    return {
        "schema_version": V5_SCHEMA_VERSION,
        "evidence_kind": "stage-target",
        "stage": stage,
        "run_id": start["run_id"],
        "deployment_identity": identity.document(),
        "stage_start_sha256": start_sha,
        "last_sample_sha256": sample_sha,
        "last_sample_ordinal": ordinal,
        "t0_utc_ns": start["t0_utc_ns"],
        "t0_boottime_ns": start["t0_boottime_ns"],
        "required_duration_ns": STAGE_DURATION_NS[stage],
        "elapsed_boottime_ns": elapsed,
        "terminal_utc_ns": sample["observed_at_utc_ns"],
        "terminal_boottime_ns": sample["observed_at_boottime_ns"],
        "boot_id": sample["boot_id"],
        "systemd_process_incarnation": sample["systemd_process_incarnation"],
        "service_instance_id": sample["service_instance_id"],
        "terminal_readiness": sample["readiness"],
        "cursor_tuple": sample["continuation"]["processed"],
        "target_high_water": sample["high_water"],
        "continuation_sha256": sample["continuation_sha256"],
        "predecessor": start["predecessor"],
        "blocking_findings": sorted(findings),
        "result": online_result(sorted(findings), elapsed=elapsed, stage=stage),
        "eligible_for_next_stage": False,
    }


def resume_v5_observer(
    *,
    evidence_root: Path,
    data_root: Path,
    identity: DeploymentIdentity,
    manager: SystemdManager,
    evaluator: VpsReadinessEvaluator,
    clock: Clock | None = None,
    **kwargs: Any,
) -> V5AcceptanceObserver:
    replay = replay_online(evidence_root, identity, require_target=False)
    if replay.target is not None or (evidence_root / "stage-final.json").exists():
        raise AcceptanceError("V5 timed stage is already closed")
    selected_clock = clock or LinuxClock()
    _state, instance = _state_instance(data_root)
    if (
        selected_clock.boot_id() != replay.start["boot_id"]
        or manager.process_incarnation() != replay.start["systemd_process_incarnation"]
        or instance != replay.start["service_instance_id"]
    ):
        raise AcceptanceError("V5 resume process/boot/service authority differs")
    observer = V5AcceptanceObserver(
        stage=replay.start["stage"],
        run_id=replay.start["run_id"],
        data_root=data_root,
        evidence_root=evidence_root,
        identity=identity,
        predecessor_path=Path(replay.start["predecessor"]["path"]),
        manager=manager,
        evaluator=evaluator,
        clock=selected_clock,
        **kwargs,
    )
    observer.t0_utc_ns = replay.start["t0_utc_ns"]
    observer.t0_boottime_ns = replay.start["t0_boottime_ns"]
    observer.t0_boot_id = replay.start["t0_boot_id"]
    observer.start_document = replay.start
    observer.stage_start_sha256 = replay.start_sha256
    observer.last_document = replay.last
    observer.last_sample_sha256 = replay.last_sha256
    observer.last_sample_boottime_ns = (
        replay.last["observed_at_boottime_ns"] if replay.sample_count else None
    )
    observer.next_sample_ordinal = replay.sample_count
    observer.continuation = replay.continuation
    observer.episode = replay.episode
    return observer
