from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from binance_market_data_recorder.audit import reconnect_boundaries as reconnect_audit
from binance_market_data_recorder.service.acceptance import (
    LEGACY_SCHEMA_VERSION,
    STAGE_DURATION_NS,
    AcceptanceError,
    AcceptanceObserver,
    _absence_aggregate,
    _continuation_from,
    _empty_common,
    _publish,
    _raw_absence_transition,
    _safe_evidence_root,
    _sample_chain,
    canonical_json,
    resume_observer,
    sha256_bytes,
    verify_completed_stage,
    verify_prior_stage,
)
from binance_market_data_recorder.service.readiness import DeploymentReadinessResult
from binance_market_data_recorder.service.state import ServiceStateStore
from binance_market_data_recorder.spool.seal import seal_partial, validate_sealed_artifact
from binance_market_data_recorder.spool.writer import RawChunkWriter, RotationPolicy
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tests.unit.test_deployment_identity import _identity
from tests.unit.test_historical_reconnect_audit import (
    build_fixture,
    seal_chunk,
    usdm_envelope,
)


class FakeClock:
    def __init__(self) -> None:
        self.utc = 1_000_000_000
        self.boot = 2_000_000_000
        self.boot_value = "boot-a"

    def utc_ns(self) -> int:
        return self.utc

    def boottime_ns(self) -> int:
        return self.boot

    def boot_id(self) -> str:
        return self.boot_value


class AdvancingClock(FakeClock):
    def boottime_ns(self) -> int:
        self.boot += 1
        return self.boot


class FakeManager:
    def __init__(self) -> None:
        self.incarnation: dict[str, object] = {
            "active_state": "active",
            "sub_state": "running",
            "main_pid": 123,
            "result": "success",
            "n_restarts": 0,
            "active_enter_timestamp_monotonic": 10,
            "invocation_id": "a" * 32,
        }

    def process_incarnation(self) -> dict[str, object]:
        return dict(self.incarnation)


class FakeEvaluator:
    def evaluate(self) -> DeploymentReadinessResult:
        return DeploymentReadinessResult("READY", (), {"authoritative": True})


def _observer(tmp_path: Path) -> tuple[AcceptanceObserver, FakeClock, FakeManager]:
    identity = _identity(tmp_path)
    data_root = tmp_path / "recorder"
    layout = ensure_storage_layout(data_root)
    with Catalog(layout.catalog) as catalog:
        assert catalog.integrity_check() == ("ok",)
    ServiceStateStore(data_root / "state" / "service_state.json").write(
        {
            "status": "RUNNING",
            "pid": 123,
            "service_instance_id": "service-a",
            "deployment_identity": {
                "identity_sha256": identity.identity_sha256,
                "source_git_sha": identity.source_git_sha,
                "wheel_sha256": identity.wheel_sha256,
                "config_sha256": identity.config_sha256,
                "systemd_unit_sha256": identity.systemd_unit_sha256,
                "capacity_profile_id": identity.capacity_profile_id,
            },
        }
    )
    clock = FakeClock()
    manager = FakeManager()
    observer = AcceptanceObserver(
        stage="2h",
        run_id="run-a",
        data_root=data_root,
        evidence_root=tmp_path / "evidence" / "2h-run-a",
        identity=identity,
        prior_stage_sha256="a" * 64,
        manager=manager,  # type: ignore[arg-type]
        evaluator=FakeEvaluator(),  # type: ignore[arg-type]
        clock=clock,
        identity_verifier=lambda _identity: {},
        disk_usage=lambda _path: SimpleNamespace(total=100 * 1024**3, free=50 * 1024**3),
    )
    return observer, clock, manager


def _publish_valid_identity_and_readiness(observer: AcceptanceObserver) -> str:
    root = observer.evidence_root.parent
    identity_document = _empty_common(
        kind="identity-result",
        stage="identity",
        run_id="identity-run",
        identity=observer.identity,
        now_utc=10,
        now_boot=10,
        boot_id="boot-a",
    )
    identity_document.update(
        {
            "identity": observer.identity.document(),
            "identity_static_verification": {},
            "observer_status": "COMPLETE",
            "result": "PASS_CANDIDATE",
        }
    )
    _identity_path, identity_sha = _publish(root, "identity-result.json", identity_document)
    readiness_document = _empty_common(
        kind="readiness-result",
        stage="readiness",
        run_id="readiness-run",
        identity=observer.identity,
        now_utc=11,
        now_boot=11,
        boot_id="boot-a",
    )
    readiness_document.update(
        {
            "prior_stage_evidence_sha256": identity_sha,
            "readiness": {
                "schema_version": "deployment-readiness.v1",
                "state": "READY",
                "reasons": [],
                "evidence": {},
            },
            "systemd_process_incarnation": observer.manager.process_incarnation(),
            "observer_status": "COMPLETE",
            "result": "PASS_CANDIDATE",
        }
    )
    _readiness_path, readiness_sha = _publish(
        root, "readiness-result.json", readiness_document
    )
    return readiness_sha


def test_clean_interval_uses_boottime_and_publishes_immutable_chain(tmp_path: Path) -> None:
    observer, clock, _manager = _observer(tmp_path)
    start_path, start_sha, start = observer.start()
    assert start_path.name == "stage-start.json"
    assert start["observed_at_boottime_ns"] == clock.boot
    for _ in range(24):
        clock.utc += 300 * 1_000_000_000
        clock.boot += 300 * 1_000_000_000
        observer.sample()
    clock.utc += 1
    clock.boot += 1
    final_path, _final_sha, final = observer.finalize()
    assert final_path.name == "stage-final.json"
    assert final["result"] == "PASS_CANDIDATE"
    assert final["eligible_for_next_stage"] is True
    assert final["stage_start_evidence_sha256"] == start_sha
    assert cast(int, final["elapsed_boottime_ns"]) >= STAGE_DURATION_NS["2h"]


def test_v2_sample_is_a_compact_transition_and_final_is_terminal_only(
    tmp_path: Path,
) -> None:
    observer, clock, _manager = _observer(tmp_path)
    observer.start()
    clock.utc += 1
    clock.boot += 1
    _sample_path, _sample_sha, sample = observer.sample()
    assert sample["schema_version"] == "m22.9-acceptance-evidence.v2"
    for forbidden in (
        "audit",
        "inventory",
        "manifest_inventory",
        "raw_loss",
        "baseline_manifest_members",
        "samples",
    ):
        assert forbidden not in sample
    assert cast(dict[str, object], sample["manifest_transition"])["added_members"] == []
    assert cast(dict[str, object], sample["raw_absence_transition"])["added"] == []
    clock.boot += STAGE_DURATION_NS["2h"]
    clock.utc += STAGE_DURATION_NS["2h"]
    _final_path, _final_sha, final = observer.finalize()
    for forbidden in (
        "manifest_baseline",
        "manifest_transition",
        "raw_absence_baseline",
        "raw_absence_transition",
        "reconnect_baseline",
        "reconnect_transition",
        "catalog_baseline",
        "catalog_transition",
        "audit",
        "inventory",
        "raw_loss",
        "samples",
    ):
        assert forbidden not in final
    assert final["last_sample_ordinal"] == 1
    assert final["last_sample_sha256"] == final["previous_sample_sha256"]
    assert final["eligible_for_next_stage"] is False
    assert len(canonical_json(final)) < 4_096


def test_process_incarnation_change_fails_even_when_pid_is_unchanged(tmp_path: Path) -> None:
    observer, clock, manager = _observer(tmp_path)
    observer.start()
    manager.incarnation["invocation_id"] = "b" * 32
    clock.boot += 1
    _path, _sha, sample = observer.sample()
    assert sample["result"] == "FAIL"
    assert "process_incarnation_changed" in cast(list[object], sample["blocking_findings"])


def test_observation_gap_over_600_seconds_blocks_eligibility(tmp_path: Path) -> None:
    observer, clock, _manager = _observer(tmp_path)
    observer.start()
    clock.boot += 600 * 1_000_000_000 + 1
    clock.utc += 600 * 1_000_000_000 + 1
    _path, _sha, sample = observer.sample()
    assert sample["result"] == "INCOMPLETE"
    assert "acceptance_observation_gap" in cast(list[object], sample["blocking_findings"])


def test_service_instance_change_fails_closed(tmp_path: Path) -> None:
    observer, clock, _manager = _observer(tmp_path)
    observer.start()
    ServiceStateStore(observer.data_root / "state" / "service_state.json").write(
        {
            "status": "RUNNING",
            "pid": 123,
            "service_instance_id": "service-b",
            "deployment_identity": {
                "identity_sha256": observer.identity.identity_sha256,
                "source_git_sha": observer.identity.source_git_sha,
                "wheel_sha256": observer.identity.wheel_sha256,
                "config_sha256": observer.identity.config_sha256,
                "systemd_unit_sha256": observer.identity.systemd_unit_sha256,
                "capacity_profile_id": observer.identity.capacity_profile_id,
            },
        }
    )
    clock.boot += 1
    _path, _sha, sample = observer.sample()
    assert sample["result"] == "FAIL"
    assert "service_instance_id_changed" in cast(list[object], sample["blocking_findings"])


def test_finding_details_are_first_occurrence_only(tmp_path: Path) -> None:
    observer, clock, manager = _observer(tmp_path)
    observer.start()
    manager.incarnation["invocation_id"] = "b" * 32
    clock.boot += 1
    _path, _sha, first = observer.sample()
    clock.boot += 1
    _path, _sha, second = observer.sample()
    first_details = cast(dict[str, object], first["new_finding_details"])
    second_details = cast(dict[str, object], second["new_finding_details"])
    assert "process_incarnation_changed" in first_details
    assert "process_incarnation_changed" not in second_details
    assert "process_incarnation_changed" in cast(list[object], second["blocking_findings"])


def _synthetic_manifest_records(count: int) -> dict[str, dict[str, object]]:
    return {
        f"data/manifests/{ordinal:08d}.manifest.json": {
            "path": f"data/manifests/{ordinal:08d}.manifest.json",
            "sha256": f"{ordinal:064x}"[-64:],
            "chunk_id": f"chunk-{ordinal:08d}",
        }
        for ordinal in range(count)
    }


def test_manifest_transition_serialization_is_delta_bounded(tmp_path: Path) -> None:
    observer, _clock, _manager = _observer(tmp_path)
    sizes: list[int] = []
    for count in (1_000, 100_000):
        baseline = _synthetic_manifest_records(count)
        current = dict(baseline)
        added_path = f"data/manifests/{count:08d}.manifest.json"
        current[added_path] = {
            "path": added_path,
            "sha256": "f" * 64,
            "chunk_id": f"chunk-{count:08d}",
        }
        observer.published_manifest_records = baseline
        transition = observer._manifest_transition(
            current,
            {path: str(member["sha256"]) for path, member in current.items()},
        )
        assert len(cast(list[object], transition["added_members"])) == 1
        sizes.append(len(canonical_json({"manifest_transition": transition})))
    assert sizes[1] - sizes[0] < 4_096
    assert sizes[1] < sizes[0] * 10


def test_hundred_thousand_historical_raw_absences_are_not_repeated(tmp_path: Path) -> None:
    members = {
        (f"chunk-{ordinal:08d}", f"data/manifests/{ordinal:08d}.manifest.json"): {
            "chunk_id": f"chunk-{ordinal:08d}",
            "manifest_path": f"data/manifests/{ordinal:08d}.manifest.json",
            "classification": "AUTHORIZED_LOCAL_DELETE",
            "has_sequence_gap_marker": "false",
        }
        for ordinal in range(100_000)
    }
    baseline = {
        "count": len(members),
        "aggregate_sha256": _absence_aggregate(members),
        "members": [dict(members[key]) for key in sorted(members)],
    }
    transition = _raw_absence_transition(members, members)
    baseline_size = len(canonical_json({"raw_absence_baseline": baseline}))
    sample_size = len(canonical_json({"raw_absence_transition": transition}))
    assert transition["added"] == []
    assert transition["reclassified"] == []
    assert transition["resolved"] == []
    assert sample_size < 4_096
    assert baseline_size > sample_size * 1_000


@pytest.mark.parametrize("sample_count", [10, 1_000, 10_000])
def test_v2_chain_verifier_keeps_only_rolling_state(
    tmp_path: Path, sample_count: int
) -> None:
    observer, clock, _manager = _observer(tmp_path)
    _start_path, start_sha, _start = observer.start()
    clock.utc += 1
    clock.boot += 1
    _sample_path, previous_sha, template = observer.sample()
    for ordinal in range(1, sample_count):
        document = dict(template)
        document["sample_ordinal"] = ordinal
        document["previous_sample_sha256"] = previous_sha
        document["observed_at_utc_ns"] = clock.utc + ordinal
        document["observed_at_boottime_ns"] = clock.boot + ordinal
        _path, previous_sha = _publish(
            observer.evidence_root, f"sample-{ordinal:08d}.json", document
        )
    state = _sample_chain(
        observer.evidence_root,
        start=_read_json(observer.evidence_root / "stage-start.json"),
        start_sha=start_sha,
        identity=observer.identity,
        require_eligible=True,
    )
    assert state.last is not None
    assert state.last.ordinal == sample_count - 1
    assert not hasattr(state, "samples")


def test_backward_utc_does_not_shorten_boottime_interval(tmp_path: Path) -> None:
    observer, clock, _manager = _observer(tmp_path)
    observer.start()
    clock.utc -= 100
    clock.boot += 1
    _path, _sha, sample = observer.sample()
    assert sample["result"] == "INCOMPLETE"
    assert "unsafe_wall_clock_backward" in cast(list[object], sample["blocking_findings"])


def test_resume_keeps_original_t0_and_rejects_published_collision(tmp_path: Path) -> None:
    observer, clock, manager = _observer(tmp_path)
    _start, _start_sha, _document = observer.start()
    original_t0 = observer.t0_boottime_ns
    clock.boot += 1
    observer.sample()
    resumed = resume_observer(
        observer.evidence_root,
        data_root=observer.data_root,
        identity=observer.identity,
        manager=manager,  # type: ignore[arg-type]
        evaluator=FakeEvaluator(),  # type: ignore[arg-type]
        clock=clock,
        disk_usage=observer.disk_usage,
    )
    assert resumed.t0_boottime_ns == original_t0
    assert resumed.run_id == observer.run_id
    assert resumed.prior_stage_sha256 == "a" * 64
    assert resumed.t0_manifest_members == observer.t0_manifest_members
    with pytest.raises(AcceptanceError, match="collision"):
        _publish(observer.evidence_root, "stage-start.json", {"x": 1})


def test_resume_restores_the_published_pre_t0_baseline_membership(tmp_path: Path) -> None:
    observer, clock, manager = _observer(tmp_path)
    layout = ensure_storage_layout(observer.data_root)
    with Catalog(layout.catalog) as catalog:
        seal_chunk(layout, catalog, [usdm_envelope("conn-a", 1)])
    observer.start()
    baseline = dict(observer.t0_manifest_members or {})
    start_document = json.loads(
        (observer.evidence_root / "stage-start.json").read_text(encoding="utf-8")
    )
    baseline_document = cast(dict[str, object], start_document["manifest_baseline"])
    assert {
        str(member["path"]): str(member["sha256"])
        for member in cast(list[dict[str, object]], baseline_document["members"])
    } == baseline
    clock.boot += 1
    observer.sample()
    resumed = resume_observer(
        observer.evidence_root,
        data_root=observer.data_root,
        identity=observer.identity,
        manager=manager,  # type: ignore[arg-type]
        evaluator=FakeEvaluator(),  # type: ignore[arg-type]
        clock=clock,
        disk_usage=observer.disk_usage,
    )
    assert resumed.t0_manifest_members == baseline


def test_post_t0_seal_with_pre_t0_event_is_current_after_baseline_freeze(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    observer, clock, _manager = _observer(tmp_path)
    layout = ensure_storage_layout(observer.data_root)
    clock.utc = 2_000_000_000
    sealed = False
    original = observer._observation

    def hooked(*args: Any, **kwargs: Any) -> tuple[dict[str, object], list[str]]:
        nonlocal sealed
        if not sealed:
            with Catalog(layout.catalog) as catalog:
                seal_chunk(
                    layout,
                    catalog,
                    [usdm_envelope("conn-before", 1), usdm_envelope("conn-after", 2)],
                )
            sealed = True
        return original(*args, **kwargs)

    monkeypatch.setattr(observer, "_observation", hooked)
    _path, _sha, start = observer.start()
    assert observer.t0_manifest_members == {}
    reconnect = cast(dict[str, object], start["reconnect_transition"])
    assert reconnect["added"]
    assert start["result"] == "FAIL"
    assert "UNMARKED_RECONNECT" in cast(list[object], start["blocking_findings"])


def test_new_manifest_is_published_once_as_exact_delta(tmp_path: Path) -> None:
    observer, clock, _manager = _observer(tmp_path)
    layout = ensure_storage_layout(observer.data_root)
    with Catalog(layout.catalog) as catalog:
        seal_chunk(layout, catalog, [usdm_envelope("conn-a", 1)])
    observer.start()
    with Catalog(layout.catalog) as catalog:
        seal_chunk(layout, catalog, [usdm_envelope("conn-b", 2)])
    clock.utc += 300 * 1_000_000_000
    clock.boot += 300 * 1_000_000_000
    _path, _sha, sample = observer.sample()
    transition = cast(dict[str, object], sample["manifest_transition"])
    added = cast(list[dict[str, object]], transition["added_members"])
    assert len(added) == 1
    assert set(added[0]) == {"path", "sha256", "chunk_id"}
    clock.utc += 300 * 1_000_000_000
    clock.boot += 300 * 1_000_000_000
    _path, _sha, unchanged = observer.sample()
    assert cast(dict[str, object], unchanged["manifest_transition"])["added_members"] == []


def test_resume_rejects_continuation_not_bound_to_sample_inventory(tmp_path: Path) -> None:
    observer, clock, manager = _observer(tmp_path)
    observer.start()
    clock.utc += 1
    clock.boot += 1
    sample_path, _sample_sha, _sample = observer.sample()
    document = json.loads(sample_path.read_text(encoding="utf-8"))
    reconnect = cast(dict[str, object], document["reconnect_transition"])
    continuation = cast(dict[str, object], reconnect["continuation"])
    continuation["streams"] = {"malformed": []}
    sample_path.write_bytes(canonical_json(document))
    with pytest.raises(AcceptanceError, match="reconnect continuation"):
        resume_observer(
            observer.evidence_root,
            data_root=observer.data_root,
            identity=observer.identity,
            manager=manager,  # type: ignore[arg-type]
            evaluator=FakeEvaluator(),  # type: ignore[arg-type]
            clock=clock,
            disk_usage=observer.disk_usage,
        )


def test_resume_accepts_post_boundary_inventory_member_deferred_from_continuation() -> None:
    included_path = "data/manifests/included.manifest.json"
    deferred_path = "data/manifests/deferred.manifest.json"
    continuation = {
        "schema_version": reconnect_audit.INCREMENTAL_SCHEMA_VERSION,
        "manifest_members": {included_path: "a" * 64},
        "streams": {},
    }
    document = {
        "manifest_inventory": {
            "members": [
                {"path": included_path, "sha256": "a" * 64, "chunk_id": "included"},
                {"path": deferred_path, "sha256": "b" * 64, "chunk_id": "deferred"},
            ]
        },
        "reconnect_summary": {"continuation": continuation},
    }

    assert _continuation_from(document) == continuation


def test_canonical_json_is_sorted_and_has_one_trailing_newline() -> None:
    assert canonical_json({"z": 1, "a": 2}) == b'{"a":2,"z":1}\n'


def test_evidence_root_inside_or_symlinked_into_data_root_is_rejected(tmp_path: Path) -> None:
    data_root = tmp_path / "recorder"
    data_root.mkdir()
    with pytest.raises(AcceptanceError):
        _safe_evidence_root(data_root / "evidence", data_root)
    alias = tmp_path / "alias"
    alias.symlink_to(data_root, target_is_directory=True)
    with pytest.raises(AcceptanceError):
        _safe_evidence_root(alias / "evidence", data_root)


def test_prior_stage_requires_exact_canonical_identity_and_stage_order(tmp_path: Path) -> None:
    identity = _identity(tmp_path)
    root = tmp_path / "prior"
    document = {
        "schema_version": "m22.9-acceptance-evidence.v1",
        "evidence_kind": "stage-final",
        "stage": "2h",
        "result": "PASS_CANDIDATE",
        "deployment_identity_sha256": identity.identity_sha256,
        "source_git_sha": identity.source_git_sha,
        "wheel_sha256": identity.wheel_sha256,
        "config_sha256": identity.config_sha256,
        "systemd_unit_sha256": identity.systemd_unit_sha256,
        "capacity_profile_id": identity.capacity_profile_id,
    }
    # This deliberately remains a non-acceptance-shaped record; the exact
    # schema validator must reject it before stage order can be trusted.
    root.mkdir()
    path = root / "stage-final.json"
    path.write_bytes(canonical_json(document))
    with pytest.raises(AcceptanceError):
        verify_prior_stage(path, identity, "12h")


def test_historical_v1_chain_remains_readable_and_unchanged(tmp_path: Path) -> None:
    observer, _clock, _manager = _observer(tmp_path)
    stage_root = _publish_legacy_chain(observer, finalize=True)
    before = {
        path.name: path.read_bytes()
        for path in stage_root.glob("*.json")
    }
    verified, _digest = verify_completed_stage(stage_root, observer.identity, expected_stage="2h")
    assert verified["schema_version"] == LEGACY_SCHEMA_VERSION
    after = {path.name: path.read_bytes() for path in stage_root.glob("*.json")}
    assert after == before


def test_v1_stage_cannot_resume_as_v2(tmp_path: Path) -> None:
    observer, _clock, _manager = _observer(tmp_path)
    stage_root = _publish_legacy_chain(observer, finalize=False)
    with pytest.raises(AcceptanceError, match="v1 failed stage"):
        resume_observer(
            stage_root,
            data_root=observer.data_root,
            identity=observer.identity,
            manager=observer.manager,
            evaluator=observer.evaluator,
            clock=observer.clock,
            disk_usage=observer.disk_usage,
        )


def _complete_2h_stage(
    tmp_path: Path,
) -> tuple[AcceptanceObserver, Path, dict[str, object]]:
    observer, clock, _manager = _observer(tmp_path)
    observer.prior_stage_sha256 = _publish_valid_identity_and_readiness(observer)
    _start_path, _start_sha, start = observer.start()
    for _ in range(24):
        clock.utc += 300 * 1_000_000_000
        clock.boot += 300 * 1_000_000_000
        observer.sample()
    final_path, _final_sha, _final = observer.finalize()
    return observer, final_path, start


def _rewrite(path: Path, **changes: object) -> None:
    document = json.loads(path.read_text(encoding="utf-8"))
    document.update(changes)
    path.write_bytes(canonical_json(document))


def _read_json(path: Path) -> dict[str, object]:
    return cast(dict[str, object], json.loads(path.read_text(encoding="utf-8")))


def _legacy_stage_document(
    observer: AcceptanceObserver,
    *,
    kind: str,
    boot: int,
    utc: int,
    run_id: str = "legacy-run",
) -> dict[str, object]:
    document = _empty_common(
        kind=kind,
        stage="2h",
        run_id=run_id,
        identity=observer.identity,
        now_utc=utc,
        now_boot=boot,
        boot_id="boot-a",
    )
    document["schema_version"] = LEGACY_SCHEMA_VERSION
    continuation = {
        "schema_version": reconnect_audit.INCREMENTAL_SCHEMA_VERSION,
        "manifest_members": {},
        "streams": {},
    }
    document.update(
        {
            "readiness": {},
            "catalog_integrity": {},
            "capacity": {},
            "discontinuity_summary": {},
            "manifest_inventory": {
                "count": 0,
                "sha256": sha256_bytes(b""),
                "members": [],
                "artifact_absences": [],
            },
            "reconnect_summary": {
                "baseline_manifest_members": {},
                "continuation": continuation,
            },
            "blocking_findings": [],
            "observer_status": "COMPLETE",
            "result": "PASS_CANDIDATE",
            "systemd_process_incarnation": observer.manager.process_incarnation(),
            "service_instance_id": "service-a",
            "prior_stage_evidence_sha256": "a" * 64,
        }
    )
    return document


def _publish_legacy_predecessors(observer: AcceptanceObserver) -> str:
    root = observer.evidence_root.parent
    identity_document = _legacy_stage_document(
        observer, kind="identity-result", boot=10, utc=10, run_id="legacy-identity"
    )
    identity_document.update(
        {
            "stage": "identity",
            "identity": observer.identity.document(),
            "identity_static_verification": {},
        }
    )
    _identity_path, identity_sha = _publish(root, "identity-result.json", identity_document)
    readiness_document = _legacy_stage_document(
        observer, kind="readiness-result", boot=11, utc=11, run_id="legacy-readiness"
    )
    readiness_document.update(
        {
            "stage": "readiness",
            "prior_stage_evidence_sha256": identity_sha,
            "readiness": {
                "schema_version": "deployment-readiness.v1",
                "state": "READY",
                "reasons": [],
                "evidence": {},
            },
        }
    )
    _readiness_path, readiness_sha = _publish(
        root, "readiness-result.json", readiness_document
    )
    return readiness_sha


def _publish_legacy_chain(observer: AcceptanceObserver, *, finalize: bool) -> Path:
    prior_sha = _publish_legacy_predecessors(observer)
    root = observer.evidence_root
    start = _legacy_stage_document(observer, kind="stage-start", boot=100, utc=100)
    start["prior_stage_evidence_sha256"] = prior_sha
    start["stage_start_evidence_sha256"] = None
    start["previous_sample_sha256"] = None
    _start_path, start_sha = _publish(root, "stage-start.json", start)
    sample_count = 24 if finalize else 1
    sample: dict[str, object] = {}
    sample_sha = ""
    previous_sha: str | None = None
    for ordinal in range(sample_count):
        sample = _legacy_stage_document(
            observer,
            kind="stage-sample",
            boot=100 + (ordinal + 1) * 300 * 1_000_000_000,
            utc=100 + (ordinal + 1) * 300 * 1_000_000_000,
        )
        sample["prior_stage_evidence_sha256"] = prior_sha
        sample["stage_start_evidence_sha256"] = start_sha
        sample["previous_sample_sha256"] = previous_sha
        _sample_path, sample_sha = _publish(root, f"sample-{ordinal:08d}.json", sample)
        previous_sha = sample_sha
    if finalize:
        final = dict(sample)
        final.update(
            {
                "evidence_kind": "stage-final",
                "stage_start_evidence_sha256": start_sha,
                "previous_sample_sha256": sample_sha,
                "elapsed_boottime_ns": STAGE_DURATION_NS["2h"],
                "required_duration_ns": STAGE_DURATION_NS["2h"],
                "observer_status": "FINALIZED",
            }
        )
        _final_path, _final_sha = _publish(root, "stage-final.json", final)
    return root


def test_stage_start_binds_exact_predecessor_and_valid_chain_is_accepted(
    tmp_path: Path,
) -> None:
    observer, final_path, start = _complete_2h_stage(tmp_path)
    assert start["prior_stage_evidence_sha256"] != "a" * 64
    verified, digest = verify_completed_stage(
        observer.evidence_root, observer.identity, expected_stage="2h"
    )
    assert verified["result"] == "PASS_CANDIDATE"
    assert len(digest) == 64
    prior, prior_digest = verify_prior_stage(final_path, observer.identity, "12h")
    assert prior == verified
    assert prior_digest == digest


def test_completed_stage_rejects_nonexistent_readiness_predecessor(
    tmp_path: Path,
) -> None:
    observer, _clock, _manager = _observer(tmp_path)
    observer.prior_stage_sha256 = "a" * 64
    observer.start()
    for _ in range(24):
        cast(FakeClock, observer.clock).utc += 300 * 1_000_000_000
        cast(FakeClock, observer.clock).boot += 300 * 1_000_000_000
        observer.sample()
    observer.finalize()
    with pytest.raises(AcceptanceError, match="readiness"):
        verify_completed_stage(observer.evidence_root, observer.identity, expected_stage="2h")


def test_readiness_predecessor_requires_actual_identity_object(tmp_path: Path) -> None:
    observer, _clock, _manager = _observer(tmp_path)
    _publish_valid_identity_and_readiness(observer)
    (observer.evidence_root.parent / "identity-result.json").unlink()
    with pytest.raises(AcceptanceError, match="published evidence"):
        verify_prior_stage(
            observer.evidence_root.parent / "readiness-result.json",
            observer.identity,
            "2h",
        )


def test_forged_not_ready_readiness_is_rejected(tmp_path: Path) -> None:
    observer, _clock, _manager = _observer(tmp_path)
    _publish_valid_identity_and_readiness(observer)
    readiness_path = observer.evidence_root.parent / "readiness-result.json"
    readiness = json.loads(readiness_path.read_text(encoding="utf-8"))
    cast(dict[str, object], readiness["readiness"])["state"] = "NOT_READY"
    readiness_path.write_bytes(canonical_json(readiness))
    with pytest.raises(AcceptanceError, match="actual READY"):
        verify_prior_stage(readiness_path, observer.identity, "2h")


def test_readiness_identity_digest_mismatch_is_rejected(tmp_path: Path) -> None:
    observer, _clock, _manager = _observer(tmp_path)
    _publish_valid_identity_and_readiness(observer)
    readiness_path = observer.evidence_root.parent / "readiness-result.json"
    readiness = json.loads(readiness_path.read_text(encoding="utf-8"))
    readiness["prior_stage_evidence_sha256"] = "b" * 64
    readiness_path.write_bytes(canonical_json(readiness))
    with pytest.raises(AcceptanceError, match="identity predecessor digest"):
        verify_prior_stage(readiness_path, observer.identity, "2h")


def _complete_12h_chain(
    tmp_path: Path,
) -> tuple[AcceptanceObserver, AcceptanceObserver, Path]:
    stage_2h, clock, manager = _observer(tmp_path)
    readiness_sha = _publish_valid_identity_and_readiness(stage_2h)
    stage_2h.prior_stage_sha256 = readiness_sha
    _start, _start_sha, _start_document = stage_2h.start()
    for _ in range(24):
        clock.utc += 300 * 1_000_000_000
        clock.boot += 300 * 1_000_000_000
        stage_2h.sample()
    stage_2h.finalize()
    stage_2h_sha = __import__("hashlib").sha256(
        (stage_2h.evidence_root / "stage-final.json").read_bytes()
    ).hexdigest()
    stage_12h = AcceptanceObserver(
        stage="12h",
        run_id="run-12h",
        data_root=stage_2h.data_root,
        evidence_root=stage_2h.evidence_root.parent / "12h-run-b",
        identity=stage_2h.identity,
        prior_stage_sha256=stage_2h_sha,
        manager=manager,  # type: ignore[arg-type]
        evaluator=stage_2h.evaluator,
        clock=clock,
        identity_verifier=stage_2h.identity_verifier,
        disk_usage=stage_2h.disk_usage,
    )
    stage_12h.start()
    for _ in range(144):
        clock.utc += 300 * 1_000_000_000
        clock.boot += 300 * 1_000_000_000
        stage_12h.sample()
    final_path, _final_sha, _final = stage_12h.finalize()
    return stage_2h, stage_12h, final_path


def test_valid_identity_readiness_2h_12h_chain_is_accepted(tmp_path: Path) -> None:
    stage_2h, stage_12h, final_path = _complete_12h_chain(tmp_path)
    verified_2h, _digest_2h = verify_completed_stage(
        stage_2h.evidence_root, stage_2h.identity, expected_stage="2h"
    )
    assert verified_2h["result"] == "PASS_CANDIDATE"
    verified_12h, _digest_12h = verify_prior_stage(
        final_path, stage_12h.identity, "24h"
    )
    assert verified_12h["stage"] == "12h"


def test_corrupted_older_predecessor_rejects_later_stage(tmp_path: Path) -> None:
    stage_2h, stage_12h, final_path = _complete_12h_chain(tmp_path)
    _rewrite(stage_2h.evidence_root / "stage-final.json", result="FAIL")
    with pytest.raises(AcceptanceError, match="predecessor authority"):
        verify_prior_stage(final_path, stage_12h.identity, "24h")


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("elapsed_boottime_ns", STAGE_DURATION_NS["2h"] - 1),
        ("required_duration_ns", STAGE_DURATION_NS["2h"] + 1),
    ],
)
def test_completed_stage_rejects_false_duration_authority(
    tmp_path: Path, field: str, value: int
) -> None:
    observer, final_path, _start = _complete_2h_stage(tmp_path)
    _rewrite(final_path, **{field: value})
    with pytest.raises(AcceptanceError, match="duration"):
        verify_completed_stage(observer.evidence_root, observer.identity)


def test_standalone_canonical_stage_final_cannot_authorize_next_stage(
    tmp_path: Path,
) -> None:
    observer, final_path, _start = _complete_2h_stage(tmp_path / "source")
    standalone = tmp_path / "standalone"
    standalone.mkdir()
    copied_final = standalone / "stage-final.json"
    copied_final.write_bytes(final_path.read_bytes())
    with pytest.raises(AcceptanceError, match="published evidence"):
        verify_prior_stage(copied_final, observer.identity, "12h")


def test_completed_stage_rejects_tampered_stage_start_digest(tmp_path: Path) -> None:
    observer, _final_path, _start = _complete_2h_stage(tmp_path)
    sample = observer.evidence_root / "sample-00000000.json"
    _rewrite(sample, stage_start_evidence_sha256="b" * 64)
    with pytest.raises(AcceptanceError, match="stage-start digest"):
        verify_completed_stage(observer.evidence_root, observer.identity)


def test_completed_stage_rejects_tampered_previous_sample_digest(tmp_path: Path) -> None:
    observer, _final_path, _start = _complete_2h_stage(tmp_path)
    sample = observer.evidence_root / "sample-00000001.json"
    _rewrite(sample, previous_sample_sha256="b" * 64)
    with pytest.raises(AcceptanceError, match="sample hash chain"):
        verify_completed_stage(observer.evidence_root, observer.identity)


def test_completed_stage_rejects_missing_sample_ordinal(tmp_path: Path) -> None:
    observer, _final_path, _start = _complete_2h_stage(tmp_path)
    (observer.evidence_root / "sample-00000001.json").rename(
        observer.evidence_root / "sample-00000099.json"
    )
    with pytest.raises(AcceptanceError, match="ordinals"):
        verify_completed_stage(observer.evidence_root, observer.identity)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("run_id", "mixed-run"),
        ("stage", "12h"),
        ("source_git_sha", "0" * 40),
    ],
)
def test_completed_stage_rejects_mixed_chain_identity(
    tmp_path: Path, field: str, value: str
) -> None:
    observer, _final_path, _start = _complete_2h_stage(tmp_path)
    sample = observer.evidence_root / "sample-00000000.json"
    _rewrite(sample, **{field: value})
    with pytest.raises(AcceptanceError, match="identity chain"):
        verify_completed_stage(observer.evidence_root, observer.identity)


def test_completed_stage_rejects_final_pointing_to_non_last_sample(tmp_path: Path) -> None:
    observer, final_path, _start = _complete_2h_stage(tmp_path)
    first_body = (observer.evidence_root / "sample-00000000.json").read_bytes()
    first_sha = __import__("hashlib").sha256(first_body).hexdigest()
    _rewrite(final_path, previous_sample_sha256=first_sha)
    with pytest.raises(AcceptanceError, match="terminus"):
        verify_completed_stage(observer.evidence_root, observer.identity)


def test_completed_stage_rejects_ineligible_sample_result(tmp_path: Path) -> None:
    observer, _final_path, _start = _complete_2h_stage(tmp_path)
    last_sample = observer.evidence_root / "sample-00000024.json"
    _rewrite(last_sample, result="INCOMPLETE")
    with pytest.raises(AcceptanceError, match="ineligible sample result"):
        verify_completed_stage(observer.evidence_root, observer.identity)


def test_closed_historical_reconnect_findings_are_bound_as_baseline(
    tmp_path: Path,
) -> None:
    build_fixture(tmp_path / "recorder")
    observer, clock, _manager = _observer(tmp_path)
    clock.utc = 2_000_000_000
    _path, _sha, start = observer.start()
    assert start["result"] == "PASS_CANDIDATE", start["blocking_findings"]
    reconnect = cast(dict[str, object], start["reconnect_baseline"])
    assert cast(list[object], reconnect["pre_t0_history"])
    assert "UNMARKED_RECONNECT" not in cast(list[object], start["blocking_findings"])


def test_unresolved_gap_open_at_t0_blocks(tmp_path: Path) -> None:
    observer, clock, _manager = _observer(tmp_path)
    with Catalog(observer.data_root / "state" / "catalog.sqlite") as catalog:
        catalog.record_operational_event(
            event_id="stream-discontinuity-started:open-at-t0",
            event_type="STREAM_DISCONTINUITY_STARTED",
            occurred_at_utc_ns=clock.utc - 1,
            evidence={
                "gap_id": "open-at-t0",
                "market": "um_perpetual",
                "symbol": "BTCUSDT",
                "stream": "book_ticker",
                "gap_started_at_utc_ns": clock.utc - 1,
            },
            symbol="BTCUSDT",
        )
    _path, _sha, start = observer.start()
    assert start["result"] == "FAIL"
    assert "unresolved_discontinuity" in cast(list[object], start["blocking_findings"])


def test_unresolved_gap_cannot_terminate_an_eligible_stage(tmp_path: Path) -> None:
    observer, clock, _manager = _observer(tmp_path)
    with Catalog(observer.data_root / "state" / "catalog.sqlite") as catalog:
        catalog.record_operational_event(
            event_id="stream-discontinuity-started:final-blocker",
            event_type="STREAM_DISCONTINUITY_STARTED",
            occurred_at_utc_ns=clock.utc - 1,
            evidence={
                "gap_id": "final-blocker",
                "market": "um_perpetual",
                "symbol": "BTCUSDT",
                "stream": "book_ticker",
                "gap_started_at_utc_ns": clock.utc - 1,
            },
            symbol="BTCUSDT",
        )
    _path, _sha, start = observer.start()
    assert start["result"] == "FAIL"
    clock.boot += STAGE_DURATION_NS["2h"]
    clock.utc += STAGE_DURATION_NS["2h"]
    observer.finalize()
    with pytest.raises(AcceptanceError, match="stage-start is not eligible"):
        verify_completed_stage(observer.evidence_root, observer.identity)


def test_boundary_crossing_t0_is_current_and_blocking(tmp_path: Path) -> None:
    observer, clock, _manager = _observer(tmp_path)
    clock.utc = 1_000_000_005
    layout = ensure_storage_layout(observer.data_root)
    with Catalog(layout.catalog) as catalog:
        seal_chunk(
            layout,
            catalog,
            [usdm_envelope("conn-before", 1), usdm_envelope("conn-after", 10)],
        )
    _path, _sha, start = observer.start()
    assert start["result"] == "FAIL"
    assert "UNMARKED_RECONNECT" in cast(list[object], start["blocking_findings"])


def test_new_post_t0_unmarked_boundary_blocks(tmp_path: Path) -> None:
    observer, clock, _manager = _observer(tmp_path)
    clock.utc = 1_000_000_005
    layout = ensure_storage_layout(observer.data_root)
    with Catalog(layout.catalog) as catalog:
        seal_chunk(layout, catalog, [usdm_envelope("conn-a", 1)])
    _start_path, _start_sha, start = observer.start()
    assert start["result"] == "PASS_CANDIDATE", start["blocking_findings"]
    with Catalog(layout.catalog) as catalog:
        seal_chunk(layout, catalog, [usdm_envelope("conn-b", 10)])
    clock.utc += 300 * 1_000_000_000
    clock.boot += 300 * 1_000_000_000
    _path, _sha, sample = observer.sample()
    assert sample["result"] == "FAIL"
    assert "UNMARKED_RECONNECT" in cast(list[object], sample["blocking_findings"])


def test_pre_t0_reconnect_sealed_after_t0_is_current_and_blocking(tmp_path: Path) -> None:
    observer, clock, _manager = _observer(tmp_path)
    layout = ensure_storage_layout(observer.data_root)
    writer = RawChunkWriter(
        layout=layout,
        catalog=Catalog(layout.catalog),
        market="um_perpetual",
        symbol="BTCUSDT",
        stream="book_ticker",
        collector_instance_id="audit-test",
        collector_version="0.1.0+test",
        rotation=RotationPolicy(seconds=60),
        durability_interval_seconds=0,
    )
    writer.append(usdm_envelope("conn-before", 1))
    writer.append(usdm_envelope("conn-after", 2))
    writer.close()
    catalog = writer.catalog
    assert not list(layout.manifests.glob("*.manifest.json"))
    clock.utc = 2_000_000_000
    observer.start()
    assert observer.reconnect_continuation is not None
    assert observer.reconnect_continuation["manifest_members"] == {}
    with catalog:
        seal_partial(writer.path, layout=layout, catalog=catalog)
    clock.utc += 300 * 1_000_000_000
    clock.boot += 300 * 1_000_000_000
    _path, _sha, sample = observer.sample()
    assert sample["result"] == "FAIL"
    assert "UNMARKED_RECONNECT" in cast(list[object], sample["blocking_findings"])


def test_post_t0_intervening_manifest_prevents_historical_grandfathering(
    tmp_path: Path,
) -> None:
    observer, clock, _manager = _observer(tmp_path)
    layout = ensure_storage_layout(observer.data_root)
    with Catalog(layout.catalog) as catalog:
        seal_chunk(layout, catalog, [usdm_envelope("conn-before", 1)])
    observer.start()
    with Catalog(layout.catalog) as catalog:
        seal_chunk(layout, catalog, [])
        seal_chunk(layout, catalog, [usdm_envelope("conn-after", 2)])
    clock.utc += 300 * 1_000_000_000
    clock.boot += 300 * 1_000_000_000
    _path, _sha, sample = observer.sample()
    assert sample["result"] == "FAIL"
    assert "UNMARKED_RECONNECT" in cast(list[object], sample["blocking_findings"])


def test_advancing_boottime_generates_a_self_verifying_duration_chain(
    tmp_path: Path,
) -> None:
    observer, _clock, _manager = _observer(tmp_path)
    clock = AdvancingClock()
    observer.clock = clock
    observer.prior_stage_sha256 = _publish_valid_identity_and_readiness(observer)
    _start_path, start_sha, start = observer.start()
    assert start["observed_at_boottime_ns"] == observer.t0_boottime_ns
    assert start["observed_at_utc_ns"] == observer.t0_utc_ns
    for _ in range(24):
        clock.utc += 300 * 1_000_000_000
        clock.boot += 300 * 1_000_000_000
        observer.sample()
    clock.utc += 1
    clock.boot += 1
    final_path, _final_sha, final = observer.finalize()
    assert final["result"] == "PASS_CANDIDATE"
    assert final["stage_start_evidence_sha256"] == start_sha
    verified, verified_sha = verify_completed_stage(
        observer.evidence_root, observer.identity, expected_stage="2h"
    )
    assert verified["result"] == "PASS_CANDIDATE"
    assert verified_sha == __import__("hashlib").sha256(final_path.read_bytes()).hexdigest()
    prior, prior_sha = verify_prior_stage(final_path, observer.identity, "12h")
    assert prior == verified
    assert prior_sha == verified_sha


def test_historical_manifest_byte_change_after_baseline_fails_closed(tmp_path: Path) -> None:
    observer, clock, _manager = _observer(tmp_path)
    layout = ensure_storage_layout(observer.data_root)
    with Catalog(layout.catalog) as catalog:
        seal_chunk(layout, catalog, [usdm_envelope("conn-a", 1)])
    observer.start()
    manifest_path = next(layout.manifests.glob("*.manifest.json"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    clock.utc += 300 * 1_000_000_000
    clock.boot += 300 * 1_000_000_000
    _path, _sha, sample = observer.sample()
    assert sample["result"] == "FAIL"
    assert "manifest_byte_mutation_or_loss" in cast(
        list[object], sample["blocking_findings"]
    )


def test_historical_manifest_loss_fails_closed(tmp_path: Path) -> None:
    observer, clock, _manager = _observer(tmp_path)
    layout = ensure_storage_layout(observer.data_root)
    with Catalog(layout.catalog) as catalog:
        seal_chunk(layout, catalog, [usdm_envelope("conn-a", 1)])
    observer.start()
    next(layout.manifests.glob("*.manifest.json")).unlink()
    clock.utc += 300 * 1_000_000_000
    clock.boot += 300 * 1_000_000_000
    _path, _sha, sample = observer.sample()
    assert sample["result"] == "FAIL"
    assert "manifest_byte_mutation_or_loss" in cast(
        list[object], sample["blocking_findings"]
    )
    details = cast(dict[str, object], sample["new_finding_details"])
    assert cast(list[object], details["manifest_byte_mutation_or_loss"])


def test_incremental_observer_does_not_rescan_old_raw_and_scans_one_new_chunk(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    observer, clock, _manager = _observer(tmp_path)
    layout = ensure_storage_layout(observer.data_root)
    with Catalog(layout.catalog) as catalog:
        seal_chunk(layout, catalog, [usdm_envelope("conn-a", 1)])
    counts = {"scan": 0, "validate": 0}
    real_scan = reconnect_audit.scan_chunk_frames
    real_validate = validate_sealed_artifact

    def counted_scan(path: Path, manifest: dict[str, object]) -> list[object]:
        counts["scan"] += 1
        return cast(list[object], real_scan(path, manifest))

    def counted_validate(path: Path, manifest: dict[str, object]) -> None:
        counts["validate"] += 1
        real_validate(path, manifest)

    monkeypatch.setattr(reconnect_audit, "scan_chunk_frames", counted_scan)
    monkeypatch.setattr(reconnect_audit, "validate_sealed_artifact", counted_validate)
    observer.start()
    assert counts == {"scan": 1, "validate": 1}
    clock.utc += 300 * 1_000_000_000
    clock.boot += 300 * 1_000_000_000
    observer.sample()
    assert counts == {"scan": 1, "validate": 1}
    with Catalog(layout.catalog) as catalog:
        seal_chunk(layout, catalog, [usdm_envelope("conn-b", 10)])
    clock.utc += 300 * 1_000_000_000
    clock.boot += 300 * 1_000_000_000
    observer.sample()
    assert counts == {"scan": 2, "validate": 2}


def test_acceptance_observation_does_not_mutate_recorder_tree(tmp_path: Path) -> None:
    observer, clock, _manager = _observer(tmp_path)

    def snapshot() -> dict[str, bytes]:
        return {
            str(path.relative_to(observer.data_root)): path.read_bytes()
            for path in sorted(observer.data_root.rglob("*"))
            if path.is_file()
        }

    before = snapshot()
    observer.start()
    clock.utc += 300 * 1_000_000_000
    clock.boot += 300 * 1_000_000_000
    observer.sample()
    assert snapshot() == before
