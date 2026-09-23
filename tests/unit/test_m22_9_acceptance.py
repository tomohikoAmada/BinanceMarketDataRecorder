from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from binance_market_data_recorder.audit import reconnect_boundaries as reconnect_audit
from binance_market_data_recorder.domain.product import ProductKey
from binance_market_data_recorder.service.acceptance import (
    LEGACY_SCHEMA_VERSION,
    PREVIOUS_SCHEMA_VERSION,
    SCHEMA_VERSION,
    STAGE_DURATION_NS,
    V4_DEADLINE_NS,
    V4_SCHEMA_VERSION,
    AcceptanceError,
    AcceptanceObserver,
    V4AcceptanceObserver,
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


def test_v3_sample_is_a_compact_transition_and_final_is_terminal_only(
    tmp_path: Path,
) -> None:
    observer, clock, _manager = _observer(tmp_path)
    observer.start()
    clock.utc += 1
    clock.boot += 1
    _sample_path, _sample_sha, sample = observer.sample()
    assert sample["schema_version"] == SCHEMA_VERSION
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


def test_v3_real_1_909730489_second_reconnect_is_eligible(tmp_path: Path) -> None:
    observer, clock, _manager = _observer(tmp_path)
    observer.prior_stage_sha256 = _publish_valid_identity_and_readiness(observer)
    observer.start()
    t0 = cast(int, observer.t0_utc_ns)
    gap_id = "real-1-91s-gap"
    started_at = t0 + 1
    _record_gap_started(
        observer,
        event_id="real-1-91s-start",
        occurred_at_utc_ns=started_at,
        gap_id=gap_id,
    )

    first_sample_at = started_at + 1_084_904_697
    clock.utc = first_sample_at
    clock.boot = cast(int, observer.t0_boottime_ns) + (first_sample_at - t0)
    _open_path, _open_sha, open_sample = observer.sample()
    open_transition = cast(dict[str, object], open_sample["catalog_transition"])
    assert [
        item["gap_id"]
        for item in cast(list[dict[str, object]], open_transition["current_open"])
    ] == [gap_id]
    assert open_sample["blocking_findings"] == []
    assert open_sample["result"] == "PASS_CANDIDATE"

    completed_at = t0 + 1_909_730_489
    _record_gap_completed(
        observer,
        event_id="real-1-91s-complete",
        occurred_at_utc_ns=completed_at,
        gap_id=gap_id,
    )
    clock.utc = completed_at + 1
    clock.boot = cast(int, observer.t0_boottime_ns) + (clock.utc - t0)
    _closed_path, _closed_sha, closed_sample = observer.sample()
    closed_transition = cast(dict[str, object], closed_sample["catalog_transition"])
    assert [
        item["gap_id"]
        for item in cast(list[dict[str, object]], closed_transition["completed"])
    ] == [gap_id]
    assert closed_transition["current_open"] == []
    assert closed_sample["blocking_findings"] == []
    assert closed_sample["result"] == "PASS_CANDIDATE"

    _advance_stage_with_samples(observer, clock)
    clock.utc += 1
    clock.boot += 1
    _final_path, _final_sha, final = observer.finalize()
    assert final["schema_version"] == SCHEMA_VERSION
    assert final["blocking_findings"] == []
    assert final["result"] == "PASS_CANDIDATE"
    assert final["eligible_for_next_stage"] is True
    verified, _verified_sha = verify_completed_stage(
        observer.evidence_root, observer.identity, expected_stage="2h"
    )
    assert verified["eligible_for_next_stage"] is True


def test_v3_sampling_phase_invariance_for_one_reconnect_history(tmp_path: Path) -> None:
    final_results: dict[str, tuple[object, object]] = {}

    for phase in ("before", "during", "after"):
        observer, clock, _manager = _observer(tmp_path / phase)
        observer.prior_stage_sha256 = _publish_valid_identity_and_readiness(observer)
        observer.start()
        t0 = cast(int, observer.t0_utc_ns)
        t0_boot = cast(int, observer.t0_boottime_ns)
        gap_id = f"phase-{phase}-gap"

        def set_time(
            offset: int,
            *,
            phase_clock: FakeClock = clock,
            phase_t0: int = t0,
            phase_t0_boot: int = t0_boot,
        ) -> None:
            phase_clock.utc = phase_t0 + offset
            phase_clock.boot = phase_t0_boot + offset

        if phase == "before":
            set_time(1)
            _before_path, _before_sha, before = observer.sample()
            assert cast(dict[str, object], before["catalog_transition"])["current_open"] == []
            _record_gap_started(
                observer,
                event_id=f"phase-{phase}-start",
                occurred_at_utc_ns=t0 + 2,
                gap_id=gap_id,
            )
            set_time(3)
            _open_path, _open_sha, open_sample = observer.sample()
            assert cast(dict[str, object], open_sample["catalog_transition"])["current_open"]
            _record_gap_completed(
                observer,
                event_id=f"phase-{phase}-complete",
                occurred_at_utc_ns=t0 + 4,
                gap_id=gap_id,
            )
            set_time(5)
            observer.sample()
        elif phase == "during":
            _record_gap_started(
                observer,
                event_id=f"phase-{phase}-start",
                occurred_at_utc_ns=t0 + 1,
                gap_id=gap_id,
            )
            set_time(2)
            _open_path, _open_sha, open_sample = observer.sample()
            assert cast(dict[str, object], open_sample["catalog_transition"])["current_open"]
            _record_gap_completed(
                observer,
                event_id=f"phase-{phase}-complete",
                occurred_at_utc_ns=t0 + 3,
                gap_id=gap_id,
            )
            set_time(4)
            observer.sample()
        else:
            _record_gap_started(
                observer,
                event_id=f"phase-{phase}-start",
                occurred_at_utc_ns=t0 + 1,
                gap_id=gap_id,
            )
            _record_gap_completed(
                observer,
                event_id=f"phase-{phase}-complete",
                occurred_at_utc_ns=t0 + 2,
                gap_id=gap_id,
            )
            set_time(3)
            _after_path, _after_sha, after = observer.sample()
            after_transition = cast(dict[str, object], after["catalog_transition"])
            assert after_transition["current_open"] == []
            assert after_transition["completed"]

        _advance_stage_with_samples(observer, clock)
        clock.utc += 1
        clock.boot += 1
        _final_path, _final_sha, final = observer.finalize()
        assert final["result"] == "PASS_CANDIDATE"
        assert final["eligible_for_next_stage"] is True
        verified, _verified_sha = verify_completed_stage(
            observer.evidence_root, observer.identity, expected_stage="2h"
        )
        final_results[phase] = (
            verified["result"],
            verified["eligible_for_next_stage"],
        )

    assert final_results == {
        "before": ("PASS_CANDIDATE", True),
        "during": ("PASS_CANDIDATE", True),
        "after": ("PASS_CANDIDATE", True),
    }


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
def test_v3_chain_verifier_keeps_only_rolling_state(
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


@pytest.mark.parametrize(
    ("findings", "result"),
    [
        (["unsafe_wall_clock_backward"], "PASS_CANDIDATE"),
        ([], "INCOMPLETE"),
        (["unresolved_discontinuity", "unsafe_wall_clock_backward"], "FAIL"),
        (["unsafe_wall_clock_backward"], "FAIL"),
        (["unsafe_wall_clock_backward"], "REVIEW_REQUIRED"),
    ],
    ids=[
        "unsafe-pass",
        "empty-incomplete",
        "unsafe-plus-another",
        "unsafe-fail",
        "unsafe-review-required",
    ],
)
def test_resume_rejects_non_exact_stage_start_admission(
    tmp_path: Path, findings: list[str], result: str
) -> None:
    observer, clock, manager = _observer(tmp_path)
    observer.start()
    start_path = observer.evidence_root / "stage-start.json"
    start = _read_json(start_path)
    start["blocking_findings"] = findings
    start["new_finding_details"] = {
        finding: {"observed_at_utc_ns": start["observed_at_utc_ns"]}
        for finding in findings
    }
    start["result"] = result
    start_path.write_bytes(canonical_json(start))

    with pytest.raises(AcceptanceError, match="stage-start is not resumable"):
        resume_observer(
            observer.evidence_root,
            data_root=observer.data_root,
            identity=observer.identity,
            manager=manager,  # type: ignore[arg-type]
            evaluator=FakeEvaluator(),  # type: ignore[arg-type]
            clock=clock,
            disk_usage=observer.disk_usage,
        )


def test_completed_stage_still_rejects_unsafe_stage_start(tmp_path: Path) -> None:
    observer, _final_path, _start = _complete_2h_stage(tmp_path)
    start_path = observer.evidence_root / "stage-start.json"
    start = _read_json(start_path)
    start["blocking_findings"] = ["unsafe_wall_clock_backward"]
    start["new_finding_details"] = {
        "unsafe_wall_clock_backward": {
            "observed_at_utc_ns": start["observed_at_utc_ns"]
        }
    }
    start["result"] = "INCOMPLETE"
    start_path.write_bytes(canonical_json(start))

    with pytest.raises(AcceptanceError, match="stage-start is not eligible"):
        verify_completed_stage(observer.evidence_root, observer.identity, expected_stage="2h")


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


def _catalog_open_item(
    *, gap_id: str = "gap-a", started_at_utc_ns: int = 1_000_000_001
) -> dict[str, object]:
    return {
        "market": "um_perpetual",
        "symbol": "BTCUSDT",
        "stream": "book_ticker",
        "gap_id": gap_id,
        "started_at_utc_ns": started_at_utc_ns,
        "timing": "OPENED_IN_STAGE",
    }


def _catalog_closed_item(
    *,
    gap_id: str = "gap-a",
    started_at_utc_ns: int = 1_000_000_001,
    ended_at_utc_ns: int = 1_000_000_002,
    timing: str = "CURRENT_STAGE",
) -> dict[str, object]:
    return {
        "market": "um_perpetual",
        "symbol": "BTCUSDT",
        "stream": "book_ticker",
        "gap_id": gap_id,
        "started_at_utc_ns": started_at_utc_ns,
        "ended_at_utc_ns": ended_at_utc_ns,
        "timing": timing,
    }


def _catalog_transition(
    *,
    started: list[dict[str, object]] | None = None,
    completed: list[dict[str, object]] | None = None,
    current_open: list[dict[str, object]] | None = None,
    current_interval_count: int = 0,
) -> dict[str, object]:
    open_items = [] if current_open is None else current_open
    return {
        "started": [] if started is None else started,
        "completed": [] if completed is None else completed,
        "current_open": open_items,
        "terminal_events": [],
        "summary": {
            "open_count": len(open_items),
            "current_interval_count": current_interval_count,
        },
    }


def _record_gap_started(
    observer: AcceptanceObserver,
    *,
    event_id: str,
    occurred_at_utc_ns: int,
    gap_id: str,
) -> None:
    with Catalog(observer.data_root / "state" / "catalog.sqlite") as catalog:
        catalog.record_operational_event(
            event_id=event_id,
            event_type="STREAM_DISCONTINUITY_STARTED",
            occurred_at_utc_ns=occurred_at_utc_ns,
            evidence={
                "market": "um_perpetual",
                "symbol": "BTCUSDT",
                "stream": "book_ticker",
                "gap_id": gap_id,
                "gap_started_at_utc_ns": occurred_at_utc_ns,
                "original_connection_id": "connection-a",
                "original_generation": 1,
            },
            symbol="BTCUSDT",
        )


def _record_gap_completed(
    observer: AcceptanceObserver,
    *,
    event_id: str,
    occurred_at_utc_ns: int,
    gap_id: str,
) -> None:
    with Catalog(observer.data_root / "state" / "catalog.sqlite") as catalog:
        catalog.record_operational_event(
            event_id=event_id,
            event_type="STREAM_DISCONTINUITY_COMPLETED",
            occurred_at_utc_ns=occurred_at_utc_ns,
            evidence={
                "market": "um_perpetual",
                "symbol": "BTCUSDT",
                "stream": "book_ticker",
                "gap_id": gap_id,
                "gap_ended_at_utc_ns": occurred_at_utc_ns,
                "new_connection_id": "connection-b",
                "new_generation": 2,
            },
            symbol="BTCUSDT",
        )


def _advance_stage_with_samples(
    observer: AcceptanceObserver,
    clock: FakeClock,
    *,
    cadence_ns: int = 300 * 1_000_000_000,
) -> None:
    if observer.t0_boottime_ns is None:
        raise AssertionError("observer T0 is not initialized")
    target = observer.t0_boottime_ns + STAGE_DURATION_NS[observer.stage]
    while clock.boot < target:
        delta = min(cadence_ns, target - clock.boot)
        clock.boot += delta
        clock.utc += delta
        observer.sample()


def _rewrite_v3_samples(
    stage_root: Path,
    mutators: Mapping[int, Callable[[dict[str, object]], None]],
    *,
    update_final: bool = False,
) -> None:
    start_path = stage_root / "stage-start.json"
    start_sha = sha256_bytes(start_path.read_bytes())
    previous_sha: str | None = None
    sample_paths = sorted(stage_root.glob("sample-*.json"))
    for ordinal, path in enumerate(sample_paths):
        document = _read_json(path)
        mutator = mutators.get(ordinal)
        if mutator is not None:
            mutator(document)
        document["sample_ordinal"] = ordinal
        document["stage_start_evidence_sha256"] = start_sha
        document["previous_sample_sha256"] = previous_sha
        path.write_bytes(canonical_json(document))
        previous_sha = sha256_bytes(path.read_bytes())
    if update_final:
        final_path = stage_root / "stage-final.json"
        final = _read_json(final_path)
        final["stage_start_evidence_sha256"] = start_sha
        final["previous_sample_sha256"] = previous_sha
        final["last_sample_sha256"] = previous_sha
        final_path.write_bytes(canonical_json(final))


def _rewrite_v2_samples(
    stage_root: Path,
    mutators: Mapping[int, Callable[[dict[str, object]], None]],
    *,
    update_final: bool = False,
) -> None:
    start_path = stage_root / "stage-start.json"
    start = _read_json(start_path)
    start["schema_version"] = PREVIOUS_SCHEMA_VERSION
    start_path.write_bytes(canonical_json(start))
    start_sha = sha256_bytes(start_path.read_bytes())
    previous_sha: str | None = None
    sample_paths = sorted(stage_root.glob("sample-*.json"))
    for ordinal, path in enumerate(sample_paths):
        document = _read_json(path)
        document["schema_version"] = PREVIOUS_SCHEMA_VERSION
        mutator = mutators.get(ordinal)
        if mutator is not None:
            mutator(document)
        document["sample_ordinal"] = ordinal
        document["stage_start_evidence_sha256"] = start_sha
        document["previous_sample_sha256"] = previous_sha
        path.write_bytes(canonical_json(document))
        previous_sha = sha256_bytes(path.read_bytes())
    if update_final:
        final_path = stage_root / "stage-final.json"
        final = _read_json(final_path)
        final["schema_version"] = PREVIOUS_SCHEMA_VERSION
        final["stage_start_evidence_sha256"] = start_sha
        final["previous_sample_sha256"] = previous_sha
        final["last_sample_sha256"] = previous_sha
        final_path.write_bytes(canonical_json(final))


def _rewrite_v2_stage_start_and_rebind(
    stage_root: Path,
    mutator: Callable[[dict[str, object]], None],
) -> tuple[str, str]:
    start_path = stage_root / "stage-start.json"
    start = _read_json(start_path)
    start["schema_version"] = PREVIOUS_SCHEMA_VERSION
    mutator(start)
    start_path.write_bytes(canonical_json(start))
    start_sha = sha256_bytes(start_path.read_bytes())

    previous_sha: str | None = None
    for ordinal, path in enumerate(sorted(stage_root.glob("sample-*.json"))):
        document = _read_json(path)
        document["schema_version"] = PREVIOUS_SCHEMA_VERSION
        document["sample_ordinal"] = ordinal
        document["stage_start_evidence_sha256"] = start_sha
        document["previous_sample_sha256"] = previous_sha
        path.write_bytes(canonical_json(document))
        previous_sha = sha256_bytes(path.read_bytes())

    if previous_sha is None:
        raise AssertionError("stage must contain at least one sample")
    final_path = stage_root / "stage-final.json"
    final = _read_json(final_path)
    final["schema_version"] = PREVIOUS_SCHEMA_VERSION
    final["stage_start_evidence_sha256"] = start_sha
    final["previous_sample_sha256"] = previous_sha
    final["last_sample_sha256"] = previous_sha
    final_path.write_bytes(canonical_json(final))
    return start_sha, previous_sha


def _convert_completed_v3_stage_to_v2(observer: AcceptanceObserver) -> None:
    root = observer.evidence_root.parent
    identity_path = root / "identity-result.json"
    readiness_path = root / "readiness-result.json"
    identity_document = _read_json(identity_path)
    identity_document["schema_version"] = PREVIOUS_SCHEMA_VERSION
    identity_path.write_bytes(canonical_json(identity_document))
    identity_sha = sha256_bytes(identity_path.read_bytes())

    readiness_document = _read_json(readiness_path)
    readiness_document["schema_version"] = PREVIOUS_SCHEMA_VERSION
    readiness_document["prior_stage_evidence_sha256"] = identity_sha
    readiness_path.write_bytes(canonical_json(readiness_document))
    readiness_sha = sha256_bytes(readiness_path.read_bytes())

    start_path = observer.evidence_root / "stage-start.json"
    start = _read_json(start_path)
    start["schema_version"] = PREVIOUS_SCHEMA_VERSION
    start["prior_stage_evidence_sha256"] = readiness_sha
    start_path.write_bytes(canonical_json(start))
    start_sha = sha256_bytes(start_path.read_bytes())

    previous_sha: str | None = None
    for ordinal, path in enumerate(sorted(observer.evidence_root.glob("sample-*.json"))):
        document = _read_json(path)
        document["schema_version"] = PREVIOUS_SCHEMA_VERSION
        document["prior_stage_evidence_sha256"] = readiness_sha
        document["stage_start_evidence_sha256"] = start_sha
        document["sample_ordinal"] = ordinal
        document["previous_sample_sha256"] = previous_sha
        path.write_bytes(canonical_json(document))
        previous_sha = sha256_bytes(path.read_bytes())
    if previous_sha is None:
        raise AssertionError("completed stage has no samples")
    final_path = observer.evidence_root / "stage-final.json"
    final = _read_json(final_path)
    final["schema_version"] = PREVIOUS_SCHEMA_VERSION
    final["prior_stage_evidence_sha256"] = readiness_sha
    final["stage_start_evidence_sha256"] = start_sha
    final["previous_sample_sha256"] = previous_sha
    final["last_sample_sha256"] = previous_sha
    final_path.write_bytes(canonical_json(final))


def _add_unresolved_finding(document: dict[str, object]) -> None:
    document["blocking_findings"] = ["unresolved_discontinuity"]
    document["new_finding_details"] = {
        "unresolved_discontinuity": {
            "observed_at_utc_ns": document["observed_at_utc_ns"]
        }
    }
    document["result"] = "FAIL"


def _two_sample_observer(
    tmp_path: Path,
) -> tuple[AcceptanceObserver, FakeClock, FakeManager]:
    observer, clock, manager = _observer(tmp_path)
    observer.start()
    clock.utc += 1
    clock.boot += 1
    observer.sample()
    clock.utc += 1
    clock.boot += 1
    observer.sample()
    return observer, clock, manager


def test_v3_resume_open_then_exact_completion_remains_eligible(tmp_path: Path) -> None:
    observer, clock, manager = _observer(tmp_path)
    observer.prior_stage_sha256 = _publish_valid_identity_and_readiness(observer)
    observer.start()
    t0 = cast(int, observer.t0_utc_ns)
    t0_boot = cast(int, observer.t0_boottime_ns)
    gap_id = "resume-close-gap"
    _record_gap_started(
        observer,
        event_id="resume-close-start",
        occurred_at_utc_ns=t0 + 1,
        gap_id=gap_id,
    )
    clock.utc, clock.boot = t0 + 2, t0_boot + 2
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
    resumed.identity_verifier = observer.identity_verifier
    assert resumed.t0_utc_ns == t0
    assert resumed.run_id == observer.run_id
    assert set(resumed.published_catalog_open) == {
        ("um_perpetual", "BTCUSDT", "book_ticker", gap_id)
    }

    _record_gap_completed(
        observer,
        event_id="resume-close-complete",
        occurred_at_utc_ns=t0 + 3,
        gap_id=gap_id,
    )
    clock.utc, clock.boot = t0 + 4, t0_boot + 4
    _path, _sha, closed = resumed.sample()
    transition = cast(dict[str, object], closed["catalog_transition"])
    assert transition["current_open"] == []
    assert transition["completed"]
    assert closed["blocking_findings"] == []
    assert closed["result"] == "PASS_CANDIDATE"

    _advance_stage_with_samples(resumed, clock)
    clock.utc += 1
    clock.boot += 1
    _final_path, _final_sha, final = resumed.finalize()
    assert final["result"] == "PASS_CANDIDATE"
    assert final["eligible_for_next_stage"] is True
    verify_completed_stage(resumed.evidence_root, resumed.identity, expected_stage="2h")


def test_v3_resume_open_at_terminal_fails_closed(tmp_path: Path) -> None:
    observer, clock, manager = _observer(tmp_path)
    observer.prior_stage_sha256 = _publish_valid_identity_and_readiness(observer)
    observer.start()
    t0 = cast(int, observer.t0_utc_ns)
    t0_boot = cast(int, observer.t0_boottime_ns)
    gap_id = "resume-terminal-open-gap"
    _record_gap_started(
        observer,
        event_id="resume-terminal-open-start",
        occurred_at_utc_ns=t0 + 1,
        gap_id=gap_id,
    )
    clock.utc, clock.boot = t0 + 2, t0_boot + 2
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
    resumed.identity_verifier = observer.identity_verifier
    assert resumed.published_catalog_open
    _advance_stage_with_samples(resumed, clock)
    clock.utc += 1
    clock.boot += 1
    _final_path, _final_sha, final = resumed.finalize()
    assert final["result"] == "FAIL"
    assert final["blocking_findings"] == ["unresolved_discontinuity"]
    assert final["eligible_for_next_stage"] is False
    terminal_detail = cast(dict[str, object], final["new_finding_details"])[
        "unresolved_discontinuity"
    ]
    assert cast(dict[str, object], terminal_detail)["gap_id"] == gap_id
    with pytest.raises(AcceptanceError, match="v3 chain terminus"):
        verify_completed_stage(resumed.evidence_root, resumed.identity, expected_stage="2h")


def test_v3_verifier_rejects_open_disappearing_without_completion(tmp_path: Path) -> None:
    observer, clock, _manager = _observer(tmp_path)
    observer.start()
    t0 = cast(int, observer.t0_utc_ns)
    t0_boot = cast(int, observer.t0_boottime_ns)
    _record_gap_started(
        observer,
        event_id="disappearing-open-start",
        occurred_at_utc_ns=t0 + 1,
        gap_id="disappearing-open-gap",
    )
    clock.utc, clock.boot = t0 + 2, t0_boot + 2
    observer.sample()
    clock.utc, clock.boot = t0 + 3, t0_boot + 3
    observer.sample()

    def disappear(document: dict[str, object]) -> None:
        document["catalog_transition"] = _catalog_transition()
        document["blocking_findings"] = []
        document["new_finding_details"] = {}
        document["result"] = "PASS_CANDIDATE"

    _rewrite_v3_samples(observer.evidence_root, {1: disappear})
    with pytest.raises(AcceptanceError, match="disappeared without completion"):
        _sample_chain(
            observer.evidence_root,
            start=_read_json(observer.evidence_root / "stage-start.json"),
            start_sha=sha256_bytes((observer.evidence_root / "stage-start.json").read_bytes()),
            identity=observer.identity,
            require_eligible=False,
        )


@pytest.mark.parametrize("wrong_field", ["gap_id", "market", "symbol", "stream"])
def test_v3_verifier_rejects_completion_for_wrong_lifecycle_identity(
    tmp_path: Path, wrong_field: str
) -> None:
    observer, clock, _manager = _observer(tmp_path / wrong_field)
    observer.start()
    t0 = cast(int, observer.t0_utc_ns)
    t0_boot = cast(int, observer.t0_boottime_ns)
    gap_id = "identity-bound-gap"
    _record_gap_started(
        observer,
        event_id=f"wrong-completion-start-{wrong_field}",
        occurred_at_utc_ns=t0 + 1,
        gap_id=gap_id,
    )
    clock.utc, clock.boot = t0 + 2, t0_boot + 2
    observer.sample()
    clock.utc, clock.boot = t0 + 3, t0_boot + 3
    observer.sample()
    bad_completion = _catalog_closed_item(
        gap_id=gap_id,
        started_at_utc_ns=t0 + 1,
        ended_at_utc_ns=t0 + 3,
    )
    replacements = {
        "gap_id": "wrong-gap-id",
        "market": "spot",
        "symbol": "ETHUSDT",
        "stream": "agg_trade",
    }
    bad_completion[wrong_field] = replacements[wrong_field]

    def mutate(document: dict[str, object]) -> None:
        document["catalog_transition"] = _catalog_transition(
            completed=[bad_completion], current_interval_count=1
        )
        document["blocking_findings"] = []
        document["new_finding_details"] = {}
        document["result"] = "PASS_CANDIDATE"

    _rewrite_v3_samples(observer.evidence_root, {1: mutate})
    with pytest.raises(AcceptanceError, match=r"orphan completion|disappeared"):
        _sample_chain(
            observer.evidence_root,
            start=_read_json(observer.evidence_root / "stage-start.json"),
            start_sha=sha256_bytes((observer.evidence_root / "stage-start.json").read_bytes()),
            identity=observer.identity,
            require_eligible=False,
        )


def test_v3_verifier_rejects_malformed_and_conflicting_completion(
    tmp_path: Path,
) -> None:
    for mode in ("malformed", "conflicting"):
        observer, clock, _manager = _observer(tmp_path / mode)
        observer.start()
        t0 = cast(int, observer.t0_utc_ns)
        t0_boot = cast(int, observer.t0_boottime_ns)
        gap_id = f"{mode}-gap"
        _record_gap_started(
            observer,
            event_id=f"{mode}-start",
            occurred_at_utc_ns=t0 + 1,
            gap_id=gap_id,
        )
        clock.utc, clock.boot = t0 + 2, t0_boot + 2
        observer.sample()
        clock.utc, clock.boot = t0 + 3, t0_boot + 3
        observer.sample()

        if mode == "malformed":
            completed: list[dict[str, object]] = [{"gap_id": gap_id}]
            match = "Catalog discontinuity identity is malformed"
        else:
            completed = [
                _catalog_closed_item(
                    gap_id=gap_id,
                    started_at_utc_ns=t0 + 1,
                    ended_at_utc_ns=t0 + 3,
                )
            ] * 2
            match = "repeats a completion identity"

        def mutate(
            document: dict[str, object],
            completion_items: list[dict[str, object]] = completed,
        ) -> None:
            document["catalog_transition"] = _catalog_transition(
                completed=completion_items, current_interval_count=1
            )
            document["blocking_findings"] = []
            document["new_finding_details"] = {}
            document["result"] = "PASS_CANDIDATE"

        _rewrite_v3_samples(observer.evidence_root, {1: mutate})
        with pytest.raises(AcceptanceError, match=match):
            _sample_chain(
                observer.evidence_root,
                start=_read_json(observer.evidence_root / "stage-start.json"),
                start_sha=sha256_bytes(
                    (observer.evidence_root / "stage-start.json").read_bytes()
                ),
                identity=observer.identity,
                require_eligible=False,
            )


def test_v3_verifier_rejects_terminal_detail_tampering(tmp_path: Path) -> None:
    observer, clock, _manager = _observer(tmp_path)
    observer.prior_stage_sha256 = _publish_valid_identity_and_readiness(observer)
    observer.start()
    t0 = cast(int, observer.t0_utc_ns)
    t0_boot = cast(int, observer.t0_boottime_ns)
    _record_gap_started(
        observer,
        event_id="tampered-terminal-start",
        occurred_at_utc_ns=t0 + 1,
        gap_id="tampered-terminal-gap",
    )
    clock.utc, clock.boot = t0 + 2, t0_boot + 2
    observer.sample()
    _advance_stage_with_samples(observer, clock)
    clock.utc += 1
    clock.boot += 1
    final_path, _final_sha, _final = observer.finalize()
    _rewrite(
        final_path,
        new_finding_details={
            "unresolved_discontinuity": {"source": "forged-terminal-state"}
        },
    )
    with pytest.raises(AcceptanceError, match="reconstructed v3 terminal state"):
        verify_completed_stage(observer.evidence_root, observer.identity, expected_stage="2h")


def test_v3_catalog_degraded_authority_remains_fail_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    observer, clock, _manager = _observer(tmp_path)
    observer.start()

    def degraded(*_args: object, **_kwargs: object) -> object:
        raise AcceptanceError("Catalog degraded authority")

    monkeypatch.setattr(observer, "_catalog_evidence", degraded)
    clock.utc += 1
    clock.boot += 1
    _path, _sha, sample = observer.sample()
    assert sample["result"] == "FAIL"
    assert "Catalog degraded authority" in cast(
        list[object], sample["blocking_findings"]
    )


def test_v3_and_v2_chain_generations_cannot_be_mixed(tmp_path: Path) -> None:
    observer, _final_path, _start = _complete_2h_stage(tmp_path)

    def use_v2_sample(document: dict[str, object]) -> None:
        document["schema_version"] = PREVIOUS_SCHEMA_VERSION

    _rewrite_v3_samples(observer.evidence_root, {0: use_v2_sample})
    with pytest.raises(AcceptanceError, match="mixed into a v3 chain"):
        verify_completed_stage(observer.evidence_root, observer.identity, expected_stage="2h")

    observer2, final_path2, _start2 = _complete_2h_stage(tmp_path / "predecessor")
    _rewrite(final_path2, schema_version=PREVIOUS_SCHEMA_VERSION)
    with pytest.raises(AcceptanceError, match="mixed-schema predecessor"):
        verify_prior_stage(final_path2, observer2.identity, "12h")


def test_historical_v2_valid_stage_remains_verifiable(tmp_path: Path) -> None:
    observer, _final_path, _start = _complete_2h_stage(tmp_path)
    _convert_completed_v3_stage_to_v2(observer)
    before = {
        path.name: path.read_bytes()
        for path in observer.evidence_root.glob("*.json")
    }
    verified, _verified_sha = verify_completed_stage(
        observer.evidence_root, observer.identity, expected_stage="2h"
    )
    assert verified["schema_version"] == PREVIOUS_SCHEMA_VERSION
    after = {path.name: path.read_bytes() for path in observer.evidence_root.glob("*.json")}
    assert after == before


def test_historical_v2_open_remains_sticky_and_ineligible(tmp_path: Path) -> None:
    observer, _clock, _manager = _two_sample_observer(tmp_path)
    open_item = _catalog_open_item()

    def first(document: dict[str, object]) -> None:
        _add_unresolved_finding(document)
        document["catalog_transition"] = _catalog_transition(
            started=[open_item], current_open=[open_item]
        )

    def second(document: dict[str, object]) -> None:
        _add_unresolved_finding(document)
        document["new_finding_details"] = {}
        document["catalog_transition"] = _catalog_transition(current_open=[open_item])

    _rewrite_v2_samples(observer.evidence_root, {0: first, 1: second})
    state = _sample_chain(
        observer.evidence_root,
        start=_read_json(observer.evidence_root / "stage-start.json"),
        start_sha=sha256_bytes((observer.evidence_root / "stage-start.json").read_bytes()),
        identity=observer.identity,
        require_eligible=False,
    )
    assert state.known_findings == {"unresolved_discontinuity"}
    assert set(state.catalog_open) == {
        ("um_perpetual", "BTCUSDT", "book_ticker", "gap-a")
    }
    with pytest.raises(AcceptanceError, match="ineligible sample findings"):
        _sample_chain(
            observer.evidence_root,
            start=_read_json(observer.evidence_root / "stage-start.json"),
            start_sha=sha256_bytes(
                (observer.evidence_root / "stage-start.json").read_bytes()
            ),
            identity=observer.identity,
            require_eligible=True,
        )


def test_v2_verifier_rejects_rehashed_manifest_anomaly_without_blocker(
    tmp_path: Path,
) -> None:
    observer, _final_path, _start = _complete_2h_stage(tmp_path)

    def mutate(document: dict[str, object]) -> None:
        transition = cast(dict[str, object], document["manifest_transition"])
        transition["state"] = "ANOMALY"
        transition["anomalies"] = [{"kind": "mutated", "path": "historical.manifest"}]
        transition["added_members"] = []
        transition["deferred_members"] = []
        document["manifest_transition"] = transition

    _rewrite_v2_samples(observer.evidence_root, {0: mutate}, update_final=True)

    previous_sha: str | None = None
    for path in sorted(observer.evidence_root.glob("sample-*.json")):
        document = _read_json(path)
        assert document["previous_sample_sha256"] == previous_sha
        previous_sha = sha256_bytes(path.read_bytes())
    final = _read_json(observer.evidence_root / "stage-final.json")
    assert final["previous_sample_sha256"] == previous_sha
    assert final["last_sample_sha256"] == previous_sha

    with pytest.raises(
        AcceptanceError, match=r"manifest anomaly.*manifest_byte_mutation_or_loss"
    ):
        verify_completed_stage(observer.evidence_root, observer.identity, expected_stage="2h")


def test_v2_verifier_rejects_rehashed_non_monotonic_completion_without_blocker(
    tmp_path: Path,
) -> None:
    observer, _final_path, _start = _complete_2h_stage(tmp_path)
    interval = _catalog_closed_item(
        started_at_utc_ns=1_000_000_100,
        ended_at_utc_ns=1_000_000_099,
    )

    def non_monotonic_without_blocker(document: dict[str, object]) -> None:
        document["blocking_findings"] = []
        document["new_finding_details"] = {}
        document["result"] = "PASS_CANDIDATE"
        document["catalog_transition"] = _catalog_transition(
            completed=[interval], current_interval_count=1
        )

    def subsequent_clean_sample(document: dict[str, object]) -> None:
        document["blocking_findings"] = []
        document["new_finding_details"] = {}
        document["result"] = "PASS_CANDIDATE"
        document["catalog_transition"] = _catalog_transition(current_interval_count=1)

    mutators: dict[int, Callable[[dict[str, object]], None]] = {
        0: non_monotonic_without_blocker
    }
    mutators.update({ordinal: subsequent_clean_sample for ordinal in range(1, 24)})
    _rewrite_v2_samples(observer.evidence_root, mutators, update_final=True)

    previous_sha: str | None = None
    for path in sorted(observer.evidence_root.glob("sample-*.json")):
        document = _read_json(path)
        assert document["previous_sample_sha256"] == previous_sha
        previous_sha = sha256_bytes(path.read_bytes())
    final = _read_json(observer.evidence_root / "stage-final.json")
    assert final["previous_sample_sha256"] == previous_sha
    assert final["last_sample_sha256"] == previous_sha

    with pytest.raises(
        AcceptanceError,
        match=r"non-monotonic completion is not bound to unsafe_wall_clock_backward",
    ):
        verify_completed_stage(observer.evidence_root, observer.identity, expected_stage="2h")


def test_v2_verifier_rejects_fully_rehashed_catalog_baseline_laundering(
    tmp_path: Path,
) -> None:
    observer, _final_path, _start = _complete_2h_stage(tmp_path)

    def launder_current_interval_into_pre_t0(start: dict[str, object]) -> None:
        t0 = cast(int, start["observed_at_utc_ns"])
        interval = _catalog_closed_item(
            gap_id="baseline-laundered",
            started_at_utc_ns=t0 + 200,
            ended_at_utc_ns=t0 + 150,
        )
        interval.pop("timing")
        baseline = cast(dict[str, object], start["catalog_baseline"])
        baseline["pre_t0_closed"] = [interval]
        start["blocking_findings"] = []
        start["new_finding_details"] = {}
        start["result"] = "PASS_CANDIDATE"

    start_sha, last_sample_sha = _rewrite_v2_stage_start_and_rebind(
        observer.evidence_root, launder_current_interval_into_pre_t0
    )

    rewritten_start = _read_json(observer.evidence_root / "stage-start.json")
    assert rewritten_start["blocking_findings"] == []
    assert rewritten_start["new_finding_details"] == {}
    assert rewritten_start["result"] == "PASS_CANDIDATE"
    forged_baseline = cast(dict[str, object], rewritten_start["catalog_baseline"])
    forged_closed = cast(list[dict[str, object]], forged_baseline["pre_t0_closed"])
    assert cast(int, forged_closed[0]["ended_at_utc_ns"]) >= cast(
        int, rewritten_start["observed_at_utc_ns"]
    )

    previous_sha: str | None = None
    sample_paths = sorted(observer.evidence_root.glob("sample-*.json"))
    assert sample_paths
    for ordinal, path in enumerate(sample_paths):
        document = _read_json(path)
        assert document["sample_ordinal"] == ordinal
        assert document["stage_start_evidence_sha256"] == start_sha
        assert document["previous_sample_sha256"] == previous_sha
        previous_sha = sha256_bytes(path.read_bytes())
    assert previous_sha == last_sample_sha
    final = _read_json(observer.evidence_root / "stage-final.json")
    assert final["stage_start_evidence_sha256"] == start_sha
    assert final["previous_sample_sha256"] == last_sample_sha
    assert final["last_sample_sha256"] == last_sample_sha

    with pytest.raises(
        AcceptanceError,
        match=r"pre_t0_closed.*before stage T0",
    ):
        verify_completed_stage(observer.evidence_root, observer.identity, expected_stage="2h")


def test_resume_rejects_manifest_authority_anomaly_without_modifying_evidence(
    tmp_path: Path,
) -> None:
    observer, clock, manager = _observer(tmp_path)
    layout = ensure_storage_layout(observer.data_root)
    with Catalog(layout.catalog) as catalog:
        seal_chunk(layout, catalog, [usdm_envelope("manifest-authority", 1)])
    observer.start()
    manifest_path = next(layout.manifests.glob("*.manifest.json"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    clock.utc += 1
    clock.boot += 1
    observer.sample()
    before = {path.name: path.read_bytes() for path in observer.evidence_root.glob("*.json")}

    with pytest.raises(
        AcceptanceError, match=r"manifest authority.*deterministically resumed"
    ):
        resume_observer(
            observer.evidence_root,
            data_root=observer.data_root,
            identity=observer.identity,
            manager=manager,  # type: ignore[arg-type]
            evaluator=FakeEvaluator(),  # type: ignore[arg-type]
            clock=clock,
            disk_usage=observer.disk_usage,
        )

    after = {path.name: path.read_bytes() for path in observer.evidence_root.glob("*.json")}
    assert after == before


def test_catalog_new_open_requires_exact_started_item(tmp_path: Path) -> None:
    observer, _clock, _manager = _two_sample_observer(tmp_path)
    open_item = _catalog_open_item()

    def mutate(document: dict[str, object]) -> None:
        _add_unresolved_finding(document)
        document["catalog_transition"] = _catalog_transition(current_open=[open_item])

    _rewrite_v2_samples(observer.evidence_root, {0: mutate})
    with pytest.raises(AcceptanceError, match="does not explain new open identities"):
        _sample_chain(
            observer.evidence_root,
            start=_read_json(observer.evidence_root / "stage-start.json"),
            start_sha=sha256_bytes((observer.evidence_root / "stage-start.json").read_bytes()),
            identity=observer.identity,
            require_eligible=False,
        )


def test_catalog_rejects_repeated_started_item(tmp_path: Path) -> None:
    observer, _clock, _manager = _two_sample_observer(tmp_path)
    open_item = _catalog_open_item()

    def first(document: dict[str, object]) -> None:
        _add_unresolved_finding(document)
        document["catalog_transition"] = _catalog_transition(
            started=[open_item], current_open=[open_item]
        )

    def repeated(document: dict[str, object]) -> None:
        document["blocking_findings"] = ["unresolved_discontinuity"]
        document["new_finding_details"] = {}
        document["result"] = "FAIL"
        document["catalog_transition"] = _catalog_transition(
            started=[open_item], current_open=[open_item]
        )

    _rewrite_v2_samples(observer.evidence_root, {0: first, 1: repeated})
    with pytest.raises(
        AcceptanceError, match=r"already-open identity|new open identities"
    ):
        _sample_chain(
            observer.evidence_root,
            start=_read_json(observer.evidence_root / "stage-start.json"),
            start_sha=sha256_bytes((observer.evidence_root / "stage-start.json").read_bytes()),
            identity=observer.identity,
            require_eligible=False,
        )


def test_catalog_rejects_silent_open_disappearance(tmp_path: Path) -> None:
    observer, _clock, _manager = _two_sample_observer(tmp_path)
    open_item = _catalog_open_item()

    def first(document: dict[str, object]) -> None:
        _add_unresolved_finding(document)
        document["catalog_transition"] = _catalog_transition(
            started=[open_item], current_open=[open_item]
        )

    def disappeared(document: dict[str, object]) -> None:
        document["blocking_findings"] = ["unresolved_discontinuity"]
        document["new_finding_details"] = {}
        document["result"] = "FAIL"
        document["catalog_transition"] = _catalog_transition()

    _rewrite_v2_samples(observer.evidence_root, {0: first, 1: disappeared})
    with pytest.raises(AcceptanceError, match="disappeared without completion"):
        _sample_chain(
            observer.evidence_root,
            start=_read_json(observer.evidence_root / "stage-start.json"),
            start_sha=sha256_bytes((observer.evidence_root / "stage-start.json").read_bytes()),
            identity=observer.identity,
            require_eligible=False,
        )


def test_catalog_rejects_orphan_completion_outside_observation_boundaries(
    tmp_path: Path,
) -> None:
    observer, _clock, _manager = _two_sample_observer(tmp_path)
    orphan = _catalog_closed_item(started_at_utc_ns=1_000_000_000, ended_at_utc_ns=1_000_000_001)

    def mutate(document: dict[str, object]) -> None:
        document["catalog_transition"] = _catalog_transition(
            completed=[orphan], current_interval_count=1
        )

    _rewrite_v2_samples(observer.evidence_root, {1: mutate})
    with pytest.raises(AcceptanceError, match="orphan completion"):
        _sample_chain(
            observer.evidence_root,
            start=_read_json(observer.evidence_root / "stage-start.json"),
            start_sha=sha256_bytes((observer.evidence_root / "stage-start.json").read_bytes()),
            identity=observer.identity,
            require_eligible=True,
        )


def test_catalog_allows_legitimate_between_observation_completion(tmp_path: Path) -> None:
    observer, _clock, _manager = _two_sample_observer(tmp_path)
    sample_paths = sorted(observer.evidence_root.glob("sample-*.json"))
    first = _read_json(sample_paths[0])
    second = _read_json(sample_paths[1])
    interval = _catalog_closed_item(
        started_at_utc_ns=cast(int, first["observed_at_utc_ns"]),
        ended_at_utc_ns=cast(int, second["observed_at_utc_ns"]),
    )

    def mutate(document: dict[str, object]) -> None:
        document["catalog_transition"] = _catalog_transition(
            completed=[interval], current_interval_count=1
        )

    _rewrite_v2_samples(observer.evidence_root, {1: mutate})
    state = _sample_chain(
        observer.evidence_root,
        start=_read_json(observer.evidence_root / "stage-start.json"),
        start_sha=sha256_bytes((observer.evidence_root / "stage-start.json").read_bytes()),
        identity=observer.identity,
        require_eligible=True,
    )
    assert state.catalog_current_interval_keys == {
        ("um_perpetual", "BTCUSDT", "book_ticker", "gap-a")
    }


@pytest.mark.parametrize(
    "ended_at_utc_ns",
    [100, 150],
    ids=["boundary-equality", "current-stage"],
)
def test_catalog_baseline_pre_t0_closed_requires_strictly_pre_t0(
    tmp_path: Path, ended_at_utc_ns: int
) -> None:
    observer, clock, _manager = _observer(tmp_path)
    clock.utc = 100
    observer.start()
    start = _read_json(observer.evidence_root / "stage-start.json")
    baseline = cast(dict[str, object], start["catalog_baseline"])
    baseline["pre_t0_closed"] = [
        _catalog_closed_item(
            gap_id="baseline-laundered",
            started_at_utc_ns=200,
            ended_at_utc_ns=ended_at_utc_ns,
        )
    ]

    with pytest.raises(AcceptanceError, match=r"pre_t0_closed.*before stage T0"):
        _sample_chain(
            observer.evidence_root,
            start=start,
            start_sha=sha256_bytes(canonical_json(start)),
            identity=observer.identity,
            require_eligible=True,
        )


@pytest.mark.parametrize(
    ("started_at_utc_ns", "ended_at_utc_ns"),
    [(50, 90), (90, 50)],
    ids=["normal", "non-monotonic"],
)
def test_catalog_baseline_accepts_historical_closed_authority(
    tmp_path: Path, started_at_utc_ns: int, ended_at_utc_ns: int
) -> None:
    observer, clock, _manager = _observer(tmp_path)
    clock.utc = 100
    observer.start()
    start = _read_json(observer.evidence_root / "stage-start.json")
    baseline = cast(dict[str, object], start["catalog_baseline"])
    baseline["pre_t0_closed"] = [
        _catalog_closed_item(
            gap_id="historical-closed",
            started_at_utc_ns=started_at_utc_ns,
            ended_at_utc_ns=ended_at_utc_ns,
        )
    ]

    state = _sample_chain(
        observer.evidence_root,
        start=start,
        start_sha=sha256_bytes(canonical_json(start)),
        identity=observer.identity,
        require_eligible=True,
    )
    key = ("um_perpetual", "BTCUSDT", "book_ticker", "historical-closed")
    assert key in state.catalog_interval_keys
    assert key not in state.catalog_current_interval_keys


@pytest.mark.parametrize(
    ("started_at_utc_ns", "ended_at_utc_ns"),
    [(110, 120), (80, 90)],
    ids=["starts-after-t0", "ends-before-t0"],
)
def test_catalog_baseline_crossing_requires_exact_t0_boundaries(
    tmp_path: Path, started_at_utc_ns: int, ended_at_utc_ns: int
) -> None:
    observer, clock, _manager = _observer(tmp_path)
    clock.utc = 100
    observer.start()
    start = _read_json(observer.evidence_root / "stage-start.json")
    baseline = cast(dict[str, object], start["catalog_baseline"])
    baseline["crossing_at_t0"] = [
        _catalog_closed_item(
            gap_id="fake-crossing",
            started_at_utc_ns=started_at_utc_ns,
            ended_at_utc_ns=ended_at_utc_ns,
            timing="CROSSES_T0",
        )
    ]

    with pytest.raises(
        AcceptanceError, match=r"crossing.*between stage T0 boundaries"
    ):
        _sample_chain(
            observer.evidence_root,
            start=start,
            start_sha=sha256_bytes(canonical_json(start)),
            identity=observer.identity,
            require_eligible=True,
        )


@pytest.mark.parametrize(
    ("timing", "started_at_utc_ns"),
    [("OPENED_IN_STAGE", 90), ("OPEN_AT_T0", 101)],
    ids=["wrong-timing", "future-open"],
)
def test_catalog_baseline_open_at_t0_requires_exact_open_authority(
    tmp_path: Path, timing: str, started_at_utc_ns: int
) -> None:
    observer, clock, _manager = _observer(tmp_path)
    clock.utc = 100
    observer.start()
    start = _read_json(observer.evidence_root / "stage-start.json")
    baseline = cast(dict[str, object], start["catalog_baseline"])
    baseline["open_at_t0"] = [
        _catalog_open_item(
            gap_id="invalid-open-at-t0",
            started_at_utc_ns=started_at_utc_ns,
        )
    ]
    cast(list[dict[str, object]], baseline["open_at_t0"])[0]["timing"] = timing

    with pytest.raises(AcceptanceError, match="Catalog baseline"):
        _sample_chain(
            observer.evidence_root,
            start=start,
            start_sha=sha256_bytes(canonical_json(start)),
            identity=observer.identity,
            require_eligible=True,
        )


@pytest.mark.parametrize(
    "ended_at_utc_ns",
    [150, 200],
    ids=["inversion", "equal-timestamp"],
)
def test_catalog_allows_blocked_non_monotonic_orphan_completion(
    tmp_path: Path, ended_at_utc_ns: int
) -> None:
    observer, clock, _manager = _observer(tmp_path)
    clock.utc = 100
    observer.start()
    clock.utc = 101
    clock.boot += 1
    observer.sample()
    clock.utc = 250
    clock.boot += 1
    observer.sample()
    interval = _catalog_closed_item(
        started_at_utc_ns=200,
        ended_at_utc_ns=ended_at_utc_ns,
    )

    def mutate(document: dict[str, object]) -> None:
        document["blocking_findings"] = ["unsafe_wall_clock_backward"]
        document["new_finding_details"] = {
            "unsafe_wall_clock_backward": {
                "market": "um_perpetual",
                "symbol": "BTCUSDT",
                "stream": "book_ticker",
                "gap_id": "gap-a",
                "started_at_utc_ns": 200,
                "ended_at_utc_ns": ended_at_utc_ns,
            }
        }
        document["result"] = "INCOMPLETE"
        document["catalog_transition"] = _catalog_transition(
            completed=[interval], current_interval_count=1
        )

    _rewrite_v2_samples(observer.evidence_root, {1: mutate})
    state = _sample_chain(
        observer.evidence_root,
        start=_read_json(observer.evidence_root / "stage-start.json"),
        start_sha=sha256_bytes((observer.evidence_root / "stage-start.json").read_bytes()),
        identity=observer.identity,
        require_eligible=False,
    )
    assert state.catalog_current_interval_keys == {
        ("um_perpetual", "BTCUSDT", "book_ticker", "gap-a")
    }
    with pytest.raises(AcceptanceError, match="ineligible sample findings"):
        _sample_chain(
            observer.evidence_root,
            start=_read_json(observer.evidence_root / "stage-start.json"),
            start_sha=sha256_bytes(
                (observer.evidence_root / "stage-start.json").read_bytes()
            ),
            identity=observer.identity,
            require_eligible=True,
        )


def test_catalog_rejects_normal_orphan_completion_after_current_observation(
    tmp_path: Path,
) -> None:
    observer, clock, _manager = _two_sample_observer(tmp_path)
    interval = _catalog_closed_item(
        started_at_utc_ns=cast(int, observer.t0_utc_ns) + 1,
        ended_at_utc_ns=clock.utc + 1,
    )

    def mutate(document: dict[str, object]) -> None:
        document["catalog_transition"] = _catalog_transition(
            completed=[interval], current_interval_count=1
        )

    _rewrite_v2_samples(observer.evidence_root, {1: mutate})
    with pytest.raises(AcceptanceError, match="current observation"):
        _sample_chain(
            observer.evidence_root,
            start=_read_json(observer.evidence_root / "stage-start.json"),
            start_sha=sha256_bytes((observer.evidence_root / "stage-start.json").read_bytes()),
            identity=observer.identity,
            require_eligible=False,
        )


def test_historical_v2_catalog_chain_preserves_non_monotonic_finding_authority(
    tmp_path: Path,
) -> None:
    observer, clock, manager = _observer(tmp_path)
    clock.utc = 100
    observer.start()
    clock.utc = 200
    clock.boot += 1
    observer.sample()
    clock.utc = 250
    clock.boot += 1
    observer.sample()
    open_item = _catalog_open_item(started_at_utc_ns=200)
    completed_item = _catalog_closed_item(
        started_at_utc_ns=200,
        ended_at_utc_ns=150,
    )

    def opened(document: dict[str, object]) -> None:
        _add_unresolved_finding(document)
        document["catalog_transition"] = _catalog_transition(
            started=[open_item], current_open=[open_item]
        )

    def completed(document: dict[str, object]) -> None:
        document["blocking_findings"] = [
            "unresolved_discontinuity",
            "unsafe_wall_clock_backward",
        ]
        document["new_finding_details"] = {
            "unsafe_wall_clock_backward": {
                "market": "um_perpetual",
                "symbol": "BTCUSDT",
                "stream": "book_ticker",
                "gap_id": "gap-a",
                "started_at_utc_ns": 200,
                "ended_at_utc_ns": 150,
            }
        }
        document["result"] = "FAIL"
        document["catalog_transition"] = _catalog_transition(
            completed=[completed_item], current_interval_count=1
        )

    _rewrite_v2_samples(observer.evidence_root, {0: opened, 1: completed})
    state = _sample_chain(
        observer.evidence_root,
        start=_read_json(observer.evidence_root / "stage-start.json"),
        start_sha=sha256_bytes((observer.evidence_root / "stage-start.json").read_bytes()),
        identity=observer.identity,
        require_eligible=False,
    )
    assert state.catalog_open == {}
    assert state.catalog_current_interval_keys == {
        ("um_perpetual", "BTCUSDT", "book_ticker", "gap-a")
    }

    with pytest.raises(AcceptanceError, match="v2 failed stage cannot resume as v3"):
        resume_observer(
            observer.evidence_root,
            data_root=observer.data_root,
            identity=observer.identity,
            manager=manager,  # type: ignore[arg-type]
            evaluator=FakeEvaluator(),  # type: ignore[arg-type]
            clock=clock,
            disk_usage=observer.disk_usage,
        )


def test_catalog_current_interval_count_must_match_rolling_authority(tmp_path: Path) -> None:
    observer, _clock, _manager = _two_sample_observer(tmp_path)

    def mutate(document: dict[str, object]) -> None:
        document["catalog_transition"] = _catalog_transition(current_interval_count=1)

    _rewrite_v2_samples(observer.evidence_root, {0: mutate})
    with pytest.raises(AcceptanceError, match="current interval count"):
        _sample_chain(
            observer.evidence_root,
            start=_read_json(observer.evidence_root / "stage-start.json"),
            start_sha=sha256_bytes((observer.evidence_root / "stage-start.json").read_bytes()),
            identity=observer.identity,
            require_eligible=True,
        )


def test_catalog_current_open_requires_unresolved_blocker(tmp_path: Path) -> None:
    observer, _clock, _manager = _two_sample_observer(tmp_path)
    open_item = _catalog_open_item()

    def mutate(document: dict[str, object]) -> None:
        document["catalog_transition"] = _catalog_transition(
            started=[open_item], current_open=[open_item]
        )

    _rewrite_v2_samples(observer.evidence_root, {0: mutate})
    with pytest.raises(AcceptanceError, match="unresolved_discontinuity"):
        _sample_chain(
            observer.evidence_root,
            start=_read_json(observer.evidence_root / "stage-start.json"),
            start_sha=sha256_bytes((observer.evidence_root / "stage-start.json").read_bytes()),
            identity=observer.identity,
            require_eligible=False,
        )


def test_v2_verifier_rejects_new_finding_without_detail(tmp_path: Path) -> None:
    observer, _clock, _manager = _two_sample_observer(tmp_path)

    def mutate(document: dict[str, object]) -> None:
        document["blocking_findings"] = ["synthetic_blocker"]
        document["new_finding_details"] = {}
        document["result"] = "FAIL"

    _rewrite_v2_samples(observer.evidence_root, {0: mutate})
    with pytest.raises(AcceptanceError, match="finding details are incomplete"):
        _sample_chain(
            observer.evidence_root,
            start=_read_json(observer.evidence_root / "stage-start.json"),
            start_sha=sha256_bytes((observer.evidence_root / "stage-start.json").read_bytes()),
            identity=observer.identity,
            require_eligible=False,
        )


def test_v2_verifier_rejects_repeated_finding_detail(tmp_path: Path) -> None:
    observer, _clock, _manager = _two_sample_observer(tmp_path)

    def first(document: dict[str, object]) -> None:
        document["blocking_findings"] = ["synthetic_blocker"]
        document["new_finding_details"] = {"synthetic_blocker": {"value": 1}}
        document["result"] = "FAIL"

    def repeated(document: dict[str, object]) -> None:
        document["blocking_findings"] = ["synthetic_blocker"]
        document["new_finding_details"] = {"synthetic_blocker": {"value": 1}}
        document["result"] = "FAIL"

    _rewrite_v2_samples(observer.evidence_root, {0: first, 1: repeated})
    with pytest.raises(AcceptanceError, match="finding detail is repeated"):
        _sample_chain(
            observer.evidence_root,
            start=_read_json(observer.evidence_root / "stage-start.json"),
            start_sha=sha256_bytes((observer.evidence_root / "stage-start.json").read_bytes()),
            identity=observer.identity,
            require_eligible=False,
        )


def test_v2_verifier_requires_complete_stage_start_finding_details(
    tmp_path: Path,
) -> None:
    observer, _clock, _manager = _observer(tmp_path)
    observer.start()
    start_path = observer.evidence_root / "stage-start.json"
    _rewrite(start_path, blocking_findings=["synthetic_blocker"])
    with pytest.raises(AcceptanceError, match="stage-start finding details are incomplete"):
        _sample_chain(
            observer.evidence_root,
            start=_read_json(start_path),
            start_sha=sha256_bytes(start_path.read_bytes()),
            identity=observer.identity,
            require_eligible=False,
        )


def test_catalog_terminal_authority_is_not_double_counted_at_stage_start(
    tmp_path: Path,
) -> None:
    observer, clock, _manager = _observer(tmp_path)
    with Catalog(observer.data_root / "state" / "catalog.sqlite") as catalog:
        catalog.record_operational_event(
            event_id="service-failed-at-start",
            event_type="SERVICE_FAILED",
            occurred_at_utc_ns=clock.utc,
            evidence={},
        )
    _path, _sha, start = observer.start()
    assert "terminal_service_or_core_failure" in cast(list[object], start["blocking_findings"])
    state = _sample_chain(
        observer.evidence_root,
        start=start,
        start_sha=sha256_bytes((observer.evidence_root / "stage-start.json").read_bytes()),
        identity=observer.identity,
        require_eligible=False,
    )
    assert state.terminal_event_keys == {"SERVICE_FAILED"}


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
    assert start["result"] == "PASS_CANDIDATE"
    assert start["blocking_findings"] == []
    transition = cast(dict[str, object], start["catalog_transition"])
    assert len(cast(list[object], transition["current_open"])) == 1


def test_unresolved_gap_cannot_terminate_an_eligible_stage(tmp_path: Path) -> None:
    observer, clock, _manager = _observer(tmp_path)
    observer.prior_stage_sha256 = _publish_valid_identity_and_readiness(observer)
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
    assert start["result"] == "PASS_CANDIDATE"
    assert start["blocking_findings"] == []
    last_sample: dict[str, object] | None = None
    for _ in range(24):
        clock.boot += 300 * 1_000_000_000
        clock.utc += 300 * 1_000_000_000
        _sample_path, _sample_sha, last_sample = observer.sample()
    assert last_sample is not None
    assert last_sample["blocking_findings"] == []
    assert last_sample["result"] == "PASS_CANDIDATE"
    _final_path, _final_sha, final = observer.finalize()
    assert final["result"] == "FAIL"
    assert final["blocking_findings"] == ["unresolved_discontinuity"]
    assert final["eligible_for_next_stage"] is False
    assert cast(dict[str, object], final["new_finding_details"])[
        "unresolved_discontinuity"
    ]
    with pytest.raises(AcceptanceError, match="v3 chain terminus"):
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


class MutableV4Evaluator:
    def __init__(self) -> None:
        self.expected_products = frozenset(
            {
                ProductKey("spot", "BTCUSDT"),
                ProductKey("um_perpetual", "BTCUSDT"),
            }
        )
        self.state = "READY"
        self.reasons: tuple[str, ...] = ()

    def evaluate(self) -> DeploymentReadinessResult:
        return DeploymentReadinessResult(
            self.state,
            self.reasons,
            {
                "service_state": {
                    "products": {
                        "spot": {"BTCUSDT": {}},
                        "um_perpetual": {"BTCUSDT": {}},
                    }
                }
            },
        )

    def set_ready(self) -> None:
        self.state = "READY"
        self.reasons = ()

    def set_recoverable(self, product: str = "spot:BTCUSDT") -> None:
        self.state = "NOT_READY"
        self.reasons = (f"{product}_core_not_ready",)

    def set_nonrecoverable(self, reason: str = "systemd_service_not_active") -> None:
        self.state = "NOT_READY"
        self.reasons = (reason,)

    def set_failed(self) -> None:
        self.state = "FAILED"
        self.reasons = ("runtime_reported_failed",)


def _v4_observer(
    tmp_path: Path,
) -> tuple[V4AcceptanceObserver, FakeClock, FakeManager, MutableV4Evaluator]:
    base, clock, manager = _observer(tmp_path)
    evaluator = MutableV4Evaluator()
    observer = V4AcceptanceObserver(
        stage=base.stage,
        run_id=base.run_id,
        data_root=base.data_root,
        evidence_root=base.evidence_root,
        identity=base.identity,
        prior_stage_sha256=base.prior_stage_sha256,
        manager=manager,  # type: ignore[arg-type]
        evaluator=evaluator,  # type: ignore[arg-type]
        clock=clock,
        disk_usage=base.disk_usage,
        identity_verifier=base.identity_verifier,
    )
    return observer, clock, manager, evaluator


def _publish_v4_predecessors(observer: V4AcceptanceObserver) -> str:
    root = observer.evidence_root.parent
    identity_document = _empty_common(
        kind="identity-result",
        stage="identity",
        run_id="identity-v4",
        identity=observer.identity,
        now_utc=10,
        now_boot=10,
        boot_id="boot-a",
        schema_version=V4_SCHEMA_VERSION,
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
        run_id="readiness-v4",
        identity=observer.identity,
        now_utc=11,
        now_boot=11,
        boot_id="boot-a",
        schema_version=V4_SCHEMA_VERSION,
    )
    readiness_document.update(
        {
            "prior_stage_evidence_sha256": identity_sha,
            "readiness": {
                "schema_version": "deployment-readiness.v1",
                "state": "READY",
                "reasons": [],
                "evidence": {
                    "service_state": {
                        "products": {
                            "spot": {"BTCUSDT": {}},
                            "um_perpetual": {"BTCUSDT": {}},
                        }
                    }
                },
            },
            "systemd_process_incarnation": observer.manager.process_incarnation(),
            "observer_status": "COMPLETE",
            "result": "PASS_CANDIDATE",
        }
    )
    _path, readiness_sha = _publish(root, "readiness-result.json", readiness_document)
    return readiness_sha


def test_v4_incident_recovery_has_no_permanent_readiness_blocker(tmp_path: Path) -> None:
    observer, clock, _manager, evaluator = _v4_observer(tmp_path)
    _path, _sha, start = observer.start()
    assert start["schema_version"] == V4_SCHEMA_VERSION
    evaluator.set_recoverable()
    clock.utc += 1
    clock.boot += 1
    observer.sample()
    clock.utc += 1_210_056_629
    clock.boot += 1_210_056_629
    _path, _sha, intermediate = observer.sample()
    assert intermediate["result"] == "PASS_CANDIDATE"
    assert intermediate["blocking_findings"] == []
    assert cast(dict[str, object], intermediate["readiness_recovery_episode"])["state"] == (
        "ACTIVE"
    )
    evaluator.set_ready()
    clock.utc += 1
    clock.boot += 1
    _path, _sha, recovered = observer.sample()
    assert recovered["result"] == "PASS_CANDIDATE"
    assert recovered["blocking_findings"] == []
    assert cast(dict[str, object], recovered["readiness_recovery_episode"])["state"] == (
        "RECOVERED"
    )


@pytest.mark.parametrize(
    ("delta", "expected_state", "expected_finding"),
    [
        (899 * 1_000_000_000, "ACTIVE", None),
        (V4_DEADLINE_NS, "ACTIVE", None),
        (901 * 1_000_000_000, "DEADLINE_EXCEEDED", "readiness_recovery_deadline_exceeded"),
    ],
)
def test_v4_deadline_boundaries_are_inclusive(
    tmp_path: Path,
    delta: int,
    expected_state: str,
    expected_finding: str | None,
) -> None:
    observer, clock, _manager, evaluator = _v4_observer(tmp_path)
    observer.start()
    evaluator.set_recoverable()
    clock.utc += 1
    clock.boot += 1
    observer.sample()
    remaining = delta
    while remaining:
        step = min(300 * 1_000_000_000, remaining)
        clock.utc += step
        clock.boot += step
        _path, _sha, sample = observer.sample()
        remaining -= step
    episode = cast(dict[str, object], sample["readiness_recovery_episode"])
    assert episode["state"] == expected_state
    if expected_finding is None:
        assert sample["blocking_findings"] == []
    else:
        assert expected_finding in cast(list[object], sample["blocking_findings"])


def test_v4_ready_after_deadline_remains_sticky_failed(tmp_path: Path) -> None:
    observer, clock, _manager, evaluator = _v4_observer(tmp_path)
    observer.start()
    evaluator.set_recoverable()
    clock.utc += 1
    clock.boot += 1
    observer.sample()
    remaining = 901 * 1_000_000_000
    while remaining:
        step = min(300 * 1_000_000_000, remaining)
        clock.utc += step
        clock.boot += step
        observer.sample()
        remaining -= step
    evaluator.set_ready()
    clock.utc += 1
    clock.boot += 1
    _path, _sha, sample = observer.sample()
    assert sample["result"] == "FAIL"
    assert "readiness_recovery_deadline_exceeded" in cast(
        list[object], sample["blocking_findings"]
    )


def test_v4_product_rotation_does_not_reset_global_deadline(tmp_path: Path) -> None:
    observer, clock, _manager, evaluator = _v4_observer(tmp_path)
    observer.start()
    evaluator.set_recoverable("spot:BTCUSDT")
    clock.utc += 1
    clock.boot += 1
    observer.sample()
    evaluator.set_recoverable("um_perpetual:BTCUSDT")
    remaining = V4_DEADLINE_NS + 1
    while remaining:
        step = min(300 * 1_000_000_000, remaining)
        clock.utc += step
        clock.boot += step
        _path, _sha, sample = observer.sample()
        remaining -= step
    assert "readiness_recovery_deadline_exceeded" in cast(
        list[object], sample["blocking_findings"]
    )
    episode = cast(dict[str, object], sample["readiness_recovery_episode"])
    assert episode["episode_start_boottime_ns"] == 2_000_000_001


def test_v4_terminal_not_ready_fails_even_inside_deadline(tmp_path: Path) -> None:
    observer, _clock, _manager, _evaluator = _v4_observer(tmp_path)
    sample = {
        "boot_id": "boot-a",
        "readiness": {"state": "NOT_READY", "reasons": ["spot:BTCUSDT_core_not_ready"]},
        "result": "PASS_CANDIDATE",
    }
    assert observer._additional_final_findings(sample) == {"readiness_not_ready"}
    assert observer._final_result(
        sample=sample,
        elapsed=V4_DEADLINE_NS,
        terminal_open_detail=None,
    ) == "FAIL"


def test_v4_failed_and_nonrecoverable_readiness_fail_closed(tmp_path: Path) -> None:
    observer, clock, _manager, evaluator = _v4_observer(tmp_path)
    observer.start()
    evaluator.set_failed()
    clock.utc += 1
    clock.boot += 1
    _path, _sha, failed = observer.sample()
    assert failed["result"] == "FAIL"
    assert "readiness_failed" in cast(list[object], failed["blocking_findings"])
    observer2, clock2, _manager2, evaluator2 = _v4_observer(tmp_path / "second")
    observer2.start()
    evaluator2.set_nonrecoverable()
    clock2.utc += 1
    clock2.boot += 1
    _path, _sha, nonrecoverable = observer2.sample()
    assert nonrecoverable["result"] == "FAIL"
    assert "readiness_not_ready" in cast(list[object], nonrecoverable["blocking_findings"])


def test_v4_pre_t0_not_ready_cannot_establish_eligible_t0(tmp_path: Path) -> None:
    observer, _clock, _manager, evaluator = _v4_observer(tmp_path)
    evaluator.set_recoverable()
    _path, _sha, start = observer.start()
    assert start["result"] == "FAIL"
    assert "readiness_not_ready" in cast(list[object], start["blocking_findings"])
    assert cast(dict[str, object], start["readiness_recovery_episode"])["state"] == (
        "NONE"
    )


def test_v4_resume_preserves_episode_start_and_deadline(tmp_path: Path) -> None:
    observer, clock, manager, evaluator = _v4_observer(tmp_path)
    observer.start()
    evaluator.set_recoverable()
    clock.utc += 100
    clock.boot += 100
    observer.sample()
    resumed = resume_observer(
        observer.evidence_root,
        data_root=observer.data_root,
        identity=observer.identity,
        manager=manager,  # type: ignore[arg-type]
        evaluator=evaluator,  # type: ignore[arg-type]
        clock=clock,
        disk_usage=observer.disk_usage,
        schema_version=V4_SCHEMA_VERSION,
        identity_verifier=observer.identity_verifier,
    )
    assert isinstance(resumed, V4AcceptanceObserver)
    assert resumed.readiness_episode.start_boottime_ns == 2_000_000_100
    remaining = V4_DEADLINE_NS + 1
    while remaining:
        step = min(300 * 1_000_000_000, remaining)
        clock.utc += step
        clock.boot += step
        _path, _sha, sample = resumed.sample()
        remaining -= step
    assert "readiness_recovery_deadline_exceeded" in cast(
        list[object], sample["blocking_findings"]
    )
    assert resumed.readiness_episode.start_boottime_ns == 2_000_000_100


def test_v4_completed_stage_uses_streaming_verifier_and_v4_predecessor(tmp_path: Path) -> None:
    observer, clock, _manager, evaluator = _v4_observer(tmp_path)
    observer.prior_stage_sha256 = _publish_v4_predecessors(observer)
    observer.start()
    for _ in range(24):
        clock.utc += 300 * 1_000_000_000
        clock.boot += 300 * 1_000_000_000
        observer.sample()
    final_path, _final_sha, final = observer.finalize()
    assert final["schema_version"] == V4_SCHEMA_VERSION
    verified, verified_sha = verify_completed_stage(
        observer.evidence_root, observer.identity, expected_stage="2h"
    )
    assert verified == final
    assert verified_sha == sha256_bytes(final_path.read_bytes())
    assert evaluator.state == "READY"


def test_v4_inherits_catalog_open_completed_and_cadence_fail_closed(
    tmp_path: Path,
) -> None:
    observer, clock, _manager, _evaluator = _v4_observer(tmp_path)
    observer.prior_stage_sha256 = _publish_v4_predecessors(observer)
    observer.start()
    t0 = cast(int, observer.t0_utc_ns)
    _record_gap_started(
        observer, event_id="v4-gap-start", occurred_at_utc_ns=t0 + 1, gap_id="v4-gap"
    )
    clock.utc += 2
    clock.boot += 2
    _path, _sha, opened = observer.sample()
    assert opened["blocking_findings"] == []
    _record_gap_completed(
        observer, event_id="v4-gap-complete", occurred_at_utc_ns=t0 + 3, gap_id="v4-gap"
    )
    clock.utc += 2
    clock.boot += 2
    _path, _sha, completed = observer.sample()
    assert completed["blocking_findings"] == []
    _advance_stage_with_samples(observer, clock)
    _path, _sha, final = observer.finalize()
    assert final["result"] == "PASS_CANDIDATE"
    assert final["eligible_for_next_stage"] is True
    verify_completed_stage(observer.evidence_root, observer.identity, expected_stage="2h")

    observer2, clock2, _manager2, _evaluator2 = _v4_observer(tmp_path / "gap")
    observer2.start()
    clock2.utc += 600 * 1_000_000_000 + 1
    clock2.boot += 600 * 1_000_000_000 + 1
    _path, _sha, gap_sample = observer2.sample()
    assert gap_sample["result"] == "INCOMPLETE"
    assert "acceptance_observation_gap" in cast(
        list[object], gap_sample["blocking_findings"]
    )


def test_v4_rejects_v3_predecessor_and_v3_resume(tmp_path: Path) -> None:
    observer, clock, manager = _observer(tmp_path)
    _publish_valid_identity_and_readiness(observer)
    with pytest.raises(AcceptanceError, match="schema cannot authorize"):
        verify_prior_stage(
            observer.evidence_root.parent / "readiness-result.json",
            observer.identity,
            "2h",
            schema_version=V4_SCHEMA_VERSION,
        )
    observer.start()
    clock.utc += 1
    clock.boot += 1
    observer.sample()
    with pytest.raises(AcceptanceError, match="v3 observer cannot resume as v4"):
        resume_observer(
            observer.evidence_root,
            data_root=observer.data_root,
            identity=observer.identity,
            manager=manager,  # type: ignore[arg-type]
            evaluator=observer.evaluator,
            clock=clock,
            identity_verifier=observer.identity_verifier,
            schema_version=V4_SCHEMA_VERSION,
        )


def test_v4_streaming_verifier_rejects_episode_blocker_boottime_and_hash_tampering(
    tmp_path: Path,
) -> None:
    observer, clock, _manager, evaluator = _v4_observer(tmp_path)
    observer.prior_stage_sha256 = _publish_v4_predecessors(observer)
    observer.start()
    evaluator.set_recoverable()
    clock.utc += 1
    clock.boot += 1
    sample_path, _sha, _sample = observer.sample()

    def tamper_episode(document: dict[str, object]) -> None:
        episode = cast(dict[str, object], document["readiness_recovery_episode"])
        episode["episode_start_boottime_ns"] = 1

    _rewrite_v3_samples(observer.evidence_root, {0: tamper_episode})
    with pytest.raises(AcceptanceError, match="V4 readiness episode"):
        _sample_chain(
            observer.evidence_root,
            start=_read_json(observer.evidence_root / "stage-start.json"),
            start_sha=sha256_bytes(
                (observer.evidence_root / "stage-start.json").read_bytes()
            ),
            identity=observer.identity,
            require_eligible=False,
        )
    assert sample_path.exists()
