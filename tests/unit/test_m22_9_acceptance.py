from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

from binance_market_data_recorder.audit import reconnect_boundaries as reconnect_audit
from binance_market_data_recorder.service.acceptance import (
    STAGE_DURATION_NS,
    AcceptanceError,
    AcceptanceObserver,
    _publish,
    _safe_evidence_root,
    canonical_json,
    resume_observer,
    verify_completed_stage,
    verify_prior_stage,
)
from binance_market_data_recorder.service.readiness import DeploymentReadinessResult
from binance_market_data_recorder.service.state import ServiceStateStore
from binance_market_data_recorder.spool.seal import validate_sealed_artifact
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
        evidence_root=tmp_path / "evidence",
        identity=identity,
        prior_stage_sha256="a" * 64,
        manager=manager,  # type: ignore[arg-type]
        evaluator=FakeEvaluator(),  # type: ignore[arg-type]
        clock=clock,
        identity_verifier=lambda _identity: {},
        disk_usage=lambda _path: SimpleNamespace(total=100 * 1024**3, free=50 * 1024**3),
    )
    return observer, clock, manager


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
    assert final["stage_start_evidence_sha256"] == start_sha
    assert cast(int, final["elapsed_boottime_ns"]) >= STAGE_DURATION_NS["2h"]


def test_process_incarnation_change_fails_even_when_pid_is_unchanged(tmp_path: Path) -> None:
    observer, clock, manager = _observer(tmp_path)
    observer.start()
    manager.incarnation["invocation_id"] = "b" * 32
    clock.boot += 1
    _path, _sha, sample = observer.sample()
    assert sample["result"] == "FAIL"
    assert "process_incarnation_changed" in cast(list[object], sample["blocking_findings"])


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
    assert resumed.prior_stage_sha256 == "a" * 64
    with pytest.raises(AcceptanceError, match="collision"):
        _publish(observer.evidence_root, "stage-start.json", {"x": 1})


def test_resume_rejects_continuation_not_bound_to_sample_inventory(tmp_path: Path) -> None:
    observer, clock, manager = _observer(tmp_path)
    observer.start()
    clock.utc += 1
    clock.boot += 1
    sample_path, _sample_sha, _sample = observer.sample()
    document = json.loads(sample_path.read_text(encoding="utf-8"))
    reconnect = cast(dict[str, object], document["reconnect_summary"])
    continuation = cast(dict[str, object], reconnect["continuation"])
    continuation["manifest_members"] = {"data/manifests/fake": "b" * 64}
    sample_path.write_bytes(canonical_json(document))
    with pytest.raises(AcceptanceError, match="bind manifest inventory"):
        resume_observer(
            observer.evidence_root,
            data_root=observer.data_root,
            identity=observer.identity,
            manager=manager,  # type: ignore[arg-type]
            evaluator=FakeEvaluator(),  # type: ignore[arg-type]
            clock=clock,
            disk_usage=observer.disk_usage,
        )


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


def _complete_2h_stage(
    tmp_path: Path,
) -> tuple[AcceptanceObserver, Path, dict[str, object]]:
    observer, clock, _manager = _observer(tmp_path)
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


def test_stage_start_binds_exact_predecessor_and_valid_chain_is_accepted(
    tmp_path: Path,
) -> None:
    observer, final_path, start = _complete_2h_stage(tmp_path)
    assert start["prior_stage_evidence_sha256"] == "a" * 64
    verified, digest = verify_completed_stage(
        observer.evidence_root, observer.identity, expected_stage="2h"
    )
    assert verified["result"] == "PASS_CANDIDATE"
    assert len(digest) == 64
    prior, prior_digest = verify_prior_stage(final_path, observer.identity, "12h")
    assert prior == verified
    assert prior_digest == digest


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
    reconnect = cast(dict[str, object], start["reconnect_summary"])
    assert cast(list[object], reconnect["baseline_history"])
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
                "stream": "book_ticker",
                "gap_started_at_utc_ns": clock.utc - 1,
            },
        )
    _path, _sha, start = observer.start()
    assert start["result"] == "FAIL"
    assert "unresolved_discontinuity" in cast(list[object], start["blocking_findings"])


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
