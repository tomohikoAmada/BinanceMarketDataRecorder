from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

from binance_market_data_recorder.service.acceptance import (
    STAGE_DURATION_NS,
    AcceptanceError,
    AcceptanceObserver,
    _publish,
    _safe_evidence_root,
    canonical_json,
    resume_observer,
    verify_prior_stage,
)
from binance_market_data_recorder.service.readiness import DeploymentReadinessResult
from binance_market_data_recorder.service.state import ServiceStateStore
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tests.unit.test_deployment_identity import _identity


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
        prior_stage_sha256="readiness-sha",
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
    with pytest.raises(AcceptanceError, match="collision"):
        _publish(observer.evidence_root, "stage-start.json", {"x": 1})


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
