from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from binance_market_data_recorder.domain.product import ProductKey
from binance_market_data_recorder.service import acceptance_v5_online as online
from binance_market_data_recorder.service import acceptance_v5_raw as raw
from binance_market_data_recorder.service.acceptance import AcceptanceError, _publish
from binance_market_data_recorder.service.acceptance_v5_corpus import catalog_available
from binance_market_data_recorder.service.acceptance_v5_io import V5_SCHEMA_VERSION, open_exact
from binance_market_data_recorder.service.acceptance_v5_raw import qualify_task
from binance_market_data_recorder.service.deployment_identity import DeploymentIdentity
from binance_market_data_recorder.service.readiness import (
    DeploymentReadinessResult,
    VpsReadinessEvaluator,
)
from binance_market_data_recorder.service.state import ServiceStateStore
from binance_market_data_recorder.storage.acceptance_delta import DeltaSnapshot
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tests.unit.test_deployment_identity import _identity
from tests.unit.test_historical_reconnect_audit import seal_chunk, usdm_envelope
from tests.unit.test_m22_9_acceptance import FakeClock, FakeManager
from tests.v5_support import production_readiness, publish_ready_state


class Evaluator:
    expected_products = frozenset({ProductKey("um_perpetual", "BTCUSDT")})

    def __init__(self) -> None:
        self.state = "READY"
        self.reasons: tuple[str, ...] = ()

    def evaluate(self) -> DeploymentReadinessResult:
        return DeploymentReadinessResult(self.state, self.reasons, {})


def observer_fixture(tmp_path: Path) -> tuple[online.V5AcceptanceObserver, FakeClock, Evaluator]:
    identity: DeploymentIdentity = _identity(tmp_path)
    data_root = tmp_path / "data"
    layout = ensure_storage_layout(data_root)
    with Catalog(layout.catalog) as catalog:
        catalog.migrate_acceptance_sequence()
        high_water = DeltaSnapshot(catalog._connection).high_water
    ServiceStateStore(layout.state / "service_state.json").write(
        {
            "status": "RUNNING",
            "pid": 123,
            "service_instance_id": "service-a",
            "deployment_identity": online.runtime_identity(identity),
        }
    )
    # Online-unit fixture: a repository audit root is a predecessor reference,
    # not a replacement for the separate full-audit integration tests.
    predecessor, _digest = _publish(
        tmp_path / "baseline",
        "audit-root.json",
        {
            "schema_version": V5_SCHEMA_VERSION,
            "evidence_kind": "baseline-audit-root",
            "deployment_identity": identity.document(),
            "result": "PASS_CANDIDATE",
            "catalog_authority": {"high_water": high_water},
            "continuation_seed": {},
            "configured_products": [["um_perpetual", "BTCUSDT"]],
        },
    )
    clock, evaluator = FakeClock(), Evaluator()
    observer = online.V5AcceptanceObserver(
        stage="2h",
        run_id="run-a",
        data_root=data_root,
        evidence_root=tmp_path / "evidence" / "2h-run-a",
        identity=identity,
        predecessor_path=predecessor,
        manager=cast(Any, FakeManager()),
        evaluator=cast(Any, evaluator),
        clock=clock,
        identity_verifier=lambda _identity: {},
        disk_usage=lambda _path: SimpleNamespace(total=100 * 1024**3, free=50 * 1024**3),
        raw_unit=lambda task, **_kwargs: qualify_task(task),
        snapshot_unit=lambda task, **_kwargs: qualify_task(task),
    )
    return observer, clock, evaluator


def advance(clock: FakeClock, seconds: int) -> None:
    clock.boot += seconds * 1_000_000_000
    clock.utc += seconds * 1_000_000_000


@pytest.mark.parametrize("resume", [False, True])
def test_direct_observer_guard_already_bounds_default_evaluator(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, resume: bool
) -> None:
    observer, clock, _fake = observer_fixture(tmp_path)
    probes = production_readiness(observer, clock)
    # Direct API compatibility: unlike the corrected CLI, this caller omits
    # catalog_ready. The pre-existing observer guard must remain effective.
    legacy = VpsReadinessEvaluator(
        expected_products=probes.expected_products,
        data_root=observer.data_root,
        identity=observer.identity,
        systemd_manager=probes.systemd_manager,
        utc_clock_ns=probes.utc_clock_ns,
        process_alive=probes.process_alive,
        identity_verifier=probes.identity_verifier,
        process_environment=probes.process_environment,
    )
    assert legacy.catalog_ready is not catalog_available
    publish_ready_state(observer, clock)
    if resume:
        observer.start()
        selected = online.resume_v5_observer(
            evidence_root=observer.evidence_root,
            data_root=observer.data_root,
            identity=observer.identity,
            manager=observer.manager,
            evaluator=legacy,
            clock=clock,
            identity_verifier=observer.identity_verifier,
            disk_usage=observer.disk_usage,
            snapshot_unit=observer.snapshot_unit,
        )
    else:
        selected = replace(observer, evaluator=legacy)
    assert selected.evaluator is legacy
    assert legacy.catalog_ready is catalog_available

    def forbidden(_catalog: Catalog) -> Any:
        pytest.fail("direct V5 observer lost its existing bounded readiness guard")

    monkeypatch.setattr(Catalog, "integrity_check", forbidden)
    if not resume:
        _path, _sha, start = selected.start()
        assert start["readiness"]["state"] == "READY"
    advance(clock, 300)
    publish_ready_state(selected, clock)
    _path, _sha, sample = selected.sample()
    assert sample["readiness"]["state"] == "READY"


def test_sql_snapshot_budget_exhaustion_remains_explicit(tmp_path: Path) -> None:
    observer, clock, _evaluator = observer_fixture(tmp_path)
    observer.start()
    observer.snapshot_unit = lambda *_args, **_kwargs: None
    advance(clock, 300)
    _path, _sha, sample = observer.sample()
    assert sample["snapshot_status"] == "budget_pending"
    assert sample["delta_pending"] is True
    assert observer.start_document is not None
    assert (
        sample["continuation"]["processed"] == observer.start_document["continuation"]["processed"]
    )
    online.replay_online(observer.evidence_root, observer.identity, require_target=False)


def test_new_manifest_consumed_once_and_resume(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    observer, clock, _evaluator = observer_fixture(tmp_path)
    observer.start()
    layout = ensure_storage_layout(observer.data_root)
    with Catalog(layout.catalog) as catalog:
        seal_chunk(layout, catalog, [usdm_envelope("one", 1)])
    reads = 0
    original = open_exact

    def counted(*args: Any, **kwargs: Any) -> Any:
        nonlocal reads
        if str(args[1]).endswith(".manifest.json"):
            reads += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(raw, "open_exact", counted)
    advance(clock, 300)
    _path, _sha, sample = observer.sample()
    assert reads == 2  # one content read, one descriptor-only path identity guard
    assert sample["continuation"]["processed"]["chunk"] == 3
    assert sample["delta_pending"] is False
    advance(clock, 300)
    observer.sample()
    assert reads == 2
    replay = online.replay_online(observer.evidence_root, observer.identity, require_target=False)
    assert replay.continuation == observer.continuation
    resumed = online.resume_v5_observer(
        evidence_root=observer.evidence_root,
        data_root=observer.data_root,
        identity=observer.identity,
        manager=observer.manager,
        evaluator=observer.evaluator,
        clock=clock,
        identity_verifier=observer.identity_verifier,
        disk_usage=observer.disk_usage,
    )
    assert resumed.t0_boottime_ns == observer.t0_boottime_ns
    assert resumed.continuation == observer.continuation
    assert resumed.last_sample_sha256 == observer.last_sample_sha256


def test_target_unique_no_post_target_sample_and_no_audit_credit(tmp_path: Path) -> None:
    observer, clock, _evaluator = observer_fixture(tmp_path)
    observer.start()
    for _ in range(23):
        advance(clock, 300)
        observer.sample()
    advance(clock, 300)
    _path, _sha, target = observer.finalize()
    assert target["result"] == "PASS_CANDIDATE"
    assert target["eligible_for_next_stage"] is False
    assert target["elapsed_boottime_ns"] == 7200_000_000_000
    advance(clock, 3600)
    with pytest.raises(AcceptanceError, match="open V5"):
        observer.sample()
    with pytest.raises(AcceptanceError, match="already closed"):
        online.resume_v5_observer(
            evidence_root=observer.evidence_root,
            data_root=observer.data_root,
            identity=observer.identity,
            manager=observer.manager,
            evaluator=observer.evaluator,
            clock=clock,
        )
    assert (
        online.replay_online(observer.evidence_root, observer.identity, require_target=True).target
        == target
    )


def test_budget_pending_cannot_be_terminally_salvaged(tmp_path: Path) -> None:
    observer, clock, _evaluator = observer_fixture(tmp_path)
    observer.raw_unit = lambda *_args, **_kwargs: None
    observer.start()
    layout = ensure_storage_layout(observer.data_root)
    with Catalog(layout.catalog) as catalog:
        seal_chunk(layout, catalog, [usdm_envelope("one", 1)])
    for _ in range(23):
        advance(clock, 300)
        observer.sample()
    advance(clock, 300)
    _path, _sha, target = observer.finalize()
    assert target["result"] == "INCOMPLETE"
    assert target["cursor_tuple"]["chunk"] == 2
    assert target["target_high_water"]["chunk"] == 3
    assert "target_delta_pending" in target["blocking_findings"]


@pytest.mark.parametrize(
    "recovery_seconds,expected", [(900, "RECOVERED"), (901, "DEADLINE_EXCEEDED")]
)
def test_readiness_deadline_unchanged(tmp_path: Path, recovery_seconds: int, expected: str) -> None:
    observer, clock, evaluator = observer_fixture(tmp_path)
    observer.start()
    evaluator.state, evaluator.reasons = "NOT_READY", ("um_perpetual:BTCUSDT_core_not_ready",)
    advance(clock, 300)
    observer.sample()
    advance(clock, 300)
    observer.sample()
    advance(clock, 300)
    observer.sample()
    evaluator.state, evaluator.reasons = "READY", ()
    advance(clock, recovery_seconds - 600)
    _path, _sha, sample = observer.sample()
    assert sample["readiness_episode"]["state"] == expected
    online.replay_online(observer.evidence_root, observer.identity, require_target=False)
