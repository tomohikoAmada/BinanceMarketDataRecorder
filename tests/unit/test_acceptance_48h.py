from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from binance_market_data_recorder import cli
from binance_market_data_recorder.service import acceptance_v5_online as online
from binance_market_data_recorder.service.acceptance import (
    STAGE_DURATION_NS,
    V5_SCHEMA_VERSION,
    AcceptanceError,
    _publish,
)
from binance_market_data_recorder.service.acceptance_v5_io import read_document
from tests.integration.test_acceptance_v5_finalize import raw_unit
from tests.unit.test_acceptance_v5_io import stopped
from tests.unit.test_acceptance_v5_online import advance, observer_fixture


def predecessor(observer: online.V5AcceptanceObserver, previous: str) -> Path:
    # Online-only control fixture; complete audit qualification has separate tests.
    audit, _ = read_document(observer.predecessor_path)
    root = observer.evidence_root.parent / f"prior-{previous}"
    _, digest = _publish(root / "terminal-audit", "audit-root.json", audit)
    path, _ = _publish(
        root,
        "stage-final.json",
        {
            "schema_version": V5_SCHEMA_VERSION,
            "evidence_kind": "stage-final",
            "deployment_identity": observer.identity.document(),
            "result": "PASS_CANDIDATE",
            "stage": previous,
            "eligible_for_next_stage": True,
            "terminal_audit_sha256": digest,
        },
    )
    return path


@pytest.mark.parametrize(
    ("stage", "previous", "wrong"),
    [("12h", "2h", "24h"), ("24h", "12h", "2h"), ("48h", "24h", "12h"),
     ("72h", "24h", "48h"), ("168h", "72h", "48h")],
)
def test_stage_branches_preserve_legacy_predecessors(
    tmp_path: Path, stage: str, previous: str, wrong: str
) -> None:
    observer, _, _ = observer_fixture(tmp_path)
    path = predecessor(observer, previous)
    assert online.predecessor_reference(path, observer.identity, stage)["path"] == str(path)
    with pytest.raises(AcceptanceError, match="predecessor stage"):
        online.predecessor_reference(predecessor(observer, wrong), observer.identity, stage)


def test_48h_own_duration_resume_target_and_replay(tmp_path: Path) -> None:
    original, clock, _ = observer_fixture(tmp_path)
    observer = replace(
        original,
        stage="48h",
        evidence_root=original.evidence_root.parent / "48h-run-a",
        predecessor_path=predecessor(original, "24h"),
    )
    _, start_sha, start = observer.start()
    assert start["required_duration_ns"] == 172_800_000_000_000
    # A predecessor's duration cannot shorten this new stage's own clock.
    for _ in range(575):
        advance(clock, 300)
        observer.sample()
    advance(clock, 299)
    with pytest.raises(AcceptanceError, match="duration not reached"):
        observer.finalize()
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
    assert resumed.stage == "48h" and resumed.stage_start_sha256 == start_sha
    assert resumed.t0_boottime_ns == start["t0_boottime_ns"]
    advance(clock, 1)
    _, _, target = resumed.finalize()
    assert target["elapsed_boottime_ns"] == 172_800_000_000_000
    assert target["result"] == "PASS_CANDIDATE" and target["blocking_findings"] == []
    replay = online.replay_online(observer.evidence_root, observer.identity, require_target=True)
    assert replay.target == target
    with pytest.raises(AcceptanceError, match="already closed"):
        online.resume_v5_observer(
            evidence_root=observer.evidence_root,
            data_root=observer.data_root,
            identity=observer.identity,
            manager=observer.manager,
            evaluator=observer.evaluator,
            clock=clock,
        )


def test_cli_accepts_48h_and_legacy_durations_remain_exact() -> None:
    parsed = cli.build_parser().parse_args(
        ["deployment", "acceptance", "stage", "--stage", "48h"]
    )
    assert parsed.stage == "48h" and parsed.schema_version == "v5"
    assert STAGE_DURATION_NS["72h"] == 259_200_000_000_000
    assert STAGE_DURATION_NS["168h"] == 604_800_000_000_000


def test_complete_new_chain_has_own_targets_and_eligible_reconstruction(tmp_path: Path) -> None:
    from binance_market_data_recorder.service.acceptance_v5_finalize import (
        baseline,
        finalize,
        verify_completed_v5_stage,
    )

    original, clock, _ = observer_fixture(tmp_path / "fixture")
    prior, _, audit = baseline(
        data_root=original.data_root, evidence_root=tmp_path / "baseline",
        identity=original.identity, products=[["um_perpetual", "BTCUSDT"]],
        archive_roots={}, probe=stopped, identity_verifier=lambda _: None,
        raw_unit=raw_unit, boot_id="boot-a",
    )
    assert audit["formal_duration_credit_ns"] == 0
    for stage in ("2h", "12h", "24h", "48h"):
        observer = replace(
            original, stage=stage, run_id=f"run-{stage}",
            evidence_root=tmp_path / f"complete-{stage}", predecessor_path=prior,
        )
        _, start_sha, start = observer.start()
        for _ in range(STAGE_DURATION_NS[stage] // 300_000_000_000 - 1):
            advance(clock, 300)
            observer.sample()
        resumed = online.resume_v5_observer(
            evidence_root=observer.evidence_root, data_root=observer.data_root,
            identity=observer.identity, manager=observer.manager, evaluator=observer.evaluator,
            clock=clock, identity_verifier=observer.identity_verifier,
            disk_usage=observer.disk_usage, raw_unit=observer.raw_unit,
            snapshot_unit=observer.snapshot_unit,
        )
        assert resumed.stage_start_sha256 == start_sha
        assert resumed.t0_boottime_ns == start["t0_boottime_ns"]
        advance(clock, 300)
        _, _, target = resumed.finalize()
        assert target["elapsed_boottime_ns"] == STAGE_DURATION_NS[stage]
        prior, digest, final = finalize(
            evidence_root=resumed.evidence_root, identity=resumed.identity,
            archive_roots={}, probe=stopped, identity_verifier=lambda _: None, raw_unit=raw_unit,
        )
        reconstructed, observed_digest = verify_completed_v5_stage(
            resumed.evidence_root, resumed.identity, archive_roots={}, expected_stage=stage,
            require_eligible=True, raw_unit=raw_unit,
        )
        assert final["eligible_for_next_stage"] and reconstructed == final
        assert observed_digest == digest
        if stage == "24h":
            assert online.predecessor_reference(prior, resumed.identity, "48h")["sha256"] == digest
            assert online.predecessor_reference(prior, resumed.identity, "72h")["sha256"] == digest
