from __future__ import annotations

import json
import os
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from binance_market_data_recorder import cli
from binance_market_data_recorder.service import acceptance_v5_online as online
from binance_market_data_recorder.service.acceptance import V5_SCHEMA_VERSION
from binance_market_data_recorder.service.acceptance_v5_corpus import catalog_available
from binance_market_data_recorder.service.readiness import VpsReadinessEvaluator
from binance_market_data_recorder.storage.catalog import Catalog
from tests.unit.test_acceptance_v5_online import advance, observer_fixture
from tests.unit.test_deployment_identity import _identity
from tests.v5_support import production_readiness, publish_ready_state


@pytest.mark.parametrize("command", ["baseline", "finalize", "verify"])
def test_v5_cli_routes_repository_owned_commands(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    command: str,
) -> None:
    # Production profile intentionally rejects environment overrides. Isolate
    # this parser-routing fixture from the runner's safe default test data root.
    for name in list(os.environ):
        if name.startswith("BINANCE_MARKET_RECORDER_"):
            monkeypatch.delenv(name)
    identity = _identity(tmp_path)
    config = tmp_path / "vps.toml"
    config.write_text(
        f'[recorder]\ncapacity_profile="vps-production-v1"\ndata_root="{tmp_path / "data"}"\n'
    )
    called: dict[str, Any] = {}
    document = {"schema_version": V5_SCHEMA_VERSION, "result": "PASS_CANDIDATE"}

    def run(**kwargs: Any) -> tuple[Path, str, dict[str, Any]]:
        called.update(kwargs)
        return tmp_path / "evidence" / "audit-root.json", "a" * 64, document

    monkeypatch.setattr(cli, "load_deployment_identity", lambda _path: identity)
    monkeypatch.setattr(cli, "enforce_vps_paths", lambda _identity: None)
    def archive_roots(_root: Path) -> Any:
        if command == "verify":
            pytest.fail("historical verification must not resolve live archive paths")
        return lambda: {}

    monkeypatch.setattr(cli, "_acceptance_archive_root_resolver", archive_roots)
    monkeypatch.setattr(cli, "v5_baseline", run)
    monkeypatch.setattr(cli, "v5_finalize", run)
    monkeypatch.setattr(cli, "verify_v5_audit", lambda **kwargs: (run(**kwargs)[2], "a" * 64))
    arguments = [
        "--config",
        str(config),
        "deployment",
        "acceptance",
        command,
        "--evidence-root",
        str(tmp_path / "evidence"),
    ]
    if command == "verify":
        arguments.append("--baseline")
    assert cli.main(arguments) == 0
    assert called["identity"] == identity
    if command == "verify":
        assert called["historical_control_only"] is True
    else:
        assert "historical_control_only" not in called
    assert json.loads(capsys.readouterr().out)["command"] == f"deployment.acceptance.{command}"


def test_v5_cli_stage_resume_and_v4_selection() -> None:
    parser = cli.build_parser()
    stage = parser.parse_args(["deployment", "acceptance", "stage", "--resume", "/private/stage"])
    assert stage.schema_version == "v5"
    assert stage.resume == Path("/private/stage")
    legacy = parser.parse_args(
        ["deployment", "acceptance", "stage", "--stage", "2h", "--schema-version", "v4"]
    )
    assert legacy.schema_version == "v4"
    final = parser.parse_args(
        ["deployment", "acceptance", "finalize", "--evidence-root", "/private/stage", "--resume"]
    )
    assert final.resume is True


@pytest.mark.parametrize("mode", ["stage", "resume", "readiness"])
@pytest.mark.parametrize("schema", ["v5", "v4"])
def test_cli_constructs_schema_specific_production_readiness(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mode: str, schema: str
) -> None:
    for name in list(os.environ):
        if name.startswith("BINANCE_MARKET_RECORDER_"):
            monkeypatch.delenv(name)
    observer, clock, _fake = observer_fixture(tmp_path)
    production = production_readiness(observer, clock)
    observer.evaluator = production
    publish_ready_state(observer, clock)
    config = tmp_path / "vps.toml"
    config.write_text(
        f'[recorder]\ncapacity_profile="vps-production-v1"\ndata_root="{observer.data_root}"\n'
        'spot_symbols=[]\nusdm_symbols=["BTCUSDT"]\n'
    )
    legacy_checks = 0

    def legacy_check(_catalog: Catalog) -> tuple[str, ...]:
        nonlocal legacy_checks
        legacy_checks += 1
        if schema == "v5":
            pytest.fail("production V5 readiness invoked legacy Catalog integrity_check")
        return ("ok",)

    monkeypatch.setattr(Catalog, "integrity_check", legacy_check)
    monkeypatch.setattr(cli, "load_deployment_identity", lambda _path: observer.identity)
    monkeypatch.setattr(cli, "enforce_vps_paths", lambda _identity: None)
    monkeypatch.setattr(
        cli, "_identity_systemd_manager", lambda *_args, **_kwargs: observer.manager
    )
    monkeypatch.setattr(cli, "_acceptance_archive_root_resolver", lambda _root: lambda: {})

    def evaluator(**kwargs: Any) -> VpsReadinessEvaluator:
        # Assert constructor injection before any observer/readiness hook can
        # mutate the callback. All evaluate() logic remains production code.
        if schema == "v5":
            assert kwargs["catalog_ready"] is catalog_available
        else:
            assert "catalog_ready" not in kwargs
        return VpsReadinessEvaluator(
            **{
                **kwargs,
                "systemd_manager": production.systemd_manager,
                "utc_clock_ns": clock.utc_ns,
                "process_alive": production.process_alive,
                "identity_verifier": production.identity_verifier,
                "process_environment": production.process_environment,
            }
        )

    monkeypatch.setattr(cli, "VpsReadinessEvaluator", evaluator)

    def construct(**kwargs: Any) -> online.V5AcceptanceObserver:
        return replace(
            observer,
            evidence_root=kwargs["evidence_root"],
            run_id=kwargs["run_id"],
            evaluator=kwargs["evaluator"],
        )

    def resume(**kwargs: Any) -> online.V5AcceptanceObserver:
        return online.resume_v5_observer(
            **kwargs,
            clock=clock,
            identity_verifier=observer.identity_verifier,
            disk_usage=observer.disk_usage,
            snapshot_unit=observer.snapshot_unit,
            raw_unit=observer.raw_unit,
        )

    def legacy_observer(*_args: Any, **kwargs: Any) -> SimpleNamespace:
        assert kwargs["evaluator"].evaluate().state == "READY"
        return SimpleNamespace(
            stage_start_sha256="a" * 64,
            start=lambda: (tmp_path / "stage-start.json", "a" * 64, {"result": "PASS_CANDIDATE"}),
        )

    def run(selected: Any) -> tuple[Path, str, dict[str, Any]]:
        if schema == "v5":
            advance(clock, 300)
            publish_ready_state(selected, clock)
            path, digest, document = selected.sample()
            assert document["readiness"]["state"] == "READY"
            assert document["blocking_findings"] == []
            return path, digest, {**document, "result": "PASS_CANDIDATE"}
        return tmp_path / "final.json", "a" * 64, {"result": "PASS_CANDIDATE"}

    def readiness(**kwargs: Any) -> tuple[Path, str, dict[str, Any]]:
        assert kwargs["evaluator"].evaluate().state == "READY"
        return tmp_path / "readiness.json", "a" * 64, {"result": "PASS_CANDIDATE"}

    monkeypatch.setattr(cli, "V5AcceptanceObserver", construct)
    monkeypatch.setattr(cli, "resume_v5_observer", resume)
    monkeypatch.setattr(cli, "V4AcceptanceObserver", legacy_observer)
    monkeypatch.setattr(cli, "resume_observer", legacy_observer)
    monkeypatch.setattr(cli, "verify_prior_stage", lambda *_args, **_kwargs: ({}, "a" * 64))
    monkeypatch.setattr(cli, "_run_acceptance_stage", run)
    monkeypatch.setattr(cli, "create_readiness_evidence", readiness)
    arguments = ["--config", str(config), "deployment", "acceptance"]
    if mode == "resume":
        if schema == "v5":
            observer.start()
        arguments += ["stage", "--resume", str(observer.evidence_root)]
    elif mode == "stage":
        arguments += [
            "stage", "--stage", "2h", "--previous-evidence", str(observer.predecessor_path),
            "--evidence-root", str(tmp_path / "cli-evidence"),
        ]
    else:
        arguments += [
            "readiness", "--identity-evidence", str(tmp_path / "identity.json"),
            "--evidence-root", str(tmp_path / "cli-readiness"),
        ]
    arguments += ["--schema-version", schema]
    assert cli.main(arguments) == 0
    assert legacy_checks == (0 if schema == "v5" else 1)
