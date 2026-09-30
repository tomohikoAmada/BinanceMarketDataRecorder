from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from binance_market_data_recorder import cli
from binance_market_data_recorder.service.acceptance import V5_SCHEMA_VERSION
from tests.unit.test_deployment_identity import _identity


@pytest.mark.parametrize("command", ["baseline", "finalize", "verify"])
def test_v5_cli_routes_repository_owned_commands(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    command: str,
) -> None:
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
    monkeypatch.setattr(cli, "_acceptance_archive_root_resolver", lambda _root: lambda: {})
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
