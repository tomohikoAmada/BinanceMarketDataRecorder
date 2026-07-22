from __future__ import annotations

import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_distribution_package_cli_and_python_contract() -> None:
    with (ROOT / "pyproject.toml").open("rb") as handle:
        project = tomllib.load(handle)

    assert project["project"]["name"] == "binance-market-data-recorder"
    assert project["project"]["requires-python"] == ">=3.12,<3.13"
    assert project["project"]["scripts"] == {
        "binance-market-recorder": "binance_market_data_recorder.cli:main"
    }


def test_dependency_scope_is_m1_only() -> None:
    with (ROOT / "pyproject.toml").open("rb") as handle:
        project = tomllib.load(handle)

    assert project["project"]["dependencies"] == ["pydantic>=2.10,<3"]
    serialized = str(project).casefold()
    for forbidden in ("websocket", "binance-sdk", "fastapi", "parquet", "pandas", "qt"):
        assert forbidden not in serialized
