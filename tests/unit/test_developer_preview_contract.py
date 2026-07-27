from __future__ import annotations

import tomllib
from pathlib import Path

from binance_market_data_recorder.version import FALLBACK_VERSION

ROOT = Path(__file__).resolve().parents[2]
LONG_RUN_NOTICE = (
    "连续72小时和168小时长期运行验收尚未执行。\n"
    "静态审查、单元测试、故障注入和短期在线测试不能替代长期运行证明。\n"
    "当前版本为Mac Developer Preview;"
    "Ubuntu ARM64/RK3588为Developer Preview / Soak Candidate;"
    "不得用于真实资金交易。"
)


def _normalized(path: Path) -> str:
    return path.read_text(encoding="utf-8").replace("  \n", "\n")


def test_developer_preview_identity_and_version_are_frozen() -> None:
    with (ROOT / "pyproject.toml").open("rb") as handle:
        project = tomllib.load(handle)["project"]
    assert project["name"] == "binance-market-data-recorder"
    assert project["version"] == "0.1.0a1"
    assert project["requires-python"] == ">=3.12,<3.13"
    assert project["scripts"] == {
        "binance-market-recorder": "binance_market_data_recorder.cli:main"
    }
    assert FALLBACK_VERSION == "0.1.0a1"


def test_required_preview_surfaces_carry_the_same_long_run_warning() -> None:
    for relative in (
        "README.md",
        "docs/known_limitations.md",
        "docs/risk_register.md",
        "release/0.1.0a1/RELEASE_NOTES.md",
    ):
        assert LONG_RUN_NOTICE in _normalized(ROOT / relative), relative


def test_operator_document_set_exists_without_claiming_binance_affiliation() -> None:
    paths = (
        ROOT / "README.md",
        ROOT / "docs/quickstart_macos.md",
        ROOT / "docs/architecture.md",
        ROOT / "docs/data_and_storage.md",
        ROOT / "docs/operations.md",
        ROOT / "docs/known_limitations.md",
        ROOT / "docs/binance_sources.md",
        ROOT / "release/0.1.0a1/RELEASE_NOTES.md",
    )
    for path in paths:
        assert path.is_file()
    release_text = "\n".join(_normalized(path).casefold() for path in paths)
    for forbidden in (
        "official binance " + "recorder",
        "binance-" + "maintained",
        "binance-" + "certified",
        "binance " + "partner",
        "com." + "binance.",
        "org." + "binance.",
        "io." + "binance.",
    ):
        assert forbidden not in release_text
