from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

MODULE_PATH = Path(__file__).resolve().parents[1] / "advance.py"
SPEC = importlib.util.spec_from_file_location("advance", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
advance = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = advance
SPEC.loader.exec_module(advance)


def test_legacy_and_container_roots_are_accepted() -> None:
    assert advance.root_path("/var/lib/example") == Path("/var/lib/example")
    assert advance.root_path(
        {"root": "/var/lib/example", "partitioned": True}
    ) == Path("/var/lib/example")
    assert advance.root_path(
        {"market_root": "/var/lib/example"}
    ) == Path("/var/lib/example")
    with pytest.raises(ValueError):
        advance.root_path({"unexpected": "x"})


def test_production_dropin_always_writes_restartsec_300(tmp_path: Path) -> None:
    advance.write_production_dropin(tmp_path)
    assert (tmp_path / "production.conf").read_text() == (
        "[Service]\nRestart=on-failure\nRestartSec=300\n"
    )


def test_finish_only_parser_does_not_require_c3_or_version() -> None:
    parser = advance.build_parser()
    args = parser.parse_args(
        [
            "--source",
            "/source",
            "--finish-only",
            "--release",
            "/release",
            "--qualification-report",
            "/evidence/progress.json",
        ]
    )
    assert args.finish_only is True
    assert args.c3_report is None
    assert args.version is None


def test_normal_mode_requires_version_and_c3(monkeypatch) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        ["advance.py", "--source", "/source"],
    )
    with pytest.raises(SystemExit):
        advance.main()
