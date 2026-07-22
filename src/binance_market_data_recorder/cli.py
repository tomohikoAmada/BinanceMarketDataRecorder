"""Offline local command-line interface."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import NoReturn, TextIO

from .config import ConfigurationError, LoadedConfig, load_config
from .diagnostics import run_doctor
from .metrics.report import DailyReporter
from .paths import discover_repository_root
from .status import service_status
from .storage.catalog import Catalog
from .version import version_string


class _ArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        _write_json({"error": "argument_error", "message": message}, stream=sys.stderr)
        raise SystemExit(2)


def _write_json(payload: object, *, stream: TextIO | None = None) -> None:
    destination = sys.stdout if stream is None else stream
    print(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str),
        file=destination,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = _ArgumentParser(prog="binance-market-recorder")
    parser.add_argument("--version", action="version", version=version_string())
    parser.add_argument("--config", type=Path, help="TOML configuration file")
    commands = parser.add_subparsers(dest="command", required=True, parser_class=_ArgumentParser)

    config_command = commands.add_parser("config", help="inspect configuration")
    config_commands = config_command.add_subparsers(
        dest="config_command",
        required=True,
        parser_class=_ArgumentParser,
    )
    config_commands.add_parser("show", help="show effective credential-free configuration")
    commands.add_parser("doctor", help="run offline platform and path checks")
    commands.add_parser("status", help="show structured runtime and storage status")
    report_command = commands.add_parser("report", help="build operational reports")
    report_commands = report_command.add_subparsers(
        dest="report_command", required=True, parser_class=_ArgumentParser
    )
    daily = report_commands.add_parser("daily", help="write and show a UTC daily report")
    daily.add_argument("--date", help="UTC date in YYYY-MM-DD; defaults to current UTC day")
    return parser


def _config_payload(loaded: LoadedConfig) -> dict[str, object]:
    return {
        "command": "config.show",
        "config": loaded.config.public_dict(),
        "config_file": str(loaded.config_file) if loaded.config_file else None,
        "sources": dict(loaded.sources),
        "contains_credentials": False,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    repository_root = discover_repository_root()
    try:
        loaded = load_config(
            config_file=getattr(args, "config", None),
            repository_root=repository_root,
        )
    except ConfigurationError as exc:
        _write_json(
            {"error": "configuration_error", "message": str(exc)},
            stream=sys.stderr,
        )
        return 2

    command = getattr(args, "command", None)
    if command == "config" and getattr(args, "config_command", None) == "show":
        _write_json(_config_payload(loaded))
        return 0
    if command == "doctor":
        result = run_doctor(loaded, repository_root=repository_root)
        _write_json(result)
        return 1 if result["status"] == "FAIL" else 0
    if command == "status":
        _write_json(service_status(loaded.config.data_root))
        return 0
    if command == "report" and getattr(args, "report_command", None) == "daily":
        selected_date = getattr(args, "date", None) or datetime.now(UTC).date().isoformat()
        catalog_path = loaded.config.data_root / "state" / "catalog.sqlite"
        if not catalog_path.is_file():
            _write_json(
                {
                    "command": "report.daily",
                    "status": "NO_DATA",
                    "utc_date": selected_date,
                    "catalog_path": str(catalog_path),
                }
            )
            return 0
        try:
            with Catalog(catalog_path) as catalog:
                reporter = DailyReporter(
                    catalog=catalog,
                    daily_directory=loaded.config.data_root / "data" / "reports" / "daily",
                )
                document = reporter.write(selected_date)
        except ValueError as exc:
            _write_json(
                {"error": "report_error", "message": str(exc)}, stream=sys.stderr
            )
            return 2
        _write_json({"command": "report.daily", **document})
        return 0
    parser.error(f"unsupported command: {command}")
