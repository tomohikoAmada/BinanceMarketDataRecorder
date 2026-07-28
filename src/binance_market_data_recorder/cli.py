"""Offline local command-line interface."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import time
from collections.abc import Sequence
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, NoReturn, TextIO

from .archive import ArchiveError, ArchiveManager, ArchiveTarget
from .archive.drain import archive_drain
from .backfill import HistoricalImporter, build_plan
from .config import ENV_PREFIX, ConfigurationError, LoadedConfig, load_config
from .diagnostics import run_doctor
from .logging import configure_logging, log_event
from .metrics.report import DailyReporter
from .normalize import NormalizationError, Normalizer, normalization_status
from .paths import discover_repository_root
from .service.archive_timer import ArchiveTimerManager, SystemdArchiveError
from .service.launchd import (
    LaunchAgentError,
    LaunchAgentManager,
    installed_service_label,
)
from .service.lock import ServiceAlreadyRunning
from .service.runtime import run_service
from .service.soak_timer import SoakTimerManager, SystemdSoakError
from .service.systemd import SystemdError, SystemdManager
from .soak.sample import soak_sample
from .status import service_status
from .storage.catalog import Catalog, CatalogStateError, ChunkState
from .storage.forecast import StorageForecaster
from .storage.layout import ensure_storage_layout
from .storage.linux import LinuxVolumeAdapter
from .storage.macos import (
    DiskArbitrationAdapter,
    EjectError,
    PlatformVolumeError,
    SafeEjectCoordinator,
    StorageRegistrationError,
    StorageRegistry,
    inspect_path,
)
from .storage.platform import volume_adapter
from .version import git_commit as current_git_commit
from .version import version_string


class _ArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        _write_json({"error": "argument_error", "message": message}, stream=sys.stderr)
        raise SystemExit(2)


_ORIGINAL_DISK_ARBITRATION_ADAPTER = DiskArbitrationAdapter


def _volume_adapter() -> Any:
    """Keep legacy macOS test injection while selecting Linux in production."""

    if DiskArbitrationAdapter is not _ORIGINAL_DISK_ARBITRATION_ADAPTER:
        return DiskArbitrationAdapter()
    return volume_adapter()


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
    backfill_command = commands.add_parser(
        "backfill", help="plan and import official Binance public archives"
    )
    backfill_commands = backfill_command.add_subparsers(
        dest="backfill_command", required=True, parser_class=_ArgumentParser
    )
    for action in ("plan", "run"):
        command = backfill_commands.add_parser(
            action, help=f"{action} a historical archive import"
        )
        command.add_argument(
            "--profile",
            choices=("baseline-bars", "microstructure-trades"),
            default="baseline-bars",
        )
        command.add_argument("--start", required=True, help="inclusive YYYY-MM-DD")
        command.add_argument("--end", required=True, help="inclusive YYYY-MM-DD")
    backfill_commands.add_parser("status", help="show historical import state")
    backfill_commands.add_parser("verify", help="verify every imported source revision")
    report_command = commands.add_parser("report", help="build operational reports")
    report_commands = report_command.add_subparsers(
        dest="report_command", required=True, parser_class=_ArgumentParser
    )
    daily = report_commands.add_parser("daily", help="write and show a UTC daily report")
    daily.add_argument("--date", help="UTC date in YYYY-MM-DD; defaults to current UTC day")
    normalize_command = commands.add_parser(
        "normalize", help="build or inspect versioned normalized Parquet"
    )
    normalize_commands = normalize_command.add_subparsers(
        dest="normalize_command",
        required=True,
        parser_class=_ArgumentParser,
    )
    normalize_commands.add_parser("run", help="build from every verified Raw chunk")
    normalize_commands.add_parser("status", help="inspect normalized build manifests")
    storage_command = commands.add_parser("storage", help="inspect registered external folders")
    storage_commands = storage_command.add_subparsers(
        dest="storage_command", required=True, parser_class=_ArgumentParser
    )
    storage_commands.add_parser("list", help="list discovered external volumes")
    inspect = storage_commands.add_parser("inspect", help="inspect a folder without writing")
    inspect.add_argument("path", type=Path)
    register = storage_commands.add_parser("register", help="register an existing folder")
    register.add_argument("folder_path", type=Path)
    unregister = storage_commands.add_parser("unregister", help="stop using a registered folder")
    unregister.add_argument("storage_id")
    storage_commands.add_parser("status", help="resolve and probe registered folders")
    eject = storage_commands.add_parser(
        "eject", help="request non-forced system unmount and eject"
    )
    eject.add_argument("storage_id")
    eject.add_argument("--timeout-seconds", type=float, default=30.0)
    storage_commands.add_parser("forecast", help="sample capacity and forecast thresholds")
    archive_command = commands.add_parser("archive", help="manage verified Raw archival")
    archive_commands = archive_command.add_subparsers(
        dest="archive_command", required=True, parser_class=_ArgumentParser
    )
    archive_commands.add_parser("status", help="show archive transactions and backlog")
    retry = archive_commands.add_parser("retry", help="advance one archive transaction")
    retry.add_argument("--storage-id")
    verify = archive_commands.add_parser("verify", help="verify committed external files")
    verify.add_argument("storage_id")
    drain = archive_commands.add_parser("drain", help="bounded drain loop")
    drain.add_argument("--storage-id", required=True)
    drain.add_argument("--max-runtime-seconds", type=float, default=50.0)
    drain.add_argument("--max-files", type=int, default=1000)
    archive_timer = archive_commands.add_parser(
        "timer", help="manage the archive companion systemd timer"
    )
    archive_timer_commands = archive_timer.add_subparsers(
        dest="archive_timer_command", required=True, parser_class=_ArgumentParser
    )
    archive_timer_install = archive_timer_commands.add_parser(
        "install", help="install and enable the archive timer"
    )
    archive_timer_install.add_argument("--user", required=True)
    archive_timer_install.add_argument("--group", required=True)
    archive_timer_install.add_argument("--storage-id", required=True)
    archive_timer_install.add_argument("--interval-seconds", type=int, default=60)
    archive_timer_install.add_argument("--max-runtime-seconds", type=int, default=50)
    archive_timer_install.add_argument("--max-files", type=int, default=1000)
    for action in ("start", "stop", "restart", "status", "uninstall"):
        archive_timer_commands.add_parser(action, help=f"{action} the archive timer")
    soak_command = commands.add_parser("soak", help="M21 soak sampling operations")
    soak_commands = soak_command.add_subparsers(
        dest="soak_command", required=True, parser_class=_ArgumentParser
    )
    soak_sample_cmd = soak_commands.add_parser(
        "sample", help="capture a single time-point observation"
    )
    soak_sample_cmd.add_argument("--output", type=Path, required=True)
    soak_timer = soak_commands.add_parser(
        "timer", help="manage the soak sampling systemd timer"
    )
    soak_timer_commands = soak_timer.add_subparsers(
        dest="soak_timer_command", required=True, parser_class=_ArgumentParser
    )
    soak_timer_install = soak_timer_commands.add_parser(
        "install", help="install and enable the soak timer"
    )
    soak_timer_install.add_argument("--user", required=True)
    soak_timer_install.add_argument("--group", required=True)
    soak_timer_install.add_argument("--interval-seconds", type=int, default=300)
    soak_timer_install.add_argument("--output", type=Path, required=True)
    for action in ("start", "stop", "restart", "status", "uninstall"):
        soak_timer_commands.add_parser(action, help=f"{action} the soak timer")
    launchd_command = commands.add_parser(
        "launchd", help="manage the user LaunchAgent"
    )
    launchd_commands = launchd_command.add_subparsers(
        dest="launchd_command", required=True, parser_class=_ArgumentParser
    )
    install = launchd_commands.add_parser(
        "install", help="install and bootstrap the user LaunchAgent"
    )
    install.add_argument("--label", required=True)
    install.add_argument(
        "--author-controls-namespace",
        action="store_true",
        help="confirm the reverse-DNS label namespace is author-controlled",
    )
    for action in ("uninstall", "start", "stop", "status"):
        command = launchd_commands.add_parser(
            action, help=f"{action} the user LaunchAgent"
        )
        command.add_argument("--label")
    systemd_command = commands.add_parser(
        "systemd", help="manage the Linux system service"
    )
    systemd_commands = systemd_command.add_subparsers(
        dest="systemd_command", required=True, parser_class=_ArgumentParser
    )
    systemd_install = systemd_commands.add_parser(
        "install", help="install and enable the Linux system service"
    )
    systemd_install.add_argument("--user", required=True)
    systemd_install.add_argument("--group", required=True)
    for action in ("uninstall", "start", "stop", "restart", "status"):
        systemd_commands.add_parser(action, help=f"{action} the Linux system service")
    private_service = commands.add_parser("_service", help=argparse.SUPPRESS)
    private_commands = private_service.add_subparsers(
        dest="service_command", required=True, parser_class=_ArgumentParser
    )
    private_commands.add_parser("run", help=argparse.SUPPRESS)
    return parser


def _config_payload(loaded: LoadedConfig) -> dict[str, object]:
    return {
        "command": "config.show",
        "config": loaded.config.public_dict(),
        "config_file": str(loaded.config_file) if loaded.config_file else None,
        "sources": dict(loaded.sources),
        "contains_credentials": False,
    }


def _archive_status(catalog: Catalog) -> dict[str, object]:
    transactions = catalog.archive_transactions()
    states: dict[str, int] = {}
    for transaction in transactions:
        state = str(transaction["state"])
        states[state] = states.get(state, 0) + 1
    backlog = catalog.chunks_in_states(
        ChunkState.SEALED,
        ChunkState.ARCHIVE_COPYING,
        ChunkState.ARCHIVE_VERIFYING,
        ChunkState.ARCHIVED_VERIFIED,
        ChunkState.LOCAL_DELETE_PENDING,
    )
    return {
        "command": "archive.status",
        "status": (
            "DISAPPEARED_DURING_COPY"
            if any(
                "DISAPPEARED_DURING_COPY" in str(row.get("last_error"))
                for row in transactions
            )
            else "OK"
        ),
        "transaction_count": len(transactions),
        "transactions_by_state": dict(sorted(states.items())),
        "backlog_files": len(backlog),
        "backlog_bytes": sum(
            value
            for row in backlog
            if isinstance((value := row.get("stored_bytes")), int)
            and not isinstance(value, bool)
        ),
        "transactions": transactions,
        "unique_copy_warning": (
            "After LOCAL_DELETED, the registered external artifact may be the only copy."
        ),
    }


def _launchd_environment(loaded: LoadedConfig) -> dict[str, str]:
    config = loaded.config
    return {
        f"{ENV_PREFIX}DATA_ROOT": str(config.data_root),
        f"{ENV_PREFIX}LOG_LEVEL": config.log_level,
        f"{ENV_PREFIX}ROTATION_SECONDS": str(config.rotation_seconds),
        f"{ENV_PREFIX}ROTATION_BYTES": str(config.rotation_bytes),
        f"{ENV_PREFIX}DURABILITY_INTERVAL_SECONDS": str(
            config.durability_interval_seconds
        ),
        f"{ENV_PREFIX}INGRESS_QUEUE_CAPACITY": str(
            config.ingress_queue_capacity
        ),
        f"{ENV_PREFIX}MAX_FRAME_BYTES": str(config.max_frame_bytes),
        f"{ENV_PREFIX}HEARTBEAT_SECONDS": str(config.heartbeat_seconds),
        f"{ENV_PREFIX}SLEEP_GAP_THRESHOLD_SECONDS": str(
            config.sleep_gap_threshold_seconds
        ),
        f"{ENV_PREFIX}PREVENT_SLEEP": (
            "true" if config.prevent_sleep else "false"
        ),
    }


def _select_archive_target(
    catalog: Catalog,
    registry: StorageRegistry,
    *,
    storage_id: str | None,
    allow_low_space: bool = False,
) -> ArchiveTarget:
    statuses = registry.statuses()
    allowed_states = {"READY", *(["LOW_SPACE"] if allow_low_space else [])}
    ready = [
        status
        for status in statuses
        if status["state"] in allowed_states
        and (storage_id is None or status["storage_id"] == storage_id)
    ]
    if not ready:
        identity = storage_id or "any registered target"
        raise ArchiveError(f"no READY archive target for {identity}")
    if storage_id is None and len(ready) != 1:
        raise ArchiveError("multiple READY targets; specify --storage-id")
    status = ready[0]
    target_rows = {
        str(row["storage_id"]): row for row in catalog.storage_targets()
    }
    target_id = str(status["storage_id"])
    row = target_rows[target_id]
    resolved_path = status.get("resolved_path")
    if not isinstance(resolved_path, str):
        raise ArchiveError("READY target lacks a resolved path")
    return ArchiveTarget(
        storage_id=target_id,
        volume_uuid=str(row["volume_uuid"]),
        registered_relative_path=str(row["relative_path"]),
        marker_nonce=str(row["marker_nonce"]),
        root=Path(resolved_path),
    )


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
        _write_json(
            service_status(
                loaded.config.data_root,
                configured_proxy_status=(
                    loaded.config.proxy_policy().status().public_dict()
                ),
            )
        )
        return 0
    if command == "backfill":
        action = getattr(args, "backfill_command", None)
        importer = HistoricalImporter(
            data_root=loaded.config.data_root,
            proxy_policy=loaded.config.proxy_policy(),
        )
        try:
            if action in {"plan", "run"}:
                plan = build_plan(
                    str(args.profile),
                    datetime.strptime(str(args.start), "%Y-%m-%d").date(),
                    datetime.strptime(str(args.end), "%Y-%m-%d").date(),
                )
                result = (
                    plan.public_dict() if action == "plan" else importer.run(plan)
                )
            elif action == "status":
                result = importer.status()
            elif action == "verify":
                result = importer.verify()
            else:
                parser.error(f"unsupported backfill command: {action}")
        except (OSError, RuntimeError, ValueError) as exc:
            _write_json(
                {"error": "backfill_error", "message": str(exc)},
                stream=sys.stderr,
            )
            return 2
        _write_json({"command": f"backfill.{action}", **result})
        return 0
    if command == "_service" and getattr(args, "service_command", None) == "run":
        logger = configure_logging(loaded.config.log_level)
        try:
            asyncio.run(run_service(loaded.config, logger=logger))
        except ServiceAlreadyRunning as exc:
            log_event(
                logger,
                logging.WARNING,
                "service_already_running",
                "another Recorder service owns the data root",
                reason=str(exc),
            )
            return 0
        except Exception:
            logger.exception(
                "supervised Recorder service failed",
                extra={"structured_event": "service_failed"},
            )
            return 1
        return 0
    if command == "systemd":
        systemd_command = getattr(args, "systemd_command", None)
        if loaded.config_file is None:
            _write_json(
                {
                    "error": "systemd_error",
                    "message": "systemd management requires an explicit --config file",
                },
                stream=sys.stderr,
            )
            return 2
        systemd_manager = SystemdManager(
            data_root=loaded.config.data_root,
            config_file=loaded.config_file,
            user=str(getattr(args, "user", "")),
            group=str(getattr(args, "group", "")),
            git_commit=current_git_commit(),
        )
        try:
            if systemd_command == "install":
                result = systemd_manager.install()
            elif systemd_command == "uninstall":
                result = systemd_manager.uninstall()
            elif systemd_command == "start":
                result = systemd_manager.start()
            elif systemd_command == "stop":
                result = systemd_manager.stop()
            elif systemd_command == "restart":
                result = systemd_manager.restart()
            elif systemd_command == "status":
                result = systemd_manager.status()
            else:
                parser.error(f"unsupported systemd command: {systemd_command}")
            _write_json({"command": f"systemd.{systemd_command}", **result})
            return 0
        except (OSError, SystemdError, ValueError) as exc:
            _write_json(
                {"error": "systemd_error", "message": str(exc)},
                stream=sys.stderr,
            )
            return 2
    if command == "launchd":
        launchd_command = getattr(args, "launchd_command", None)
        try:
            label = getattr(args, "label", None)
            if launchd_command != "install" and label is None:
                label = installed_service_label(loaded.config.data_root)
            if label is None:
                raise LaunchAgentError(
                    "no installed LaunchAgent metadata; specify --label"
                )
            launchd_manager = LaunchAgentManager(
                data_root=loaded.config.data_root,
                label=label,
            )
            if launchd_command == "install":
                result = launchd_manager.install(
                    author_controls_namespace=bool(
                        getattr(args, "author_controls_namespace", False)
                    ),
                    config_file=loaded.config_file,
                    git_commit=current_git_commit(),
                    environment=_launchd_environment(loaded),
                )
            elif launchd_command == "uninstall":
                result = launchd_manager.uninstall()
            elif launchd_command == "start":
                result = launchd_manager.start()
            elif launchd_command == "stop":
                result = launchd_manager.stop()
            elif launchd_command == "status":
                result = launchd_manager.status()
            else:
                parser.error(f"unsupported launchd command: {launchd_command}")
            _write_json({"command": f"launchd.{launchd_command}", **result})
            return 0
        except (LaunchAgentError, OSError, ValueError) as exc:
            _write_json(
                {"error": "launchd_error", "message": str(exc)},
                stream=sys.stderr,
            )
            return 2
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
    if command == "normalize":
        normalize_command = getattr(args, "normalize_command", None)
        if normalize_command == "status":
            try:
                result = normalization_status(loaded.config.data_root)
            except NormalizationError as exc:
                _write_json(
                    {"error": "normalization_error", "message": str(exc)},
                    stream=sys.stderr,
                )
                return 2
            _write_json({"command": "normalize.status", **result})
            return 0
        layout = ensure_storage_layout(loaded.config.data_root)
        try:
            with Catalog(layout.catalog) as catalog:
                external_roots: dict[str, Path] = {}
                try:
                    statuses = StorageRegistry(
                        catalog=catalog,
                        volumes=_volume_adapter(),
                    ).statuses()
                except (OSError, PlatformVolumeError):
                    statuses = []
                for status in statuses:
                    resolved = status.get("resolved_path")
                    storage_id = status.get("storage_id")
                    if (
                        status.get("state") in {"READY", "LOW_SPACE"}
                        and isinstance(resolved, str)
                        and isinstance(storage_id, str)
                    ):
                        external_roots[storage_id] = Path(resolved)
                normalization_result = Normalizer(
                    layout=layout,
                    catalog=catalog,
                    external_roots=external_roots,
                ).run()
        except (NormalizationError, OSError, ValueError) as exc:
            _write_json(
                {"error": "normalization_error", "message": str(exc)},
                stream=sys.stderr,
            )
            return 2
        _write_json(
            {"command": "normalize.run", **normalization_result.public_dict()}
        )
        return 0
    if command == "storage":
        adapter = _volume_adapter()
        storage_command = getattr(args, "storage_command", None)
        try:
            if storage_command == "list":
                volumes = [volume.public_dict() for volume in adapter.inventory()]
                _write_json(
                    {
                        "command": "storage.list",
                        "status": "OK",
                        "volumes": volumes,
                        "external_volume_count": len(volumes),
                        "filesystem_mutated": False,
                    }
                )
                return 0
            if storage_command == "inspect":
                _write_json(
                    {
                        "command": "storage.inspect",
                        **inspect_path(args.path, adapter.inventory()),
                    }
                )
                return 0
            catalog_path = loaded.config.data_root / "state" / "catalog.sqlite"
            if storage_command == "status" and not catalog_path.is_file():
                _write_json(
                    {
                        "command": "storage.status",
                        "status": "NO_REGISTERED_TARGETS",
                        "targets": [],
                    }
                )
                return 0
            with Catalog(catalog_path) as catalog:
                registry = StorageRegistry(catalog=catalog, volumes=adapter)
                if storage_command == "register":
                    result = registry.register(args.folder_path)
                    _write_json({"command": "storage.register", **result})
                    return 0
                if storage_command == "unregister":
                    result = registry.unregister(args.storage_id)
                    _write_json({"command": "storage.unregister", **result})
                    return 0
                if storage_command == "status":
                    targets = registry.statuses()
                    _write_json(
                        {
                            "command": "storage.status",
                            "status": "OK",
                            "targets": targets,
                        }
                    )
                    return 0
                if storage_command == "eject":
                    if isinstance(adapter, LinuxVolumeAdapter):
                        _write_json(
                            {
                                "command": "storage.eject",
                                "storage_id": args.storage_id,
                                "status": "MANUAL_ACTION_REQUIRED",
                                "safe_to_remove": False,
                                "forced": False,
                                "message": (
                                    "Automatic Linux unmount/eject is unavailable; "
                                    "no safe-removal success is claimed."
                                ),
                            }
                        )
                        return 1
                    eject_result = SafeEjectCoordinator(
                        catalog=catalog,
                        platform=adapter,
                    ).eject(
                        args.storage_id,
                        timeout_seconds=args.timeout_seconds,
                    )
                    _write_json(
                        {"command": "storage.eject", **eject_result.public_dict()}
                    )
                    return 0 if eject_result.safe_to_remove else 1
                if storage_command == "forecast":
                    observed_at = time.time_ns()
                    observed_at -= observed_at % 60_000_000_000
                    forecaster = StorageForecaster(
                        catalog=catalog,
                        data_root=loaded.config.data_root,
                        utc_clock_ns=lambda: observed_at,
                    )
                    forecaster.observe_internal(
                        observed_at_utc_ns=observed_at
                    )
                    target_statuses = registry.statuses()
                    scope_ids = ["internal"]
                    for target_status in target_statuses:
                        storage_id = str(target_status["storage_id"])
                        scope_id = f"external:{storage_id}"
                        scope_ids.append(scope_id)
                        total = target_status.get("total_bytes")
                        free = target_status.get("free_bytes")
                        if (
                            isinstance(total, int)
                            and not isinstance(total, bool)
                            and isinstance(free, int)
                            and not isinstance(free, bool)
                        ):
                            forecaster.observe(
                                scope_id=scope_id,
                                storage_id=storage_id,
                                total_bytes=total,
                                free_bytes=free,
                                observed_at_utc_ns=observed_at,
                            )
                    document = forecaster.document(
                        scope_ids, now_utc_ns=observed_at
                    )
                    _write_json(
                        {
                            "command": "storage.forecast",
                            **document,
                            "storage_states": target_statuses,
                            "alerts": catalog.storage_alert_events(),
                        }
                    )
                    return 0
        except (
            CatalogStateError,
            EjectError,
            OSError,
            PlatformVolumeError,
            StorageRegistrationError,
            ValueError,
        ) as exc:
            _write_json(
                {"error": "storage_error", "message": str(exc)}, stream=sys.stderr
            )
            return 2
    if command == "archive":
        archive_command = getattr(args, "archive_command", None)
        if archive_command == "timer":
            timer_command = getattr(args, "archive_timer_command", None)
            if loaded.config_file is None:
                _write_json(
                    {"error": "archive_timer_error",
                     "message": "archive timer requires an explicit --config file"},
                    stream=sys.stderr,
                )
                return 2
            timer_manager = ArchiveTimerManager(
                config_file=loaded.config_file,
                user=str(getattr(args, "user", "")),
                group=str(getattr(args, "group", "")),
                storage_id=str(getattr(args, "storage_id", "")),
                interval_seconds=int(getattr(args, "interval_seconds", 60)),
                max_runtime_seconds=int(getattr(args, "max_runtime_seconds", 50)),
                max_files=int(getattr(args, "max_files", 1000)),
            )
            try:
                if timer_command == "install":
                    result = timer_manager.install()
                elif timer_command == "start":
                    result = timer_manager.start()
                elif timer_command == "stop":
                    result = timer_manager.stop()
                elif timer_command == "restart":
                    result = timer_manager.restart()
                elif timer_command == "status":
                    result = timer_manager.status()
                elif timer_command == "uninstall":
                    result = timer_manager.uninstall()
                else:
                    parser.error(f"unsupported archive timer command: {timer_command}")
                _write_json({"command": f"archive.timer.{timer_command}", **result})
                return 0
            except (OSError, SystemdArchiveError, ValueError) as exc:
                _write_json(
                    {"error": "archive_timer_error", "message": str(exc)},
                    stream=sys.stderr,
                )
                return 2
        catalog_path = loaded.config.data_root / "state" / "catalog.sqlite"
        if not catalog_path.is_file():
            _write_json(
                {
                    "command": f"archive.{archive_command}",
                    "status": "NO_DATA",
                    "catalog_path": str(catalog_path),
                }
            )
            return 0
        try:
            with Catalog(catalog_path) as catalog:
                if archive_command == "status":
                    _write_json(_archive_status(catalog))
                    return 0
                if archive_command == "drain":
                    drain_result = archive_drain(
                        layout=ensure_storage_layout(loaded.config.data_root),
                        catalog=catalog,
                        storage_id=args.storage_id,
                        max_runtime_seconds=args.max_runtime_seconds,
                        max_files=args.max_files,
                    )
                    _write_json(drain_result)
                    return (
                        0
                        if drain_result.get("exit_reason")
                        in ("BACKLOG_EMPTY", "MAX_FILES", "DEADLINE",
                            "ALREADY_RUNNING", "INTERRUPTED",
                            "TARGET_ABSENT", "TARGET_NOT_READY")
                        else 1
                    )
                registry = StorageRegistry(
                    catalog=catalog, volumes=_volume_adapter()
                )
                requested = (
                    args.storage_id
                    if archive_command in {"retry", "verify"}
                    else None
                )
                target = _select_archive_target(
                    catalog,
                    registry,
                    storage_id=requested,
                    allow_low_space=archive_command == "verify",
                )
                manager = ArchiveManager(
                    layout=ensure_storage_layout(loaded.config.data_root),
                    catalog=catalog,
                    target=target,
                )
                if archive_command == "retry":
                    archive_result = manager.run_once()
                    _write_json(
                        {
                            "command": "archive.retry",
                            **asdict(archive_result),
                        }
                    )
                    return 0
                if archive_command == "verify":
                    verify_result = manager.verify_all()
                    _write_json({"command": "archive.verify", **verify_result})
                    return 1 if verify_result["status"] == "FAILED" else 0
        except (
            ArchiveError,
            CatalogStateError,
            OSError,
            PlatformVolumeError,
            StorageRegistrationError,
            ValueError,
        ) as exc:
            _write_json(
                {"error": "archive_error", "message": str(exc)}, stream=sys.stderr
            )
            return 2
    if command == "soak":
        soak_command = getattr(args, "soak_command", None)
        if soak_command == "timer":
            timer_command = getattr(args, "soak_timer_command", None)
            if loaded.config_file is None:
                _write_json(
                    {"error": "soak_timer_error",
                     "message": "soak timer requires an explicit --config file"},
                    stream=sys.stderr,
                )
                return 2
            soak_timer_manager = SoakTimerManager(
                config_file=loaded.config_file,
                user=str(getattr(args, "user", "")),
                group=str(getattr(args, "group", "")),
                interval_seconds=int(getattr(args, "interval_seconds", 300)),
                output_path=Path(str(getattr(args, "output", ""))).resolve()
                if getattr(args, "output", None) else Path(
                    "/var/lib/binance-market-data-recorder/operations/soak/samples.jsonl"
                ),
            )
            try:
                if timer_command == "install":
                    result = soak_timer_manager.install()
                elif timer_command == "start":
                    result = soak_timer_manager.start()
                elif timer_command == "stop":
                    result = soak_timer_manager.stop()
                elif timer_command == "restart":
                    result = soak_timer_manager.restart()
                elif timer_command == "status":
                    result = soak_timer_manager.status()
                elif timer_command == "uninstall":
                    result = soak_timer_manager.uninstall()
                else:
                    parser.error(f"unsupported soak timer command: {timer_command}")
                _write_json({"command": f"soak.timer.{timer_command}", **result})
                return 0
            except (OSError, SystemdSoakError, ValueError) as exc:
                _write_json(
                    {"error": "soak_timer_error", "message": str(exc)},
                    stream=sys.stderr,
                )
                return 2
        if soak_command == "sample":
            try:
                sample_result = soak_sample(
                    data_root=loaded.config.data_root,
                    output_path=args.output,
                    config_dict=loaded.config.public_dict(),
                    recorder_version=version_string(),
                )
                _write_json({"command": "soak.sample", **sample_result})
                return 0
            except (OSError, ValueError) as exc:
                _write_json(
                    {"error": "soak_sample_error", "message": str(exc)},
                    stream=sys.stderr,
                )
                return 2
    parser.error(f"unsupported command: {command}")
