"""Offline local command-line interface."""

from __future__ import annotations

import argparse
import asyncio
import grp
import json
import logging
import os
import sys
import time
import uuid
from collections.abc import Sequence
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, NoReturn, TextIO

from .archive import ArchiveError, ArchiveManager, ArchiveTarget
from .archive.catalog_snapshot import CatalogSnapshotExporter
from .archive.drain import archive_drain
from .archive.remote_authorization import RemoteAuthorizer
from .archive.remote_delete import RemoteDeleter
from .archive.remote_source import RemoteSourceExporter
from .archive.remote_transport import (
    authority_status_from_catalog,
    require_chunk_id,
    require_receipt_id,
    require_sha256,
)
from .backfill import HistoricalImporter, build_plan
from .config import ENV_PREFIX, ConfigurationError, LoadedConfig, load_config
from .diagnostics import run_doctor
from .logging import configure_logging, log_event
from .metrics.report import DailyReporter
from .normalize import NormalizationError, Normalizer, normalization_status
from .paths import discover_repository_root
from .service.acceptance import (
    STAGE_DURATION_NS,
    AcceptanceError,
    AcceptanceObserver,
    create_identity_evidence,
    create_readiness_evidence,
    resume_observer,
    verify_prior_stage,
)
from .service.archive_timer import ArchiveTimerManager, SystemdArchiveError
from .service.deployment_identity import (
    DeploymentIdentityError,
    create_deployment_identity,
    deployment_identity_path,
    enforce_vps_paths,
    load_deployment_identity,
    rollback_compatibility,
    runtime_deployment_identity,
    verify_identity_files,
    verify_retained_rollback_artifacts,
    verify_vps_identity_permissions,
    write_deployment_identity,
)
from .service.launchd import (
    LaunchAgentError,
    LaunchAgentManager,
    installed_service_label,
)
from .service.lock import ServiceAlreadyRunning
from .service.readiness import VpsReadinessEvaluator, wait_for_readiness
from .service.runtime import run_service
from .service.soak_timer import SoakTimerManager, SystemdSoakError
from .service.systemd import SystemdError, SystemdManager
from .soak.sample import soak_sample
from .spool.legacy_reconnect import (
    LegacyClassificationAuthority,
    LegacyReconnectConflictError,
    classification_authority_path,
    evaluate_legacy_reconnect_decisions,
)
from .status import service_status
from .storage.capacity import selected_capacity_profile
from .storage.catalog import Catalog, CatalogStateError, RemoteArchiveState
from .storage.forecast import StorageForecaster
from .storage.layout import StorageLayout, ensure_storage_layout
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
        command = backfill_commands.add_parser(action, help=f"{action} a historical archive import")
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
    recovery_command = commands.add_parser("recovery", help="startup recovery diagnostics")
    recovery_commands = recovery_command.add_subparsers(
        dest="recovery_command",
        required=True,
        parser_class=_ArgumentParser,
    )
    recovery_commands.add_parser(
        "legacy-reconnect-preflight",
        help="read-only legacy reconnect classification inventory",
    )
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
    eject = storage_commands.add_parser("eject", help="request non-forced system unmount and eject")
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
    soak_sample_cmd.add_argument("--storage-id", required=True)
    soak_sample_cmd.add_argument("--output", type=Path, required=True)
    soak_timer = soak_commands.add_parser("timer", help="manage the soak sampling systemd timer")
    soak_timer_commands = soak_timer.add_subparsers(
        dest="soak_timer_command", required=True, parser_class=_ArgumentParser
    )
    soak_timer_install = soak_timer_commands.add_parser(
        "install", help="install and enable the soak timer"
    )
    soak_timer_install.add_argument("--user", required=True)
    soak_timer_install.add_argument("--group", required=True)
    soak_timer_install.add_argument("--storage-id", required=True)
    soak_timer_install.add_argument("--interval-seconds", type=int, default=300)
    soak_timer_install.add_argument("--output", type=Path, required=True)
    for action in ("start", "stop", "restart", "status", "uninstall"):
        soak_timer_commands.add_parser(action, help=f"{action} the soak timer")
    launchd_command = commands.add_parser("launchd", help="manage the user LaunchAgent")
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
        command = launchd_commands.add_parser(action, help=f"{action} the user LaunchAgent")
        command.add_argument("--label")
    systemd_command = commands.add_parser("systemd", help="manage the Linux system service")
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
    deployment_command = commands.add_parser(
        "deployment", help="manage exact VPS deployment evidence"
    )
    deployment_commands = deployment_command.add_subparsers(
        dest="deployment_command", required=True, parser_class=_ArgumentParser
    )
    identity_create = deployment_commands.add_parser(
        "identity-create", help="create root-controlled exact deployment identity"
    )
    identity_create.add_argument("--source-git-sha", required=True)
    identity_create.add_argument("--wheel", type=Path, required=True)
    identity_create.add_argument("--dependency-lock", type=Path, required=True)
    deployment_commands.add_parser(
        "verify", help="verify installed files and effective systemd authority"
    )
    deployment_commands.add_parser(
        "readiness", help="wait up to 300 seconds for exact deployment readiness"
    )
    acceptance = deployment_commands.add_parser(
        "acceptance", help="produce read-only M22.9 acceptance evidence"
    )
    acceptance_commands = acceptance.add_subparsers(
        dest="acceptance_command", required=True, parser_class=_ArgumentParser
    )
    identity_acceptance = acceptance_commands.add_parser(
        "identity", help="verify and publish exact artifact identity"
    )
    identity_acceptance.add_argument("--expected-source-git-sha", required=True)
    identity_acceptance.add_argument("--evidence-root", type=Path, required=True)
    readiness_acceptance = acceptance_commands.add_parser(
        "readiness", help="verify and publish exact deployment readiness"
    )
    readiness_acceptance.add_argument("--identity-evidence", type=Path, required=True)
    readiness_acceptance.add_argument("--evidence-root", type=Path, required=True)
    stage_acceptance = acceptance_commands.add_parser(
        "stage", help="observe one independent duration stage"
    )
    stage_mode = stage_acceptance.add_mutually_exclusive_group(required=True)
    stage_mode.add_argument("--stage", choices=("2h", "12h", "24h", "72h", "168h"))
    stage_mode.add_argument("--resume", type=Path)
    stage_acceptance.add_argument("--previous-evidence", type=Path)
    stage_acceptance.add_argument("--evidence-root", type=Path)
    rollback = deployment_commands.add_parser(
        "rollback-check", help="fail closed unless a preserved target understands Catalog state"
    )
    rollback.add_argument("--target-identity", type=Path, required=True)
    private_service = commands.add_parser("_service", help=argparse.SUPPRESS)
    private_commands = private_service.add_subparsers(
        dest="service_command", required=True, parser_class=_ArgumentParser
    )
    private_commands.add_parser("run", help=argparse.SUPPRESS)
    private_remote = commands.add_parser("_remote", help=argparse.SUPPRESS)
    remote_commands = private_remote.add_subparsers(
        dest="remote_command", required=True, parser_class=_ArgumentParser
    )
    remote_commands.add_parser("select-oldest", help=argparse.SUPPRESS)
    for action in ("manifest", "raw", "authorize"):
        remote = remote_commands.add_parser(action, help=argparse.SUPPRESS)
        remote.add_argument("chunk_id")
        remote.add_argument("descriptor_sha256")
    for action in ("authority", "delete"):
        remote = remote_commands.add_parser(action, help=argparse.SUPPRESS)
        remote.add_argument("receipt_id")
    snapshot = remote_commands.add_parser("catalog-snapshot", help=argparse.SUPPRESS)
    snapshot.add_argument("receipt_id")
    snapshot.add_argument(
        "required_state",
        choices=(
            RemoteArchiveState.REMOTE_DELETE_PENDING.value,
            RemoteArchiveState.REMOTE_DELETED.value,
        ),
    )
    return parser


def _config_payload(loaded: LoadedConfig) -> dict[str, object]:
    return {
        "command": "config.show",
        "config": loaded.config.public_dict(),
        "config_file": str(loaded.config_file) if loaded.config_file else None,
        "sources": dict(loaded.sources),
        "contains_credentials": False,
    }


def _identity_systemd_manager(
    loaded: LoadedConfig,
    *,
    identity_user: object,
    identity_group: object,
) -> SystemdManager:
    if loaded.config_file is None:
        raise DeploymentIdentityError("VPS deployment requires an explicit config file")
    if not isinstance(identity_user, str) or not isinstance(identity_group, str):
        raise DeploymentIdentityError("deployment service principal is malformed")
    return SystemdManager(
        data_root=loaded.config.data_root,
        config_file=loaded.config_file,
        user=identity_user,
        group=identity_group,
        python_executable=Path(sys.executable),
        capacity_profile_id=loaded.config.capacity_profile,
    )


def _archive_status(catalog: Catalog) -> dict[str, object]:
    transactions = catalog.archive_transactions()
    lifecycle = catalog.source_lifecycle_aggregate()
    states: dict[str, int] = {}
    for transaction in transactions:
        state = str(transaction["state"])
        states[state] = states.get(state, 0) + 1
    return {
        "command": "archive.status",
        "status": (
            "DISAPPEARED_DURING_COPY"
            if any("DISAPPEARED_DURING_COPY" in str(row.get("last_error")) for row in transactions)
            else "OK"
        ),
        "transaction_count": len(transactions),
        "transactions_by_state": dict(sorted(states.items())),
        "backlog_files": lifecycle["unarchived_backlog_files"],
        "backlog_bytes": lifecycle["unarchived_backlog_bytes"],
        "ordinary_sealed_files": lifecycle["ordinary_sealed_files"],
        "remote_pending_files": lifecycle["remote_pending_files"],
        "remote_pending_source_bytes": lifecycle["remote_pending_source_bytes"],
        "remote_deleted_files": lifecycle["remote_deleted_files"],
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
        f"{ENV_PREFIX}DURABILITY_INTERVAL_SECONDS": str(config.durability_interval_seconds),
        f"{ENV_PREFIX}INGRESS_QUEUE_CAPACITY": str(config.ingress_queue_capacity),
        f"{ENV_PREFIX}MAX_FRAME_BYTES": str(config.max_frame_bytes),
        f"{ENV_PREFIX}HEARTBEAT_SECONDS": str(config.heartbeat_seconds),
        f"{ENV_PREFIX}SLEEP_GAP_THRESHOLD_SECONDS": str(config.sleep_gap_threshold_seconds),
        f"{ENV_PREFIX}PREVENT_SLEEP": ("true" if config.prevent_sleep else "false"),
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
    target_rows = {str(row["storage_id"]): row for row in catalog.storage_targets()}
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


def _run_remote_command(args: argparse.Namespace, loaded: LoadedConfig) -> int:
    """Run one fixed transport verb with payload-only stdout."""

    action = str(args.remote_command)
    layout = StorageLayout.from_root(loaded.config.data_root)
    try:
        if not layout.catalog.is_file():
            raise CatalogStateError("remote Catalog does not exist")
        if action == "catalog-snapshot":
            receipt_id = str(args.receipt_id)
            require_receipt_id(receipt_id)
            required_state = RemoteArchiveState(str(args.required_state))
            with CatalogSnapshotExporter(layout=layout).open_catalog_snapshot(
                receipt_id, required_state
            ) as snapshot:
                while block := snapshot.read(1024 * 1024):
                    sys.stdout.buffer.write(block)
                sys.stdout.buffer.flush()
            return 0
        if action in {"select-oldest", "manifest", "raw", "authority"}:
            with Catalog(layout.catalog, read_only=True) as catalog:
                if action == "select-oldest":
                    selection = RemoteSourceExporter(layout=layout, catalog=catalog).select_oldest()
                    if selection is not None:
                        sys.stdout.buffer.write(selection.descriptor_bytes)
                        sys.stdout.buffer.flush()
                    return 0
                if action == "authority":
                    receipt_id = str(args.receipt_id)
                    require_receipt_id(receipt_id)
                    status = authority_status_from_catalog(catalog, receipt_id)
                    sys.stdout.buffer.write(
                        b"null\n" if status is None else status.canonical_bytes()
                    )
                    sys.stdout.buffer.flush()
                    return 0
                chunk_id = str(args.chunk_id)
                expected_digest = str(args.descriptor_sha256)
                require_chunk_id(chunk_id)
                require_sha256(expected_digest, "descriptor_sha256")
                selection = RemoteSourceExporter(layout=layout, catalog=catalog).select_chunk(
                    chunk_id
                )
                if selection.descriptor_sha256 != expected_digest:
                    raise ValueError("selected source descriptor identity changed")
                if action == "manifest":
                    sys.stdout.buffer.write(selection.manifest_bytes)
                    sys.stdout.buffer.flush()
                    return 0
                with selection.sealed_path.open("rb", buffering=0) as source:
                    while block := source.read(1024 * 1024):
                        sys.stdout.buffer.write(block)
                    sys.stdout.buffer.flush()
                return 0
        if action == "authorize":
            chunk_id = str(args.chunk_id)
            expected_digest = str(args.descriptor_sha256)
            require_chunk_id(chunk_id)
            require_sha256(expected_digest, "descriptor_sha256")
            receipt_bytes = sys.stdin.buffer.read()
            with Catalog(layout.catalog) as catalog:
                selection = RemoteSourceExporter(layout=layout, catalog=catalog).select_chunk(
                    chunk_id
                )
                if selection.descriptor_sha256 != expected_digest:
                    raise ValueError("selected source descriptor identity changed")
                authorization = RemoteAuthorizer(layout=layout, catalog=catalog).authorize(
                    receipt_bytes, selection
                )
                status = authority_status_from_catalog(catalog, authorization.receipt_id)
                if status is None:
                    raise CatalogStateError("authorization readback is absent")
                sys.stdout.buffer.write(status.canonical_bytes())
                sys.stdout.buffer.flush()
            return 0
        if action == "delete":
            receipt_id = str(args.receipt_id)
            require_receipt_id(receipt_id)
            with Catalog(layout.catalog) as catalog:
                deletion = RemoteDeleter(layout=layout, catalog=catalog).delete_authorized(
                    receipt_id
                )
                status = authority_status_from_catalog(catalog, deletion.receipt_id)
                if status is None:
                    raise CatalogStateError("delete readback is absent")
                sys.stdout.buffer.write(status.canonical_bytes())
                sys.stdout.buffer.flush()
            return 0
        raise ValueError(f"unsupported fixed remote command: {action}")
    except Exception as exc:
        _write_json(
            {"error": "remote_transport_error", "message": str(exc)},
            stream=sys.stderr,
        )
        return 2


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
    if command == "_remote":
        return _run_remote_command(args, loaded)
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
                configured_proxy_status=(loaded.config.proxy_policy().status().public_dict()),
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
                result = plan.public_dict() if action == "plan" else importer.run(plan)
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
            runtime_identity = None
            if loaded.config.capacity_profile == "vps-production-v1":
                if loaded.config_file is None:
                    raise DeploymentIdentityError(
                        "vps-production-v1 requires an explicit config file"
                    )
                identity_path = deployment_identity_path(loaded.config_file)
                identity = load_deployment_identity(identity_path)
                enforce_vps_paths(identity)
                verify_vps_identity_permissions(
                    identity_path,
                    expected_group=str(identity.systemd_effective.get("group", "")),
                )
                verify_identity_files(
                    identity,
                    expected_config_path=loaded.config_file,
                    expected_profile_id=loaded.config.capacity_profile,
                    require_root_controlled=True,
                )
                runtime_identity = runtime_deployment_identity(identity)
            asyncio.run(
                run_service(
                    loaded.config,
                    logger=logger,
                    authority_path=classification_authority_path(
                        config_file=loaded.config_file,
                        data_root=loaded.config.data_root,
                    ),
                    deployment_identity=runtime_identity,
                )
            )
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
            capacity_profile_id=loaded.config.capacity_profile,
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
    if command == "deployment":
        action = getattr(args, "deployment_command", None)
        try:
            if loaded.config.capacity_profile != "vps-production-v1":
                raise DeploymentIdentityError(
                    "deployment commands require capacity_profile=vps-production-v1"
                )
            if loaded.config_file is None:
                raise DeploymentIdentityError("VPS deployment requires an explicit --config file")
            identity_path = deployment_identity_path(loaded.config_file)
            if action == "acceptance":
                acceptance_action = str(args.acceptance_command)
                identity = load_deployment_identity(identity_path)
                enforce_vps_paths(identity)
                acceptance_manager = _identity_systemd_manager(
                    loaded,
                    identity_user=identity.systemd_effective.get("user"),
                    identity_group=identity.systemd_effective.get("group"),
                )
                if acceptance_action == "identity":
                    path, digest, document = create_identity_evidence(
                        config_file=loaded.config_file,
                        expected_source_git_sha=str(args.expected_source_git_sha),
                        evidence_root=args.evidence_root,
                        data_root=loaded.config.data_root,
                        identity=identity,
                        manager=acceptance_manager,
                    )
                    _write_json(
                        {
                            "command": "deployment.acceptance.identity",
                            "path": str(path),
                            "evidence_sha256": digest,
                            **document,
                        }
                    )
                    return 0
                evaluator = VpsReadinessEvaluator(
                    data_root=loaded.config.data_root,
                    identity=identity,
                    systemd_manager=acceptance_manager,
                )
                if acceptance_action == "readiness":
                    path, digest, document = create_readiness_evidence(
                        identity_evidence_path=args.identity_evidence,
                        identity=identity,
                        manager=acceptance_manager,
                        evaluator=evaluator,
                        evidence_root=args.evidence_root,
                        data_root=loaded.config.data_root,
                    )
                    _write_json(
                        {
                            "command": "deployment.acceptance.readiness",
                            "path": str(path),
                            "evidence_sha256": digest,
                            **document,
                        }
                    )
                    return 0 if document["result"] == "PASS_CANDIDATE" else 2
                if acceptance_action == "stage":
                    if args.resume is not None:
                        observer = resume_observer(
                            args.resume,
                            data_root=loaded.config.data_root,
                            identity=identity,
                            manager=acceptance_manager,
                            evaluator=evaluator,
                        )
                    else:
                        if args.previous_evidence is None or args.evidence_root is None:
                            raise AcceptanceError(
                                "new stage requires --previous-evidence and --evidence-root"
                            )
                        stage = str(args.stage)
                        _prior, prior_sha = verify_prior_stage(
                            args.previous_evidence, identity, stage
                        )
                        stage_root = args.evidence_root / f"{stage}-{uuid.uuid4().hex}"
                        observer = AcceptanceObserver(
                            stage=stage,
                            run_id=uuid.uuid4().hex,
                            data_root=loaded.config.data_root,
                            evidence_root=stage_root,
                            identity=identity,
                            prior_stage_sha256=prior_sha,
                            manager=acceptance_manager,
                            evaluator=evaluator,
                        )
                        path, digest, start_document = observer.start()
                        _write_json(
                            {
                                "command": "deployment.acceptance.stage",
                                "status": "STARTED",
                                "path": str(path),
                                "evidence_sha256": digest,
                            }
                        )
                        if start_document["result"] != "PASS_CANDIDATE":
                            return 1
                    try:
                        if observer.stage_start_sha256 is None:
                            observer.start()
                        while (
                            observer.last_sample_boottime_ns is None
                            or int(observer.last_sample_boottime_ns)
                            - int(observer.t0_boottime_ns or 0)
                            < STAGE_DURATION_NS[observer.stage]
                        ):
                            _path, _digest, _sample = observer.sample()
                            remaining = STAGE_DURATION_NS[observer.stage] - (
                                (observer.last_sample_boottime_ns or 0)
                                - (observer.t0_boottime_ns or 0)
                            )
                            if remaining <= 0:
                                break
                            time.sleep(min(300.0, remaining / 1_000_000_000))
                        path, digest, document = observer.finalize()
                    except KeyboardInterrupt:
                        _write_json(
                            {
                                "command": "deployment.acceptance.stage",
                                "status": "INCOMPLETE",
                                "reason": "observer_interrupted",
                            }
                        )
                        return 2
                    _write_json(
                        {
                            "command": "deployment.acceptance.stage",
                            "path": str(path),
                            "evidence_sha256": digest,
                            **document,
                        }
                    )
                    return 0 if document["result"] == "PASS_CANDIDATE" else 1
                raise AcceptanceError(f"unsupported acceptance command: {acceptance_action}")
            if action == "identity-create":
                probe = SystemdManager(
                    data_root=loaded.config.data_root,
                    config_file=loaded.config_file,
                    user="",
                    group="",
                    python_executable=Path(sys.executable),
                    capacity_profile_id=loaded.config.capacity_profile,
                )
                user, group = probe.observed_service_principal()
                systemd_identity_manager = _identity_systemd_manager(
                    loaded, identity_user=user, identity_group=group
                )
                install_contract = systemd_identity_manager.verify_install_contract()
                effective = systemd_identity_manager.verify_effective_properties()
                deployment_identity = create_deployment_identity(
                    source_git_sha=str(args.source_git_sha),
                    wheel_path=Path(args.wheel),
                    dependency_lock_path=Path(args.dependency_lock),
                    config_path=loaded.config_file,
                    systemd_unit_path=systemd_identity_manager.unit_path,
                    capacity_profile_id=loaded.config.capacity_profile,
                    startup_sidecar_path=classification_authority_path(
                        config_file=loaded.config_file,
                        data_root=loaded.config.data_root,
                    ),
                    systemd_effective=effective,
                )
                enforce_vps_paths(deployment_identity)
                evidence = verify_identity_files(
                    deployment_identity,
                    expected_config_path=loaded.config_file,
                    expected_profile_id=loaded.config.capacity_profile,
                    require_root_controlled=True,
                )
                if os.geteuid() != 0:
                    raise DeploymentIdentityError("deployment identity creation must run as root")
                group_id = grp.getgrnam(group).gr_gid
                write_deployment_identity(
                    identity_path,
                    deployment_identity,
                    owner_uid=0,
                    group_gid=group_id,
                )
                if load_deployment_identity(identity_path) != deployment_identity:
                    raise DeploymentIdentityError("deployment identity readback mismatch")
                identity_permissions = verify_vps_identity_permissions(
                    identity_path, expected_group=group
                )
                result = {
                    "status": "CREATED",
                    "identity_path": str(identity_path),
                    "install_contract": install_contract,
                    "identity_permissions": identity_permissions,
                    **evidence,
                }
            elif action in {"verify", "readiness"}:
                deployment_identity = load_deployment_identity(identity_path)
                enforce_vps_paths(deployment_identity)
                systemd_identity_manager = _identity_systemd_manager(
                    loaded,
                    identity_user=deployment_identity.systemd_effective.get("user"),
                    identity_group=deployment_identity.systemd_effective.get("group"),
                )
                identity_permissions = verify_vps_identity_permissions(
                    identity_path,
                    expected_group=str(deployment_identity.systemd_effective.get("group", "")),
                )
                if action == "verify":
                    result = {
                        "status": "VERIFIED",
                        "install_contract": (systemd_identity_manager.verify_install_contract()),
                        "identity_permissions": identity_permissions,
                        **verify_identity_files(
                            deployment_identity,
                            expected_config_path=loaded.config_file,
                            expected_profile_id=loaded.config.capacity_profile,
                            require_root_controlled=True,
                        ),
                        "systemd_effective": systemd_identity_manager.verify_effective_properties(
                            expected=dict(deployment_identity.systemd_effective)
                        ),
                    }
                else:
                    readiness = wait_for_readiness(
                        VpsReadinessEvaluator(
                            data_root=loaded.config.data_root,
                            identity=deployment_identity,
                            systemd_manager=systemd_identity_manager,
                        )
                    )
                    _write_json({"command": "deployment.readiness", **readiness.public_dict()})
                    return 0 if readiness.state == "READY" else 2
            elif action == "rollback-check":
                rollback_target_identity = load_deployment_identity(Path(args.target_identity))
                enforce_vps_paths(rollback_target_identity)
                target_permissions = verify_vps_identity_permissions(
                    Path(args.target_identity),
                    expected_group=str(rollback_target_identity.systemd_effective.get("group", "")),
                )
                retained_artifacts = verify_retained_rollback_artifacts(rollback_target_identity)
                with Catalog(
                    loaded.config.data_root / "state" / "catalog.sqlite",
                    read_only=True,
                ) as catalog:
                    result = {
                        "status": "COMPATIBLE",
                        "target_identity_sha256": rollback_target_identity.identity_sha256,
                        "target_identity_permissions": target_permissions,
                        "retained_artifacts": retained_artifacts,
                        **rollback_compatibility(rollback_target_identity, catalog),
                    }
            else:
                parser.error(f"unsupported deployment command: {action}")
            _write_json({"command": f"deployment.{action}", **result})
            return 0
        except (
            DeploymentIdentityError,
            AcceptanceError,
            KeyError,
            OSError,
            SystemdError,
            ValueError,
        ) as exc:
            _write_json(
                {"error": "deployment_error", "message": str(exc)},
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
                raise LaunchAgentError("no installed LaunchAgent metadata; specify --label")
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
            _write_json({"error": "report_error", "message": str(exc)}, stream=sys.stderr)
            return 2
        _write_json({"command": "report.daily", **document})
        return 0
    if command == "recovery" and (
        getattr(args, "recovery_command", None) == "legacy-reconnect-preflight"
    ):
        # M21.4.11-R3.3: the preflight is INTRINSICALLY read-only.  The
        # layout is derived without any filesystem mutation (no mkdir,
        # touch, chmod, or creation fsync); required existing paths are
        # validated explicitly and a missing path is an error, never a
        # repair.  Exit status: 0 = eligible, 2 = ineligible (full JSON
        # report on stdout) or runtime error.
        layout = StorageLayout.from_root(loaded.config.data_root)
        try:
            if not layout.root.is_dir():
                raise LegacyReconnectConflictError(
                    "RECOVERY_LEGACY_PREFLIGHT_LAYOUT_ERROR data root does "
                    f"not exist: {layout.root}"
                )
            if not layout.catalog.is_file():
                raise LegacyReconnectConflictError(
                    "RECOVERY_LEGACY_PREFLIGHT_LAYOUT_ERROR Catalog does "
                    f"not exist: {layout.catalog}"
                )
            authority_location = classification_authority_path(
                config_file=loaded.config_file,
                data_root=loaded.config.data_root,
            )
            with Catalog(layout.catalog, read_only=True) as catalog:
                authority = LegacyClassificationAuthority.load(authority_location)
                report = evaluate_legacy_reconnect_decisions(catalog=catalog, authority=authority)
        except LegacyReconnectConflictError as exc:
            _write_json(
                {"error": "legacy_reconnect_preflight_error", "message": str(exc)},
                stream=sys.stderr,
            )
            return 2
        _write_json(
            {
                "command": "recovery.legacy-reconnect-preflight",
                **report.public_dict(),
            }
        )
        return 0 if report.first_corrected_startup_eligible else 2
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
        _write_json({"command": "normalize.run", **normalization_result.public_dict()})
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
            with Catalog(catalog_path, read_only=storage_command == "status") as catalog:
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
                    targets = registry.observe_statuses()
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
                    _write_json({"command": "storage.eject", **eject_result.public_dict()})
                    return 0 if eject_result.safe_to_remove else 1
                if storage_command == "forecast":
                    observed_at = time.time_ns()
                    observed_at -= observed_at % 60_000_000_000
                    forecaster = StorageForecaster(
                        catalog=catalog,
                        data_root=loaded.config.data_root,
                        utc_clock_ns=lambda: observed_at,
                    )
                    forecaster.observe_internal(observed_at_utc_ns=observed_at)
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
                        scope_ids,
                        now_utc_ns=observed_at,
                        capacity_profile=selected_capacity_profile(loaded.config.capacity_profile),
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
            _write_json({"error": "storage_error", "message": str(exc)}, stream=sys.stderr)
            return 2
    if command == "archive":
        archive_command = getattr(args, "archive_command", None)
        if archive_command == "timer":
            timer_command = getattr(args, "archive_timer_command", None)
            if loaded.config_file is None:
                _write_json(
                    {
                        "error": "archive_timer_error",
                        "message": "archive timer requires an explicit --config file",
                    },
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
            with Catalog(catalog_path, read_only=archive_command == "status") as catalog:
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
                        in (
                            "BACKLOG_EMPTY",
                            "MAX_FILES",
                            "DEADLINE",
                            "ALREADY_RUNNING",
                            "INTERRUPTED",
                            "TARGET_ABSENT",
                            "TARGET_NOT_READY",
                            "TARGET_LOW_SPACE",
                        )
                        else 1
                    )
                registry = StorageRegistry(catalog=catalog, volumes=_volume_adapter())
                requested = args.storage_id if archive_command in {"retry", "verify"} else None
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
            _write_json({"error": "archive_error", "message": str(exc)}, stream=sys.stderr)
            return 2
    if command == "soak":
        soak_command = getattr(args, "soak_command", None)
        if soak_command == "timer":
            timer_command = getattr(args, "soak_timer_command", None)
            if loaded.config_file is None:
                _write_json(
                    {
                        "error": "soak_timer_error",
                        "message": "soak timer requires an explicit --config file",
                    },
                    stream=sys.stderr,
                )
                return 2
            soak_timer_manager = SoakTimerManager(
                config_file=loaded.config_file,
                user=str(getattr(args, "user", "")),
                group=str(getattr(args, "group", "")),
                storage_id=str(getattr(args, "storage_id", "")),
                interval_seconds=int(getattr(args, "interval_seconds", 300)),
                output_path=Path(str(getattr(args, "output", ""))).resolve()
                if getattr(args, "output", None)
                else Path("/var/lib/binance-market-data-recorder/operations/soak/samples.jsonl"),
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
                    storage_id=args.storage_id,
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
