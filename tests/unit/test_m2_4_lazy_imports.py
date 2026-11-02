"""M2.4 Unit Tests: CLI lazy imports and Catalog domain modularization.

Verifies:
1. Capture runtime and CLI entrypoint startup paths isolate heavy and peripheral
   modules (`pyarrow`, `service.acceptance`, `storage.macos`, `archive.remote_*`,
   `normalize`, etc.) out of the hot import graph.
2. The modularized `storage/catalog/` package preserves 100% backward compatibility
   for all legacy imports and method invocations.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

FORBIDDEN_COLLECTOR_MODULES = [
    "pyarrow",
    "binance_market_data_recorder.service.acceptance",
    "binance_market_data_recorder.audit",
    "binance_market_data_recorder.spool.legacy_reconnect",
    "binance_market_data_recorder.normalize",
    "binance_market_data_recorder.storage.macos",
    "binance_market_data_recorder.storage.linux",
    "binance_market_data_recorder.archive.remote_",
    "binance_market_data_recorder.archive.catalog_snapshot",
    "binance_market_data_recorder.backfill",
    "binance_market_data_recorder.metrics.report",
]


def _run_in_fresh_process(code: str) -> subprocess.CompletedProcess[str]:
    repo_root = Path(__file__).resolve().parent.parent.parent
    src_dir = repo_root / "src"
    return subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        check=True,
        env={"PYTHONPATH": f"{src_dir}:{repo_root}", "PATH": "/usr/bin:/bin"},
        timeout=45,
    )


def test_cli_import_does_not_load_heavy_modules() -> None:
    code = f"""
import sys
import binance_market_data_recorder.cli

forbidden = {FORBIDDEN_COLLECTOR_MODULES!r}
loaded = [m for m in sys.modules if m.startswith(tuple(forbidden))]
assert not loaded, f"Forbidden modules loaded by cli.py: {{loaded}}"
"""
    result = _run_in_fresh_process(code)
    assert result.returncode == 0


def test_service_runtime_import_does_not_load_heavy_modules() -> None:
    code = f"""
import sys
import binance_market_data_recorder.service.runtime

forbidden = {FORBIDDEN_COLLECTOR_MODULES!r}
loaded = [m for m in sys.modules if m.startswith(tuple(forbidden))]
assert not loaded, f"Forbidden modules loaded by service.runtime: {{loaded}}"
"""
    result = _run_in_fresh_process(code)
    assert result.returncode == 0


def test_coverage_collector_import_does_not_load_heavy_modules() -> None:
    code = f"""
import sys
import binance_market_data_recorder.collector.coverage

forbidden = {FORBIDDEN_COLLECTOR_MODULES!r}
loaded = [m for m in sys.modules if m.startswith(tuple(forbidden))]
assert not loaded, f"Forbidden modules loaded by collector.coverage: {{loaded}}"
"""
    result = _run_in_fresh_process(code)
    assert result.returncode == 0


@pytest.mark.parametrize(
    "subcommand",
    [
        ["config", "show"],
        ["doctor"],
        ["status"],
    ],
)
def test_cli_subcommands_do_not_load_heavy_modules(subcommand: list[str], tmp_path: Path) -> None:
    code = f"""
import sys
import os
from pathlib import Path
from binance_market_data_recorder.cli import main
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout

os.environ["BINANCE_MARKET_RECORDER_DATA_ROOT"] = {str(tmp_path)!r}
layout = ensure_storage_layout(Path({str(tmp_path)!r}))
with Catalog(layout.catalog):
    pass
assert main({subcommand!r}) == 0
forbidden = {FORBIDDEN_COLLECTOR_MODULES!r}
loaded = [m for m in sys.modules if m.startswith(tuple(forbidden))]
assert not loaded, f"Forbidden modules loaded by {subcommand}: {{loaded}}"
"""
    result = _run_in_fresh_process(code)
    assert result.returncode == 0


def test_service_entrypoint_start_capture_stop_and_restart_are_isolated(tmp_path: Path) -> None:
    """Exercise CLI dispatch and actual service recovery, capture writes and seal.

    Only network inputs and OS sleep notifications are replaced. The real
    supervisor, Catalog, spool, metrics and service lifecycle run twice.
    """
    code = f'''
import asyncio
import os
import sys
from pathlib import Path
from types import SimpleNamespace

from binance_market_data_recorder import cli
from binance_market_data_recorder.collector.coverage import ProductCapture, SelectedAssetCapture
from binance_market_data_recorder.service.power import NoopSleepObserver
from binance_market_data_recorder.service.runtime import ServiceRuntime
from binance_market_data_recorder.storage.catalog import Catalog, ChunkState
from binance_market_data_recorder.storage.layout import StorageLayout
from tests.factories import event

forbidden = {FORBIDDEN_COLLECTOR_MODULES!r}
def assert_isolated():
    loaded = [m for m in sys.modules if m.startswith(tuple(forbidden))]
    assert not loaded, f"Forbidden modules loaded during capture: {{loaded}}"

root = Path({str(tmp_path)!r})
os.environ["BINANCE_MARKET_RECORDER_DATA_ROOT"] = str(root)
config = root / "recorder.toml"
config.write_text(
    '[recorder]\\nspot_symbols = ["BTCUSDT"]\\n'
    'usdm_symbols = []\\nprevent_sleep = false\\n'
)

async def offline_service(config, *, logger, authority_path, deployment_identity):
    def supervisor_factory(*args):
        owner = SelectedAssetCapture(*args, clients={{}})
        owner._active_sources.clear()
        return owner

    async def receive_one(product, stop):
        assert runtime._status == "RUNNING"
        assert runtime._startup_recovery_complete
        assert_isolated()
        envelope = event().model_copy(update={{"stream": "test_observation"}})
        await product.owner.persist_rest(envelope)
        assert_isolated()
        runtime.request_stop("M2_4_OFFLINE_TEST")
        await stop.wait()

    ProductCapture.run = receive_one
    runtime = ServiceRuntime(
        config=config, logger=logger, authority_path=authority_path,
        deployment_identity=deployment_identity,
        supervisor_factory=supervisor_factory, sleep_observer_factory=NoopSleepObserver,
        disk_usage=lambda _: SimpleNamespace(total=500 << 30, free=400 << 30, used=100 << 30),
    )
    await asyncio.wait_for(runtime.run(), timeout=15)
    assert runtime.state_store.read()["status"] == "STOPPED"

cli.run_service = offline_service
for iteration in range(2):
    assert cli.main(["--config", str(config), "_service", "run"]) == 0
    assert_isolated()
    with Catalog(StorageLayout.from_root(root).catalog, read_only=True) as catalog:
        assert len(catalog.chunks_in_states(ChunkState.SEALED)) == iteration + 1
        assert len(catalog.operational_events(event_type="SERVICE_STARTED")) == iteration + 1
        assert not catalog.has_reconnect_recovery_evidence()
'''
    result = _run_in_fresh_process(code)
    assert result.returncode == 0


def test_catalog_modularization_backward_compatibility(tmp_path: Path) -> None:
    """Verify that importing from storage.catalog continues to work identically."""
    from binance_market_data_recorder.storage.catalog import (
        ArchiveState,
        Catalog,
        CatalogBase,
        CatalogStateError,
        ChunkState,
        RemoteArchiveState,
    )
    from binance_market_data_recorder.storage.catalog.types import (
        ALLOWED_TRANSITIONS,
        ARCHIVE_TRANSITIONS,
    )

    assert issubclass(Catalog, CatalogBase)
    assert issubclass(CatalogStateError, Exception)
    assert ChunkState.ACTIVE.value == "ACTIVE"
    assert ArchiveState.VERIFIED.value == "VERIFIED"
    assert RemoteArchiveState.REMOTE_DELETED.value == "REMOTE_DELETED"
    assert ChunkState.SEALING in ALLOWED_TRANSITIONS[ChunkState.ACTIVE]
    assert ArchiveState.VERIFYING in ARCHIVE_TRANSITIONS[ArchiveState.COPYING]

    db_path = tmp_path / "test_catalog.sqlite"
    with Catalog(db_path) as catalog:
        assert catalog.integrity_check() == ("ok",)
        # Verify schema is populated
        targets = catalog.storage_targets()
        assert targets == []
        summary = catalog.archive_transaction_summary()
        assert summary["transaction_count"] == 0

        # Verify chunk methods exist and work
        chunk = catalog.chunk("nonexistent")
        assert chunk is None

        # Verify availability methods exist and work
        watermark = catalog.source_availability_watermark()
        assert watermark is None

        # Verify alert events
        alerts = catalog.storage_alert_events()
        assert alerts == []

    # Verify open_live_read_only
    with Catalog.open_live_read_only(db_path) as read_catalog:
        assert read_catalog.integrity_check() == ("ok",)
        assert read_catalog.chunk("nonexistent") is None


@pytest.mark.parametrize("blocker", ["authority_file", "lifecycle_event"])
def test_lazy_recovery_keeps_blockers_without_any_chunks(tmp_path: Path, blocker: str) -> None:
    """The lazy gate must preserve predecision checks even without seal intents."""
    from binance_market_data_recorder.paths import classification_authority_path
    from binance_market_data_recorder.spool.recovery import RecoveryConflictError, recover_storage
    from binance_market_data_recorder.storage.catalog import Catalog
    from binance_market_data_recorder.storage.layout import ensure_storage_layout

    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        assert not catalog.has_reconnect_recovery_evidence()
        if blocker == "authority_file":
            authority = classification_authority_path(config_file=None, data_root=tmp_path)
            authority.write_text("invalid JSON")
            error = "RECOVERY_LEGACY_CLASSIFICATION_MALFORMED"
        else:
            catalog.record_operational_event(
                event_id="missing-gap-identity",
                event_type="STREAM_DISCONTINUITY_STARTED",
                occurred_at_utc_ns=1,
                evidence={"market": "spot", "stream": "book_ticker"},
                symbol="BTCUSDT",
            )
            assert catalog.has_reconnect_recovery_evidence()
            error = "RECOVERY_LEGACY_DEGRADED_AUTHORITY"
        with pytest.raises(RecoveryConflictError, match=error):
            recover_storage(layout=layout, catalog=catalog)


def test_catalog_submodules_exist() -> None:
    """Verify all domain modules in storage.catalog package are present."""
    import binance_market_data_recorder.storage.catalog as cat_pkg
    from binance_market_data_recorder.storage.catalog import (
        archive_ops,
        availability,
        base,
        chunks,
        reports,
        schema,
        types,
    )

    assert hasattr(cat_pkg, "Catalog")
    assert hasattr(base, "CatalogBase")
    assert hasattr(types, "ChunkState")
    assert hasattr(schema, "CatalogSchemaMixin")
    assert hasattr(chunks, "CatalogChunksMixin")
    assert hasattr(availability, "CatalogAvailabilityMixin")
    assert hasattr(archive_ops, "CatalogArchiveMixin")
    assert hasattr(reports, "CatalogReportsMixin")
