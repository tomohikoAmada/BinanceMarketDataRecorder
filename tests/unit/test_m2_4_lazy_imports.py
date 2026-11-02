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
    "binance_market_data_recorder.service.acceptance_v5_corpus",
    "binance_market_data_recorder.service.acceptance_v5_finalize",
    "binance_market_data_recorder.service.acceptance_v5_online",
    "binance_market_data_recorder.normalize",
    "binance_market_data_recorder.storage.macos",
    "binance_market_data_recorder.archive.remote_authorization",
    "binance_market_data_recorder.archive.remote_delete",
    "binance_market_data_recorder.archive.remote_source",
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
        env={"PYTHONPATH": str(src_dir), "PATH": "/usr/bin:/bin"},
    )


def test_cli_import_does_not_load_heavy_modules() -> None:
    code = f"""
import sys
import binance_market_data_recorder.cli

forbidden = {FORBIDDEN_COLLECTOR_MODULES!r}
loaded = [m for m in forbidden if m in sys.modules]
assert not loaded, f"Forbidden modules loaded by cli.py: {{loaded}}"
"""
    result = _run_in_fresh_process(code)
    assert result.returncode == 0


def test_service_runtime_import_does_not_load_heavy_modules() -> None:
    code = f"""
import sys
import binance_market_data_recorder.service.runtime

forbidden = {FORBIDDEN_COLLECTOR_MODULES!r}
loaded = [m for m in forbidden if m in sys.modules]
assert not loaded, f"Forbidden modules loaded by service.runtime: {{loaded}}"
"""
    result = _run_in_fresh_process(code)
    assert result.returncode == 0


def test_coverage_collector_import_does_not_load_heavy_modules() -> None:
    code = f"""
import sys
import binance_market_data_recorder.collector.coverage

forbidden = {FORBIDDEN_COLLECTOR_MODULES!r}
loaded = [m for m in forbidden if m in sys.modules]
assert not loaded, f"Forbidden modules loaded by collector.coverage: {{loaded}}"
"""
    result = _run_in_fresh_process(code)
    assert result.returncode == 0


@pytest.mark.parametrize(
    "subcommand",
    [
        ["config", "show"],
        ["doctor"],
    ],
)
def test_cli_subcommands_do_not_load_heavy_modules(subcommand: list[str]) -> None:
    code = f"""
import sys
from binance_market_data_recorder.cli import main

main({subcommand!r})
forbidden = {FORBIDDEN_COLLECTOR_MODULES!r}
loaded = [m for m in forbidden if m in sys.modules]
assert not loaded, f"Forbidden modules loaded by {subcommand}: {{loaded}}"
"""
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
