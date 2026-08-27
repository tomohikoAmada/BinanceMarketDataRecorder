from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import pytest

from binance_market_data_recorder.audit.reconnect_boundaries import (
    EXPLICIT_SEQUENCE_GAP,
    UNMARKED_RECONNECT,
    audit_data_root,
    strict_manifest_inventory,
)
from binance_market_data_recorder.spool.seal import SealError
from tests.unit.test_historical_reconnect_audit import build_fixture


def test_installed_strict_audit_reuses_real_raw_manifest_fixture(tmp_path: Path) -> None:
    build_fixture(tmp_path)
    result = audit_data_root(tmp_path)
    payload = cast(dict[str, Any], result)
    assert result["schema_version"] == "m22.9-reconnect-audit.v1"
    assert payload["manifest_inventory"]["count"] == 4
    assert payload["summary"]["explicit_gap"] == 2
    assert payload["summary"]["unmarked_reconnect"] == 2
    assert payload["summary"]["unknown"] == 1
    assert EXPLICIT_SEQUENCE_GAP in {
        item["kind"] for stream in payload["streams"] for item in stream["transitions"]
    }
    assert UNMARKED_RECONNECT in {
        item["kind"] for stream in payload["streams"] for item in stream["transitions"]
    }


@pytest.mark.parametrize("body", [b"{", b"[]\n", b'{"manifest_schema_version":"future"}\n'])
def test_strict_inventory_never_skips_malformed_manifest(tmp_path: Path, body: bytes) -> None:
    manifests = tmp_path / "data" / "manifests"
    manifests.mkdir(parents=True)
    (manifests / "bad.manifest.json").write_bytes(body)
    with pytest.raises(SealError):
        strict_manifest_inventory(tmp_path)


def test_acceptance_source_contains_no_direct_catalog_sql(tmp_path: Path) -> None:
    source = Path("src/binance_market_data_recorder/service/acceptance.py").read_text()
    assert "sqlite3.connect" not in source
    assert "cursor.execute" not in source
    assert "SELECT " not in source
