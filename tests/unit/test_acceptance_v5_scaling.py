from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from binance_market_data_recorder.service import acceptance_v5_raw as raw
from binance_market_data_recorder.service.acceptance import _publish, sha256_bytes
from binance_market_data_recorder.service.acceptance_v5_io import V5_SCHEMA_VERSION, open_exact
from binance_market_data_recorder.storage.acceptance_delta import DeltaSnapshot
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tests.unit.test_acceptance_v5_online import advance, observer_fixture
from tests.unit.test_historical_reconnect_audit import seal_chunk, usdm_envelope
from tests.v5_support import synthetic_history


def test_ten_thousand_history_is_not_reread_by_consecutive_online_samples(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observer, clock, _evaluator = observer_fixture(tmp_path)
    synthetic_history(observer.data_root, 10_000)
    layout = ensure_storage_layout(observer.data_root)
    with Catalog(layout.catalog) as catalog:
        cursors = DeltaSnapshot(catalog._connection).high_water
    predecessor, _sha = _publish(
        tmp_path / "synthetic-predecessor",
        "audit-root.json",
        {
            "schema_version": V5_SCHEMA_VERSION,
            "evidence_kind": "baseline-audit-root",
            "deployment_identity": observer.identity.document(),
            "result": "PASS_CANDIDATE",
            "configured_products": [["um_perpetual", "BTCUSDT"]],
            "catalog_authority": {"high_water": cursors},
            "continuation_seed": {},
        },
    )
    observer.predecessor_path = predecessor
    reads = hashes = 0

    def forbidden(*_args: Any, **_kwargs: Any) -> Any:
        pytest.fail("full-history inventory called during bounded V5 observation")

    monkeypatch.setattr(
        "binance_market_data_recorder.audit.reconnect_boundaries.strict_manifest_inventory",
        forbidden,
    )

    def counted(root: Path, relative: str) -> Any:
        nonlocal reads
        if relative.endswith(".manifest.json"):
            reads += 1
        return open_exact(root, relative)

    real_hash = sha256_bytes

    def counted_hash(body: bytes) -> str:
        nonlocal hashes
        if b'"manifest_schema_version"' in body:
            hashes += 1
        return real_hash(body)

    monkeypatch.setattr(raw, "open_exact", counted)
    monkeypatch.setattr(raw, "sha256_bytes", counted_hash)
    observer.start()
    assert reads == hashes == 0
    with Catalog(layout.catalog) as catalog:
        seal_chunk(layout, catalog, [usdm_envelope("one", 2)])
    for expected in (3, 0, 0):
        advance(clock, 300)
        _path, _sha, sample = observer.sample()
        assert sum(len(page) for page in sample["delta_pages"].values()) == expected
        assert sample["delta_pending"] is False
    assert reads == 2  # one byte read plus one descriptor-only guard, independent of N
    assert hashes == 1
