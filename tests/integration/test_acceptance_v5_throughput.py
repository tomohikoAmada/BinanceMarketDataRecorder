"""Real four-product seal/archive load; only time, OS and process probes are fixtures."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from binance_market_data_recorder.archive import ArchiveManager
from binance_market_data_recorder.domain.product import ProductKey
from binance_market_data_recorder.service.acceptance import canonical_json, sha256_bytes
from binance_market_data_recorder.service.acceptance_v5_codec import delta_bytes
from binance_market_data_recorder.service.acceptance_v5_delta import BOUNDED_BATCH_POLICY
from binance_market_data_recorder.service.acceptance_v5_finalize import baseline
from binance_market_data_recorder.service.acceptance_v5_online import (
    replay_online,
    resume_v5_observer,
)
from binance_market_data_recorder.spool.seal import seal_partial
from binance_market_data_recorder.spool.writer import RawChunkWriter
from binance_market_data_recorder.storage.catalog import Catalog
from tests.archive_support import prepare_archive
from tests.factories import event
from tests.integration.test_acceptance_v5_finalize import raw_unit
from tests.unit.test_acceptance_v5_io import stopped
from tests.unit.test_acceptance_v5_online import advance, observer_fixture
from tests.v5_support import production_readiness, publish_ready_state


@pytest.mark.parametrize("policy", ["compact", "bounded-v1", "original"])
def test_four_products_and_archive_keep_up_without_changing_legacy_policy(
    tmp_path: Path, policy: str
) -> None:
    prepared = prepare_archive(tmp_path / "archive", chunk_count=0)
    observer, clock, evaluator = observer_fixture(tmp_path / "observer")
    products = [
        ProductKey("spot", "BTCUSDT"),
        ProductKey("spot", "ETHUSDT"),
        ProductKey("um_perpetual", "BTCUSDT"),
        ProductKey("um_perpetual", "ETHUSDT"),
    ]
    evaluator.expected_products = frozenset(products)
    identity = replace(
        observer.identity,
        systemd_effective={
            **observer.identity.systemd_effective,
            "working_directory": str(prepared.layout.root),
        },
    )
    roots = {prepared.target.storage_id: prepared.target.root}
    predecessor, _, _ = baseline(
        data_root=prepared.layout.root,
        evidence_root=tmp_path / "baseline",
        identity=identity,
        products=[[p.market, p.symbol] for p in products],
        archive_roots=roots,
        probe=stopped,
        identity_verifier=lambda _identity: None,
        raw_unit=raw_unit,
        boot_id="boot-a",
    )
    observer = replace(
        observer,
        data_root=prepared.layout.root,
        identity=identity,
        predecessor_path=predecessor,
        archive_root_resolver=lambda: roots,
    )
    observer.evaluator = production_readiness(observer, clock)
    publish_ready_state(observer, clock)
    path, _, start = observer.start()
    legacy = policy == "original"
    if policy != "compact":
        # Reconstruct an empty old-format T0. No duration/cursor authority changes.
        start.pop("archive_companions")
        if legacy:
            del start["delta_policy"]
        else:
            start["delta_policy"] = dict(BOUNDED_BATCH_POLICY)
        path.write_bytes(canonical_json(start))
        observer.start_document = start
        observer.last_document = start
        observer.stage_start_sha256 = sha256_bytes(path.read_bytes())
    ordinal = 0
    with Catalog(prepared.layout.catalog) as catalog:
        manager = ArchiveManager(layout=prepared.layout, catalog=catalog, target=prepared.target)
        for window in range(5):
            # Five 60s chunks for each of twelve core contexts: 60 chunks/300s.
            for _minute in range(5):
                for product in products:
                    for stream in ("diff_depth", "agg_trade", "book_ticker"):
                        ordinal += 1
                        writer = RawChunkWriter(
                            layout=prepared.layout,
                            catalog=catalog,
                            market=product.market,
                            symbol=product.symbol,
                            stream=stream,
                            collector_instance_id="collector-1",
                            collector_version="0.1.0+test",
                            durability_interval_seconds=0,
                        )
                        writer.append(
                            event(ordinal).model_copy(
                                update={
                                    "market": product.market,
                                    "symbol": product.symbol,
                                    "stream": stream,
                                }
                            )
                        )
                        writer.close()
                        seal_partial(writer.path, layout=prepared.layout, catalog=catalog)
                        assert manager.run_once().state == "LOCAL_DELETED"
            advance(clock, 300)
            if window == 1:
                continue  # One missed target cadence; next gap is exactly 600s.
            publish_ready_state(observer, clock)
            _, _, sample = observer.sample()
            assert sample["blocking_findings"] == []
            if legacy:
                assert len(sample["delta_pages"]["chunk"]) == 256
                assert sample["delta_pending"] is True
                break  # The old one-page policy is intentionally not widened on replay/resume.
            else:
                assert delta_bytes(sample) <= 7 * 1024 * 1024
                assert all(len(page) <= 1024 for page in sample["delta_pages"].values())
                if window in {0, 4}:
                    assert sample["continuation"]["processed"] == sample["high_water"]
                    assert sample["continuation"]["pending_causal_references"] == []
                    assert sample["delta_pending"] is False
                if window == 0:
                    assert len(sample["delta_pages"]["chunk"]) == 480
    replay = replay_online(observer.evidence_root, identity, require_target=False)
    assert replay.sample_count == (1 if legacy else 4)
    assert replay.continuation == sample["continuation"]
    if policy != "compact":
        resumed = resume_v5_observer(
            evidence_root=observer.evidence_root,
            data_root=observer.data_root,
            identity=identity,
            manager=observer.manager,
            evaluator=observer.evaluator,
            clock=clock,
            identity_verifier=observer.identity_verifier,
            disk_usage=observer.disk_usage,
            snapshot_unit=observer.snapshot_unit,
            raw_unit=observer.raw_unit,
            archive_root_resolver=lambda: roots,
        )
        assert resumed.t0_boottime_ns == observer.t0_boottime_ns
        assert resumed.continuation == replay.continuation
        advance(clock, 300)
        publish_ready_state(resumed, clock)
        _, _, resumed_sample = resumed.sample()
        if legacy:
            assert "delta_policy" not in resumed_sample
        else:
            assert resumed_sample["delta_policy"] == BOUNDED_BATCH_POLICY
        assert resumed_sample["blocking_findings"] == []
        row_cap = 256 if legacy else 1024
        assert all(len(page) <= row_cap for page in resumed_sample["delta_pages"].values())
        resumed_replay = replay_online(resumed.evidence_root, identity, require_target=False)
        assert resumed_replay.sample_count == (2 if legacy else 5)
        assert resumed_replay.continuation == resumed_sample["continuation"]
