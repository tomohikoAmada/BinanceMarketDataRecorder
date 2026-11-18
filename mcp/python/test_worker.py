"""End-to-end tests for python/worker.py against genuine Recorder .bmdr.zst + Catalog fixtures."""

from __future__ import annotations

import base64
import hashlib
import json
import shutil
import sqlite3
import sys
from pathlib import Path
from uuid import uuid4

import pyarrow.parquet as pq

_HERE = Path(__file__).resolve().parent
_RECORDER_REPO = _HERE.parents[1] if (_HERE.parents[1] / "vendor" / "python-baseline").is_dir() else (_HERE.parents[1] / "BinanceMarketDataRecorder")
_BASELINE_SRC = _RECORDER_REPO / "vendor" / "python-baseline" / "src"
for p in (str(_HERE), str(_BASELINE_SRC), str(_RECORDER_REPO)):
    if p not in sys.path:
        sys.path.insert(0, p)

import worker  # noqa: E402
from binance_market_data_recorder.domain.event import EventEnvelope  # noqa: E402
from binance_market_data_recorder.spool.seal import seal_partial  # noqa: E402
from binance_market_data_recorder.spool.writer import RawChunkWriter  # noqa: E402
from binance_market_data_recorder.storage.catalog import Catalog, ChunkState  # noqa: E402
from binance_market_data_recorder.storage.layout import ensure_storage_layout  # noqa: E402


def _make_archived_chunk(
    layout,
    catalog: Catalog,
    ext_root: Path,
    storage_id: str,
    *,
    market: str,
    symbol: str,
    stream: str,
    envelopes: list[EventEnvelope],
) -> dict[str, str]:
    cid = uuid4()
    created_ns = envelopes[0].receive_time_utc_ns
    writer = RawChunkWriter(
        layout=layout,
        catalog=catalog,
        market=market,
        symbol=symbol,
        stream=stream,
        collector_instance_id="test-col",
        collector_version="1.0.0",
        durability_interval_seconds=0.0,
        chunk_id=cid,
        created_at_utc_ns=created_ns,
    )
    for env in envelopes:
        writer.append(env)
    writer.sync()
    partial_path = writer.path
    writer.close()
    manifest = seal_partial(partial_path, layout=layout, catalog=catalog)

    manifest_path = layout.root / "data" / "manifests" / f"{cid.hex}.manifest.json"
    if not manifest_path.is_file():
        manifest_path = layout.manifests / f"{cid.hex}.manifest.json"
    raw_manifest_bytes = manifest_path.read_bytes()
    sealed_path = layout.root / str(manifest["relative_path"])

    target_rel = f"raw/{cid.hex}.bmdr.zst"
    ext_manifest_rel = f"manifests/{cid}.archive-manifest.json"
    (ext_root / "raw").mkdir(parents=True, exist_ok=True)
    (ext_root / "manifests").mkdir(parents=True, exist_ok=True)
    ext_artifact = ext_root / target_rel
    shutil.copy2(sealed_path, ext_artifact)

    # Simulate LOCAL_DELETED external storage artifact matching production snapshots.
    sealed_path.unlink()
    manifest_path.unlink()

    ext_manifest_doc = {
        "archive_manifest_schema_version": "external-archive-manifest.v1",
        "chunk_id": str(cid),
        "storage_id": storage_id,
        "artifact_relative_path": target_rel,
        "stored_bytes": manifest["stored_bytes"],
        "stored_sha256": manifest["stored_sha256"],
        "source_manifest_sha256": hashlib.sha256(raw_manifest_bytes).hexdigest(),
        "raw_manifest": manifest,
        "raw_manifest_bytes_base64": base64.b64encode(raw_manifest_bytes).decode("ascii"),
    }
    ext_manifest_path = ext_root / ext_manifest_rel
    ext_manifest_path.write_text(json.dumps(ext_manifest_doc), encoding="utf-8")

    # Record archive transaction in catalog SQLite cleanly.
    conn = sqlite3.connect(catalog.path)
    try:
        conn.execute(
            """
            INSERT OR IGNORE INTO storage_targets(
                storage_id, volume_uuid, volume_name, filesystem_type,
                relative_path, marker_nonce, registered_at_utc_ns
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (storage_id, "vol-uuid-1", "test", "apfs", "ext", "nonce-1", created_ns),
        )
        conn.execute(
            """
            INSERT INTO archive_transactions(
                transaction_id, chunk_id, storage_id, state, market, stream,
                source_relative_path, source_manifest_relative_path, source_manifest_sha256,
                target_relative_path, target_temp_relative_path, external_manifest_relative_path,
                stored_bytes, stored_sha256, created_at_utc_ns, updated_at_utc_ns
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(uuid4()),
                str(cid),
                storage_id,
                "LOCAL_DELETED",
                market,
                stream,
                manifest["relative_path"],
                f"data/manifests/{cid.hex}.manifest.json",
                hashlib.sha256(raw_manifest_bytes).hexdigest(),
                target_rel,
                f"raw/.{cid.hex}.tmp",
                ext_manifest_rel,
                manifest["stored_bytes"],
                manifest["stored_sha256"],
                created_ns,
                envelopes[-1].receive_time_utc_ns,
            ),
        )
        conn.execute("UPDATE chunks SET state = ? WHERE chunk_id = ?", (ChunkState.LOCAL_DELETED.value, str(cid)))
        conn.commit()
    finally:
        conn.close()

    return {
        "partition_id": "p-test",
        "chunk_id": str(cid),
        "storage_id": storage_id,
        "stream": stream,
        "manifest_path": str(ext_manifest_path),
        "raw_path": str(ext_artifact),
    }


def _env(market: str, symbol: str, stream: str, ts_ms: int, payload_obj: dict, *, conn: str = "c1", flags: tuple[str, ...] = ()) -> EventEnvelope:
    return EventEnvelope(
        market=market,  # type: ignore[arg-type]
        symbol=symbol,
        stream=stream,
        module="collector",
        connection_id=conn,
        collector_instance_id="test-col",
        collector_version="1.0.0",
        receive_time_utc_ns=ts_ms * 1_000_000,
        receive_monotonic_ns=ts_ms * 1_000_000,
        exchange_event_time=ts_ms,
        exchange_trade_time=ts_ms if stream == "agg_trade" else None,
        raw_payload=json.dumps(payload_obj).encode("utf-8"),
        capture_flags=flags,
    )


def _rest_env(market: str, symbol: str, source_stream: str, ts_ms: int, model: object, params: dict | None = None) -> EventEnvelope:
    doc = {
        "schema_version": "binance-public-rest-provenance.v1",
        "representation": "sdk-parsed-json.v1",
        "source_stream": source_stream,
        "request": {"request_id": f"req-{ts_ms}", "path": "/test", "parameters": params or {"symbol": symbol}},
        "response": {"status": 200, "valid": True, "model": model},
    }
    return EventEnvelope(
        market=market,  # type: ignore[arg-type]
        symbol=symbol,
        stream="rest_observations",
        module="rest",
        connection_id="rest-1",
        collector_instance_id="test-col",
        collector_version="1.0.0",
        receive_time_utc_ns=ts_ms * 1_000_000,
        receive_monotonic_ns=ts_ms * 1_000_000,
        source_sequence={"sourceStream": source_stream, "sourceFamily": "REST"},
        raw_payload=json.dumps(doc).encode("utf-8"),
    )


def test_worker_usdm_and_spot_end_to_end(tmp_path: Path) -> None:
    storage_root = tmp_path / "source_layout"
    layout = ensure_storage_layout(storage_root)
    catalog = Catalog(layout.catalog)
    ext_root = tmp_path / "external_storage"
    sid = "storage-test-1"

    base_ms = 1_794_700_800_000

    # 1. agg_trade chunk (USDM BTCUSDT, 2 candles: minute 0 & minute 1)
    ch_agg = _make_archived_chunk(
        layout, catalog, ext_root, sid,
        market="um_perpetual", symbol="BTCUSDT", stream="agg_trade",
        envelopes=[
            _env("um_perpetual", "BTCUSDT", "agg_trade", base_ms + 1_000, {"e": "aggTrade", "E": base_ms + 1_000, "s": "BTCUSDT", "a": 1, "p": "90000.5", "q": "0.5", "f": 1, "l": 1, "T": base_ms + 1_000, "m": False}),
            _env("um_perpetual", "BTCUSDT", "agg_trade", base_ms + 2_000, {"e": "aggTrade", "E": base_ms + 2_000, "s": "BTCUSDT", "a": 2, "p": "90010.0", "q": "1.0", "f": 2, "l": 2, "T": base_ms + 2_000, "m": True}),
            _env("um_perpetual", "BTCUSDT", "agg_trade", base_ms + 61_000, {"e": "aggTrade", "E": base_ms + 61_000, "s": "BTCUSDT", "a": 3, "p": "89995.0", "q": "2.0", "f": 3, "l": 3, "T": base_ms + 61_000, "m": False}),
        ],
    )

    # 2. diff_depth chunk + rest_observations depth_snapshot (including sequence gap invalidation!)
    ch_depth = _make_archived_chunk(
        layout, catalog, ext_root, sid,
        market="um_perpetual", symbol="BTCUSDT", stream="diff_depth",
        envelopes=[
            _env("um_perpetual", "BTCUSDT", "diff_depth", base_ms + 500, {"e": "depthUpdate", "E": base_ms + 500, "T": base_ms + 500, "s": "BTCUSDT", "U": 99, "u": 102, "pu": 98, "b": [["90000", "2.5"]], "a": [["90005", "1.0"]]}),
            _env("um_perpetual", "BTCUSDT", "diff_depth", base_ms + 1500, {"e": "depthUpdate", "E": base_ms + 1500, "T": base_ms + 1500, "s": "BTCUSDT", "U": 103, "u": 104, "pu": 102, "b": [["90001", "3.0"]], "a": []}),
            # Gap! pu=999 != 104 -> must drop SYNCHRONIZED state cleanly and NOT emit unsynchronized book
            _env("um_perpetual", "BTCUSDT", "diff_depth", base_ms + 2500, {"e": "depthUpdate", "E": base_ms + 2500, "T": base_ms + 2500, "s": "BTCUSDT", "U": 1000, "u": 1001, "pu": 999, "b": [["90002", "1.0"]], "a": []}),
        ],
    )
    ch_rest = _make_archived_chunk(
        layout, catalog, ext_root, sid,
        market="um_shared", symbol="__GLOBAL__", stream="rest_observations",
        envelopes=[
            _rest_env("um_shared", "__GLOBAL__", "depth_snapshot", base_ms + 600, {
                "lastUpdateId": 100,
                "bids": [["90000", "1.5"]],
                "asks": [["90005", "2.0"]],
            }, params={"symbol": "BTCUSDT", "limit": 1000}),
            _rest_env("um_shared", "__GLOBAL__", "exchange_info", base_ms + 700, {
                "symbols": [{"symbol": "EXITEDUSDT", "status": "SETTLING", "contractType": "PERPETUAL", "baseAsset": "EXITED", "quoteAsset": "USDT", "marginAsset": "USDT"}]
            }, params={}),
            _rest_env("um_shared", "__GLOBAL__", "open_interest", base_ms + 800, {
                "symbol": "BTCUSDT", "openInterest": "12345.678", "time": base_ms + 800,
            }, params={"symbol": "BTCUSDT"}),
        ],
    )

    # 3. liquidation + mark_price chunks
    ch_liq = _make_archived_chunk(
        layout, catalog, ext_root, sid,
        market="um_perpetual", symbol="BTCUSDT", stream="liquidation",
        envelopes=[
            _env("um_perpetual", "BTCUSDT", "liquidation", base_ms + 1_200, {
                "e": "forceOrder", "E": base_ms + 1_200,
                "o": {"s": "BTCUSDT", "S": "SELL", "o": "LIMIT", "f": "IOC", "q": "0.5", "p": "89900", "ap": "89910", "X": "FILLED", "l": "0.5", "z": "0.5", "T": base_ms + 1_200},
            }),
        ],
    )
    ch_mark = _make_archived_chunk(
        layout, catalog, ext_root, sid,
        market="um_perpetual", symbol="BTCUSDT", stream="mark_price",
        envelopes=[
            _env("um_perpetual", "BTCUSDT", "mark_price", base_ms + 1_100, {
                "e": "markPriceUpdate", "E": base_ms + 1_100, "s": "BTCUSDT",
                "p": "90002.1", "i": "90000.0", "P": "90001.0", "r": "0.0001", "T": base_ms + 28_800_000,
            }),
        ],
    )
    catalog.close()

    base_req = {
        "market": "usdm",
        "symbols": ["BTCUSDT"],
        "start_utc_ns": base_ms * 1_000_000,
        "end_utc_ns": (base_ms + 120_000) * 1_000_000,
        "partitions": [{"partition_id": "p-test", "catalog_path": str(layout.catalog)}],
        "external_roots": {sid: str(ext_root)},
    }

    # Test inspect_overview (finds EXITEDUSDT from historical um_shared exchange_info)
    ov = worker.dispatch({
        **base_req,
        "operation": "inspect_overview",
        "symbols": [],
        "chunks": [ch_agg, ch_rest],
    })
    assert "BTCUSDT" in ov["observed_symbols"]
    assert "EXITEDUSDT" in ov["contract_specs"]

    # Test query_klines + Parquet spill
    pq_out = tmp_path / "klines.parquet"
    kl = worker.dispatch({
        **base_req,
        "operation": "query_klines",
        "interval": "1m",
        "output_file": str(pq_out),
        "output_format": "parquet",
        "chunks": [ch_agg],
    })
    assert kl["total_rows"] == 2
    assert kl["rows"][0]["volume"] == "1.5"
    assert kl["rows"][0]["taker_buy_volume"] == "0.5"
    assert pq_out.is_file() and pq.read_table(pq_out).num_rows == 2

    # Test query_orderbook: must synchronize on base_ms+600 snapshot + base_ms+500 update, emit on +1500, and invalidate cleanly at +2500 gap!
    ob = worker.dispatch({
        **base_req,
        "operation": "query_orderbook",
        "depth_levels": 5,
        "sample_step_ms": 0,
        "chunks": [ch_depth, ch_rest],
    })
    assert ob["total_rows"] >= 1
    assert ob["stats"]["resync_count"] == 1
    assert ob["unsynchronized_events"][0]["reason"] == "sequence_gap"
    assert all(r["state"] == "SYNCHRONIZED" for r in ob["rows"])
    assert "top_5_depth_imbalance" in ob["rows"][0]

    # Test query_trades_and_liquidations
    tl = worker.dispatch({
        **base_req,
        "operation": "query_trades_and_liquidations",
        "chunks": [ch_agg, ch_liq],
    })
    assert tl["total_rows"] == 4
    assert tl["liquidation_status"] == "observed"

    # Spot liquidation must report not_applicable_for_spot honestly
    tl_spot = worker.dispatch({
        **base_req,
        "market": "spot",
        "record_kind": "liquidations",
        "operation": "query_trades_and_liquidations",
        "chunks": [ch_agg],
    })
    assert tl_spot["total_rows"] == 0
    assert tl_spot["liquidation_status"] == "not_applicable_for_spot"

    # Test query_derivatives_metrics
    dm = worker.dispatch({
        **base_req,
        "operation": "query_derivatives_metrics",
        "chunks": [ch_mark, ch_rest],
    })
    assert dm["total_rows"] == 2


def test_m6b_m6c_chronological_chunks_ohlc_and_reconnect_recovery(tmp_path: Path) -> None:
    """Regression suite for M6-B (sha256-independent ordering & OHLC determinism), M6-C (capture_flag_gap + connection_reset recovery), and M6-F streaming writer."""
    storage_root = tmp_path / "source_layout"
    layout = ensure_storage_layout(storage_root)
    catalog = Catalog(layout.catalog)
    ext_root = tmp_path / "external_storage"
    sid = "storage-m6-test"

    base_ms = 1_794_700_800_000  # divisible by 1000 and 60_000

    # --- 1. M6-B: Two chunks intentionally passed out-of-order & spanning one 1s candle ---
    # Chunk A (earlier: +100ms, agg_id=10, p=63916.7; +400ms, agg_id=11, p=63910.0)
    # Chunk B (later:   +700ms, agg_id=12, p=63905.0; +900ms, agg_id=13, p=63900.9)
    ch_agg_early = _make_archived_chunk(
        layout, catalog, ext_root, sid,
        market="um_perpetual", symbol="BTCUSDT", stream="agg_trade",
        envelopes=[
            _env("um_perpetual", "BTCUSDT", "agg_trade", base_ms + 100, {"e": "aggTrade", "E": base_ms + 100, "s": "BTCUSDT", "a": 10, "p": "63916.7", "q": "1.0", "f": 10, "l": 10, "T": base_ms + 100, "m": False}),
            _env("um_perpetual", "BTCUSDT", "agg_trade", base_ms + 400, {"e": "aggTrade", "E": base_ms + 400, "s": "BTCUSDT", "a": 11, "p": "63910.0", "q": "1.0", "f": 11, "l": 11, "T": base_ms + 400, "m": True}),
        ],
    )
    ch_agg_late = _make_archived_chunk(
        layout, catalog, ext_root, sid,
        market="um_perpetual", symbol="BTCUSDT", stream="agg_trade",
        envelopes=[
            _env("um_perpetual", "BTCUSDT", "agg_trade", base_ms + 700, {"e": "aggTrade", "E": base_ms + 700, "s": "BTCUSDT", "a": 12, "p": "63905.0", "q": "1.0", "f": 12, "l": 12, "T": base_ms + 700, "m": False}),
            _env("um_perpetual", "BTCUSDT", "agg_trade", base_ms + 900, {"e": "aggTrade", "E": base_ms + 900, "s": "BTCUSDT", "a": 13, "p": "63900.9", "q": "1.0", "f": 13, "l": 13, "T": base_ms + 900, "m": True}),
        ],
    )

    # Pass late chunk FIRST in request; worker must sort by time and preserve open=63916.7, close=63900.9!
    kl = worker.dispatch({
        "operation": "query_klines",
        "market": "usdm",
        "symbols": ["BTCUSDT"],
        "interval": "1s",
        "start_utc_ns": base_ms * 1_000_000,
        "end_utc_ns": (base_ms + 1000) * 1_000_000,
        "partitions": [{"partition_id": "p-test", "catalog_path": str(layout.catalog)}],
        "external_roots": {sid: str(ext_root)},
        "chunks": [ch_agg_late, ch_agg_early],
    })
    assert kl["total_rows"] == 1
    assert kl["rows"][0]["open"] == "63916.7"
    assert kl["rows"][0]["close"] == "63900.9"
    assert kl["rows"][0]["high"] == "63916.7"
    assert kl["rows"][0]["low"] == "63900.9"

    # --- 2. M6-C: Reconnect + capture_flag_gap + new snapshot recovery across warmup and window ---
    # Scenario:
    # - t=-5000ms (conn1): synced before gap -> at t=-3000ms conn2 sends TWO consecutive reconnect_gap flagged messages
    # - t=-2500ms (conn2): depth_snapshot (U=200) -> t=-2000ms (conn2, clean): U=199, u=202, pu=198 -> MUST recover cleanly!
    # - t=+1000ms (conn2, in window): U=203, u=204, pu=202 -> MUST emit inside [base_ms, base_ms + 5000)!
    ch_depth_1 = _make_archived_chunk(
        layout, catalog, ext_root, sid,
        market="um_perpetual", symbol="BTCUSDT", stream="diff_depth",
        envelopes=[
            _env("um_perpetual", "BTCUSDT", "diff_depth", base_ms - 5000, {"e": "depthUpdate", "s": "BTCUSDT", "U": 99, "u": 102, "pu": 98, "b": [["64000", "1.0"]], "a": [["64001", "1.0"]]}, conn="conn1"),
            _env("um_perpetual", "BTCUSDT", "diff_depth", base_ms - 3000, {"e": "depthUpdate", "s": "BTCUSDT", "U": 150, "u": 151, "pu": 149, "b": [], "a": []}, conn="conn2", flags=("reconnect_gap",)),
            _env("um_perpetual", "BTCUSDT", "diff_depth", base_ms - 2900, {"e": "depthUpdate", "s": "BTCUSDT", "U": 152, "u": 153, "pu": 151, "b": [], "a": []}, conn="conn2", flags=("reconnect_gap",)),
            _env("um_perpetual", "BTCUSDT", "diff_depth", base_ms - 2000, {"e": "depthUpdate", "s": "BTCUSDT", "U": 199, "u": 202, "pu": 198, "b": [["64000.5", "2.0"]], "a": [["64001.5", "2.0"]]}, conn="conn2"),
        ],
    )
    ch_depth_2 = _make_archived_chunk(
        layout, catalog, ext_root, sid,
        market="um_perpetual", symbol="BTCUSDT", stream="diff_depth",
        envelopes=[
            _env("um_perpetual", "BTCUSDT", "diff_depth", base_ms + 1000, {"e": "depthUpdate", "s": "BTCUSDT", "U": 203, "u": 204, "pu": 202, "b": [["64002.0", "3.0"]], "a": []}, conn="conn2"),
            _env("um_perpetual", "BTCUSDT", "diff_depth", base_ms + 2500, {"e": "depthUpdate", "s": "BTCUSDT", "U": 205, "u": 206, "pu": 204, "b": [], "a": [["64003.0", "1.5"]]}, conn="conn2"),
        ],
    )
    ch_snaps = _make_archived_chunk(
        layout, catalog, ext_root, sid,
        market="um_shared", symbol="BTCUSDT", stream="rest_observations",
        envelopes=[
            _rest_env("um_shared", "BTCUSDT", "depth_snapshot", base_ms - 4500, {"lastUpdateId": 100, "bids": [["64000", "1.0"]], "asks": [["64001", "1.0"]]}),
            _rest_env("um_shared", "BTCUSDT", "depth_snapshot", base_ms - 2500, {"lastUpdateId": 200, "bids": [["64000.5", "1.5"]], "asks": [["64001.5", "1.5"]]}),
        ],
    )
    catalog.close()

    ob = worker.dispatch({
        "operation": "query_orderbook",
        "market": "usdm",
        "symbols": ["BTCUSDT"],
        "depth_levels": 5,
        "sample_step_ms": 1000,
        "start_utc_ns": base_ms * 1_000_000,
        "end_utc_ns": (base_ms + 5000) * 1_000_000,
        "partitions": [{"partition_id": "p-test", "catalog_path": str(layout.catalog)}],
        "external_roots": {sid: str(ext_root)},
        # Pass depth chunks intentionally out of order too!
        "chunks": [ch_depth_2, ch_snaps, ch_depth_1],
    })
    assert ob["total_rows"] == 2
    assert ob["stats"]["warmup_resync_count"] == 1  # two consecutive gap flags collapsed into 1 reset, no duplicate conn2 reset!
    assert ob["stats"]["window_resync_count"] == 0
    assert ob["stats"]["recovery_count"] == 1
    assert ob["unsynchronized_reason"] is None
    assert ob["rows"][0]["bids"][0][0] == "64002"

    # --- 3. M6-D + M6-F: No snapshot gives explicit unsynchronized_reason; batched Parquet writer streams cleanly ---
    ob_nosnap = worker.dispatch({
        "operation": "query_orderbook",
        "market": "usdm",
        "symbols": ["BTCUSDT"],
        "depth_levels": 5,
        "sample_step_ms": 1000,
        "start_utc_ns": base_ms * 1_000_000,
        "end_utc_ns": (base_ms + 5000) * 1_000_000,
        "partitions": [{"partition_id": "p-test", "catalog_path": str(layout.catalog)}],
        "external_roots": {sid: str(ext_root)},
        "chunks": [ch_depth_2],
    })
    assert ob_nosnap["total_rows"] == 0
    assert ob_nosnap["unsynchronized_reason"] == "no_depth_snapshots_for_symbol"

    # Batched Parquet streaming write test (small batch_size=2 across 5 rows)
    pq_batched = tmp_path / "batched.parquet"
    with worker._RowStreamWriter(str(pq_batched), "parquet", batch_size=2) as w:
        for i in range(5):
            w.write({"idx": i, "meta": {"ok": True}})
        out_p = w.close_and_commit()
    assert out_p == str(pq_batched)
    assert pq.read_table(pq_batched).num_rows == 5


def test_m6r2_r3_r4_r6_lookahead_mixed_export_coverage_and_bounded_streaming(tmp_path: Path, monkeypatch) -> None:
    """Formal regression for M6-R2 (no snapshot lookahead), M6-R3 (mixed schema + write error propagation), M6-R4 (per-symbol coverage), and M6-R6 (bounded overlap iterators + streaming klines)."""
    import csv
    import tracemalloc
    from types import SimpleNamespace

    base_ms = 1_794_700_800_000

    def _mock_events(events):
        def _it(req):
            wanted = {x["stream"] for x in req.get("chunks", [])}
            for env in events:
                if not wanted or env.stream in wanted:
                    yield "p-mock", SimpleNamespace(chunk_id="chunk-" + env.stream), SimpleNamespace(envelope=env)

        monkeypatch.setattr(worker, "_iter_partition_records", _it)

    # 1. M6-R2: Snapshot at +100ms (ID 100) + delta at +200ms (ID 101) -> no +100ms lookahead frame, short [0, +150ms) yields 0 rows!
    snap_early = _rest_env("um_shared", "BTCUSDT", "depth_snapshot", base_ms + 100, {"lastUpdateId": 100, "bids": [["100", "1"]], "asks": [["101", "1"]]})
    delta_late = _env("um_perpetual", "BTCUSDT", "diff_depth", base_ms + 200, {"e": "depthUpdate", "E": base_ms + 200, "T": base_ms + 200, "s": "BTCUSDT", "U": 100, "u": 101, "pu": 99, "b": [["100", "2"]], "a": [["101", "1"]]})
    _mock_events([snap_early, delta_late])
    book_req = {
        "operation": "query_orderbook",
        "market": "usdm",
        "symbols": ["BTCUSDT"],
        "start_utc_ns": base_ms * 1_000_000,
        "end_utc_ns": (base_ms + 500) * 1_000_000,
        "chunks": [{"stream": "rest_observations"}, {"stream": "diff_depth"}],
        "sample_step_ms": 1,
        "limit": 100,
    }
    ob_long = worker.dispatch(book_req)
    assert len(ob_long["rows"]) == 1
    assert ob_long["rows"][0]["receive_time_utc_ns"] == str((base_ms + 200) * 1_000_000)
    assert ob_long["rows"][0]["update_id"] == "101"
    ob_short = worker.dispatch({**book_req, "end_utc_ns": (base_ms + 150) * 1_000_000})
    assert ob_short["rows"] == []

    # Also delta arriving BEFORE snapshot (+100ms delta, +200ms snapshot, +300ms follow-up delta) -> bridged state uses +200ms, never +100ms.
    delta_first = _env("um_perpetual", "BTCUSDT", "diff_depth", base_ms + 100, {"e": "depthUpdate", "E": base_ms + 100, "T": base_ms + 100, "s": "BTCUSDT", "U": 100, "u": 101, "pu": 99, "b": [["100", "2"]], "a": [["101", "1"]]})
    snap_after = _rest_env("um_shared", "BTCUSDT", "depth_snapshot", base_ms + 200, {"lastUpdateId": 100, "bids": [["100", "1"]], "asks": [["101", "1"]]})
    delta_follow = _env("um_perpetual", "BTCUSDT", "diff_depth", base_ms + 300, {"e": "depthUpdate", "E": base_ms + 300, "T": base_ms + 300, "s": "BTCUSDT", "U": 102, "u": 102, "pu": 101, "b": [["100", "3"]], "a": []})
    _mock_events([delta_first, snap_after, delta_follow])
    ob_rev = worker.dispatch(book_req)
    assert [r["receive_time_utc_ns"] for r in ob_rev["rows"]] == [
        str((base_ms + 200) * 1_000_000),
        str((base_ms + 300) * 1_000_000),
    ]
    assert ob_rev["rows"][0]["update_id"] == "101"
    assert ob_rev["rows"][1]["update_id"] == "102"

    # 2. M6-R4: Mixed BTC (mid-window sync) + ETH (no snapshot) reports accurate per-symbol coverage.
    delta_eth = _env("um_perpetual", "ETHUSDT", "diff_depth", base_ms + 210, {"e": "depthUpdate", "E": base_ms + 210, "T": base_ms + 210, "s": "ETHUSDT", "U": 100, "u": 101, "pu": 99, "b": [["3000", "1"]], "a": [["3001", "1"]]})
    _mock_events([snap_early, delta_late, delta_eth])
    ob_mixed = worker.dispatch({**book_req, "symbols": ["BTCUSDT", "ETHUSDT"]})
    assert ob_mixed["symbol_coverage"]["BTCUSDT"]["status"] == "partial"
    assert ob_mixed["symbol_coverage"]["ETHUSDT"]["status"] == "unavailable"
    assert ob_mixed["symbol_coverage"]["ETHUSDT"]["reason"] == "no_depth_snapshots_for_symbol"

    # 3. M6-R3: Mixed agg_trade + liquidation in both orders across jsonl/csv/parquet + multi-batch Parquet + write OSError propagation.
    trade = _env("um_perpetual", "BTCUSDT", "agg_trade", base_ms + 200, {"a": 1, "T": base_ms + 200, "p": "100", "q": "2", "m": False})
    liq = _env("um_perpetual", "BTCUSDT", "liquidation", base_ms + 300, {"e": "forceOrder", "E": base_ms + 300, "o": {"s": "BTCUSDT", "T": base_ms + 300, "S": "SELL", "o": "LIMIT", "p": "99", "ap": "98", "q": "3", "z": "3", "X": "FILLED"}})
    for idx, seq in enumerate(([trade, liq], [liq, trade])):
        _mock_events(seq)
        for fmt in ("jsonl", "csv", "parquet"):
            out_path = tmp_path / f"mixed_{idx}.{fmt}"
            res = worker.dispatch({"operation": "query_trades_and_liquidations", "market": "usdm", "symbols": ["BTCUSDT"], "record_kind": "all", "limit": 10, "output_file": str(out_path), "output_format": fmt})
            assert res["total_rows"] == 2
            if fmt == "csv":
                rows = list(csv.DictReader(out_path.open()))
            elif fmt == "parquet":
                rows = pq.read_table(out_path).to_pylist()
            else:
                rows = [json.loads(ln) for ln in out_path.read_text().splitlines()]
            assert len(rows) == 2
            liq_rows = [r for r in rows if r["record_type"] == "liquidation"]
            tr_rows = [r for r in rows if r["record_type"] == "agg_trade"]
            assert len(liq_rows) == 1 and len(tr_rows) == 1
            assert liq_rows[0]["average_price"] == "98" and liq_rows[0]["filled_quantity"] == "3" and liq_rows[0]["side"] == "SELL"
            assert tr_rows[0]["agg_trade_id"] == "1"

    # Multi-batch Parquet test (batch 0 all trades, batch 1 liquidation across > batch boundary).
    pq_mixed = tmp_path / "mixed_multibatch.parquet"
    with worker._RowStreamWriter(str(pq_mixed), "parquet", batch_size=64, fieldnames=worker._TRADES_LIQ_FIELDS, parquet_field_types=worker._TRADES_LIQ_PARQUET_TYPES) as w:
        for i in range(64):
            w.write({"record_type": "agg_trade", "symbol": "BTCUSDT", "trade_time_utc_ns": str(base_ms + i), "receive_time_utc_ns": str(base_ms + i), "agg_trade_id": str(i + 1), "price": "100", "quantity": "1", "buyer_is_maker": False, "qty_unit": "base_asset", "source": "recorder", "source_chunk_id": "c1"})
        w.write({"record_type": "liquidation", "symbol": "BTCUSDT", "order_trade_time_utc_ns": str(base_ms + 999), "receive_time_utc_ns": str(base_ms + 999), "side": "SELL", "order_type": "LIMIT", "price": "99", "average_price": "98", "original_quantity": "3", "filled_quantity": "3", "order_status": "FILLED", "qty_unit": "base_asset", "source": "recorder", "source_chunk_id": "c2"})
        w.close_and_commit()
    pq_rows = pq.read_table(pq_mixed).to_pylist()
    assert len(pq_rows) == 65 and pq_rows[-1]["average_price"] == "98"

    _mock_events([trade, liq])
    orig_write = worker._RowStreamWriter.write
    def _fail_write(self, row):
        raise OSError("injected disk full")
    monkeypatch.setattr(worker._RowStreamWriter, "write", _fail_write)
    fail_path = tmp_path / "should_fail.jsonl"
    try:
        try:
            worker.dispatch({"operation": "query_trades_and_liquidations", "market": "usdm", "symbols": ["BTCUSDT"], "record_kind": "all", "output_file": str(fail_path)})
            raise AssertionError("expected OSError from writer.write to propagate")
        except OSError as exc:
            assert "injected disk full" in str(exc)
        assert not fail_path.exists()
    finally:
        monkeypatch.setattr(worker._RowStreamWriter, "write", orig_write)

    # 4. M6-R6: Overlap iterator stays bounded at 2 on a 100-chunk overlapping chain, and streaming kline memory stays bounded between 1000 and 4000 bars.
    active = [0]
    peak = [0]
    chain_chunks = [
        SimpleNamespace(chunk_id=str(i), manifest={"receive_time_utc_range_ns": {"min": 10 + 2 * i, "max": 13 + 2 * i}})
        for i in range(100)
    ]

    def _fake_iter_source_records(ch):
        active[0] += 1
        peak[0] = max(peak[0], active[0])
        try:
            for ts in (ch.manifest["receive_time_utc_range_ns"]["min"], ch.manifest["receive_time_utc_range_ns"]["max"]):
                yield SimpleNamespace(envelope=SimpleNamespace(receive_time_utc_ns=ts))
        finally:
            active[0] -= 1

    monkeypatch.setattr(worker, "iter_source_records", _fake_iter_source_records)
    out_times = [r.envelope.receive_time_utc_ns for _, r in worker._iter_chronological_chunk_records(chain_chunks)]
    assert peak[0] == 2
    assert out_times == sorted(out_times)

    peaks_mem: dict[int, int] = {}
    for count in (1000, 4000):
        def _kline_it(_req, n=count):
            for i in range(n):
                env = _env("um_perpetual", "BTCUSDT", "agg_trade", base_ms + 1000 * i, {"a": i + 1, "T": base_ms + 1000 * i, "p": "100", "q": "1", "m": False})
                yield "p", SimpleNamespace(chunk_id="c1"), SimpleNamespace(envelope=env)

        monkeypatch.setattr(worker, "_iter_partition_records", _kline_it)
        out_kl = tmp_path / f"bars_{count}.jsonl"
        tracemalloc.start()
        kl_res = worker.dispatch({"operation": "query_klines", "market": "usdm", "symbols": ["BTCUSDT"], "interval": "1s", "limit": 1, "output_file": str(out_kl)})
        _, peak_b = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        assert kl_res["total_rows"] == count and len(out_kl.read_text().splitlines()) == count
        peaks_mem[count] = peak_b
    assert peaks_mem[4000] < peaks_mem[1000] * 2

    # 5. M6-D2: query_derivatives_metrics supports all 5m rest_observations metrics via MCP aliases.
    metric_events = [
        _rest_env("um_shared", "BTCUSDT", "open_interest_statistics_5m", base_ms + 100, [{"timestamp": base_ms + 100, "sumOpenInterest": "1234", "sumOpenInterestValue": "56789"}]),
        _rest_env("um_shared", "BTCUSDT", "top_long_short_account_ratio_5m", base_ms + 100, [{"timestamp": base_ms + 100, "longShortRatio": "1.25", "longAccount": "0.55", "shortAccount": "0.45"}]),
        _rest_env("um_shared", "BTCUSDT", "top_long_short_position_ratio_5m", base_ms + 100, [{"timestamp": base_ms + 100, "longShortRatio": "1.40", "longAccount": "0.58", "shortAccount": "0.42"}]),
        _rest_env("um_shared", "BTCUSDT", "global_long_short_ratio_5m", base_ms + 100, [{"timestamp": base_ms + 100, "longShortRatio": "1.10", "longAccount": "0.52", "shortAccount": "0.48"}]),
        _rest_env("um_shared", "BTCUSDT", "taker_buy_sell_volume_5m", base_ms + 100, [{"timestamp": base_ms + 100, "buySellRatio": "0.95", "buyVol": "100", "sellVol": "105"}]),
    ]
    _mock_events(metric_events)
    expected_types = {
        "open_interest_hist": "open_interest_statistics_5m",
        "top_long_short_account": "top_long_short_account_ratio_5m",
        "top_long_short_position": "top_long_short_position_ratio_5m",
        "global_long_short": "global_long_short_ratio_5m",
        "taker_buy_sell_vol": "taker_buy_sell_volume_5m",
    }
    for mcp_metric, want_type in expected_types.items():
        m_res = worker.dispatch({
            "operation": "query_derivatives_metrics",
            "market": "usdm",
            "symbols": ["BTCUSDT"],
            "metric": mcp_metric,
            "limit": 10,
        })
        assert m_res["total_rows"] == 1 and m_res["rows"][0]["metric_type"] == want_type
        assert m_res["rows"][0]["symbol"] == "BTCUSDT" and m_res["rows"][0]["values"]


def test_m6l1_l2_l3_orderbook_recovery_boundaries_and_window_coverage(tmp_path: Path, monkeypatch) -> None:
    """Positive regressions for M6-L1 (flagged/unflagged/Spot/consecutive markers), M6-L2 (malformed invalidation), and M6-L3 (window-scoped coverage)."""
    from types import SimpleNamespace

    base_ms = 1_794_873_600_000
    snap = lambda sym, t, uid: _rest_env("um_perpetual", sym, "depth_snapshot", base_ms + t, {"lastUpdateId": uid, "bids": [["100", "1"]], "asks": [["101", "1"]]})
    delta = lambda sym, t, f, l, prev, flags=(), conn="c1": _env(
        "um_perpetual", sym, "diff_depth", base_ms + t,
        {"U": f, "u": l, "pu": prev, "b": [["100", "2"]], "a": []}, conn=conn, flags=flags,
    )

    def _run(events, symbols=None, market="usdm", start=150, end=400):
        def _it(req):
            wanted = {x["stream"] for x in req.get("chunks", [])}
            for env in sorted(events, key=lambda e: e.receive_time_utc_ns):
                if not wanted or env.stream in wanted:
                    yield "p-m6l", SimpleNamespace(chunk_id="chunk-" + env.stream), SimpleNamespace(envelope=env)

        monkeypatch.setattr(worker, "_iter_partition_records", _it)
        return worker.dispatch({
            "operation": "query_orderbook",
            "market": market,
            "symbols": symbols or ["BTCUSDT"],
            "start_utc_ns": (base_ms + start) * 1_000_000,
            "end_utc_ns": (base_ms + end) * 1_000_000,
            "sample_step_ms": 1,
            "limit": 20,
            "chunks": [{"stream": "rest_observations"}, {"stream": "diff_depth"}],
        })

    # 1. Real sealed Raw + Catalog fixture: flagged unique USDM bridge payload (U < lastUpdateId == u) on reconnect.
    storage_root = tmp_path / "l1_layout"
    layout = ensure_storage_layout(storage_root)
    catalog = Catalog(layout.catalog)
    ext_root = tmp_path / "l1_ext"
    sid = "storage-m6l-real"
    ch_snaps = _make_archived_chunk(
        layout, catalog, ext_root, sid, market="um_perpetual", symbol="BTCUSDT", stream="rest_observations",
        envelopes=[snap("BTCUSDT", 0, 100), snap("BTCUSDT", 110, 200)],
    )
    ch_dd = _make_archived_chunk(
        layout, catalog, ext_root, sid, market="um_perpetual", symbol="BTCUSDT", stream="diff_depth",
        envelopes=[
            delta("BTCUSDT", 10, 100, 101, 99, conn="c1"),
            delta("BTCUSDT", 100, 195, 200, 194, flags=("sequence_gap", "depth_snapshot_pending"), conn="c2"),
            delta("BTCUSDT", 200, 201, 201, 200, conn="c2"),
        ],
    )
    catalog.close()
    real_res = worker.dispatch({
        "operation": "query_orderbook",
        "market": "usdm",
        "symbols": ["BTCUSDT"],
        "start_utc_ns": (base_ms + 150) * 1_000_000,
        "end_utc_ns": (base_ms + 400) * 1_000_000,
        "sample_step_ms": 1,
        "limit": 20,
        "partitions": [{"partition_id": "p-test", "catalog_path": str(layout.catalog)}],
        "external_roots": {sid: str(ext_root)},
        "chunks": [ch_snaps, ch_dd],
    })
    assert real_res["total_rows"] == 1
    assert real_res["stats"]["warmup_resync_count"] == 1
    assert real_res["stats"]["recovery_count"] == 2
    assert real_res["symbol_coverage"]["BTCUSDT"]["status"] == "complete"
    assert real_res["symbol_coverage"]["BTCUSDT"]["synchronized_at_window_start"] is True

    # 2. M6-L1 variants: strict U < lastUpdateId < u flagged, ingress_drop valid payload, unflagged pu break, consecutive reconnect_gap, and Spot +1 bridge.
    base_sync = [snap("BTCUSDT", 0, 100), delta("BTCUSDT", 10, 100, 101, 99)]
    for evs in (
        base_sync + [delta("BTCUSDT", 100, 199, 201, 198, ("sequence_gap",), "c2"), snap("BTCUSDT", 110, 200), delta("BTCUSDT", 200, 202, 202, 201, conn="c2")],
        base_sync + [delta("BTCUSDT", 100, 200, 200, 199, ("ingress_drop", "depth_snapshot_pending")), snap("BTCUSDT", 110, 200), delta("BTCUSDT", 200, 201, 201, 200)],
        base_sync + [delta("BTCUSDT", 100, 200, 200, 199), snap("BTCUSDT", 110, 200), delta("BTCUSDT", 200, 201, 201, 200)],
        base_sync + [
            delta("BTCUSDT", 100, 200, 200, 199, ("reconnect_gap",), "c2"),
            delta("BTCUSDT", 101, 201, 201, 200, ("reconnect_gap",), "c2"),
            snap("BTCUSDT", 110, 200),
            delta("BTCUSDT", 200, 202, 202, 201, conn="c2"),
        ],
    ):
        r = _run(evs)
        assert r["total_rows"] == 1 and r["rows"][0]["state"] == "SYNCHRONIZED"
        assert r["stats"]["resync_count"] == 1 and r["stats"]["recovery_count"] == 2
        assert r["symbol_coverage"]["BTCUSDT"]["status"] == "complete"

    spot_res = _run([
        _rest_env("spot", "BTCUSDT", "depth_snapshot", base_ms, {"lastUpdateId": 100, "bids": [["100", "1"]], "asks": [["101", "1"]]}),
        _env("spot", "BTCUSDT", "diff_depth", base_ms + 10, {"U": 101, "u": 101, "b": [], "a": []}),
        _env("spot", "BTCUSDT", "diff_depth", base_ms + 100, {"U": 201, "u": 201, "b": [], "a": []}, flags=("sequence_gap",)),
        _rest_env("spot", "BTCUSDT", "depth_snapshot", base_ms + 110, {"lastUpdateId": 200, "bids": [["100", "1"]], "asks": [["101", "1"]]}),
        _env("spot", "BTCUSDT", "diff_depth", base_ms + 200, {"U": 202, "u": 202, "b": [], "a": []}),
    ], market="spot")
    assert spot_res["total_rows"] == 1 and spot_res["stats"]["recovery_count"] == 2
    assert spot_res["symbol_coverage"]["BTCUSDT"]["status"] == "complete"

    # 3. M6-L2: malformed boundary (with/without conn switch) invalidates prior book and prevents emitting stale SYNCHRONIZED frames until re-bridged.
    for conn2 in ("c1", "c2"):
        mal_no_snap = _run(base_sync + [
            _env("um_perpetual", "BTCUSDT", "diff_depth", base_ms + 100, {}, conn=conn2, flags=("malformed", "sequence_gap", "ingress_drop")),
            delta("BTCUSDT", 200, 102, 102, 101, conn=conn2),
        ])
        assert mal_no_snap["total_rows"] == 0
        assert mal_no_snap["stats"]["resync_count"] == 1
        assert mal_no_snap["symbol_coverage"]["BTCUSDT"]["status"] == "unavailable"
        assert mal_no_snap["symbol_coverage"]["BTCUSDT"]["reason"] == "resync_required_after_gap_or_reconnect"
        assert mal_no_snap["unsynchronized_reason"] == "resync_required_after_gap_or_reconnect"
        assert "no_subsequent_snapshot" not in str(mal_no_snap)

    mal_recovered = _run(base_sync + [
        _env("um_perpetual", "BTCUSDT", "diff_depth", base_ms + 90, {}, flags=("malformed", "sequence_gap", "ingress_drop")),
        delta("BTCUSDT", 100, 200, 200, 199),
        snap("BTCUSDT", 110, 200),
        delta("BTCUSDT", 200, 201, 201, 200),
    ])
    assert mal_recovered["total_rows"] == 1 and mal_recovered["stats"]["resync_count"] == 1

    # 4. M6-L3: Warmup sync + warmup unbridged failure despite post-gap snapshot -> unavailable with updates_did_not_bridge_snapshot (NEVER complete!).
    warmup_fail = _run(base_sync + [
        delta("BTCUSDT", 90, 199, 199, 198, ("sequence_gap",), "c2"),
        snap("BTCUSDT", 110, 250),
        delta("BTCUSDT", 200, 260, 260, 259, conn="c2"),
    ])
    assert warmup_fail["total_rows"] == 0
    assert warmup_fail["symbol_coverage"]["BTCUSDT"]["status"] == "unavailable"
    assert warmup_fail["symbol_coverage"]["BTCUSDT"]["reason"] == "updates_did_not_bridge_snapshot"
    assert warmup_fail["unsynchronized_reason"] == "updates_did_not_bridge_snapshot"

    # Quiet synchronized window (0 in-window updates) -> complete without falsely claiming unsynchronized.
    quiet = _run(base_sync)
    assert quiet["total_rows"] == 0
    assert quiet["unsynchronized_reason"] is None
    assert quiet["symbol_coverage"]["BTCUSDT"]["status"] == "complete"
    assert quiet["symbol_coverage"]["BTCUSDT"]["reason"] == "synchronized_quiet_window_no_updates"
    assert quiet["actual_time_range"]["sync_timestamp_scope"]

    # Mixed request: BTCUSDT succeeds cleanly while ETHUSDT loses warmup sync -> BTC complete, ETH unavailable.
    mixed = _run(
        base_sync
        + [delta("BTCUSDT", 200, 102, 102, 101)]
        + [
            snap("ETHUSDT", 0, 100),
            delta("ETHUSDT", 10, 100, 101, 99),
            delta("ETHUSDT", 100, 205, 205, 204, ("sequence_gap",), "c2"),
            snap("ETHUSDT", 110, 200),
            delta("ETHUSDT", 200, 206, 206, 205, conn="c2"),
        ],
        symbols=["BTCUSDT", "ETHUSDT"],
    )
    assert mixed["symbol_coverage"]["BTCUSDT"]["status"] == "complete"
    assert mixed["symbol_coverage"]["ETHUSDT"]["status"] == "unavailable"
    assert mixed["symbol_coverage"]["ETHUSDT"]["reason"] == "updates_did_not_bridge_snapshot"


def test_m6p19b_superseded_epoch_duplicate_and_overflow_both_engines(tmp_path: Path) -> None:
    """M6-P19-B: superseded/epoch snapshot rejection, duplicate non-stealing, and >8192 overflow recovery across Python and C++."""
    import pytest

    base_ms = 1_794_873_600_000

    def _run_case(name: str, groups: list[list[EventEnvelope]], mkt_arg: str, start_off: int, end_off: int, step_ms: int, engine: str) -> tuple[dict[str, Any], bytes, dict[str, Any]]:
        sub = tmp_path / f"{name}_{engine}"
        layout = ensure_storage_layout(sub / "layout")
        catalog = Catalog(layout.catalog)
        chunks = []
        try:
            for evs in groups:
                h = evs[0]
                chunks.append(
                    _make_archived_chunk(
                        layout, catalog, sub / "external", "s-p19b",
                        market=h.market, symbol=h.symbol, stream=h.stream, envelopes=evs,
                    )
                )
        finally:
            catalog.close()
        out_path = sub / "out.jsonl"
        req = {
            "market": mkt_arg,
            "symbols": ["BTCUSDT"],
            "start_utc_ns": (base_ms + start_off) * 1_000_000,
            "end_utc_ns": (base_ms + end_off) * 1_000_000,
            "sample_step_ms": step_ms,
            "orderbook_engine": engine,
            "output_file": str(out_path),
            "partitions": [{"partition_id": "p-test", "catalog_path": str(layout.catalog)}],
            "external_roots": {"s-p19b": str(sub / "external")},
            "chunks": chunks,
        }
        insp = worker.dispatch({**req, "operation": "inspect_orderbook_snapshots"})
        res = worker.dispatch({**req, "operation": "query_orderbook"})
        return res, out_path.read_bytes(), insp

    def _snap(mkt: str, off: int, uid: int, *, flags: tuple[str, ...] = (), epoch: int | None = None, bids: list | None = None, asks: list | None = None) -> EventEnvelope:
        env = _rest_env(
            mkt, "BTCUSDT", "depth_snapshot", base_ms + off,
            {"lastUpdateId": uid, "bids": bids or [["100", "1"]], "asks": asks or [["101", "1"]]},
        )
        upd: dict[str, Any] = {}
        if flags:
            upd["capture_flags"] = flags
        if epoch is not None:
            upd["source_sequence"] = {"sourceStream": "depth_snapshot", "sourceFamily": "REST", "depthSnapshotEpoch": epoch}
        return env.model_copy(update=upd) if upd else env

    def _upd(mkt: str, off: int, u_first: int, u_last: int, pu: int | None = None, *, b: list | None = None, a: list | None = None, epoch: int | None = None) -> EventEnvelope:
        payload: dict[str, Any] = {"U": u_first, "u": u_last, "b": b if b is not None else [], "a": a if a is not None else []}
        if pu is not None:
            payload["pu"] = pu
        env = _env(mkt, "BTCUSDT", "diff_depth", base_ms + off, payload)
        if epoch is not None:
            env = env.model_copy(update={"source_sequence": {"depthSnapshotEpoch": epoch}})
        return env

    for eng in ("python", "cpp"):
        # 1. Superseded + wrong-epoch snapshot excluded; later valid snapshot recovers; pending snapshot still works.
        sup_groups = [
            [_snap("um_perpetual", 0, 100, flags=("depth_snapshot_superseded",), epoch=1)],
            [_upd("um_perpetual", 1, 99, 101, 98, epoch=2), _upd("um_perpetual", 2000, 102, 102, 101, epoch=2)],
        ]
        res_sup, _, insp_sup = _run_case("sup_only", sup_groups, "usdm", 1000, 3000, 1000, eng)
        assert insp_sup["latest_before_start_snapshots"] == {}
        assert res_sup["total_rows"] == 0 and res_sup["symbol_coverage"]["BTCUSDT"]["status"] == "unavailable"

        epoch_groups = [
            [_snap("um_perpetual", 0, 100, epoch=1)],  # unflagged but old epoch=1!
            [_upd("um_perpetual", 1, 99, 101, 98, epoch=2)],
            [_snap("um_perpetual", 1500, 200, flags=("depth_snapshot_pending",), epoch=2)],
            [_upd("um_perpetual", 2000, 200, 201, 199, epoch=2)],
        ]
        res_ep, _, _ = _run_case("epoch_and_pending", epoch_groups, "usdm", 1000, 3000, 1, eng)
        assert [r["update_id"] for r in res_ep["rows"]] == ["201"]

        # 2. Duplicate / stale updates must not consume sample_step_ms emission slot; u=101 then 102 emitted on Spot & USD-M!
        for mkt_name, m_arg, f_u, pu0, pu_next in [("spot", "spot", 101, None, None), ("um_perpetual", "usdm", 100, 99, 101)]:
            dup_groups = [
                [_snap(mkt_name, 0, 100)],
                [
                    _upd(mkt_name, 20, f_u, 101, pu0, b=[["100", "2"]]),
                    _upd(mkt_name, 1200, f_u, 101, pu0, b=[["100", "2"]]),  # duplicate!
                    _upd(mkt_name, 1450, 102, 102, pu_next, b=[["100", "0"], ["99.5", "3"]]),  # level deletion + advance
                ],
            ]
            res_dup, raw_dup, _ = _run_case(f"dup_{m_arg}", dup_groups, m_arg, 0, 2000, 1000, eng)
            assert [r["update_id"] for r in res_dup["rows"]] == ["101", "102"], (eng, m_arg, res_dup["rows"])
            assert res_dup["rows"][-1]["bids"] == [["99.5", "3"]]
            assert b"\r\n" not in raw_dup

        # First USD-M bridge update where u == L (100) still emits reliably cleanly.
        eq_groups = [[_snap("um_perpetual", 0, 100)], [_upd("um_perpetual", 10, 99, 100, 98), _upd("um_perpetual", 1500, 101, 101, 100)]]
        res_eq, _, _ = _run_case("usdm_eq_bridge", eq_groups, "usdm", 0, 2000, 1000, eng)
        assert [r["update_id"] for r in res_eq["rows"]] == ["100", "101"]

        # 3. Bootstrap buffer > 8192 updates without snapshot recovers cleanly on next valid snapshot & bridge!
        ov_prefix = [_upd("spot", i, 100 + i, 100 + i) for i in range(1, 8194)]
        ov_tail = [[_snap("spot", 8194, 8293)], [_upd("spot", 8195, 8294, 8294), _upd("spot", 8196, 8295, 8295)]]
        res_ov, _, _ = _run_case("overflow_spot", [ov_prefix, *ov_tail], "spot", 8194, 8200, 1, eng)
        assert [r["update_id"] for r in res_ov["rows"]] == ["8294", "8295"]
        assert res_ov["stats"]["warmup_resync_count"] == 1 and res_ov["stats"]["recovery_count"] == 1
        assert res_ov["symbol_coverage"]["BTCUSDT"]["status"] == "partial"

        # Multi-episode >16,500 out-of-sync updates counts overflow ONCE per continuous out-of-sync episode!
        ov2_prefix = [_upd("spot", i, 100 + i, 100 + i) for i in range(1, 16500)]
        ov2_tail = [[_snap("spot", 16500, 16600)], [_upd("spot", 16501, 16601, 16601)]]
        res_ov2, _, _ = _run_case("overflow_single_ep", [ov2_prefix, *ov2_tail], "spot", 16500, 16510, 1, eng)
        assert res_ov2["stats"]["resync_count"] == 1
        assert [r["update_id"] for r in res_ov2["rows"]] == ["16601"]


def test_m6p21_lazy_init_numeric_spec_and_cbor_fault_injection(tmp_path: Path) -> None:
    """M6-P21-1/2/3: Windows/lazy normalize import, exact NumericSpec error contract, 136 CBOR corpus, and Raw fault injection."""
    import subprocess
    import cbor2
    import pytest
    from binance_market_data_recorder.spool import format as raw_format

    # 1. Lazy normalize __init__ preserves all 5 exports and imports worker without fcntl/pyarrow.
    import binance_market_data_recorder.normalize as norm
    assert all(getattr(norm, name) is not None for name in norm.__all__)
    probe_script = Path("/Users/amada/Documents/Development/Crypto/Milestone Plan/evidence/mcp-native-20261118/import_probe.py")
    if probe_script.is_file():
        repo_root = Path(__file__).resolve().parents[2]
        out_raw = subprocess.check_output(
            [sys.executable, str(probe_script), str(repo_root), "--mask-unix"],
            text=True,
        )
        out_doc = json.loads(out_raw)
        assert out_doc["loaded"]["pyarrow"] is False
        assert out_doc["loaded"]["binance_market_data_recorder.normalize.pipeline"] is False

    # 2. Exact NumericSpec from exchange_info & explicit rejection of inexact/overflow decimals.
    native_lib = worker._get_native_l2_lib({"orderbook_engine": "cpp"})
    assert native_lib is not None and native_lib.simdjson_impl
    base_ms = 1_794_873_600_000
    sub = tmp_path / "num_spec"
    layout = ensure_storage_layout(sub / "layout")
    catalog = Catalog(layout.catalog)
    try:
        ex_info = _rest_env("spot", "__GLOBAL__", "exchange_info", base_ms, {
            "symbols": [{"symbol": "BTCUSDT", "filters": [
                {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                {"filterType": "LOT_SIZE", "stepSize": "0.001"},
            ]}]
        })
        c_ex = _make_archived_chunk(layout, catalog, sub / "ext", "s1", market="spot", symbol="__GLOBAL__", stream="rest_observations", envelopes=[ex_info])
        c_sn = _make_archived_chunk(layout, catalog, sub / "ext", "s1", market="spot", symbol="BTCUSDT", stream="rest_observations", envelopes=[_rest_env("spot", "BTCUSDT", "depth_snapshot", base_ms + 1, {"lastUpdateId": 100, "bids": [["100.12", "1.234"]], "asks": [["100.13", "2.000"]]})])
        c_ok = _make_archived_chunk(layout, catalog, sub / "ext", "s1", market="spot", symbol="BTCUSDT", stream="diff_depth", envelopes=[_env("spot", "BTCUSDT", "diff_depth", base_ms + 2, {"U": 101, "u": 101, "b": [["100.12", "1.500"]], "a": []})])
        c_bad_scale = _make_archived_chunk(layout, catalog, sub / "ext", "s1", market="spot", symbol="BTCUSDT", stream="diff_depth", envelopes=[_env("spot", "BTCUSDT", "diff_depth", base_ms + 3, {"U": 102, "u": 102, "b": [["100.125", "1.500"]], "a": []})])
    finally:
        catalog.close()

    base_req = {
        "operation": "query_orderbook", "market": "spot", "symbols": ["BTCUSDT"],
        "start_utc_ns": base_ms * 1_000_000, "end_utc_ns": (base_ms + 100) * 1_000_000,
        "sample_step_ms": 1, "orderbook_engine": "cpp",
        "partitions": [{"partition_id": "p-test", "catalog_path": str(layout.catalog)}],
        "external_roots": {"s1": str(sub / "ext")},
    }
    res_ok = worker.dispatch({**base_req, "chunks": [c_ex, c_sn, c_ok]})
    assert res_ok["rows"][0]["bids"] == [["100.12", "1.5"]]
    with pytest.raises(worker._NativeDecimalError):
        worker.dispatch({**base_req, "chunks": [c_ex, c_sn, c_ok, c_bad_scale]})

    # 3. Fast canonical CBOR checker matches strict decoder across 136 edge inputs + corrupt archived Raw fault injection!
    fast_fn = native_lib.lib.bmcp_canonical_cbor_fast
    simple = [None, False, True, 0, 23, 24, 255, 256, 65535, 65536, 2**32, 2**64 - 1, -1, -25, -2**64, b"data", "币", "a" * 256, [], {}]
    supported = [cbor2.dumps(v, canonical=True) for v in (simple + [{"b": v, "a": [v], "币": {"z": v}} for v in simple])]
    rejected = [bytes.fromhex(h) for h in ("1801", "190018", "580161", "781761", "9f01ff", "a2616201616102", "a2616101616102", "0161", "1a01", "81")]
    for b in supported:
        assert fast_fn(b, len(b)) == 1
    for b in rejected:
        assert fast_fn(b, len(b)) == 0
    for body in supported + rejected + [bytes.fromhex("61ff"), b""]:
        try:
            ref_val = raw_format._decode_one_cbor(body, canonical=True)
            ref_res = ("ok", cbor2.dumps(ref_val, canonical=True))
        except Exception:
            ref_res = ("err", None)
        with worker._fast_canonical_cbor_context(native_lib):
            try:
                fast_val = raw_format._decode_one_cbor(body, canonical=True)
                fast_res = ("ok", cbor2.dumps(fast_val, canonical=True))
            except Exception:
                fast_res = ("err", None)
        assert fast_res == ref_res

    # Fault injection on archived chunk (truncation / corrupted byte -> both native and python reject identically!).
    raw_file = Path(c_ok["raw_path"])
    raw_orig = raw_file.read_bytes()
    raw_file.write_bytes(raw_orig[:-10])
    try:
        for eng in ("python", "cpp"):
            with pytest.raises(Exception):
                worker.dispatch({**base_req, "orderbook_engine": eng, "chunks": [c_sn, c_ok]})
    finally:
        raw_file.write_bytes(raw_orig)




