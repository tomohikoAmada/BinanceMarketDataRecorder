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

