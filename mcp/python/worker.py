#!/usr/bin/env python3
"""Offline streaming Recorder reader worker reusing BinanceMarketDataRecorder libraries.

Reads synced local catalog.sqlite + external storage_id-mapped .bmdr.zst chunks via
load_source_chunks / iter_source_records without scanning unindexed servers or
materializing large datasets in RAM.
"""

from __future__ import annotations

import base64
import csv
import datetime as dt
import json
import os
import sys
import tempfile
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterator

# Ensure Recorder baseline & prototype packages are importable cleanly.
_PARENTS = Path(__file__).resolve().parents
_DEFAULT_RECORDER_REPO = (
    _PARENTS[2]
    if len(_PARENTS) > 2 and (_PARENTS[2] / "vendor" / "python-baseline").is_dir()
    else _PARENTS[2] / "BinanceMarketDataRecorder"
)
_RECORDER_REPO = Path(os.environ.get("SBMDR_REPO_DIR", str(_DEFAULT_RECORDER_REPO)))
_BASELINE_SRC = _RECORDER_REPO / "vendor" / "python-baseline" / "src"
for _p in (str(_BASELINE_SRC), str(_RECORDER_REPO)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from binance_market_data_recorder.domain.event import EventEnvelope  # noqa: E402
from binance_market_data_recorder.normalize.raw import (  # noqa: E402
    SourceChunk,
    SourceRecord,
    iter_source_records,
    load_source_chunks,
)
from binance_market_data_recorder.orderbook.model import (  # noqa: E402
    BookSnapshot,
    DepthUpdate,
    canonical_decimal,
)
from binance_market_data_recorder.orderbook.reconstructor import (  # noqa: E402
    LocalBookReconstructor,
    ReconstructionState,
    SynchronizeResult,
)
from binance_market_data_recorder.storage.catalog import Catalog  # noqa: E402
from binance_market_data_recorder.storage.layout import StorageLayout  # noqa: E402

INTERVAL_MS_MAP: dict[str, int] = {
    "1s": 1_000,
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "2h": 7_200_000,
    "4h": 14_400_000,
    "6h": 21_600_000,
    "8h": 28_800_000,
    "12h": 43_200_000,
    "1d": 86_400_000,
}


def _d(val: Any) -> str:
    """Format Decimal-safe string without trailing noise or scientific notation."""
    if isinstance(val, Decimal):
        return canonical_decimal(val)
    return canonical_decimal(Decimal(str(val)))


def _unwrap_ws_json(envelope: EventEnvelope) -> dict[str, Any]:
    raw = json.loads(envelope.raw_payload.decode("utf-8"))
    if isinstance(raw, dict) and isinstance(raw.get("data"), dict) and "stream" in raw:
        return raw["data"]
    if isinstance(raw, dict):
        return raw
    raise ValueError(f"WS payload is not a JSON object for {envelope.stream}")


def _decode_rest_observation(envelope: EventEnvelope) -> tuple[str, dict[str, Any], Any, bool, str | None]:
    """Decode binance-public-rest-provenance.v1 cleanly across Spot & USD-M."""
    doc = json.loads(envelope.raw_payload.decode("utf-8"))
    if not isinstance(doc, dict) or doc.get("schema_version") != "binance-public-rest-provenance.v1":
        raise ValueError("expected binance-public-rest-provenance.v1 payload")
    request = doc.get("request") if isinstance(doc.get("request"), dict) else {}
    response = doc.get("response") if isinstance(doc.get("response"), dict) else {}
    source_stream = str(
        doc.get("source_stream")
        or envelope.source_sequence.get("sourceStream")
        or envelope.stream
    )
    status = response.get("status")
    valid = response.get("valid")
    if (
        "malformed" in envelope.capture_flags
        or valid is False
        or not isinstance(status, int)
        or status < 200
        or status >= 300
    ):
        err = str(response.get("error_type") or f"HTTP_{status}")
        return source_stream, request, None, False, err

    rep = doc.get("representation")
    if rep == "http-body-base64.v1":
        b64 = response.get("raw_body_base64")
        if not isinstance(b64, str):
            return source_stream, request, None, False, "MISSING_RAW_BODY_BASE64"
        model = json.loads(base64.b64decode(b64, validate=True).decode("utf-8"))
    elif rep == "sdk-parsed-json.v1":
        model = response.get("model")
    else:
        return source_stream, request, None, False, f"UNSUPPORTED_REP:{rep}"
    return source_stream, request, model, True, None


def _materialize_raw_manifest(archive_manifest_path: Path, out_dir: Path, chunk_id: str) -> Path:
    """Extract raw-manifest.v1 bytes from .archive-manifest.json into an ephemeral manifests dir."""
    doc = json.loads(archive_manifest_path.read_text(encoding="utf-8"))
    dest = out_dir / f"{chunk_id}.manifest.json"
    b64 = doc.get("raw_manifest_bytes_base64")
    if isinstance(b64, str) and b64:
        dest.write_bytes(base64.b64decode(b64, validate=True))
    elif isinstance(doc.get("raw_manifest"), dict):
        dest.write_text(json.dumps(doc["raw_manifest"], sort_keys=True), encoding="utf-8")
    elif doc.get("manifest_schema_version") == "raw-manifest.v1":
        dest.write_bytes(archive_manifest_path.read_bytes())
    else:
        raise ValueError(f"cannot extract raw-manifest.v1 from {archive_manifest_path}")
    return dest


def _iter_partition_records(req: dict[str, Any]) -> Iterator[tuple[str, SourceChunk, SourceRecord]]:
    """Build ephemeral read-only StorageLayout per partition and yield verified SourceRecords."""
    ext_roots = {
        str(k): Path(str(v))
        for k, v in (req.get("external_roots") or {}).items()
    }
    partitions = {
        str(p["partition_id"]): Path(str(p["catalog_path"]))
        for p in (req.get("partitions") or [])
    }
    chunks_by_part: dict[str, list[dict[str, Any]]] = {}
    for ch in req.get("chunks") or []:
        pid = str(ch["partition_id"])
        chunks_by_part.setdefault(pid, []).append(ch)

    for pid in sorted(partitions):
        part_chunks = chunks_by_part.get(pid)
        if not part_chunks:
            continue
        catalog_path = partitions[pid]
        with tempfile.TemporaryDirectory(prefix="bmcp-layout-") as tmpdir:
            root = Path(tmpdir)
            (root / "data" / "manifests").mkdir(parents=True, exist_ok=True)
            (root / "data" / "active").mkdir(parents=True, exist_ok=True)
            (root / "data" / "sealed").mkdir(parents=True, exist_ok=True)
            (root / "data" / "checkpoints").mkdir(parents=True, exist_ok=True)
            (root / "data" / "quarantine").mkdir(parents=True, exist_ok=True)
            (root / "data" / "reports").mkdir(parents=True, exist_ok=True)
            (root / "state").mkdir(parents=True, exist_ok=True)
            layout = StorageLayout.from_root(root)
            manifest_paths: list[Path] = []
            for ch in part_chunks:
                mp = Path(str(ch["manifest_path"]))
                cid = str(ch["chunk_id"])
                manifest_paths.append(_materialize_raw_manifest(mp, layout.manifests, cid))

            catalog = Catalog(catalog_path, read_only=True)
            try:
                loaded_chunks = load_source_chunks(
                    layout=layout,
                    catalog=catalog,
                    external_roots=ext_roots,
                    manifest_paths=manifest_paths,
                )
            finally:
                catalog.close()

            for chunk in loaded_chunks:
                for rec in iter_source_records(chunk):
                    yield pid, chunk, rec


def _write_rows(output_file: str | None, output_format: str, rows: list[dict[str, Any]]) -> str | None:
    if not output_file:
        return None
    out_path = Path(output_file)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fmt = (output_format or "jsonl").lower()
    if fmt == "csv":
        if not rows:
            out_path.write_text("", encoding="utf-8")
            return str(out_path)
        fieldnames = list(rows[0].keys())
        with out_path.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            for r in rows:
                flat = {
                    k: json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else v
                    for k, v in r.items()
                }
                writer.writerow(flat)
    elif fmt == "parquet":
        import pyarrow as pa  # type: ignore
        import pyarrow.parquet as pq  # type: ignore

        normalized_rows = [
            {
                k: json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else v
                for k, v in r.items()
            }
            for r in rows
        ]
        table = pa.Table.from_pylist(normalized_rows)
        pq.write_table(table, out_path)
    else:
        with out_path.open("w", encoding="utf-8") as fh:
            for r in rows:
                fh.write(json.dumps(r, ensure_ascii=False) + "\n")
    return str(out_path)


def _in_window(ts_ns: int, start_ns: int, end_ns: int) -> bool:
    if start_ns > 0 and ts_ns < start_ns:
        return False
    if end_ns > 0 and ts_ns >= end_ns:
        return False
    return True


def _run_inspect_overview(req: dict[str, Any]) -> dict[str, Any]:
    symbols_requested = {s.upper() for s in (req.get("symbols") or []) if s}
    observed_symbols: set[str] = set()
    streams_seen: dict[str, int] = {}
    contract_specs: dict[str, dict[str, Any]] = {}
    first_ts: int | None = None
    last_ts: int | None = None

    for _pid, chunk, rec in _iter_partition_records(req):
        env = rec.envelope
        streams_seen[env.stream] = streams_seen.get(env.stream, 0) + 1
        if first_ts is None or env.receive_time_utc_ns < first_ts:
            first_ts = env.receive_time_utc_ns
        if last_ts is None or env.receive_time_utc_ns > last_ts:
            last_ts = env.receive_time_utc_ns

        if env.symbol != "__GLOBAL__":
            observed_symbols.add(env.symbol)

        if env.stream == "rest_observations":
            try:
                src_stream, _req_meta, model, ok, _err = _decode_rest_observation(env)
            except Exception:
                continue
            if ok and src_stream == "exchange_info" and isinstance(model, dict):
                for entry in model.get("symbols") or []:
                    if not isinstance(entry, dict):
                        continue
                    sym = str(entry.get("symbol") or "").upper()
                    if not sym:
                        continue
                    if symbols_requested and sym not in symbols_requested:
                        continue
                    contract_specs[sym] = {
                        "symbol": sym,
                        "status": entry.get("status"),
                        "contract_type": entry.get("contractType"),
                        "base_asset": entry.get("baseAsset"),
                        "quote_asset": entry.get("quoteAsset"),
                        "margin_asset": entry.get("marginAsset"),
                        "price_precision": entry.get("pricePrecision"),
                        "quantity_precision": entry.get("quantityPrecision"),
                        "observed_at_utc_ns": str(env.receive_time_utc_ns),
                    }

    return {
        "operation": "inspect_overview",
        "observed_symbols": sorted(observed_symbols),
        "streams_record_counts": dict(sorted(streams_seen.items())),
        "receive_time_min_utc_ns": str(first_ts or 0),
        "receive_time_max_utc_ns": str(last_ts or 0),
        "contract_specs": contract_specs,
    }


@dataclass
class _KlineAccum:
    symbol: str
    open_ms: int
    close_ms: int
    open_px: Decimal
    high_px: Decimal
    low_px: Decimal
    close_px: Decimal
    volume: Decimal = Decimal("0")
    quote_volume: Decimal = Decimal("0")
    taker_buy_volume: Decimal = Decimal("0")
    trade_count: int = 0
    chunk_ids: set[str] = field(default_factory=set)

    def to_row(self, qty_unit: str) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "open_time_utc_ns": str(self.open_ms * 1_000_000),
            "close_time_utc_ns": str(self.close_ms * 1_000_000),
            "open": _d(self.open_px),
            "high": _d(self.high_px),
            "low": _d(self.low_px),
            "close": _d(self.close_px),
            "volume": _d(self.volume),
            "quote_volume": _d(self.quote_volume),
            "taker_buy_volume": _d(self.taker_buy_volume),
            "trade_count": self.trade_count,
            "qty_unit": qty_unit,
            "source": "recorder",
            "source_chunk_count": len(self.chunk_ids),
        }


def _run_query_klines(req: dict[str, Any]) -> dict[str, Any]:
    interval = str(req.get("interval") or "1m")
    width_ms = INTERVAL_MS_MAP.get(interval)
    if width_ms is None:
        raise ValueError(f"unsupported kline interval {interval!r}")

    symbols = {s.upper() for s in (req.get("symbols") or []) if s}
    start_ns = int(req.get("start_utc_ns") or 0)
    end_ns = int(req.get("end_utc_ns") or 0)
    limit = int(req.get("limit") or 100)
    market = str(req.get("market") or "")
    qty_unit = "contracts" if market == "coinm" else "base_asset"

    buckets: dict[tuple[str, int], _KlineAccum] = {}
    scanned_records = 0

    for _pid, chunk, rec in _iter_partition_records(req):
        env = rec.envelope
        if env.stream != "agg_trade" or "malformed" in env.capture_flags:
            continue
        sym = env.symbol.upper()
        if symbols and sym not in symbols:
            continue
        scanned_records += 1
        try:
            payload = _unwrap_ws_json(env)
            trade_ms = int(payload.get("T") or env.exchange_trade_time or (env.receive_time_utc_ns // 1_000_000))
            trade_ns = trade_ms * 1_000_000
            if not _in_window(trade_ns, start_ns, end_ns):
                continue
            price = Decimal(str(payload["p"]))
            qty = Decimal(str(payload["q"]))
            buyer_is_maker = bool(payload["m"])
        except (KeyError, ValueError, TypeError, InvalidOperation):
            continue

        b_open_ms = (trade_ms // width_ms) * width_ms
        b_close_ms = b_open_ms + width_ms - 1
        key = (sym, b_open_ms)
        acc = buckets.get(key)
        q_vol = price * qty
        tb_vol = Decimal("0") if buyer_is_maker else qty
        if acc is None:
            buckets[key] = _KlineAccum(
                symbol=sym,
                open_ms=b_open_ms,
                close_ms=b_close_ms,
                open_px=price,
                high_px=price,
                low_px=price,
                close_px=price,
                volume=qty,
                quote_volume=q_vol,
                taker_buy_volume=tb_vol,
                trade_count=1,
                chunk_ids={chunk.chunk_id},
            )
        else:
            if price > acc.high_px:
                acc.high_px = price
            if price < acc.low_px:
                acc.low_px = price
            acc.close_px = price
            acc.volume += qty
            acc.quote_volume += q_vol
            acc.taker_buy_volume += tb_vol
            acc.trade_count += 1
            acc.chunk_ids.add(chunk.chunk_id)

    rows = [buckets[k].to_row(qty_unit) for k in sorted(buckets)]
    written_file = _write_rows(req.get("output_file"), str(req.get("output_format") or "jsonl"), rows)
    return {
        "operation": "query_klines",
        "interval": interval,
        "scanned_agg_trades": scanned_records,
        "total_rows": len(rows),
        "rows": rows[:limit],
        "output_file": written_file,
    }


def _try_bridge(
    recon: LocalBookReconstructor,
    pending_snapshot: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if pending_snapshot is None or recon.is_reliable:
        return pending_snapshot
    snap = pending_snapshot["snapshot"]
    res = recon.synchronize(snap)
    if res in (SynchronizeResult.SYNCHRONIZED, SynchronizeResult.NEED_MORE_EVENTS):
        return None if res is SynchronizeResult.SYNCHRONIZED else pending_snapshot
    # SNAPSHOT_TOO_OLD / SEQUENCE_GAP -> discard old pending snapshot
    return None


def _run_query_orderbook(req: dict[str, Any]) -> dict[str, Any]:
    symbols = {s.upper() for s in (req.get("symbols") or []) if s}
    start_ns = int(req.get("start_utc_ns") or 0)
    end_ns = int(req.get("end_utc_ns") or 0)
    top_k = max(1, min(int(req.get("depth_levels") or 5), 50))
    limit = int(req.get("limit") or 100)
    sample_step_ns = max(0, int(req.get("sample_step_ms") or 1000)) * 1_000_000

    reconstructors: dict[tuple[str, str], LocalBookReconstructor] = {}
    last_conn: dict[tuple[str, str], str] = {}
    pending_snaps: dict[tuple[str, str], dict[str, Any] | None] = {}
    last_emitted_ns: dict[tuple[str, str], int] = {}

    rows: list[dict[str, Any]] = []
    unsync_events: list[dict[str, Any]] = []
    stats = {
        "snapshots_seen": 0,
        "depth_updates_seen": 0,
        "resync_count": 0,
        "reliable_emissions": 0,
    }

    snapshot_queues: dict[tuple[str, str], list[tuple[int, int, BookSnapshot, str]]] = {}

    def get_recon(mkt: str, sym: str) -> LocalBookReconstructor:
        key = (mkt, sym)
        r = reconstructors.get(key)
        if r is None:
            r = LocalBookReconstructor(mkt, sym)  # type: ignore[arg-type]
            reconstructors[key] = r
        return r

    def maybe_emit(mkt: str, sym: str, chunk_id: str, ts_ns: int, force: bool = False) -> None:
        key = (mkt, sym)
        recon = reconstructors.get(key)
        if recon is None or not recon.is_reliable:
            return
        if not _in_window(ts_ns, start_ns, end_ns):
            return
        prev_ns = last_emitted_ns.get(key, -1)
        if not force and sample_step_ns > 0 and prev_ns >= 0 and (ts_ns - prev_ns) < sample_step_ns:
            return
        book = recon.book
        bid_prices = sorted(book.bids, reverse=True)[:top_k]
        ask_prices = sorted(book.asks)[:top_k]
        bids = [[canonical_decimal(p), canonical_decimal(book.bids[p])] for p in bid_prices]
        asks = [[canonical_decimal(p), canonical_decimal(book.asks[p])] for p in ask_prices]
        bid_sum = sum((book.bids[p] for p in bid_prices), Decimal("0"))
        ask_sum = sum((book.asks[p] for p in ask_prices), Decimal("0"))
        denom = bid_sum + ask_sum
        imbalance = canonical_decimal((bid_sum - ask_sum) / denom) if denom > 0 else "0"
        best_bid = book.best_bid
        best_ask = book.best_ask
        spread = canonical_decimal(best_ask[0] - best_bid[0]) if (best_bid and best_ask) else None
        mid = canonical_decimal((best_bid[0] + best_ask[0]) / Decimal("2")) if (best_bid and best_ask) else None

        rows.append(
            {
                "symbol": sym,
                "market": mkt,
                "receive_time_utc_ns": str(ts_ns),
                "update_id": str(book.update_id),
                "mid_price": mid,
                "spread": spread,
                f"top_{top_k}_depth_imbalance": imbalance,
                "top_k": top_k,
                "bids": bids,
                "asks": asks,
                "state": "SYNCHRONIZED",
                "source": "recorder",
                "source_chunk_id": chunk_id,
            }
        )
        last_emitted_ns[key] = ts_ns
        stats["reliable_emissions"] += 1

    def drain_due_snapshots(key: tuple[str, str], up_to_ts_ns: int | None) -> None:
        q = snapshot_queues.get(key)
        if not q:
            return
        recon = get_recon(*key)
        while q and (up_to_ts_ns is None or q[0][0] <= up_to_ts_ns):
            snap_ts, _uid, snap, snap_chunk_id = q.pop(0)
            pending_snaps[key] = {"snapshot": snap, "ts_ns": snap_ts}
            pending_snaps[key] = _try_bridge(recon, pending_snaps[key])
            if recon.is_reliable:
                maybe_emit(key[0], key[1], snap_chunk_id, snap_ts, force=True)

    # First pass collects lightweight rest_observations depth_snapshots, sorted chronologically,
    # then second pass streams diff_depth in chunk order interleaved by receive_time_utc_ns.
    rest_req = {**req, "chunks": [c for c in (req.get("chunks") or []) if c.get("stream") == "rest_observations"]}
    diff_req = {**req, "chunks": [c for c in (req.get("chunks") or []) if c.get("stream") != "rest_observations"]}

    for _pid, chunk, rec in _iter_partition_records(rest_req):
        env = rec.envelope
        if env.stream != "rest_observations":
            continue
        try:
            src_stream, req_meta, model, ok, _err = _decode_rest_observation(env)
        except Exception:
            continue
        if not ok or src_stream != "depth_snapshot" or not isinstance(model, dict):
            continue
        params = req_meta.get("parameters") if isinstance(req_meta.get("parameters"), dict) else {}
        sym = str(params.get("symbol") or env.symbol).upper()
        if sym == "__GLOBAL__" or (symbols and sym not in symbols):
            continue
        mkt = "spot" if env.market == "spot" else env.market
        if mkt not in {"spot", "um_perpetual", "um_delivery", "cm_perpetual", "cm_delivery"}:
            mkt = "um_perpetual"
        try:
            snap = BookSnapshot(
                market=mkt,  # type: ignore[arg-type]
                symbol=sym,
                last_update_id=int(model["lastUpdateId"]),
                bids=tuple((str(p), str(q)) for p, q in model.get("bids") or []),
                asks=tuple((str(p), str(q)) for p, q in model.get("asks") or []),
            )
        except Exception:
            continue
        stats["snapshots_seen"] += 1
        snapshot_queues.setdefault((mkt, sym), []).append((env.receive_time_utc_ns, snap.last_update_id, snap, chunk.chunk_id))

    for q in snapshot_queues.values():
        q.sort(key=lambda item: (item[0], item[1]))

    for _pid, chunk, rec in _iter_partition_records(diff_req):
        env = rec.envelope
        if end_ns > 0 and env.receive_time_utc_ns >= end_ns and env.stream == "diff_depth":
            continue
        if env.stream != "diff_depth" or "malformed" in env.capture_flags:
            continue
        sym = env.symbol.upper()
        if symbols and sym not in symbols:
            continue
        mkt = "spot" if env.market == "spot" else env.market
        key = (mkt, sym)
        recon = get_recon(mkt, sym)
        drain_due_snapshots(key, env.receive_time_utc_ns)

        if any(f in env.capture_flags for f in ("sequence_gap", "reconnect_gap", "ingress_drop")):
            if recon.state is not ReconstructionState.RESYNC_REQUIRED:
                recon.restart_bootstrap()
                pending_snaps[key] = None
                stats["resync_count"] += 1
                unsync_events.append(
                    {
                        "symbol": sym,
                        "receive_time_utc_ns": str(env.receive_time_utc_ns),
                        "reason": "capture_flag_gap",
                    }
                )
            continue

        prev_conn = last_conn.get(key)
        if prev_conn is not None and prev_conn != env.connection_id:
            recon.restart_bootstrap()
            pending_snaps[key] = None
            stats["resync_count"] += 1
            unsync_events.append(
                {
                    "symbol": sym,
                    "receive_time_utc_ns": str(env.receive_time_utc_ns),
                    "reason": "connection_reset",
                }
            )
        last_conn[key] = env.connection_id

        try:
            payload = _unwrap_ws_json(env)
            pu_val = int(payload["pu"]) if (mkt != "spot" and "pu" in payload and payload["pu"] is not None) else None
            upd = DepthUpdate(
                market=mkt,  # type: ignore[arg-type]
                symbol=sym,
                first_update_id=int(payload["U"]),
                final_update_id=int(payload["u"]),
                previous_final_update_id=pu_val,
                bids=tuple((str(p), str(q)) for p, q in payload.get("b") or []),
                asks=tuple((str(p), str(q)) for p, q in payload.get("a") or []),
                receive_time_utc_ns=env.receive_time_utc_ns,
            )
        except Exception:
            continue

        stats["depth_updates_seen"] += 1
        applied = recon.offer(upd)
        if recon.state is ReconstructionState.RESYNC_REQUIRED and not applied:
            recon.restart_bootstrap()
            pending_snaps[key] = None
            stats["resync_count"] += 1
            unsync_events.append(
                {
                    "symbol": sym,
                    "receive_time_utc_ns": str(env.receive_time_utc_ns),
                    "reason": "sequence_gap",
                }
            )
            continue

        pending_snaps[key] = _try_bridge(recon, pending_snaps.get(key))
        if recon.is_reliable:
            maybe_emit(mkt, sym, chunk.chunk_id, env.receive_time_utc_ns)

    for key in list(snapshot_queues):
        drain_due_snapshots(key, None)

    written_file = _write_rows(req.get("output_file"), str(req.get("output_format") or "jsonl"), rows)
    return {
        "operation": "query_orderbook",
        "stats": stats,
        "unsynchronized_events": unsync_events[:50],
        "imbalance_definition": f"top_{top_k}_depth_imbalance = (sum(bid_qty_top_{top_k}) - sum(ask_qty_top_{top_k})) / (sum(bid_qty_top_{top_k}) + sum(ask_qty_top_{top_k})); NOT order-flow imbalance (OFI)",
        "total_rows": len(rows),
        "rows": rows[:limit],
        "output_file": written_file,
    }


def _run_query_trades_and_liquidations(req: dict[str, Any]) -> dict[str, Any]:
    symbols = {s.upper() for s in (req.get("symbols") or []) if s}
    start_ns = int(req.get("start_utc_ns") or 0)
    end_ns = int(req.get("end_utc_ns") or 0)
    limit = int(req.get("limit") or 100)
    kind = str(req.get("record_kind") or "all")  # all | trades | liquidations
    market = str(req.get("market") or "")
    qty_unit = "contracts" if market == "coinm" else "base_asset"

    rows: list[dict[str, Any]] = []
    saw_liq_stream = False
    saw_trade_stream = False

    for _pid, chunk, rec in _iter_partition_records(req):
        env = rec.envelope
        if "malformed" in env.capture_flags:
            continue
        sym = env.symbol.upper()
        if symbols and sym not in symbols:
            continue

        if env.stream == "agg_trade" and kind in ("all", "trades"):
            saw_trade_stream = True
            try:
                p = _unwrap_ws_json(env)
                t_ms = int(p.get("T") or env.exchange_trade_time or (env.receive_time_utc_ns // 1_000_000))
                t_ns = t_ms * 1_000_000
                if not _in_window(t_ns, start_ns, end_ns):
                    continue
                rows.append(
                    {
                        "record_type": "agg_trade",
                        "symbol": sym,
                        "trade_time_utc_ns": str(t_ns),
                        "receive_time_utc_ns": str(env.receive_time_utc_ns),
                        "agg_trade_id": str(p["a"]),
                        "price": str(p["p"]),
                        "quantity": str(p["q"]),
                        "buyer_is_maker": bool(p["m"]),
                        "qty_unit": qty_unit,
                        "source": "recorder",
                        "source_chunk_id": chunk.chunk_id,
                    }
                )
            except Exception:
                continue

        elif env.stream == "liquidation" and kind in ("all", "liquidations"):
            saw_liq_stream = True
            try:
                p = _unwrap_ws_json(env)
                o = p.get("o")
                if not isinstance(o, dict):
                    continue
                t_ms = int(o.get("T") or p.get("E") or (env.receive_time_utc_ns // 1_000_000))
                t_ns = t_ms * 1_000_000
                if not _in_window(t_ns, start_ns, end_ns):
                    continue
                rows.append(
                    {
                        "record_type": "liquidation",
                        "symbol": sym,
                        "order_trade_time_utc_ns": str(t_ns),
                        "receive_time_utc_ns": str(env.receive_time_utc_ns),
                        "side": str(o.get("S")),
                        "order_type": str(o.get("o")),
                        "price": str(o.get("p")),
                        "average_price": str(o.get("ap")) if o.get("ap") is not None else None,
                        "original_quantity": str(o.get("q")),
                        "filled_quantity": str(o.get("z")),
                        "order_status": str(o.get("X")),
                        "qty_unit": qty_unit,
                        "source": "recorder",
                        "source_chunk_id": chunk.chunk_id,
                    }
                )
            except Exception:
                continue

    liq_status = "observed"
    if market == "spot" and kind in ("all", "liquidations"):
        liq_status = "not_applicable_for_spot"
    elif not saw_liq_stream and kind in ("all", "liquidations"):
        liq_status = "no_liquidation_chunks_or_events_in_window"

    written_file = _write_rows(req.get("output_file"), str(req.get("output_format") or "jsonl"), rows)
    return {
        "operation": "query_trades_and_liquidations",
        "saw_trade_stream": saw_trade_stream,
        "saw_liquidation_stream": saw_liq_stream,
        "liquidation_status": liq_status,
        "total_rows": len(rows),
        "rows": rows[:limit],
        "output_file": written_file,
    }


def _run_query_derivatives_metrics(req: dict[str, Any]) -> dict[str, Any]:
    symbols = {s.upper() for s in (req.get("symbols") or []) if s}
    start_ns = int(req.get("start_utc_ns") or 0)
    end_ns = int(req.get("end_utc_ns") or 0)
    limit = int(req.get("limit") or 100)
    rows: list[dict[str, Any]] = []
    contract_specs: dict[str, dict[str, Any]] = {}

    for _pid, chunk, rec in _iter_partition_records(req):
        env = rec.envelope
        if "malformed" in env.capture_flags:
            continue

        if env.stream == "mark_price":
            sym = env.symbol.upper()
            if symbols and sym not in symbols:
                continue
            try:
                p = _unwrap_ws_json(env)
                e_ms = int(p.get("E") or (env.receive_time_utc_ns // 1_000_000))
                e_ns = e_ms * 1_000_000
                if not _in_window(e_ns, start_ns, end_ns):
                    continue
                rows.append(
                    {
                        "metric_type": "mark_price_funding",
                        "symbol": sym,
                        "event_time_utc_ns": str(e_ns),
                        "receive_time_utc_ns": str(env.receive_time_utc_ns),
                        "mark_price": str(p["p"]),
                        "index_price": str(p["i"]),
                        "estimated_settle_price": str(p.get("P", "")),
                        "funding_rate": str(p["r"]),
                        "next_funding_time_utc_ns": str(int(p["T"]) * 1_000_000),
                        "source": "recorder",
                        "source_chunk_id": chunk.chunk_id,
                    }
                )
            except Exception:
                continue

        elif env.stream == "rest_observations":
            try:
                src_stream, req_meta, model, ok, _err = _decode_rest_observation(env)
            except Exception:
                continue
            if not ok:
                continue
            params = req_meta.get("parameters") if isinstance(req_meta.get("parameters"), dict) else {}
            scope_sym = str(params.get("symbol") or env.symbol).upper()

            if src_stream == "exchange_info" and isinstance(model, dict):
                for entry in model.get("symbols") or []:
                    if not isinstance(entry, dict):
                        continue
                    sym = str(entry.get("symbol") or "").upper()
                    if not sym or (symbols and sym not in symbols):
                        continue
                    contract_specs[sym] = {
                        "symbol": sym,
                        "status": entry.get("status"),
                        "contract_type": entry.get("contractType"),
                        "base_asset": entry.get("baseAsset"),
                        "quote_asset": entry.get("quoteAsset"),
                        "margin_asset": entry.get("marginAsset"),
                    }
            elif src_stream == "open_interest" and isinstance(model, dict):
                sym = str(model.get("symbol") or scope_sym).upper()
                if symbols and sym not in symbols:
                    continue
                ts_ns = int(model.get("time") or (env.receive_time_utc_ns // 1_000_000)) * 1_000_000
                if not _in_window(ts_ns, start_ns, end_ns):
                    continue
                rows.append(
                    {
                        "metric_type": "open_interest",
                        "symbol": sym,
                        "event_time_utc_ns": str(ts_ns),
                        "receive_time_utc_ns": str(env.receive_time_utc_ns),
                        "open_interest": str(model.get("openInterest")),
                        "source": "recorder",
                        "source_chunk_id": chunk.chunk_id,
                    }
                )
            elif src_stream in {
                "open_interest_statistics_5m",
                "top_long_short_account_ratio_5m",
                "top_long_short_position_ratio_5m",
                "global_long_short_ratio_5m",
                "taker_buy_sell_volume_5m",
            } and isinstance(model, list):
                if symbols and scope_sym not in symbols:
                    continue
                for item in model:
                    if not isinstance(item, dict):
                        continue
                    ts_ns = int(item.get("timestamp") or (env.receive_time_utc_ns // 1_000_000)) * 1_000_000
                    if not _in_window(ts_ns, start_ns, end_ns):
                        continue
                    rows.append(
                        {
                            "metric_type": src_stream,
                            "symbol": str(item.get("symbol") or scope_sym).upper(),
                            "event_time_utc_ns": str(ts_ns),
                            "receive_time_utc_ns": str(env.receive_time_utc_ns),
                            "values": item,
                            "source": "recorder",
                            "source_chunk_id": chunk.chunk_id,
                        }
                    )

    written_file = _write_rows(req.get("output_file"), str(req.get("output_format") or "jsonl"), rows)
    return {
        "operation": "query_derivatives_metrics",
        "contract_specs": contract_specs,
        "total_rows": len(rows),
        "rows": rows[:limit],
        "output_file": written_file,
    }


def dispatch(req: dict[str, Any]) -> dict[str, Any]:
    op = req.get("operation")
    if op == "inspect_overview":
        return _run_inspect_overview(req)
    if op == "query_klines":
        return _run_query_klines(req)
    if op == "query_orderbook":
        return _run_query_orderbook(req)
    if op == "query_trades_and_liquidations":
        return _run_query_trades_and_liquidations(req)
    if op == "query_derivatives_metrics":
        return _run_query_derivatives_metrics(req)
    raise ValueError(f"unsupported worker operation: {op!r}")


def main() -> int:
    raw = sys.stdin.read()
    if not raw.strip():
        print(json.dumps({"error": "empty worker request"}))
        return 1
    try:
        req = json.loads(raw)
        res = dispatch(req)
        print(json.dumps(res, ensure_ascii=False))
        return 0
    except Exception as exc:
        print(json.dumps({"error": str(exc)}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
