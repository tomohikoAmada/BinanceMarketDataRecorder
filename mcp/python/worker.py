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
import heapq
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


def _chunk_sort_key(chunk: SourceChunk) -> tuple[int, int, int, str]:
    """Order SourceChunks chronologically using manifest receive time bounds rather than sha256."""
    m = chunk.manifest if isinstance(chunk.manifest, dict) else {}
    r = m.get("receive_time_utc_range_ns")
    created_ns = int(m.get("created_at_utc_ns") or 0)
    if isinstance(r, dict) and r.get("max"):
        return (int(r.get("min") or created_ns), int(r.get("max") or created_ns), created_ns, chunk.chunk_id)
    return (created_ns, created_ns, created_ns, chunk.chunk_id)


def _chunk_time_bounds_ns(chunk: SourceChunk) -> tuple[int, int]:
    m = chunk.manifest if isinstance(chunk.manifest, dict) else {}
    r = m.get("receive_time_utc_range_ns")
    created_ns = int(m.get("created_at_utc_ns") or 0)
    if isinstance(r, dict) and r.get("max"):
        return (int(r.get("min") or created_ns), int(r.get("max") or created_ns))
    return (created_ns, created_ns)


def _close_iter(it: Any) -> None:
    close_fn = getattr(it, "close", None)
    if callable(close_fn):
        try:
            close_fn()
        except Exception:
            pass


def _iter_chronological_chunk_records(loaded_chunks: list[SourceChunk]) -> Iterator[tuple[SourceChunk, SourceRecord]]:
    """Yield (SourceChunk, SourceRecord) ordered chronologically, lazily opening overlapping chunks only on demand."""
    ordered_chunks = sorted(loaded_chunks, key=_chunk_sort_key)
    n = len(ordered_chunks)
    next_idx = 0
    # Heap items: (receive_time_utc_ns, chunk_idx, rec_seq, chunk, rec, it)
    heap: list[tuple[int, int, int, SourceChunk, SourceRecord, Iterator[SourceRecord]]] = []

    def _activate_chunk(cidx: int) -> None:
        chunk = ordered_chunks[cidx]
        it = iter_source_records(chunk)
        try:
            first_rec = next(it)
        except StopIteration:
            _close_iter(it)
            return
        heapq.heappush(heap, (first_rec.envelope.receive_time_utc_ns, cidx, 0, chunk, first_rec, it))

    try:
        while next_idx < n or heap:
            while not heap and next_idx < n:
                _activate_chunk(next_idx)
                next_idx += 1
            if not heap:
                break
            # Activate only chunks whose min time bound <= earliest buffered heap timestamp.
            # Adjacent non-simultaneous overlapping chunks stay closed until the heap reaches them.
            while next_idx < n and _chunk_time_bounds_ns(ordered_chunks[next_idx])[0] <= heap[0][0]:
                _activate_chunk(next_idx)
                next_idx += 1

            ts_ns, cidx, seq, chunk, rec, it = heapq.heappop(heap)
            yield chunk, rec
            try:
                nxt = next(it)
                heapq.heappush(heap, (nxt.envelope.receive_time_utc_ns, cidx, seq + 1, chunk, nxt, it))
            except StopIteration:
                _close_iter(it)
    finally:
        for item in heap:
            _close_iter(item[5])
        heap.clear()


def _iter_partition_records(req: dict[str, Any]) -> Iterator[tuple[str, SourceChunk, SourceRecord]]:
    """Build ephemeral read-only StorageLayout per partition and yield verified SourceRecords in chronological order."""
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

    def _part_sort_key(pid: str) -> tuple[int, str]:
        pchunks = chunks_by_part.get(pid) or []
        return (len(pchunks), pid)

    # Order partitions chronologically by earliest chunk manifest timestamp if possible.
    part_order_keys: dict[str, tuple[int, str]] = {}
    for pid, pchunks in chunks_by_part.items():
        min_ts: int | None = None
        for ch in pchunks:
            try:
                doc = json.loads(Path(str(ch["manifest_path"])).read_text(encoding="utf-8"))
                rm = doc.get("raw_manifest") if isinstance(doc.get("raw_manifest"), dict) else doc
                r = rm.get("receive_time_utc_range_ns") if isinstance(rm, dict) else None
                t = int(r["min"]) if isinstance(r, dict) and "min" in r else int(rm.get("created_at_utc_ns") or 0)
                if min_ts is None or (t > 0 and t < min_ts):
                    min_ts = t
            except Exception:
                continue
        part_order_keys[pid] = (min_ts if min_ts is not None else 0, pid)

    for pid in sorted(partitions, key=lambda p: part_order_keys.get(p, (0, p))):
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

            for chunk, rec in _iter_chronological_chunk_records(loaded_chunks):
                yield pid, chunk, rec


_KLINES_FIELDS = [
    "symbol",
    "open_time_utc_ns",
    "close_time_utc_ns",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_volume",
    "taker_buy_volume",
    "trade_count",
    "qty_unit",
    "source",
    "source_chunk_count",
]
_KLINES_PARQUET_TYPES = {"trade_count": "int64", "source_chunk_count": "int64"}

_TRADES_LIQ_FIELDS = [
    "record_type",
    "symbol",
    "trade_time_utc_ns",
    "order_trade_time_utc_ns",
    "receive_time_utc_ns",
    "agg_trade_id",
    "price",
    "average_price",
    "quantity",
    "original_quantity",
    "filled_quantity",
    "buyer_is_maker",
    "side",
    "order_type",
    "order_status",
    "qty_unit",
    "source",
    "source_chunk_id",
]
_TRADES_LIQ_PARQUET_TYPES = {"buyer_is_maker": "bool"}

_METRICS_FIELDS = [
    "metric_type",
    "symbol",
    "event_time_utc_ns",
    "receive_time_utc_ns",
    "mark_price",
    "index_price",
    "estimated_settle_price",
    "funding_rate",
    "next_funding_time_utc_ns",
    "open_interest",
    "values",
    "source",
    "source_chunk_id",
]


def _orderbook_fields(top_k: int) -> tuple[list[str], dict[str, str]]:
    return (
        [
            "symbol",
            "market",
            "receive_time_utc_ns",
            "update_id",
            "mid_price",
            "spread",
            f"top_{top_k}_depth_imbalance",
            "top_k",
            "bids",
            "asks",
            "state",
            "source",
            "source_chunk_id",
        ],
        {"top_k": "int64"},
    )


class _RowStreamWriter:
    """Streaming incremental writer (.part -> atomic replace) for JSONL, CSV, and batched Parquet."""

    def __init__(
        self,
        output_file: str | None,
        output_format: str,
        batch_size: int = 4096,
        fieldnames: list[str] | None = None,
        parquet_field_types: dict[str, str] | None = None,
    ) -> None:
        self.output_file = output_file
        self.fmt = (output_format or "jsonl").lower()
        self.batch_size = max(64, batch_size)
        self.fieldnames = list(fieldnames) if fieldnames else None
        self.parquet_field_types = dict(parquet_field_types or {})
        self.out_path: Path | None = Path(output_file) if output_file else None
        self.part_path: Path | None = (
            self.out_path.with_name(f".{self.out_path.name}.{os.getpid()}.part")
            if self.out_path is not None
            else None
        )
        self._fh: Any = None
        self._csv_writer: csv.DictWriter[str] | None = None
        self._pq_writer: Any = None
        self._pq_schema: Any = None
        self._pq_batch: list[dict[str, Any]] = []
        self._pa: Any = None
        self._pq: Any = None

    def _build_parquet_schema(self) -> Any:
        if not self.fieldnames:
            return None
        fields = []
        for col in self.fieldnames:
            tname = self.parquet_field_types.get(col, "string")
            if tname == "bool":
                pa_type = self._pa.bool_()
            elif tname == "int64":
                pa_type = self._pa.int64()
            else:
                pa_type = self._pa.string()
            fields.append(self._pa.field(col, pa_type, nullable=True))
        return self._pa.schema(fields)

    def __enter__(self) -> _RowStreamWriter:
        if self.out_path is not None and self.part_path is not None:
            self.out_path.parent.mkdir(parents=True, exist_ok=True)
            if self.fmt == "parquet":
                import pyarrow as pa  # type: ignore
                import pyarrow.parquet as pq  # type: ignore

                self._pa = pa
                self._pq = pq
                self._pq_schema = self._build_parquet_schema()
            elif self.fmt == "csv":
                self._fh = self.part_path.open("w", encoding="utf-8", newline="")
                if self.fieldnames:
                    self._csv_writer = csv.DictWriter(self._fh, fieldnames=self.fieldnames, restval="")
                    self._csv_writer.writeheader()
            else:
                self._fh = self.part_path.open("w", encoding="utf-8")
        return self

    def write(self, row: dict[str, Any]) -> None:
        if self.part_path is None:
            return
        if self.fmt == "csv":
            assert self._fh is not None
            flat = {
                k: json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else v
                for k, v in row.items()
            }
            if self._csv_writer is None:
                cols = self.fieldnames if self.fieldnames else list(flat.keys())
                self._csv_writer = csv.DictWriter(self._fh, fieldnames=cols, restval="")
                self._csv_writer.writeheader()
            if self.fieldnames:
                projected = {k: ("" if flat.get(k) is None else flat.get(k)) for k in self.fieldnames}
                self._csv_writer.writerow(projected)
            else:
                self._csv_writer.writerow(flat)
        elif self.fmt == "parquet":
            flat = {
                k: json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else v
                for k, v in row.items()
            }
            if self.fieldnames:
                projected: dict[str, Any] = {}
                for k in self.fieldnames:
                    val = flat.get(k)
                    if val is None:
                        projected[k] = None
                    elif self.parquet_field_types.get(k) == "bool":
                        projected[k] = bool(val)
                    elif self.parquet_field_types.get(k) == "int64":
                        projected[k] = int(val)
                    else:
                        projected[k] = str(val)
                self._pq_batch.append(projected)
            else:
                self._pq_batch.append(flat)
            if len(self._pq_batch) >= self.batch_size:
                self._flush_parquet_batch()
        else:
            assert self._fh is not None
            self._fh.write(json.dumps(row, ensure_ascii=False) + "\n")

    def _flush_parquet_batch(self) -> None:
        if not self._pq_batch or self.part_path is None:
            return
        if self._pq_schema is not None:
            table = self._pa.Table.from_pylist(self._pq_batch, schema=self._pq_schema)
        else:
            table = self._pa.Table.from_pylist(self._pq_batch)
            if self._pq_writer is None:
                self._pq_schema = table.schema
        if self._pq_writer is None:
            self._pq_writer = self._pq.ParquetWriter(self.part_path, table.schema)
        self._pq_writer.write_table(table)
        self._pq_batch.clear()

    def close_and_commit(self) -> str | None:
        if self.out_path is None or self.part_path is None:
            return None
        try:
            if self.fmt == "parquet":
                self._flush_parquet_batch()
                if self._pq_writer is not None:
                    self._pq_writer.close()
                    self._pq_writer = None
                elif self._pq_schema is not None:
                    empty_tbl = self._pa.Table.from_pylist([], schema=self._pq_schema)
                    self._pq.write_table(empty_tbl, self.part_path)
                else:
                    self._pq.write_table(self._pa.table({}), self.part_path)
            elif self._fh is not None:
                self._fh.flush()
                self._fh.close()
                self._fh = None
            os.replace(self.part_path, self.out_path)
            return str(self.out_path)
        except Exception:
            self._cleanup_abort()
            raise

    def _cleanup_abort(self) -> None:
        try:
            if self._pq_writer is not None:
                self._pq_writer.close()
        except Exception:
            pass
        self._pq_writer = None
        try:
            if self._fh is not None:
                self._fh.close()
        except Exception:
            pass
        self._fh = None
        if self.part_path is not None and self.part_path.exists():
            try:
                self.part_path.unlink()
            except Exception:
                pass

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if exc_type is not None:
            self._cleanup_abort()


def _write_rows(output_file: str | None, output_format: str, rows: list[dict[str, Any]]) -> str | None:
    if not output_file:
        return None
    with _RowStreamWriter(output_file, output_format) as w:
        for r in rows:
            w.write(r)
        return w.close_and_commit()


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
    first_key: tuple[int, int]
    last_key: tuple[int, int]
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
    observed_symbols: set[str] = set()
    scanned_records = 0
    preview_rows: list[dict[str, Any]] = []
    total_rows = 0
    max_seen_receive_ms = 0
    # Bound active buckets by flushing finalized bars whose close time trails receive watermark.
    flush_lag_ms = 60_000

    def _emit_bar(writer: _RowStreamWriter, acc: _KlineAccum) -> None:
        nonlocal total_rows
        row = acc.to_row(qty_unit)
        writer.write(row)
        total_rows += 1
        if len(preview_rows) < limit:
            preview_rows.append(row)

    def _flush_due(writer: _RowStreamWriter, up_to_close_ms: int | None) -> None:
        if not buckets:
            return
        if up_to_close_ms is None:
            due = list(buckets.keys())
        else:
            due = [k for k, acc in buckets.items() if acc.close_ms < up_to_close_ms]
        if not due:
            return
        due.sort(key=lambda k: (buckets[k].open_ms, buckets[k].symbol))
        for k in due:
            acc = buckets.pop(k)
            _emit_bar(writer, acc)

    with _RowStreamWriter(
        req.get("output_file"),
        str(req.get("output_format") or "jsonl"),
        fieldnames=_KLINES_FIELDS,
        parquet_field_types=_KLINES_PARQUET_TYPES,
    ) as writer:
        for _pid, chunk, rec in _iter_partition_records(req):
            env = rec.envelope
            if env.receive_time_utc_ns > 0:
                recv_ms = env.receive_time_utc_ns // 1_000_000
                if recv_ms > max_seen_receive_ms:
                    max_seen_receive_ms = recv_ms
            if env.stream != "agg_trade" or "malformed" in env.capture_flags:
                continue
            sym = env.symbol.upper()
            if symbols and sym not in symbols:
                continue
            observed_symbols.add(sym)
            scanned_records += 1
            try:
                payload = _unwrap_ws_json(env)
                trade_ms = int(payload.get("T") or env.exchange_trade_time or (env.receive_time_utc_ns // 1_000_000))
                trade_ns = trade_ms * 1_000_000
                if not _in_window(trade_ns, start_ns, end_ns):
                    continue
                agg_id = int(payload.get("a") or 0)
                price = Decimal(str(payload["p"]))
                qty = Decimal(str(payload["q"]))
                buyer_is_maker = bool(payload["m"])
            except (KeyError, ValueError, TypeError, InvalidOperation, json.JSONDecodeError):
                continue

            t_key = (trade_ms, agg_id)
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
                    first_key=t_key,
                    last_key=t_key,
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
                if t_key < acc.first_key:
                    acc.first_key = t_key
                    acc.open_px = price
                if t_key >= acc.last_key:
                    acc.last_key = t_key
                    acc.close_px = price
                acc.volume += qty
                acc.quote_volume += q_vol
                acc.taker_buy_volume += tb_vol
                acc.trade_count += 1
                acc.chunk_ids.add(chunk.chunk_id)

            if len(buckets) >= 128 and max_seen_receive_ms > flush_lag_ms:
                _flush_due(writer, max_seen_receive_ms - flush_lag_ms)

        _flush_due(writer, None)
        written_file = writer.close_and_commit()

    return {
        "operation": "query_klines",
        "interval": interval,
        "scanned_agg_trades": scanned_records,
        "observed_symbols": sorted(observed_symbols),
        "total_rows": total_rows,
        "rows": preview_rows,
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


def _run_inspect_orderbook_snapshots(req: dict[str, Any]) -> dict[str, Any]:
    """Lightweight pre-scan of rest_observations chunks to locate latest depth_snapshot timestamps."""
    symbols = {s.upper() for s in (req.get("symbols") or []) if s}
    start_ns = int(req.get("start_utc_ns") or 0)
    end_ns = int(req.get("end_utc_ns") or 0)
    latest_before_start: dict[str, int] = {}
    latest_in_window: dict[str, int] = {}
    earliest_any: dict[str, int] = {}
    snapshots_found = 0

    for _pid, _chunk, rec in _iter_partition_records(req):
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
        ts = env.receive_time_utc_ns
        if end_ns > 0 and ts >= end_ns:
            continue
        snapshots_found += 1
        if sym not in earliest_any or ts < earliest_any[sym]:
            earliest_any[sym] = ts
        if start_ns <= 0 or ts <= start_ns:
            if sym not in latest_before_start or ts > latest_before_start[sym]:
                latest_before_start[sym] = ts
        elif sym not in latest_in_window or ts > latest_in_window[sym]:
            latest_in_window[sym] = ts

    return {
        "operation": "inspect_orderbook_snapshots",
        "snapshots_found": snapshots_found,
        "latest_before_start_ns": {k: str(v) for k, v in sorted(latest_before_start.items())},
        "latest_in_window_ns": {k: str(v) for k, v in sorted(latest_in_window.items())},
        "earliest_snapshot_ns": {k: str(v) for k, v in sorted(earliest_any.items())},
    }


def _run_query_orderbook(req: dict[str, Any]) -> dict[str, Any]:
    symbols = {s.upper() for s in (req.get("symbols") or []) if s}
    start_ns = int(req.get("start_utc_ns") or 0)
    end_ns = int(req.get("end_utc_ns") or 0)
    top_k = max(1, min(int(req.get("depth_levels") or 5), 50))
    limit = int(req.get("limit") or 100)
    sample_step_ns = max(0, int(req.get("sample_step_ms") or 1000)) * 1_000_000

    reconstructors: dict[tuple[str, str], LocalBookReconstructor] = {}
    last_conn: dict[tuple[str, str], str] = {}
    last_event_ts: dict[tuple[str, str], int] = {}
    gap_episode_open: dict[tuple[str, str], bool] = {}
    pending_snaps: dict[tuple[str, str], dict[str, Any] | None] = {}
    last_emitted_ns: dict[tuple[str, str], int] = {}
    buffered_upds: dict[tuple[str, str], list[tuple[int, int, str]]] = {}
    snap_timestamps_by_sym: dict[str, list[int]] = {}
    first_sync_ns: int | None = None
    last_sync_ns: int | None = None
    in_window_depth_seen: dict[str, int] = {}

    # Per-symbol tracking for M6-R4 coverage.
    sym_snapshots_seen: dict[str, int] = {}
    sym_depth_updates_seen: dict[str, int] = {}
    sym_emissions: dict[str, int] = {}
    sym_warmup_resyncs: dict[str, int] = {}
    sym_window_resyncs: dict[str, int] = {}
    sym_recoveries: dict[str, int] = {}
    sym_reliable_at_start: dict[str, bool] = {}
    sym_in_window_unsync_updates: dict[str, int] = {}
    sym_mid_window_partial: dict[str, bool] = {}
    sym_first_sync_ns: dict[str, int] = {}
    sym_last_sync_ns: dict[str, int] = {}

    preview_rows: list[dict[str, Any]] = []
    unsync_events: list[dict[str, Any]] = []
    stats: dict[str, Any] = {
        "snapshots_seen": 0,
        "depth_updates_seen": 0,
        "resync_count": 0,
        "warmup_resync_count": 0,
        "window_resync_count": 0,
        "recovery_count": 0,
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

    def record_resync(sym: str, ts_ns: int, reason: str) -> None:
        stats["resync_count"] += 1
        if start_ns > 0 and ts_ns < start_ns:
            stats["warmup_resync_count"] += 1
            sym_warmup_resyncs[sym] = sym_warmup_resyncs.get(sym, 0) + 1
            sym_reliable_at_start[sym] = False
            phase = "warmup"
        else:
            stats["window_resync_count"] += 1
            sym_window_resyncs[sym] = sym_window_resyncs.get(sym, 0) + 1
            sym_mid_window_partial[sym] = True
            phase = "window"
        unsync_events.append(
            {
                "symbol": sym,
                "receive_time_utc_ns": str(ts_ns),
                "reason": reason,
                "phase": phase,
            }
        )

    def _ready_ts_and_chunk(key: tuple[str, str], snap_ts: int, snap_uid: int, snap_chunk_id: str) -> tuple[int, str]:
        target_uid = snap_uid + 1 if key[0] == "spot" else snap_uid
        matched = [item for item in buffered_upds.get(key, []) if item[0] >= target_uid]
        if matched:
            _uid, max_upd_ts, last_cid = max(matched, key=lambda x: (x[1], x[0]))
            if max_upd_ts >= snap_ts:
                return max_upd_ts, last_cid
        return snap_ts, snap_chunk_id

    def _mark_synchronized(sym: str, snap_ts: int, ready_ts_ns: int) -> None:
        nonlocal first_sync_ns, last_sync_ns
        if first_sync_ns is None or ready_ts_ns < first_sync_ns:
            first_sync_ns = ready_ts_ns
        if last_sync_ns is None or ready_ts_ns > last_sync_ns:
            last_sync_ns = ready_ts_ns
        if sym not in sym_first_sync_ns or ready_ts_ns < sym_first_sync_ns[sym]:
            sym_first_sync_ns[sym] = ready_ts_ns
        if sym not in sym_last_sync_ns or ready_ts_ns > sym_last_sync_ns[sym]:
            sym_last_sync_ns[sym] = ready_ts_ns
        if start_ns <= 0 or ready_ts_ns < start_ns:
            sym_reliable_at_start[sym] = True
        else:
            if not sym_reliable_at_start.get(sym, False):
                if snap_ts >= start_ns or sym_in_window_unsync_updates.get(sym, 0) > 0:
                    sym_mid_window_partial[sym] = True

    def write_frame(writer: _RowStreamWriter, mkt: str, sym: str, chunk_id: str, ts_ns: int, force: bool = False) -> None:
        nonlocal first_sync_ns, last_sync_ns
        key = (mkt, sym)
        recon = reconstructors.get(key)
        if recon is None or not recon.is_reliable:
            return
        if first_sync_ns is None or ts_ns < first_sync_ns:
            first_sync_ns = ts_ns
        if last_sync_ns is None or ts_ns > last_sync_ns:
            last_sync_ns = ts_ns
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

        row = {
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
        writer.write(row)
        if len(preview_rows) < limit:
            preview_rows.append(row)
        last_emitted_ns[key] = ts_ns
        stats["reliable_emissions"] += 1
        sym_emissions[sym] = sym_emissions.get(sym, 0) + 1

    def drain_due_snapshots(writer: _RowStreamWriter, key: tuple[str, str], up_to_ts_ns: int | None) -> None:
        q = snapshot_queues.get(key)
        if not q:
            return
        recon = get_recon(*key)
        while q and (up_to_ts_ns is None or q[0][0] <= up_to_ts_ns):
            was_reliable = recon.is_reliable
            snap_ts, snap_uid, snap, snap_chunk_id = q.pop(0)
            if was_reliable:
                continue
            pending_snaps[key] = {"snapshot": snap, "ts_ns": snap_ts, "uid": snap_uid, "chunk_id": snap_chunk_id}
            pending_snaps[key] = _try_bridge(recon, pending_snaps[key])
            if recon.is_reliable:
                gap_episode_open[key] = False
                stats["recovery_count"] += 1
                sym_recoveries[key[1]] = sym_recoveries.get(key[1], 0) + 1
                ready_ts_ns, ready_cid = _ready_ts_and_chunk(key, snap_ts, snap_uid, snap_chunk_id)
                buffered_upds[key] = []
                _mark_synchronized(key[1], snap_ts, ready_ts_ns)
                write_frame(writer, key[0], key[1], ready_cid, ready_ts_ns, force=True)

    # First pass collects lightweight rest_observations depth_snapshots, sorted chronologically,
    # then second pass streams diff_depth chronologically.
    rest_req = {**req, "chunks": [c for c in (req.get("chunks") or []) if c.get("stream") == "rest_observations"]}
    diff_req = {**req, "chunks": [c for c in (req.get("chunks") or []) if c.get("stream") != "rest_observations"]}

    for _pid, chunk, rec in _iter_partition_records(rest_req):
        env = rec.envelope
        if env.stream != "rest_observations":
            continue
        if end_ns > 0 and env.receive_time_utc_ns >= end_ns:
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
        sym_snapshots_seen[sym] = sym_snapshots_seen.get(sym, 0) + 1
        snap_timestamps_by_sym.setdefault(sym, []).append(env.receive_time_utc_ns)
        snapshot_queues.setdefault((mkt, sym), []).append((env.receive_time_utc_ns, snap.last_update_id, snap, chunk.chunk_id))

    for q in snapshot_queues.values():
        q.sort(key=lambda item: (item[0], item[1]))

    ob_fields, ob_types = _orderbook_fields(top_k)
    with _RowStreamWriter(
        req.get("output_file"),
        str(req.get("output_format") or "jsonl"),
        fieldnames=ob_fields,
        parquet_field_types=ob_types,
    ) as writer:
        for _pid, chunk, rec in _iter_partition_records(diff_req):
            env = rec.envelope
            if end_ns > 0 and env.receive_time_utc_ns >= end_ns and env.stream == "diff_depth":
                continue
            if env.stream != "diff_depth" or "malformed" in env.capture_flags:
                continue
            sym = env.symbol.upper()
            if symbols and sym not in symbols:
                continue
            if _in_window(env.receive_time_utc_ns, start_ns, end_ns):
                in_window_depth_seen[sym] = in_window_depth_seen.get(sym, 0) + 1
            mkt = "spot" if env.market == "spot" else env.market
            key = (mkt, sym)
            recon = get_recon(mkt, sym)

            # Drain snapshots up to previous event before processing connection/gap boundary on this event
            # so a post-disconnect snapshot is never wiped by the new connection's first message.
            if key in last_event_ts:
                drain_due_snapshots(writer, key, last_event_ts[key])
            last_event_ts[key] = env.receive_time_utc_ns

            gap_flag = any(f in env.capture_flags for f in ("sequence_gap", "reconnect_gap", "ingress_drop"))
            prev_conn = last_conn.get(key)
            conn_changed = prev_conn is not None and prev_conn != env.connection_id

            if gap_flag:
                last_conn[key] = env.connection_id
                q = snapshot_queues.get(key)
                if q:
                    snapshot_queues[key] = [item for item in q if item[0] > env.receive_time_utc_ns]
                if not gap_episode_open.get(key, False) or recon.is_reliable or pending_snaps.get(key) is not None:
                    recon.restart_bootstrap()
                    pending_snaps[key] = None
                    buffered_upds[key] = []
                    record_resync(sym, env.receive_time_utc_ns, "capture_flag_gap")
                    gap_episode_open[key] = True
                continue

            last_conn[key] = env.connection_id
            if conn_changed:
                recon.restart_bootstrap()
                pending_snaps[key] = None
                buffered_upds[key] = []
                record_resync(sym, env.receive_time_utc_ns, "connection_reset")
                gap_episode_open[key] = False

            # Drain snapshots arriving up to this event BEFORE offering upd so an already-due
            # snapshot synchronizes prior buffered deltas at its true arrival time without peeking at upd.
            drain_due_snapshots(writer, key, env.receive_time_utc_ns)

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
            except (KeyError, ValueError, TypeError, json.JSONDecodeError):
                continue

            gap_episode_open[key] = False
            stats["depth_updates_seen"] += 1
            sym_depth_updates_seen[sym] = sym_depth_updates_seen.get(sym, 0) + 1
            was_reliable = recon.is_reliable
            applied = recon.offer(upd)
            if was_reliable and recon.state is ReconstructionState.RESYNC_REQUIRED and not applied:
                recon.restart_bootstrap()
                pending_snaps[key] = None
                buffered_upds[key] = []
                record_resync(sym, env.receive_time_utc_ns, "sequence_gap")
                gap_episode_open[key] = True
                continue

            if not recon.is_reliable:
                buffered_upds.setdefault(key, []).append((upd.final_update_id, env.receive_time_utc_ns, chunk.chunk_id))
                if start_ns <= 0 or env.receive_time_utc_ns >= start_ns:
                    if pending_snaps.get(key) is None:
                        sym_in_window_unsync_updates[sym] = sym_in_window_unsync_updates.get(sym, 0) + 1

            prev_pending = pending_snaps.get(key)
            pending_snaps[key] = _try_bridge(recon, prev_pending)
            just_bridged = (not was_reliable) and recon.is_reliable
            if just_bridged:
                pending_snap_ts = prev_pending["ts_ns"] if prev_pending is not None else env.receive_time_utc_ns
                ready_ts_ns = max(pending_snap_ts, env.receive_time_utc_ns)
                if buffered_upds.get(key):
                    ready_ts_ns = max(ready_ts_ns, max(t for _, t, _ in buffered_upds[key]))
                buffered_upds[key] = []
                stats["recovery_count"] += 1
                sym_recoveries[sym] = sym_recoveries.get(sym, 0) + 1
                _mark_synchronized(sym, pending_snap_ts, ready_ts_ns)
                write_frame(writer, mkt, sym, chunk.chunk_id, ready_ts_ns, force=True)
            elif recon.is_reliable and applied:
                _mark_synchronized(sym, env.receive_time_utc_ns, env.receive_time_utc_ns)
                write_frame(writer, mkt, sym, chunk.chunk_id, env.receive_time_utc_ns)

        for key in list(snapshot_queues):
            drain_due_snapshots(writer, key, None)

        written_file = writer.close_and_commit()

    eval_syms = sorted(
        symbols
        | set(sym_snapshots_seen.keys())
        | set(sym_depth_updates_seen.keys())
        | set(sym_emissions.keys())
    )
    symbol_coverage: dict[str, dict[str, Any]] = {}
    for sym in eval_syms:
        em = sym_emissions.get(sym, 0)
        sn = sym_snapshots_seen.get(sym, 0)
        du = sym_depth_updates_seen.get(sym, 0)
        wr = sym_window_resyncs.get(sym, 0)
        sym_unsync = [e for e in unsync_events if e.get("symbol") == sym]
        if em > 0 or sym in sym_first_sync_ns:
            if wr > 0:
                s_status = "partial"
                s_reason: str | None = "window_resync_occurred"
            elif sym_mid_window_partial.get(sym, False):
                s_status = "partial"
                s_reason = "mid_window_baseline_recovery_only"
            else:
                s_status = "complete"
                s_reason = None
        else:
            s_status = "unavailable"
            if sn == 0:
                s_reason = "no_depth_snapshots_for_symbol"
            elif du == 0:
                s_reason = "no_diff_depth_events_before_end"
            elif any(e.get("phase") == "window" or e.get("reason") in ("capture_flag_gap", "connection_reset", "sequence_gap") for e in sym_unsync):
                s_reason = "resync_required_after_gap_or_reconnect_no_subsequent_snapshot"
            else:
                s_reason = "updates_did_not_bridge_snapshot"
        symbol_coverage[sym] = {
            "status": s_status,
            "reason": s_reason,
            "snapshots_seen": sn,
            "depth_updates_seen": du,
            "reliable_emissions": em,
            "warmup_resync_count": sym_warmup_resyncs.get(sym, 0),
            "window_resync_count": wr,
            "recovery_count": sym_recoveries.get(sym, 0),
            "synchronized_at_window_start": sym_reliable_at_start.get(sym, False),
            "first_synchronized_utc_ns": str(sym_first_sync_ns[sym]) if sym in sym_first_sync_ns else None,
            "last_synchronized_utc_ns": str(sym_last_sync_ns[sym]) if sym in sym_last_sync_ns else None,
        }

    stats["symbol_coverage"] = symbol_coverage

    all_snaps = [ts for vals in snap_timestamps_by_sym.values() for ts in vals]
    unsync_reason = None
    if stats["reliable_emissions"] == 0:
        if stats["snapshots_seen"] == 0:
            unsync_reason = "no_depth_snapshots_for_symbol"
        elif stats["depth_updates_seen"] == 0:
            unsync_reason = "no_diff_depth_events_before_end"
        elif any(e.get("phase") == "window" or e.get("reason") in ("capture_flag_gap", "connection_reset", "sequence_gap") for e in unsync_events):
            unsync_reason = "resync_required_after_gap_or_reconnect_no_subsequent_snapshot"
        else:
            unsync_reason = "updates_did_not_bridge_snapshot"

    return {
        "operation": "query_orderbook",
        "stats": stats,
        "symbol_coverage": symbol_coverage,
        "unsynchronized_reason": unsync_reason,
        "unsynchronized_events": unsync_events[:50],
        "actual_time_range": {
            "first_snapshot_utc_ns": str(min(all_snaps)) if all_snaps else None,
            "last_snapshot_utc_ns": str(max(all_snaps)) if all_snaps else None,
            "first_synchronized_utc_ns": str(first_sync_ns) if first_sync_ns is not None else None,
            "last_synchronized_utc_ns": str(last_sync_ns) if last_sync_ns is not None else None,
            "first_emitted_utc_ns": preview_rows[0]["receive_time_utc_ns"] if preview_rows else None,
            "in_window_depth_updates": sum(in_window_depth_seen.values()),
        },
        "sampling_semantics": "emit_on_synchronized_update_with_min_step (irregular event-driven timestamps, NOT uniform forward-fill)",
        "imbalance_definition": f"top_{top_k}_depth_imbalance = (sum(bid_qty_top_{top_k}) - sum(ask_qty_top_{top_k})) / (sum(bid_qty_top_{top_k}) + sum(ask_qty_top_{top_k})); NOT order-flow imbalance (OFI)",
        "total_rows": stats["reliable_emissions"],
        "rows": preview_rows,
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

    preview_rows: list[dict[str, Any]] = []
    total_rows = 0
    saw_liq_stream = False
    saw_trade_stream = False
    observed_symbols: set[str] = set()

    with _RowStreamWriter(
        req.get("output_file"),
        str(req.get("output_format") or "jsonl"),
        fieldnames=_TRADES_LIQ_FIELDS,
        parquet_field_types=_TRADES_LIQ_PARQUET_TYPES,
    ) as writer:
        for _pid, chunk, rec in _iter_partition_records(req):
            env = rec.envelope
            if "malformed" in env.capture_flags:
                continue
            sym = env.symbol.upper()
            if sym and sym != "__GLOBAL__":
                observed_symbols.add(sym)
            if symbols and sym not in symbols:
                continue

            row: dict[str, Any] | None = None
            if env.stream == "agg_trade" and kind in ("all", "trades"):
                saw_trade_stream = True
                try:
                    p = _unwrap_ws_json(env)
                    t_ms = int(p.get("T") or env.exchange_trade_time or (env.receive_time_utc_ns // 1_000_000))
                    t_ns = t_ms * 1_000_000
                    if not _in_window(t_ns, start_ns, end_ns):
                        continue
                    row = {
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
                except (KeyError, ValueError, TypeError, json.JSONDecodeError):
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
                    row = {
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
                except (KeyError, ValueError, TypeError, json.JSONDecodeError):
                    continue

            if row is not None:
                writer.write(row)
                total_rows += 1
                if len(preview_rows) < limit:
                    preview_rows.append(row)

        written_file = writer.close_and_commit()

    liq_status = "observed"
    if market == "spot" and kind in ("all", "liquidations"):
        liq_status = "not_applicable_for_spot"
    elif not saw_liq_stream and kind in ("all", "liquidations"):
        liq_status = "no_liquidation_chunks_or_events_in_window"

    out: dict[str, Any] = {
        "operation": "query_trades_and_liquidations",
        "saw_trade_stream": saw_trade_stream,
        "saw_liquidation_stream": saw_liq_stream,
        "liquidation_status": liq_status,
        "observed_symbols": sorted(observed_symbols),
        "total_rows": total_rows,
        "rows": preview_rows,
        "output_file": written_file,
    }
    if kind in ("all", "liquidations") and market != "spot":
        out["liquidation_semantics"] = (
            "Binance USDM <symbol>@forceOrder is a sampled liquidation stream per recorded symbol (not full market-wide or per-fill complete ledger); "
            "prefer average_price * filled_quantity (ap * z) for executed liquidation notional rather than limit order price * original_quantity (p * q)."
        )
    return out


_METRIC_MAP = {
    "": "all",
    "all": "all",
    "mark_price_funding": "mark_price_funding",
    "open_interest": "open_interest",
    "open_interest_hist": "open_interest_statistics_5m",
    "open_interest_statistics_5m": "open_interest_statistics_5m",
    "top_long_short_account_ratio": "top_long_short_account_ratio_5m",
    "top_long_short_account_ratio_5m": "top_long_short_account_ratio_5m",
    "top_long_short_position_ratio": "top_long_short_position_ratio_5m",
    "top_long_short_position_ratio_5m": "top_long_short_position_ratio_5m",
    "global_long_short_account_ratio": "global_long_short_ratio_5m",
    "global_long_short_ratio_5m": "global_long_short_ratio_5m",
    "taker_buy_sell_ratio": "taker_buy_sell_volume_5m",
    "taker_buy_sell_volume_5m": "taker_buy_sell_volume_5m",
    "contract_specs": "contract_specs",
}


def _run_query_derivatives_metrics(req: dict[str, Any]) -> dict[str, Any]:
    symbols = {s.upper() for s in (req.get("symbols") or []) if s}
    start_ns = int(req.get("start_utc_ns") or 0)
    end_ns = int(req.get("end_utc_ns") or 0)
    limit = int(req.get("limit") or 100)
    raw_metric = str(req.get("metric") or "").strip().lower()
    target_metric = _METRIC_MAP.get(raw_metric, raw_metric)

    preview_rows: list[dict[str, Any]] = []
    total_rows = 0
    contract_specs: dict[str, dict[str, Any]] = {}
    observed_symbols: set[str] = set()

    with _RowStreamWriter(
        req.get("output_file"),
        str(req.get("output_format") or "jsonl"),
        fieldnames=_METRICS_FIELDS,
    ) as writer:
        for _pid, chunk, rec in _iter_partition_records(req):
            env = rec.envelope
            if "malformed" in env.capture_flags:
                continue

            if env.stream == "mark_price" and target_metric in ("all", "mark_price_funding"):
                sym = env.symbol.upper()
                if sym and sym != "__GLOBAL__":
                    observed_symbols.add(sym)
                if symbols and sym not in symbols:
                    continue
                row: dict[str, Any] | None = None
                try:
                    p = _unwrap_ws_json(env)
                    e_ms = int(p.get("E") or (env.receive_time_utc_ns // 1_000_000))
                    e_ns = e_ms * 1_000_000
                    if not _in_window(e_ns, start_ns, end_ns):
                        continue
                    row = {
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
                except (KeyError, ValueError, TypeError, json.JSONDecodeError):
                    continue
                if row is not None:
                    writer.write(row)
                    total_rows += 1
                    if len(preview_rows) < limit:
                        preview_rows.append(row)

            elif env.stream == "rest_observations":
                try:
                    src_stream, req_meta, model, ok, _err = _decode_rest_observation(env)
                except Exception:
                    continue
                if not ok:
                    continue
                params = req_meta.get("parameters") if isinstance(req_meta.get("parameters"), dict) else {}
                scope_sym = str(params.get("symbol") or env.symbol).upper()

                if src_stream == "exchange_info" and isinstance(model, dict) and target_metric in ("all", "contract_specs"):
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
                elif src_stream == "open_interest" and isinstance(model, dict) and target_metric in ("all", "open_interest"):
                    sym = str(model.get("symbol") or scope_sym).upper()
                    observed_symbols.add(sym)
                    if symbols and sym not in symbols:
                        continue
                    try:
                        ts_ns = int(model.get("time") or (env.receive_time_utc_ns // 1_000_000)) * 1_000_000
                        if not _in_window(ts_ns, start_ns, end_ns):
                            continue
                        row = {
                            "metric_type": "open_interest",
                            "symbol": sym,
                            "event_time_utc_ns": str(ts_ns),
                            "receive_time_utc_ns": str(env.receive_time_utc_ns),
                            "open_interest": str(model.get("openInterest")),
                            "source": "recorder",
                            "source_chunk_id": chunk.chunk_id,
                        }
                    except (ValueError, TypeError):
                        continue
                    writer.write(row)
                    total_rows += 1
                    if len(preview_rows) < limit:
                        preview_rows.append(row)
                elif src_stream in {
                    "open_interest_statistics_5m",
                    "top_long_short_account_ratio_5m",
                    "top_long_short_position_ratio_5m",
                    "global_long_short_ratio_5m",
                    "taker_buy_sell_volume_5m",
                } and isinstance(model, list):
                    if target_metric not in ("all", src_stream):
                        continue
                    if scope_sym and scope_sym != "__GLOBAL__":
                        observed_symbols.add(scope_sym)
                    if symbols and scope_sym not in symbols:
                        continue
                    pending_metric_rows: list[dict[str, Any]] = []
                    for item in model:
                        if not isinstance(item, dict):
                            continue
                        try:
                            ts_ns = int(item.get("timestamp") or (env.receive_time_utc_ns // 1_000_000)) * 1_000_000
                        except (ValueError, TypeError):
                            continue
                        if not _in_window(ts_ns, start_ns, end_ns):
                            continue
                        sym = str(item.get("symbol") or scope_sym).upper()
                        observed_symbols.add(sym)
                        pending_metric_rows.append({
                            "metric_type": src_stream,
                            "symbol": sym,
                            "event_time_utc_ns": str(ts_ns),
                            "receive_time_utc_ns": str(env.receive_time_utc_ns),
                            "values": item,
                            "source": "recorder",
                            "source_chunk_id": chunk.chunk_id,
                        })
                    for row in pending_metric_rows:
                        writer.write(row)
                        total_rows += 1
                        if len(preview_rows) < limit:
                            preview_rows.append(row)

        written_file = writer.close_and_commit()

    return {
        "operation": "query_derivatives_metrics",
        "metric": raw_metric or "all",
        "contract_specs": contract_specs,
        "observed_symbols": sorted(observed_symbols),
        "total_rows": total_rows,
        "rows": preview_rows,
        "output_file": written_file,
    }


def dispatch(req: dict[str, Any]) -> dict[str, Any]:
    op = req.get("operation")
    if op == "inspect_overview":
        return _run_inspect_overview(req)
    if op == "inspect_orderbook_snapshots":
        return _run_inspect_orderbook_snapshots(req)
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
