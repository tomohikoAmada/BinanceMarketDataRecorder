"""内容寻址、崩溃安全且确定性的 Raw 到 Parquet 管道。

Normalizer 将已验证的 SEALED Raw chunk(以及可选的 archive-verified 外部副本)
转换为 normalized-dataset.v1 层次结构下的不可变、内容寻址 Parquet 分区。

管道不变量:
- 所有 Raw chunk 必须在任何行被解析前通过验证(sealed artifact 的大小/哈希
  与 manifest 匹配)。不可用或未验证的 Raw 中止构建。
- 候选行写入 NDJSON 文件,然后使用有界 10,000 行运行和 heapq k 路归并
  进行外部归并排序。这避免了将所有行保存在内存中。
- 排序候选行在一次遍历中去重:相同 semantic_key_sha256 + 相同
  logical_record_sha256 合并为一行,具有最小的稳定来源元组。
  相同语义键 + 不同逻辑内容创建 identity_conflict=true 行(保留所有变体)。
- 分区 spool 将去重行按 (market, stream, date, hour) 键写入 NDJSON 文件。
  有界打开 spool 数量防止 fd 耗尽。
- 每个分区通过写入、fsync、Parquet 逻辑回读、哈希比较、原子重命名来提交。
  分区 manifest 绑定逻辑/存储哈希。
- 构建 manifest 将所有分区 manifest 和已验证 M6 checkpoint 绑定到
  一个内容寻址的 build ID。构建一旦写入即不可变。
- 基于 kernel flock 的锁防止对同一 data root 进行并发规范化运行。
- 中断构建留下的过期 .partial 文件在启动时清理。
"""

from __future__ import annotations

import fcntl
import hashlib
import heapq
import json
import os
import shutil
from collections import OrderedDict
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import BinaryIO
from uuid import uuid4

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from ..metrics.model import MetricAggregate
from ..orderbook.checkpoint import OrderBookCheckpointStore
from ..storage.catalog import Catalog
from ..storage.layout import StorageLayout, fsync_directory
from .model import (
    DATASET_VERSION,
    DEDUP_VERSION,
    PARQUET_PROFILE,
    ParsedEvent,
    schema_for,
    schema_version,
)
from .parser import canonical_json, parse_envelope, sha256_json
from .raw import (
    RawSourceError,
    SourceChunk,
    SourceRecord,
    iter_source_records,
    load_source_chunks,
)

BUILD_MANIFEST_SCHEMA = "normalized-build-manifest.v1"
PARTITION_MANIFEST_SCHEMA = "normalized-partition-manifest.v1"
PARQUET_ROWS_PER_GROUP = 10_000
SORT_ROWS_PER_RUN = 10_000


class NormalizationError(RuntimeError):
    """A derived build cannot be proven complete and deterministic."""


@dataclass(frozen=True, slots=True)
class NormalizationResult:
    status: str
    dataset_version: str
    build_id: str | None
    build_manifest: str | None
    source_chunks: int
    partitions: int
    normalized_rows: int
    duplicate_rows_removed: int
    identity_conflicts: int

    def public_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "dataset_version": self.dataset_version,
            "build_id": self.build_id,
            "build_manifest": self.build_manifest,
            "source_chunks": self.source_chunks,
            "partitions": self.partitions,
            "normalized_rows": self.normalized_rows,
            "duplicate_rows_removed": self.duplicate_rows_removed,
            "identity_conflicts": self.identity_conflicts,
        }


@dataclass(frozen=True, slots=True)
class _PartitionResult:
    market: str
    stream: str
    date: str
    hour: str
    relative_path: str
    manifest_relative_path: str
    logical_sha256: str
    stored_sha256: str
    stored_bytes: int
    row_count: int
    input_candidate_count: int
    duplicate_rows_removed: int
    identity_conflicts: int
    source_chunk_hashes: tuple[str, ...]

    def build_entry(self) -> dict[str, object]:
        return {
            "market": self.market,
            "stream": self.stream,
            "date": self.date,
            "hour": self.hour,
            "relative_path": self.relative_path,
            "manifest_relative_path": self.manifest_relative_path,
            "logical_sha256": self.logical_sha256,
            "stored_sha256": self.stored_sha256,
            "stored_bytes": self.stored_bytes,
            "row_count": self.row_count,
            "input_candidate_count": self.input_candidate_count,
            "duplicate_rows_removed": self.duplicate_rows_removed,
            "identity_conflicts": self.identity_conflicts,
            "source_chunk_hashes": list(self.source_chunk_hashes),
        }


class _NormalizationLock:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._descriptor: int | None = None

    def __enter__(self) -> _NormalizationLock:
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        descriptor = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o600)
        os.fchmod(descriptor, 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            os.close(descriptor)
            raise NormalizationError("another normalization build is active") from exc
        self._descriptor = descriptor
        return self

    def __exit__(self, *_args: object) -> None:
        descriptor = self._descriptor
        self._descriptor = None
        if descriptor is not None:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            finally:
                os.close(descriptor)


class _PartitionSpools:
    def __init__(self, root: Path, *, maximum_open: int = 32) -> None:
        self.root = root
        self.maximum_open = maximum_open
        self.paths: dict[tuple[str, str, str, str], Path] = {}
        self._open: OrderedDict[tuple[str, str, str, str], BinaryIO] = OrderedDict()

    def write(self, key: tuple[str, str, str, str], row: dict[str, object]) -> None:
        path = self.paths.get(key)
        if path is None:
            digest = hashlib.sha256("\0".join(key).encode()).hexdigest()
            path = self.root / f"partition-{digest}.ndjson"
            self.paths[key] = path
        handle = self._open.pop(key, None)
        if handle is None:
            handle = path.open("ab", buffering=0)
        self._open[key] = handle
        if len(self._open) > self.maximum_open:
            _old_key, old_handle = self._open.popitem(last=False)
            old_handle.close()
        handle.write((canonical_json(row) + "\n").encode())

    def close(self) -> None:
        for handle in self._open.values():
            handle.close()
        self._open.clear()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb", buffering=0) as source:
        while block := source.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def _required_int(value: object, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise NormalizationError(f"{name} must be an integer")
    return value


def _required_strings(value: object, name: str) -> tuple[str, ...]:
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise NormalizationError(f"{name} must be a string list")
    return tuple(value)


def _atomic_json(path: Path, document: Mapping[str, object]) -> None:
    body = (canonical_json(dict(document)) + "\n").encode()
    if path.exists():
        if path.read_bytes() != body:
            raise NormalizationError(f"existing immutable manifest differs: {path}")
        return
    partial = path.with_name(f".{path.name}.{uuid4().hex}.partial")
    descriptor = os.open(partial, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        view = memoryview(body)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise OSError("manifest write returned no progress")
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(partial, path)
    fsync_directory(path.parent)


def _cleanup_stale_partials(*roots: Path) -> None:
    for root in roots:
        for partial in root.rglob(".*.partial"):
            if partial.is_file() or partial.is_symlink():
                partial.unlink()


def _partition_time(receive_time_utc_ns: int) -> tuple[str, str]:
    seconds, remainder = divmod(receive_time_utc_ns, 1_000_000_000)
    timestamp = datetime.fromtimestamp(seconds, tz=UTC).replace(
        microsecond=remainder // 1_000
    )
    return timestamp.date().isoformat(), f"{timestamp.hour:02d}"


def _source_flags(chunk: SourceChunk) -> list[str]:
    value = chunk.manifest.get("capture_flags", [])
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise NormalizationError(f"invalid capture_flags in Raw chunk {chunk.chunk_id}")
    return sorted(set(value))


def _manifest_bool(chunk: SourceChunk, name: str) -> bool:
    value = chunk.manifest.get(name)
    if not isinstance(value, bool):
        raise NormalizationError(f"invalid {name} in Raw chunk {chunk.chunk_id}")
    return value


def _provenance(record: SourceRecord, parsed: ParsedEvent) -> dict[str, object]:
    envelope = record.envelope
    return {
        "source_chunk_id": record.chunk.chunk_id,
        "source_chunk_sha256": record.chunk.uncompressed_sha256,
        "source_record_ordinal": record.ordinal,
        "source_subrecord_ordinal": parsed.subrecord_ordinal,
        "receive_time_utc_ns": envelope.receive_time_utc_ns,
        "collector_instance_id": envelope.collector_instance_id,
        "connection_id": envelope.connection_id,
    }


def _candidate(record: SourceRecord, parsed: ParsedEvent) -> dict[str, object]:
    envelope = record.envelope
    receive_date, receive_hour = _partition_time(envelope.receive_time_utc_ns)
    semantic_hash = sha256_json(parsed.semantic_identity)
    logical_hash = sha256_json(
        {
            "event_kind": parsed.event_kind,
            "valid": parsed.valid,
            "error_code": parsed.error_code,
            "exchange_event_time_ms": envelope.exchange_event_time,
            "exchange_transaction_time_ms": envelope.exchange_transaction_time,
            "exchange_trade_time_ms": envelope.exchange_trade_time,
            "content": parsed.logical_content,
        }
    )
    flags = _source_flags(record.chunk)
    complete = _manifest_bool(record.chunk, "complete")
    row = {
        "dataset_version": DATASET_VERSION,
        "schema_version": schema_version(envelope.market, envelope.stream),
        "dedup_version": DEDUP_VERSION,
        "venue": envelope.venue,
        "market": envelope.market,
        "symbol": envelope.symbol,
        "stream": envelope.stream,
        "event_kind": parsed.event_kind,
        "receive_time_utc_ns": envelope.receive_time_utc_ns,
        "receive_date": receive_date,
        "receive_hour": int(receive_hour),
        "receive_monotonic_ns": envelope.receive_monotonic_ns,
        "exchange_event_time_ms": envelope.exchange_event_time,
        "exchange_transaction_time_ms": envelope.exchange_transaction_time,
        "exchange_trade_time_ms": envelope.exchange_trade_time,
        "module": envelope.module,
        "connection_id": envelope.connection_id,
        "collector_instance_id": envelope.collector_instance_id,
        "collector_version": envelope.collector_version,
        "source_sequence_json": canonical_json(envelope.source_sequence),
        "capture_flags_json": canonical_json(sorted(set(envelope.capture_flags))),
        "source_chunk_id": record.chunk.chunk_id,
        "source_chunk_sha256": record.chunk.uncompressed_sha256,
        "source_record_ordinal": record.ordinal,
        "source_subrecord_ordinal": parsed.subrecord_ordinal,
        "raw_payload_sha256": hashlib.sha256(envelope.raw_payload).hexdigest(),
        "semantic_key_sha256": semantic_hash,
        "logical_record_sha256": logical_hash,
        "duplicate_count": 1,
        "duplicate_sources_json": canonical_json(
            [_provenance(record, parsed)]
        ),
        "identity_conflict": False,
        "valid": parsed.valid,
        "error_code": parsed.error_code,
        "source_complete": complete,
        "source_gap": _manifest_bool(record.chunk, "gap"),
        "source_resync": _manifest_bool(record.chunk, "resync"),
        "source_recovered": _manifest_bool(record.chunk, "recovered"),
        "source_capture_flags_json": canonical_json(flags),
        **parsed.fields,
    }
    provenance = _provenance(record, parsed)
    return {
        "semantic_key_sha256": semantic_hash,
        "logical_record_sha256": logical_hash,
        "provenance": provenance,
        "row": row,
    }


def _candidate_sort_key(document: dict[str, object]) -> tuple[object, ...]:
    provenance = document["provenance"]
    if not isinstance(provenance, dict):
        raise NormalizationError("candidate provenance is invalid")
    return (
        document["semantic_key_sha256"],
        document["logical_record_sha256"],
        provenance["receive_time_utc_ns"],
        provenance["source_chunk_sha256"],
        provenance["source_record_ordinal"],
        provenance["source_subrecord_ordinal"],
        provenance["collector_instance_id"],
        provenance["connection_id"],
    )


def _write_sorted_run(path: Path, rows: list[dict[str, object]]) -> None:
    rows.sort(key=_candidate_sort_key)
    with path.open("wb", buffering=0) as target:
        for row in rows:
            target.write((canonical_json(row) + "\n").encode())


def _read_documents(path: Path) -> Iterator[dict[str, object]]:
    with path.open("rb", buffering=0) as source:
        for line in source:
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                raise NormalizationError(f"invalid normalization work file {path}") from exc
            if not isinstance(value, dict):
                raise NormalizationError(f"normalization work row is not an object: {path}")
            yield value


def _external_sort(source_path: Path, run_directory: Path) -> Iterator[dict[str, object]]:
    run_directory.mkdir(mode=0o700)
    runs: list[Path] = []
    batch: list[dict[str, object]] = []
    for document in _read_documents(source_path):
        batch.append(document)
        if len(batch) >= SORT_ROWS_PER_RUN:
            run = run_directory / f"run-{len(runs):08d}.ndjson"
            _write_sorted_run(run, batch)
            runs.append(run)
            batch = []
    if batch:
        run = run_directory / f"run-{len(runs):08d}.ndjson"
        _write_sorted_run(run, batch)
        runs.append(run)
    if not runs:
        return
    iterators = [_read_documents(path) for path in runs]
    heap: list[tuple[tuple[object, ...], int, dict[str, object]]] = []
    for index, iterator in enumerate(iterators):
        try:
            document = next(iterator)
        except StopIteration:
            continue
        heapq.heappush(heap, (_candidate_sort_key(document), index, document))
    while heap:
        _key, index, document = heapq.heappop(heap)
        yield document
        try:
            following = next(iterators[index])
        except StopIteration:
            continue
        heapq.heappush(
            heap, (_candidate_sort_key(following), index, following)
        )


def _winner(variant: list[dict[str, object]], *, conflict: bool) -> dict[str, object]:
    ordered = sorted(variant, key=_candidate_sort_key)
    selected = ordered[0]
    row = selected.get("row")
    if not isinstance(row, dict):
        raise NormalizationError("candidate row is invalid")
    sources = [item["provenance"] for item in ordered]
    output = dict(row)
    output["duplicate_count"] = len(ordered)
    output["duplicate_sources_json"] = canonical_json(sources)
    output["identity_conflict"] = conflict
    return output


def _deduplicate_to_partitions(
    sorted_candidates: Iterable[dict[str, object]],
    spools: _PartitionSpools,
) -> None:
    current_semantic: str | None = None
    group: list[dict[str, object]] = []

    def emit(selected: list[dict[str, object]]) -> None:
        variants: dict[str, list[dict[str, object]]] = {}
        for item in selected:
            logical = item.get("logical_record_sha256")
            if not isinstance(logical, str):
                raise NormalizationError("candidate logical identity is invalid")
            variants.setdefault(logical, []).append(item)
        conflict = len(variants) > 1
        for logical in sorted(variants):
            row = _winner(variants[logical], conflict=conflict)
            key = (
                str(row["market"]),
                str(row["stream"]),
                str(row["receive_date"]),
                f"{_required_int(row['receive_hour'], 'receive_hour'):02d}",
            )
            spools.write(key, row)

    for document in sorted_candidates:
        semantic = document.get("semantic_key_sha256")
        if not isinstance(semantic, str):
            raise NormalizationError("candidate semantic identity is invalid")
        if current_semantic is not None and semantic != current_semantic:
            emit(group)
            group = []
        current_semantic = semantic
        group.append(document)
    if group:
        emit(group)


def _partition_statistics(path: Path) -> dict[str, object]:
    digest = hashlib.sha256()
    rows = 0
    input_candidates = 0
    conflicts = 0
    source_hashes: set[str] = set()
    minimum_receive: int | None = None
    maximum_receive: int | None = None
    for document in _read_documents(path):
        body = (canonical_json(document) + "\n").encode()
        digest.update(body)
        rows += 1
        duplicate_count = document.get("duplicate_count")
        receive_time = document.get("receive_time_utc_ns")
        source_hash = document.get("source_chunk_sha256")
        if (
            not isinstance(duplicate_count, int)
            or isinstance(duplicate_count, bool)
            or duplicate_count < 1
            or not isinstance(receive_time, int)
            or isinstance(receive_time, bool)
            or not isinstance(source_hash, str)
        ):
            raise NormalizationError("partition work row has invalid statistics")
        input_candidates += duplicate_count
        conflicts += int(document.get("identity_conflict") is True)
        source_hashes.add(source_hash)
        sources_json = document.get("duplicate_sources_json")
        if isinstance(sources_json, str):
            sources = json.loads(sources_json)
            if isinstance(sources, list):
                for source in sources:
                    if isinstance(source, dict) and isinstance(
                        source.get("source_chunk_sha256"), str
                    ):
                        source_hashes.add(source["source_chunk_sha256"])
        minimum_receive = (
            receive_time
            if minimum_receive is None
            else min(minimum_receive, receive_time)
        )
        maximum_receive = (
            receive_time
            if maximum_receive is None
            else max(maximum_receive, receive_time)
        )
    return {
        "logical_sha256": digest.hexdigest(),
        "row_count": rows,
        "input_candidate_count": input_candidates,
        "identity_conflicts": conflicts,
        "source_chunk_hashes": sorted(source_hashes),
        "receive_time_utc_min_ns": minimum_receive,
        "receive_time_utc_max_ns": maximum_receive,
    }


def _parquet_logical_hash(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    row_count = 0
    parquet = pq.ParquetFile(path)
    for batch in parquet.iter_batches(batch_size=PARQUET_ROWS_PER_GROUP):
        for row in pa.Table.from_batches([batch]).to_pylist():
            digest.update((canonical_json(row) + "\n").encode())
            row_count += 1
    return digest.hexdigest(), row_count


def _write_parquet(
    *,
    spool_path: Path,
    target: Path,
    schema: pa.Schema,
    logical_sha256: str,
) -> None:
    target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    partial = target.with_name(f".{target.name}.{uuid4().hex}.partial")
    schema_metadata = dict(schema.metadata or {})
    schema_metadata[b"bmdr.logical_sha256"] = logical_sha256.encode()
    write_schema = schema.with_metadata(schema_metadata)
    writer = pq.ParquetWriter(
        partial,
        write_schema,
        version="2.6",
        compression="zstd",
        compression_level=3,
        use_dictionary=True,
        write_statistics=True,
        data_page_version="1.0",
        write_page_checksum=True,
        use_compliant_nested_type=True,
    )
    try:
        batch: list[dict[str, object]] = []
        for document in _read_documents(spool_path):
            batch.append(document)
            if len(batch) >= PARQUET_ROWS_PER_GROUP:
                writer.write_table(
                    pa.Table.from_pylist(batch, schema=write_schema),
                    row_group_size=PARQUET_ROWS_PER_GROUP,
                )
                batch = []
        if batch:
            writer.write_table(
                pa.Table.from_pylist(batch, schema=write_schema),
                row_group_size=PARQUET_ROWS_PER_GROUP,
            )
    finally:
        writer.close()
    descriptor = os.open(partial, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    observed_hash, _rows = _parquet_logical_hash(partial)
    if observed_hash != logical_sha256:
        partial.unlink()
        raise NormalizationError("Parquet logical readback hash mismatch")
    os.replace(partial, target)
    os.chmod(target, 0o600)
    fsync_directory(target.parent)


def _checkpoint_entries(
    *, layout: StorageLayout, catalog: Catalog
) -> list[dict[str, object]]:
    store = OrderBookCheckpointStore(layout, catalog)
    output: list[dict[str, object]] = []
    for path in sorted(layout.checkpoints.glob("*.orderbook.json")):
        try:
            store.restore(path)
            document = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            raise NormalizationError(
                f"checkpoint verification failed for {path.name}: {exc}"
            ) from exc
        if not isinstance(document, dict):
            raise NormalizationError(f"checkpoint {path.name} is not an object")
        output.append(
            {
                "relative_path": layout.relative(path),
                "file_sha256": _sha256_file(path),
                "schema_version": document.get("schema_version"),
                "algorithm_version": document.get("algorithm_version"),
                "checkpoint_id": document.get("checkpoint_id"),
                "checkpoint_sha256": document.get("checkpoint_sha256"),
                "book_hash": document.get("book_hash"),
                "source_chunk_hashes": document.get("source_chunk_hashes"),
                "unreliable_intervals": document.get("unreliable_intervals"),
            }
        )
    return output


def _build_id(
    chunks: list[SourceChunk], checkpoints: list[dict[str, object]]
) -> str:
    identity = {
        "dataset_version": DATASET_VERSION,
        "raw_sources": [
            {
                "chunk_id": chunk.chunk_id,
                "manifest_sha256": chunk.manifest_sha256,
                "stored_sha256": chunk.manifest["stored_sha256"],
                "uncompressed_sha256": chunk.uncompressed_sha256,
            }
            for chunk in chunks
        ],
        "checkpoints": [
            {
                "checkpoint_id": item["checkpoint_id"],
                "file_sha256": item["file_sha256"],
            }
            for item in checkpoints
        ],
    }
    return sha256_json(identity)


def _metric_partition(
    *, catalog: Catalog, partition: _PartitionResult
) -> None:
    aggregate = MetricAggregate()
    aggregate.increment("normalized_rows", partition.row_count)
    aggregate.increment("normalized_bytes", partition.stored_bytes)
    batch_id = (
        f"normalize:{DATASET_VERSION}:{partition.market}:{partition.stream}:"
        f"{partition.date}:{partition.hour}:{partition.logical_sha256}"
    )
    catalog.record_metric_batch(
        batch_id=batch_id,
        rows=[(partition.date, partition.market, partition.stream, aggregate.document())],
    )


class Normalizer:
    def __init__(
        self,
        *,
        layout: StorageLayout,
        catalog: Catalog,
        external_roots: Mapping[str, Path] | None = None,
    ) -> None:
        self.layout = layout
        self.catalog = catalog
        self.external_roots = {} if external_roots is None else dict(external_roots)
        self.dataset_root = (
            self.layout.root / "data" / "normalized" / DATASET_VERSION
        )
        self.artifacts_root = self.dataset_root / "artifacts"
        self.builds_root = self.dataset_root / "builds"
        self.work_root = self.dataset_root / ".work"

    def run(self) -> NormalizationResult:
        with _NormalizationLock(self.layout.state / "normalizer.lock"):
            return self._run_locked()

    def _run_locked(self) -> NormalizationResult:
        try:
            chunks = load_source_chunks(
                layout=self.layout,
                catalog=self.catalog,
                external_roots=self.external_roots,
            )
        except RawSourceError as exc:
            raise NormalizationError(str(exc)) from exc
        if not chunks:
            return NormalizationResult(
                "NO_RAW_CHUNKS", DATASET_VERSION, None, None, 0, 0, 0, 0, 0
            )
        checkpoints = _checkpoint_entries(layout=self.layout, catalog=self.catalog)
        build_id = _build_id(chunks, checkpoints)
        self.dataset_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.artifacts_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.builds_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.work_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        _cleanup_stale_partials(self.artifacts_root, self.builds_root)
        for stale in self.work_root.iterdir():
            if stale.is_dir() and not stale.is_symlink():
                shutil.rmtree(stale)
            else:
                stale.unlink()
        run_root = self.work_root / f"run-{uuid4().hex}"
        run_root.mkdir(mode=0o700)
        candidates_path = run_root / "candidates.ndjson"
        partitions = _PartitionSpools(run_root / "partitions")
        partitions.root.mkdir(mode=0o700)
        try:
            with candidates_path.open("wb", buffering=0) as candidates:
                for chunk in chunks:
                    for record in iter_source_records(chunk):
                        for parsed in parse_envelope(record.envelope):
                            candidates.write(
                                (canonical_json(_candidate(record, parsed)) + "\n").encode()
                            )
            sorted_candidates = _external_sort(
                candidates_path, run_root / "sort-runs"
            )
            _deduplicate_to_partitions(sorted_candidates, partitions)
            partitions.close()
            results = [
                self._commit_partition(key, path)
                for key, path in sorted(partitions.paths.items())
            ]
            build_manifest = {
                "schema_version": BUILD_MANIFEST_SCHEMA,
                "dataset_version": DATASET_VERSION,
                "dedup_version": DEDUP_VERSION,
                "parquet_profile": PARQUET_PROFILE,
                "build_id": build_id,
                "raw_sources": [
                    {
                        "chunk_id": chunk.chunk_id,
                        "manifest_relative_path": self.layout.relative(
                            chunk.manifest_path
                        ),
                        "manifest_sha256": chunk.manifest_sha256,
                        "stored_sha256": chunk.manifest["stored_sha256"],
                        "uncompressed_sha256": chunk.uncompressed_sha256,
                    }
                    for chunk in chunks
                ],
                "checkpoints": checkpoints,
                "partitions": [item.build_entry() for item in results],
                "partition_count": len(results),
                "normalized_rows": sum(item.row_count for item in results),
                "input_candidate_count": sum(
                    item.input_candidate_count for item in results
                ),
                "duplicate_rows_removed": sum(
                    item.duplicate_rows_removed for item in results
                ),
                "identity_conflicts": sum(
                    item.identity_conflicts for item in results
                ),
            }
            build_path = self.builds_root / f"{build_id}.manifest.json"
            _atomic_json(build_path, build_manifest)
            return NormalizationResult(
                status="BUILT",
                dataset_version=DATASET_VERSION,
                build_id=build_id,
                build_manifest=self.layout.relative(build_path),
                source_chunks=len(chunks),
                partitions=len(results),
                normalized_rows=sum(item.row_count for item in results),
                duplicate_rows_removed=sum(
                    item.duplicate_rows_removed for item in results
                ),
                identity_conflicts=sum(item.identity_conflicts for item in results),
            )
        finally:
            partitions.close()
            shutil.rmtree(run_root, ignore_errors=True)

    def _commit_partition(
        self, key: tuple[str, str, str, str], spool_path: Path
    ) -> _PartitionResult:
        market, stream, date, hour = key
        statistics = _partition_statistics(spool_path)
        logical_sha = str(statistics["logical_sha256"])
        directory = (
            self.artifacts_root
            / f"market={market}"
            / f"stream={stream}"
            / f"date={date}"
            / f"hour={hour}"
        )
        target = directory / f"part-{logical_sha}.parquet"
        manifest_path = directory / f"part-{logical_sha}.manifest.json"
        if not target.exists():
            _write_parquet(
                spool_path=spool_path,
                target=target,
                schema=schema_for(market, stream),
                logical_sha256=logical_sha,
            )
        observed_logical, observed_rows = _parquet_logical_hash(target)
        if observed_logical != logical_sha or observed_rows != statistics["row_count"]:
            raise NormalizationError("existing Parquet artifact has different logical rows")
        stored_sha = _sha256_file(target)
        stored_bytes = target.stat().st_size
        document = {
            "schema_version": PARTITION_MANIFEST_SCHEMA,
            "dataset_version": DATASET_VERSION,
            "stream_schema_version": schema_version(market, stream),
            "dedup_version": DEDUP_VERSION,
            "parquet_profile": PARQUET_PROFILE,
            "market": market,
            "stream": stream,
            "date": date,
            "hour": hour,
            "relative_path": self.layout.relative(target),
            "logical_sha256": logical_sha,
            "stored_sha256": stored_sha,
            "stored_bytes": stored_bytes,
            "row_count": statistics["row_count"],
            "input_candidate_count": statistics["input_candidate_count"],
            "duplicate_rows_removed": (
                _required_int(
                    statistics["input_candidate_count"], "input_candidate_count"
                )
                - _required_int(statistics["row_count"], "row_count")
            ),
            "identity_conflicts": statistics["identity_conflicts"],
            "source_chunk_hashes": statistics["source_chunk_hashes"],
            "receive_time_utc_min_ns": statistics["receive_time_utc_min_ns"],
            "receive_time_utc_max_ns": statistics["receive_time_utc_max_ns"],
        }
        _atomic_json(manifest_path, document)
        result = _PartitionResult(
            market=market,
            stream=stream,
            date=date,
            hour=hour,
            relative_path=self.layout.relative(target),
            manifest_relative_path=self.layout.relative(manifest_path),
            logical_sha256=logical_sha,
            stored_sha256=stored_sha,
            stored_bytes=stored_bytes,
            row_count=_required_int(statistics["row_count"], "row_count"),
            input_candidate_count=_required_int(
                statistics["input_candidate_count"], "input_candidate_count"
            ),
            duplicate_rows_removed=(
                _required_int(
                    statistics["input_candidate_count"], "input_candidate_count"
                )
                - _required_int(statistics["row_count"], "row_count")
            ),
            identity_conflicts=_required_int(
                statistics["identity_conflicts"], "identity_conflicts"
            ),
            source_chunk_hashes=_required_strings(
                statistics["source_chunk_hashes"], "source_chunk_hashes"
            ),
        )
        _metric_partition(catalog=self.catalog, partition=result)
        return result


def normalization_status(data_root: Path) -> dict[str, object]:
    root = data_root / "data" / "normalized" / DATASET_VERSION
    builds = sorted((root / "builds").glob("*.manifest.json"))
    summaries: list[dict[str, object]] = []
    for path in builds:
        try:
            decoded = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise NormalizationError(
                f"cannot read normalized build manifest: {type(exc).__name__}"
            ) from exc
        if (
            not isinstance(decoded, dict)
            or decoded.get("schema_version") != BUILD_MANIFEST_SCHEMA
        ):
            raise NormalizationError("unsupported normalized build manifest")
        build_id = decoded.get("build_id")
        partition_count = decoded.get("partition_count")
        normalized_rows = decoded.get("normalized_rows")
        raw_sources = decoded.get("raw_sources")
        if (
            not isinstance(build_id, str)
            or not isinstance(partition_count, int)
            or isinstance(partition_count, bool)
            or not isinstance(normalized_rows, int)
            or isinstance(normalized_rows, bool)
            or not isinstance(raw_sources, list)
        ):
            raise NormalizationError("invalid normalized build manifest summary")
        summaries.append(
            {
                "build_id": build_id,
                "partition_count": partition_count,
                "normalized_rows": normalized_rows,
                "source_chunk_count": len(raw_sources),
                "manifest_relative_path": path.relative_to(data_root).as_posix(),
            }
        )
    return {
        "status": "NO_BUILDS" if not summaries else "AVAILABLE",
        "dataset_version": DATASET_VERSION,
        "build_count": len(builds),
        "builds": summaries,
    }
