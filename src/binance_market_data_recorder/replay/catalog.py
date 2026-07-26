"""经验证的 Manifest Catalog,隐藏物理 artifact 解析。

ManifestCatalog 是公共消费者边界(ADR-0021)。它打开一个显式内容寻址的
规范化构建,验证所有选定分区和 checkpoint 身份,并将相对路径解析到内部
Recorder 根目录。它从不暴露外部 archive 挂载点或 storage_ids。

- 消费者仅配置 data_root 和显式 build ID。没有 "latest" 推断或跨构建 glob。
- 每个分区和 checkpoint manifest 在返回给消费者前通过内容哈希验证。
  缺失或篡改的 artifact 中止打开。
- 路径相对于 data root 解析,具有包含性检查以防止目录遍历。
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Any

from ..normalize.model import DATASET_VERSION
from ..orderbook.model import OrderBook, OrderBookDataError
from ..orderbook.reconstructor import LocalBookReconstructor
from .model import BuildSummary, CheckpointDescriptor, PartitionDescriptor

if TYPE_CHECKING:
    from .reader import ReplayDataset

BUILD_MANIFEST_SCHEMA = "normalized-build-manifest.v1"
PARTITION_MANIFEST_SCHEMA = "normalized-partition-manifest.v1"
CHECKPOINT_SCHEMA = "orderbook-checkpoint.v1"


class ReplayCatalogError(RuntimeError):
    """A selected published dataset cannot be verified."""


@dataclass(frozen=True, slots=True)
class _PartitionSource:
    descriptor: PartitionDescriptor
    artifact_path: Path


@dataclass(frozen=True, slots=True)
class _OpenedBuild:
    summary: BuildSummary
    partitions: tuple[_PartitionSource, ...]
    checkpoints: tuple[CheckpointDescriptor, ...]


def _canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb", buffering=0) as source:
        while block := source.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def _sha256_json(value: object) -> str:
    return hashlib.sha256(_canonical_json(value).encode()).hexdigest()


def _freeze(value: object) -> object:
    if isinstance(value, dict):
        return MappingProxyType({str(key): _freeze(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(_freeze(item) for item in value)
    return value


def _freeze_mapping(value: dict[str, object]) -> Mapping[str, object]:
    frozen = _freeze(value)
    if not isinstance(frozen, Mapping):
        raise ReplayCatalogError("cannot freeze checkpoint mapping")
    return frozen


def _object(value: object, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ReplayCatalogError(f"{name} must be an object")
    return value


def _array(value: object, name: str) -> list[Any]:
    if not isinstance(value, list):
        raise ReplayCatalogError(f"{name} must be an array")
    return value


def _text(value: Mapping[str, object], name: str) -> str:
    item = value.get(name)
    if not isinstance(item, str) or not item:
        raise ReplayCatalogError(f"{name} must be non-empty text")
    return item


def _integer(value: Mapping[str, object], name: str) -> int:
    item = value.get(name)
    if not isinstance(item, int) or isinstance(item, bool) or item < 0:
        raise ReplayCatalogError(f"{name} must be a non-negative integer")
    return item


def _sha256(value: Mapping[str, object], name: str) -> str:
    item = _text(value, name)
    if len(item) != 64 or any(character not in "0123456789abcdef" for character in item):
        raise ReplayCatalogError(f"{name} must be lowercase SHA-256")
    return item


def _sha_list(value: object, name: str) -> tuple[str, ...]:
    items = _array(value, name)
    output: list[str] = []
    for item in items:
        if not isinstance(item, str):
            raise ReplayCatalogError(f"{name} must contain strings")
        checked = _sha256({name: item}, name)
        output.append(checked)
    if output != sorted(set(output)):
        raise ReplayCatalogError(f"{name} must be sorted and unique")
    return tuple(output)


def _safe_path(root: Path, relative: object) -> Path:
    if not isinstance(relative, str) or not relative:
        raise ReplayCatalogError("manifest relative path is invalid")
    candidate = (root / relative).resolve()
    try:
        candidate.relative_to(root.resolve())
    except ValueError as exc:
        raise ReplayCatalogError("manifest path escapes Recorder data root") from exc
    return candidate


def _decode(path: Path, schema: str) -> dict[str, Any]:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ReplayCatalogError(
            f"cannot read manifest {path.name}: {type(exc).__name__}"
        ) from exc
    value = _object(document, path.name)
    if value.get("schema_version") != schema:
        raise ReplayCatalogError(f"unsupported manifest schema in {path.name}")
    return value


def _build_identity(document: Mapping[str, object]) -> str:
    raw_sources = _array(document.get("raw_sources"), "raw_sources")
    checkpoints = _array(document.get("checkpoints"), "checkpoints")
    identity = {
        "dataset_version": document.get("dataset_version"),
        "raw_sources": [
            {
                "chunk_id": _text(_object(item, "raw source"), "chunk_id"),
                "manifest_sha256": _sha256(
                    _object(item, "raw source"), "manifest_sha256"
                ),
                "stored_sha256": _sha256(
                    _object(item, "raw source"), "stored_sha256"
                ),
                "uncompressed_sha256": _sha256(
                    _object(item, "raw source"), "uncompressed_sha256"
                ),
            }
            for item in raw_sources
        ],
        "checkpoints": [
            {
                "checkpoint_id": _text(
                    _object(item, "checkpoint entry"), "checkpoint_id"
                ),
                "file_sha256": _sha256(
                    _object(item, "checkpoint entry"), "file_sha256"
                ),
            }
            for item in checkpoints
        ],
    }
    return _sha256_json(identity)


def _summary(document: Mapping[str, object]) -> BuildSummary:
    if document.get("dataset_version") != DATASET_VERSION:
        raise ReplayCatalogError("unsupported normalized dataset version")
    build_id = _sha256(document, "build_id")
    if _build_identity(document) != build_id:
        raise ReplayCatalogError("normalized build identity mismatch")
    partitions = _array(document.get("partitions"), "partitions")
    raw_sources = _array(document.get("raw_sources"), "raw_sources")
    checkpoints = _array(document.get("checkpoints"), "checkpoints")
    partition_count = _integer(document, "partition_count")
    if partition_count != len(partitions):
        raise ReplayCatalogError("normalized partition count mismatch")
    return BuildSummary(
        build_id=build_id,
        dataset_version=DATASET_VERSION,
        dedup_version=_text(document, "dedup_version"),
        parquet_profile=_text(document, "parquet_profile"),
        partition_count=partition_count,
        normalized_rows=_integer(document, "normalized_rows"),
        source_chunk_count=len(raw_sources),
        checkpoint_count=len(checkpoints),
    )


def _partition(
    *,
    data_root: Path,
    build_entry: object,
    build: BuildSummary,
) -> _PartitionSource:
    entry = _object(build_entry, "partition entry")
    artifact = _safe_path(data_root, entry.get("relative_path"))
    manifest_path = _safe_path(data_root, entry.get("manifest_relative_path"))
    manifest = _decode(manifest_path, PARTITION_MANIFEST_SCHEMA)
    for field in (
        "market",
        "stream",
        "date",
        "hour",
        "relative_path",
        "logical_sha256",
        "stored_sha256",
        "stored_bytes",
        "row_count",
        "source_chunk_hashes",
    ):
        if entry.get(field) != manifest.get(field):
            raise ReplayCatalogError(f"partition build/manifest mismatch: {field}")
    if manifest.get("dataset_version") != build.dataset_version:
        raise ReplayCatalogError("partition dataset version mismatch")
    if manifest.get("dedup_version") != build.dedup_version:
        raise ReplayCatalogError("partition dedup version mismatch")
    if manifest.get("parquet_profile") != build.parquet_profile:
        raise ReplayCatalogError("partition writer profile mismatch")
    if not artifact.is_file():
        raise ReplayCatalogError("normalized partition artifact is unavailable")
    stored_bytes = _integer(manifest, "stored_bytes")
    if artifact.stat().st_size != stored_bytes:
        raise ReplayCatalogError("normalized partition size mismatch")
    stored_sha = _sha256(manifest, "stored_sha256")
    if _sha256_file(artifact) != stored_sha:
        raise ReplayCatalogError("normalized partition SHA-256 mismatch")
    descriptor = PartitionDescriptor(
        market=_text(manifest, "market"),
        stream=_text(manifest, "stream"),
        date=_text(manifest, "date"),
        hour=_text(manifest, "hour"),
        schema_version=_text(manifest, "stream_schema_version"),
        logical_sha256=_sha256(manifest, "logical_sha256"),
        stored_sha256=stored_sha,
        stored_bytes=stored_bytes,
        row_count=_integer(manifest, "row_count"),
        receive_time_utc_min_ns=_integer(manifest, "receive_time_utc_min_ns"),
        receive_time_utc_max_ns=_integer(manifest, "receive_time_utc_max_ns"),
        source_chunk_hashes=_sha_list(
            manifest.get("source_chunk_hashes"), "source_chunk_hashes"
        ),
    )
    return _PartitionSource(descriptor, artifact)


def _checkpoint(
    *,
    data_root: Path,
    entry_value: object,
    build_source_hashes: frozenset[str],
) -> CheckpointDescriptor:
    entry = _object(entry_value, "checkpoint entry")
    path = _safe_path(data_root, entry.get("relative_path"))
    if not path.is_file():
        raise ReplayCatalogError("checkpoint artifact is unavailable")
    file_sha = _sha256(entry, "file_sha256")
    if _sha256_file(path) != file_sha:
        raise ReplayCatalogError("checkpoint file SHA-256 mismatch")
    try:
        document_value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ReplayCatalogError("checkpoint document is invalid") from exc
    document = _object(document_value, "checkpoint")
    if document.get("schema_version") != CHECKPOINT_SCHEMA:
        raise ReplayCatalogError("unsupported checkpoint schema")
    if document.get("algorithm_version") != LocalBookReconstructor.algorithm_version:
        raise ReplayCatalogError("unsupported checkpoint algorithm")
    checkpoint_id = _text(document, "checkpoint_id")
    if checkpoint_id != _text(entry, "checkpoint_id"):
        raise ReplayCatalogError("checkpoint identity differs from build")
    expected_document_sha = _sha256(document, "checkpoint_sha256")
    unsigned = dict(document)
    unsigned.pop("checkpoint_sha256", None)
    if _sha256_json(unsigned) != expected_document_sha:
        raise ReplayCatalogError("checkpoint document hash mismatch")
    if entry.get("checkpoint_sha256") != expected_document_sha:
        raise ReplayCatalogError("checkpoint hash differs from build")
    book_value = _object(document.get("book"), "checkpoint book")
    try:
        book = OrderBook.from_mapping(book_value)
    except OrderBookDataError as exc:
        raise ReplayCatalogError("checkpoint book is invalid") from exc
    book_hash = _sha256(document, "book_hash")
    if book.logical_hash() != book_hash or entry.get("book_hash") != book_hash:
        raise ReplayCatalogError("checkpoint book hash mismatch")
    source_hashes = _sha_list(document.get("source_chunk_hashes"), "source_chunk_hashes")
    if not set(source_hashes) <= build_source_hashes:
        raise ReplayCatalogError("checkpoint source is outside selected build")
    if entry.get("source_chunk_hashes") != list(source_hashes):
        raise ReplayCatalogError("checkpoint source lineage differs from build")
    intervals = _array(document.get("unreliable_intervals"), "unreliable_intervals")
    frozen_intervals: list[Mapping[str, object]] = []
    for interval in intervals:
        frozen_intervals.append(MappingProxyType(dict(_object(interval, "gap interval"))))
    return CheckpointDescriptor(
        checkpoint_id=checkpoint_id,
        schema_version=CHECKPOINT_SCHEMA,
        algorithm_version=_text(document, "algorithm_version"),
        market=book.market,
        symbol=book.symbol,
        update_id=book.update_id,
        created_at_utc_ns=_integer(document, "created_at_utc_ns"),
        book_hash=book_hash,
        file_sha256=file_sha,
        source_chunk_hashes=source_hashes,
        book=_freeze_mapping(book.canonical_mapping()),
        unreliable_intervals=tuple(frozen_intervals),
    )


class ManifestCatalog:
    """Read-only entry point for published normalized builds."""

    def __init__(self, data_root: Path) -> None:
        self.data_root = data_root.expanduser().resolve()
        self.builds_root = (
            self.data_root / "data" / "normalized" / DATASET_VERSION / "builds"
        )

    def list_builds(self) -> tuple[BuildSummary, ...]:
        summaries: list[BuildSummary] = []
        for path in sorted(self.builds_root.glob("*.manifest.json")):
            document = _decode(path, BUILD_MANIFEST_SCHEMA)
            summary = _summary(document)
            if path.name != f"{summary.build_id}.manifest.json":
                raise ReplayCatalogError("build manifest filename/identity mismatch")
            summaries.append(summary)
        return tuple(summaries)

    def open_build(self, build_id: str) -> ReplayDataset:
        if len(build_id) != 64 or any(
            character not in "0123456789abcdef" for character in build_id
        ):
            raise ReplayCatalogError("build_id must be lowercase SHA-256")
        path = self.builds_root / f"{build_id}.manifest.json"
        if not path.is_file():
            raise ReplayCatalogError(f"normalized build is unavailable: {build_id}")
        document = _decode(path, BUILD_MANIFEST_SCHEMA)
        summary = _summary(document)
        if summary.build_id != build_id:
            raise ReplayCatalogError("selected build identity mismatch")
        partitions = tuple(
            _partition(data_root=self.data_root, build_entry=item, build=summary)
            for item in _array(document.get("partitions"), "partitions")
        )
        if sum(item.descriptor.row_count for item in partitions) != summary.normalized_rows:
            raise ReplayCatalogError("build normalized row count mismatch")
        raw_sources = _array(document.get("raw_sources"), "raw_sources")
        source_hashes = frozenset(
            _sha256(_object(item, "raw source"), "uncompressed_sha256")
            for item in raw_sources
        )
        checkpoints = tuple(
            _checkpoint(
                data_root=self.data_root,
                entry_value=item,
                build_source_hashes=source_hashes,
            )
            for item in _array(document.get("checkpoints"), "checkpoints")
        )
        opened = _OpenedBuild(summary, partitions, checkpoints)
        from .reader import ReplayDataset

        return ReplayDataset(opened)
