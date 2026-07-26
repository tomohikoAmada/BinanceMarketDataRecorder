"""原子、哈希验证的本地订单簿 checkpoint。

Checkpoint 记录 (market, 重建后的 logical_orderbook 的 SHA-256, last_update_id, 时间戳)。
M6 规范化管道将 checkpoint 作为 content-addressed artifact 绑定到 normalized build
manifest 中, 供 replay 消费者验证深度状态恢复。

原子性: checkpoint JSON 写入临时文件, fsync, 然后原子 rename 到最终路径。
写入中断时残留的 .tmp 文件在下次写入前被清理。
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from collections.abc import Callable, Sequence
from dataclasses import asdict
from pathlib import Path
from typing import Any
from uuid import uuid4

from ..storage.catalog import Catalog
from ..storage.layout import StorageLayout, fsync_directory
from .model import OrderBook, OrderBookDataError
from .reconstructor import LocalBookReconstructor, UnreliableInterval

CHECKPOINT_SCHEMA = "orderbook-checkpoint.v1"


class CheckpointError(RuntimeError):
    """Raised when a checkpoint cannot be trusted."""


class OrderBookCheckpointStore:
    def __init__(self, layout: StorageLayout, catalog: Catalog) -> None:
        self.layout = layout
        self.catalog = catalog

    def save(
        self,
        reconstructor: LocalBookReconstructor,
        *,
        collector_version: str,
        source_chunk_hashes: Sequence[str] = (),
        utc_clock_ns: Callable[[], int] = time.time_ns,
    ) -> Path:
        if not reconstructor.is_reliable:
            raise CheckpointError("cannot checkpoint an unreliable order book")
        if not collector_version:
            raise CheckpointError("collector version is required")
        if not source_chunk_hashes:
            raise CheckpointError("at least one source chunk hash is required")
        if any(not _is_sha256(value) for value in source_chunk_hashes):
            raise CheckpointError("source chunk hashes must be lowercase SHA-256")
        book = reconstructor.book
        checkpoint_id = str(uuid4())
        created_at = utc_clock_ns()
        document: dict[str, object] = {
            "schema_version": CHECKPOINT_SCHEMA,
            "algorithm_version": reconstructor.algorithm_version,
            "checkpoint_id": checkpoint_id,
            "created_at_utc_ns": created_at,
            "collector_version": collector_version,
            "book": book.canonical_mapping(),
            "book_hash": book.logical_hash(),
            "source_chunk_hashes": sorted(set(source_chunk_hashes)),
            "unreliable_intervals": [
                asdict(interval) for interval in reconstructor.unreliable_intervals
            ],
        }
        document["checkpoint_sha256"] = _document_hash(document)
        encoded = json.dumps(
            document, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode()
        final = self.layout.checkpoints / f"{checkpoint_id}.orderbook.json"
        partial = final.with_suffix(final.suffix + ".partial")
        descriptor = os.open(partial, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            with os.fdopen(descriptor, "wb", closefd=False) as target:
                target.write(encoded)
                target.flush()
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.replace(partial, final)
        fsync_directory(self.layout.checkpoints)
        self.catalog.register_orderbook_checkpoint(
            checkpoint_id=checkpoint_id,
            market=book.market,
            symbol=book.symbol,
            update_id=book.update_id,
            book_hash=book.logical_hash(),
            relative_path=self.layout.relative(final),
            created_at_utc_ns=created_at,
        )
        return final

    def restore(self, path: Path) -> LocalBookReconstructor:
        try:
            document = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(document, dict):
                raise TypeError
            if document.get("schema_version") != CHECKPOINT_SCHEMA:
                raise CheckpointError("unsupported checkpoint schema")
            if document.get("algorithm_version") != LocalBookReconstructor.algorithm_version:
                raise CheckpointError("unsupported reconstruction algorithm")
            checkpoint_id = document.get("checkpoint_id")
            collector_version = document.get("collector_version")
            created_at = document.get("created_at_utc_ns")
            source_hashes = document.get("source_chunk_hashes")
            if (
                not isinstance(checkpoint_id, str)
                or not checkpoint_id
                or not isinstance(collector_version, str)
                or not collector_version
                or not isinstance(created_at, int)
                or isinstance(created_at, bool)
                or created_at < 0
                or not isinstance(source_hashes, list)
                or not source_hashes
                or any(
                    not isinstance(value, str) or not _is_sha256(value) for value in source_hashes
                )
            ):
                raise CheckpointError("invalid checkpoint provenance")
            expected_document_hash = document.get("checkpoint_sha256")
            unsigned_document = dict(document)
            unsigned_document.pop("checkpoint_sha256", None)
            if (
                not isinstance(expected_document_hash, str)
                or _document_hash(unsigned_document) != expected_document_hash
            ):
                raise CheckpointError("checkpoint document hash mismatch")
            book_value = document["book"]
            if not isinstance(book_value, dict):
                raise TypeError
            book = OrderBook.from_mapping(book_value)
            expected_hash = document["book_hash"]
            if not isinstance(expected_hash, str) or book.logical_hash() != expected_hash:
                raise CheckpointError("checkpoint book hash mismatch")
            intervals = _intervals(document.get("unreliable_intervals", []))
        except (OSError, json.JSONDecodeError, KeyError, TypeError, OrderBookDataError) as exc:
            raise CheckpointError("invalid order-book checkpoint") from exc
        return LocalBookReconstructor.from_checkpoint(book, intervals)


def _document_hash(document: dict[str, object]) -> str:
    encoded = json.dumps(
        document, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _is_sha256(value: str) -> bool:
    return len(value) == 64 and all(character in "0123456789abcdef" for character in value)


def _intervals(value: object) -> list[UnreliableInterval]:
    if not isinstance(value, list):
        raise TypeError
    intervals: list[UnreliableInterval] = []
    for item in value:
        if not isinstance(item, dict):
            raise TypeError
        intervals.append(_interval(item))
    return intervals


def _interval(value: dict[str, Any]) -> UnreliableInterval:
    market = value.get("market")
    if market not in {"spot", "um_perpetual"}:
        raise TypeError
    reason = value.get("reason")
    if not isinstance(reason, str) or not reason:
        raise TypeError
    try:
        return UnreliableInterval(
            market=market,
            reason=reason,
            last_reliable_update_id=_non_negative_int(value["last_reliable_update_id"]),
            offending_first_update_id=_non_negative_int(value["offending_first_update_id"]),
            offending_final_update_id=_non_negative_int(value["offending_final_update_id"]),
            started_at_receive_time_utc_ns=_non_negative_int(
                value["started_at_receive_time_utc_ns"]
            ),
            ended_at_update_id=(
                _non_negative_int(value["ended_at_update_id"])
                if value.get("ended_at_update_id") is not None
                else None
            ),
            complete=False,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise TypeError from exc


def _non_negative_int(value: object) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise TypeError
    return value
