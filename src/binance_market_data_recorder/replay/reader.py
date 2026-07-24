"""Bounded deterministic replay over one verified normalized build."""

from __future__ import annotations

import heapq
import json
import tempfile
from collections.abc import Iterator, Mapping, Sequence
from contextlib import ExitStack
from pathlib import Path
from typing import BinaryIO

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from .catalog import _OpenedBuild, _PartitionSource
from .clock import EventClock
from .model import (
    BuildSummary,
    CheckpointDescriptor,
    CheckpointSeekError,
    GapPolicy,
    PartitionDescriptor,
    ReplayClock,
    ReplayError,
    ReplayEvent,
    ReplayGapError,
    ReplayQuery,
)

REPLAY_BATCH_ROWS = 10_000
MERGE_FAN_IN = 32


def _canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _text(row: Mapping[str, object], name: str) -> str:
    value = row.get(name)
    if not isinstance(value, str):
        raise ReplayError(f"normalized row {name} must be text")
    return value


def _integer(row: Mapping[str, object], name: str) -> int:
    value = row.get(name)
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ReplayError(f"normalized row {name} must be a non-negative integer")
    return value


def _boolean(row: Mapping[str, object], name: str) -> bool:
    value = row.get(name)
    if not isinstance(value, bool):
        raise ReplayError(f"normalized row {name} must be boolean")
    return value


def _order_key(
    row: Mapping[str, object],
    *,
    clock: ReplayClock,
    event_time_ns: int,
) -> tuple[int | str, ...]:
    common: tuple[int | str, ...] = (
        _text(row, "market"),
        _text(row, "stream"),
        _text(row, "symbol"),
        _text(row, "collector_instance_id"),
        _text(row, "connection_id"),
        _integer(row, "receive_monotonic_ns"),
        _text(row, "source_sequence_json"),
        _text(row, "source_chunk_sha256"),
        _integer(row, "source_record_ordinal"),
        _integer(row, "source_subrecord_ordinal"),
        _text(row, "logical_record_sha256"),
    )
    if clock is ReplayClock.EXCHANGE_TIME:
        return (event_time_ns, _integer(row, "receive_time_utc_ns"), *common)
    return (event_time_ns, *common)


def _document_key(document: Mapping[str, object]) -> tuple[int | str, ...]:
    key = document.get("order_key")
    if not isinstance(key, list) or any(
        not isinstance(value, (int, str)) or isinstance(value, bool) for value in key
    ):
        raise ReplayError("replay work file has an invalid order key")
    return tuple(key)


def _write_run(path: Path, documents: list[dict[str, object]]) -> None:
    documents.sort(key=_document_key)
    with path.open("wb", buffering=0) as target:
        for document in documents:
            target.write((_canonical_json(document) + "\n").encode())


def _iterator(handle: BinaryIO, path: Path) -> Iterator[dict[str, object]]:
    for line in handle:
        try:
            document = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ReplayError(f"invalid ephemeral replay run {path.name}") from exc
        if not isinstance(document, dict):
            raise ReplayError(f"ephemeral replay row is not an object: {path.name}")
        yield document


def _merge_documents(paths: Sequence[Path]) -> Iterator[dict[str, object]]:
    with ExitStack() as stack:
        iterators: list[Iterator[dict[str, object]]] = []
        for path in paths:
            handle = stack.enter_context(path.open("rb", buffering=0))
            iterators.append(_iterator(handle, path))
        heap: list[
            tuple[tuple[int | str, ...], int, dict[str, object]]
        ] = []
        for index, iterator in enumerate(iterators):
            try:
                document = next(iterator)
            except StopIteration:
                continue
            heapq.heappush(heap, (_document_key(document), index, document))
        while heap:
            _key, index, document = heapq.heappop(heap)
            yield document
            try:
                following = next(iterators[index])
            except StopIteration:
                continue
            heapq.heappush(
                heap, (_document_key(following), index, following)
            )


def _collapse_runs(paths: list[Path], root: Path) -> list[Path]:
    generation = 0
    current = paths
    while len(current) > MERGE_FAN_IN:
        following: list[Path] = []
        for group_start in range(0, len(current), MERGE_FAN_IN):
            group = current[group_start : group_start + MERGE_FAN_IN]
            target = root / (
                f"merge-{generation:04d}-{len(following):08d}.ndjson"
            )
            with target.open("wb", buffering=0) as output:
                for document in _merge_documents(group):
                    output.write((_canonical_json(document) + "\n").encode())
            following.append(target)
            for path in group:
                path.unlink()
        current = following
        generation += 1
    return current


def _matches(query: ReplayQuery, row: Mapping[str, object]) -> bool:
    market = _text(row, "market")
    stream = _text(row, "stream")
    symbol = _text(row, "symbol")
    return (
        (not query.markets or market in query.markets)
        and (not query.streams or stream in query.streams)
        and symbol == query.symbol
    )


def _in_range(query: ReplayQuery, event_time_ns: int) -> bool:
    return not (
        query.start_time_ns is not None and event_time_ns < query.start_time_ns
    ) and not (
        query.end_time_ns is not None and event_time_ns >= query.end_time_ns
    )


def _checkpoint_for(
    query: ReplayQuery,
    checkpoints: tuple[CheckpointDescriptor, ...],
) -> CheckpointDescriptor | None:
    if query.checkpoint_id is None:
        return None
    matches = [
        checkpoint
        for checkpoint in checkpoints
        if checkpoint.checkpoint_id == query.checkpoint_id
    ]
    if len(matches) != 1:
        raise CheckpointSeekError("checkpoint is not part of the selected build")
    checkpoint = matches[0]
    if (
        query.markets != (checkpoint.market,)
        or query.streams != ("diff_depth",)
        or query.symbol != checkpoint.symbol
    ):
        raise CheckpointSeekError(
            "checkpoint seek requires its single market/symbol diff_depth query"
        )
    return checkpoint


def _after_checkpoint(
    row: Mapping[str, object], checkpoint: CheckpointDescriptor | None
) -> bool:
    if checkpoint is None:
        return True
    final_update_id = row.get("final_update_id")
    if (
        not isinstance(final_update_id, int)
        or isinstance(final_update_id, bool)
        or final_update_id < 0
    ):
        raise CheckpointSeekError(
            "checkpoint seek encountered depth data without final_update_id"
        )
    return final_update_id > checkpoint.update_id


def _selected_partitions(
    sources: tuple[_PartitionSource, ...], query: ReplayQuery
) -> tuple[_PartitionSource, ...]:
    return tuple(
        source
        for source in sources
        if (not query.markets or source.descriptor.market in query.markets)
        and (not query.streams or source.descriptor.stream in query.streams)
    )


class ReplayDataset:
    """One immutable, explicitly selected normalized build."""

    def __init__(self, opened: _OpenedBuild) -> None:
        self._opened = opened

    @property
    def summary(self) -> BuildSummary:
        return self._opened.summary

    def partitions(
        self,
        *,
        markets: tuple[str, ...] = (),
        streams: tuple[str, ...] = (),
    ) -> tuple[PartitionDescriptor, ...]:
        return tuple(
            source.descriptor
            for source in self._opened.partitions
            if (not markets or source.descriptor.market in markets)
            and (not streams or source.descriptor.stream in streams)
        )

    def checkpoints(self) -> tuple[CheckpointDescriptor, ...]:
        return self._opened.checkpoints

    def checkpoint(self, checkpoint_id: str) -> CheckpointDescriptor:
        matches = [
            value
            for value in self._opened.checkpoints
            if value.checkpoint_id == checkpoint_id
        ]
        if len(matches) != 1:
            raise CheckpointSeekError("checkpoint is not part of the selected build")
        return matches[0]

    def replay(self, query: ReplayQuery) -> Iterator[ReplayEvent]:
        checkpoint = _checkpoint_for(query, self._opened.checkpoints)
        clock = EventClock(query.clock, query.missing_exchange_time)
        with tempfile.TemporaryDirectory(prefix="bmdr-replay-") as temporary:
            root = Path(temporary)
            runs: list[Path] = []
            batch: list[dict[str, object]] = []
            for source in _selected_partitions(self._opened.partitions, query):
                parquet = pq.ParquetFile(source.artifact_path)
                for arrow_batch in parquet.iter_batches(batch_size=REPLAY_BATCH_ROWS):
                    for row_value in pa.Table.from_batches([arrow_batch]).to_pylist():
                        if not isinstance(row_value, dict):
                            raise ReplayError("normalized Parquet row is not an object")
                        row = dict(row_value)
                        if row.get("dataset_version") != self.summary.dataset_version:
                            raise ReplayError("normalized row dataset version mismatch")
                        if not _matches(query, row):
                            continue
                        reading = clock.resolve(row)
                        if reading is None:
                            continue
                        if not _in_range(query, reading.event_time_ns):
                            continue
                        if not _after_checkpoint(row, checkpoint):
                            continue
                        unreliable = _boolean(row, "source_gap") or not _boolean(
                            row, "source_complete"
                        )
                        if unreliable and query.gap_policy is GapPolicy.ERROR:
                            raise ReplayGapError(
                                "selected replay range contains unreliable source data"
                            )
                        if unreliable and query.gap_policy is GapPolicy.EXCLUDE:
                            continue
                        batch.append(
                            {
                                "order_key": list(
                                    _order_key(
                                        row,
                                        clock=query.clock,
                                        event_time_ns=reading.event_time_ns,
                                    )
                                ),
                                "event_time_ns": reading.event_time_ns,
                                "used_receive_time_fallback": (
                                    reading.used_receive_time_fallback
                                ),
                                "is_unreliable": unreliable,
                                "row": row,
                            }
                        )
                        if len(batch) >= REPLAY_BATCH_ROWS:
                            path = root / f"run-{len(runs):08d}.ndjson"
                            _write_run(path, batch)
                            runs.append(path)
                            batch = []
            if batch:
                path = root / f"run-{len(runs):08d}.ndjson"
                _write_run(path, batch)
                runs.append(path)
            runs = _collapse_runs(runs, root)
            for document in _merge_documents(runs):
                replay_row = document.get("row")
                if not isinstance(replay_row, dict):
                    raise ReplayError("replay work row has invalid normalized content")
                yield ReplayEvent.create(
                    dataset_version=self.summary.dataset_version,
                    build_id=self.summary.build_id,
                    clock=query.clock,
                    event_time_ns=_integer(document, "event_time_ns"),
                    used_receive_time_fallback=_boolean(
                        document, "used_receive_time_fallback"
                    ),
                    is_unreliable=_boolean(document, "is_unreliable"),
                    row=replay_row,
                )
