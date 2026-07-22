"""Offline stream spool orchestration for bounded enqueue, rotation, and seal."""

from __future__ import annotations

from collections.abc import Callable

from ..domain.event import EventEnvelope
from ..storage.catalog import Catalog
from ..storage.layout import StorageLayout
from .queue import BoundedEventQueue
from .seal import seal_partial
from .writer import RawChunkWriter, RotationPolicy


class StreamSpool:
    """One homogeneous stream's queue and current chunk; no network ownership."""

    def __init__(
        self,
        *,
        layout: StorageLayout,
        catalog: Catalog,
        market: str,
        symbol: str,
        stream: str,
        collector_instance_id: str,
        collector_version: str,
        queue_capacity: int,
        rotation: RotationPolicy,
        durability_interval_seconds: float,
        max_frame_bytes: int,
        writer_factory: Callable[..., RawChunkWriter] = RawChunkWriter,
    ) -> None:
        self.layout = layout
        self.catalog = catalog
        self.market = market
        self.symbol = symbol
        self.stream = stream
        self.collector_instance_id = collector_instance_id
        self.collector_version = collector_version
        self.rotation = rotation
        self.durability_interval_seconds = durability_interval_seconds
        self.max_frame_bytes = max_frame_bytes
        self.queue = BoundedEventQueue(queue_capacity)
        self._writer_factory = writer_factory
        self._writer: RawChunkWriter | None = None

    def enqueue(self, envelope: EventEnvelope) -> None:
        self.queue.put_nowait(envelope)

    def _new_writer(self) -> RawChunkWriter:
        return self._writer_factory(
            layout=self.layout,
            catalog=self.catalog,
            market=self.market,
            symbol=self.symbol,
            stream=self.stream,
            collector_instance_id=self.collector_instance_id,
            collector_version=self.collector_version,
            rotation=self.rotation,
            durability_interval_seconds=self.durability_interval_seconds,
            max_frame_bytes=self.max_frame_bytes,
        )

    def drain_one(self) -> bool:
        envelope = self.queue.get_nowait()
        if envelope is None:
            if self._writer is not None:
                if self._writer.should_rotate():
                    self._seal_current()
                else:
                    self._writer.sync_if_due()
            return False
        if self._writer is None:
            self._writer = self._new_writer()
        self._writer.append(envelope)
        if self._writer.should_rotate():
            self._seal_current()
        return True

    def drain_all(self) -> int:
        count = 0
        while self.drain_one():
            count += 1
        return count

    def _seal_current(self) -> dict[str, object] | None:
        if self._writer is None:
            return None
        writer = self._writer
        self._writer = None
        writer.close()
        return seal_partial(writer.path, layout=self.layout, catalog=self.catalog)

    def close_and_seal(self) -> dict[str, object] | None:
        self.drain_all()
        return self._seal_current()
