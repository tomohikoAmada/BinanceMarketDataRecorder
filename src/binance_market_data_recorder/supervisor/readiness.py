"""Thread-safe health and order-book readiness evidence for one market instance."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from threading import RLock

from ..domain.event import EventEnvelope, Market
from ..orderbook.model import OrderBookDataError
from ..orderbook.parser import depth_update_from_envelope, snapshot_from_envelope
from ..orderbook.reconstructor import (
    LocalBookReconstructor,
    QualityAudit,
    SynchronizeResult,
)

CORE_STREAMS = frozenset({"diff_depth", "agg_trade", "book_ticker"})


@dataclass(frozen=True, slots=True)
class ReadinessSnapshot:
    market: Market
    symbol: str
    collector_instance_id: str
    collector_version: str
    connected_streams: frozenset[str]
    persisted_streams: frozenset[str]
    snapshot_persisted: bool
    orderbook_synchronized: bool
    event_count: int
    last_receive_time_utc_ns: int | None
    failure: str | None

    @property
    def ready(self) -> bool:
        return (
            self.failure is None
            and self.connected_streams >= CORE_STREAMS
            and self.persisted_streams >= CORE_STREAMS
            and self.snapshot_persisted
            and self.orderbook_synchronized
        )

    def public_dict(self) -> dict[str, object]:
        return {
            "market": self.market,
            "symbol": self.symbol,
            "collector_instance_id": self.collector_instance_id,
            "collector_version": self.collector_version,
            "connected_streams": sorted(self.connected_streams),
            "persisted_streams": sorted(self.persisted_streams),
            "snapshot_persisted": self.snapshot_persisted,
            "orderbook_synchronized": self.orderbook_synchronized,
            "event_count": self.event_count,
            "last_receive_time_utc_ns": self.last_receive_time_utc_ns,
            "failure": self.failure,
            "ready": self.ready,
        }


class CollectorReadiness:
    """Build readiness only from persisted core events and official snapshot sync."""

    def __init__(
        self,
        *,
        market: Market,
        collector_instance_id: str,
        collector_version: str,
        symbol: str = "BTCUSDT",
        audit_observer: Callable[[QualityAudit, int | None], None] | None = None,
        bootstrap_buffer_capacity: int = 8192,
        bootstrap_overflow_observer: Callable[[], None] | None = None,
    ) -> None:
        if not collector_instance_id or not collector_version:
            raise ValueError("Collector readiness identity must be non-empty")
        self._market = market
        self._symbol = symbol
        self._instance_id = collector_instance_id
        self._version = collector_version
        self._connected: set[str] = set()
        self._persisted: set[str] = set()
        self._snapshot_persisted = False
        self._event_count = 0
        self._last_receive_time_utc_ns: int | None = None
        self._failure: str | None = None
        self._audit_observer = audit_observer
        self._bootstrap_buffer_capacity = bootstrap_buffer_capacity
        self._bootstrap_overflow_observer = bootstrap_overflow_observer
        self._book = LocalBookReconstructor(
            market,
            symbol,
            audit_observer=audit_observer,
            bootstrap_buffer_capacity=bootstrap_buffer_capacity,
        )
        self._lock = RLock()

    def observe_connected(self, stream: str) -> None:
        if stream in CORE_STREAMS:
            with self._lock:
                self._connected.add(stream)

    def observe_disconnected(self, stream: str) -> None:
        if stream in CORE_STREAMS:
            with self._lock:
                self._connected.discard(stream)
                self._persisted.discard(stream)

    def observe_persisted(self, envelope: EventEnvelope) -> None:
        self._check_identity(envelope)
        if envelope.stream not in CORE_STREAMS:
            return
        with self._lock:
            self._persisted.add(envelope.stream)
            self._event_count += 1
            self._last_receive_time_utc_ns = envelope.receive_time_utc_ns
            if envelope.stream == "diff_depth":
                try:
                    self._book.offer(depth_update_from_envelope(envelope))
                except OrderBookDataError:
                    return
                if (
                    self._book.bootstrap_buffer_overflowed
                    and self._bootstrap_overflow_observer is not None
                ):
                    self._bootstrap_overflow_observer()

    def observe_snapshot_persisted(self, envelope: EventEnvelope) -> SynchronizeResult:
        self._check_identity(envelope)
        with self._lock:
            self._snapshot_persisted = True
            try:
                snapshot = snapshot_from_envelope(envelope)
            except OrderBookDataError:
                return SynchronizeResult.NEED_MORE_EVENTS
            return self._book.synchronize(snapshot)

    def record_failure(self, reason: str) -> None:
        if not reason:
            raise ValueError("readiness failure must be non-empty")
        with self._lock:
            self._failure = reason

    def restart_bootstrap(self) -> None:
        """Reset readiness after bounded-buffer invalidation and before reconnect."""

        with self._lock:
            self._connected.clear()
            self._persisted.clear()
            self._snapshot_persisted = False
            self._failure = None
            self._book = LocalBookReconstructor(
                self._market,
                self._symbol,
                audit_observer=self._audit_observer,
                bootstrap_buffer_capacity=self._bootstrap_buffer_capacity,
            )

    def snapshot(self) -> ReadinessSnapshot:
        with self._lock:
            return ReadinessSnapshot(
                market=self._market,
                symbol=self._symbol,
                collector_instance_id=self._instance_id,
                collector_version=self._version,
                connected_streams=frozenset(self._connected),
                persisted_streams=frozenset(self._persisted),
                snapshot_persisted=self._snapshot_persisted,
                orderbook_synchronized=self._book.is_reliable,
                event_count=self._event_count,
                last_receive_time_utc_ns=self._last_receive_time_utc_ns,
                failure=self._failure,
            )

    @property
    def reliable_update_id(self) -> int | None:
        """Expose only the fully bridged and applied local-book update ID."""

        with self._lock:
            return self._book.reliable_update_id

    def _check_identity(self, envelope: EventEnvelope) -> None:
        if (
            envelope.market != self._market
            or envelope.symbol != self._symbol
            or envelope.collector_instance_id != self._instance_id
            or envelope.collector_version != self._version
        ):
            raise ValueError("readiness envelope identity differs from Collector")
