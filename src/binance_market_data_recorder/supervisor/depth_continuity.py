"""Bounded sequence-only capture health; full book reconstruction is a consumer.

Uses the same Spot U/u and USD-M U/u/pu bridge rules as the offline reconstructor.
No price levels, Decimal conversion, best-price scans or raw payloads are retained.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Callable
from dataclasses import dataclass

from ..domain.event import EventEnvelope, Market
from ..orderbook.model import OrderBookDataError
from ..orderbook.parser import depth_update_from_envelope, snapshot_from_envelope
from ..orderbook.reconstructor import QualityAudit, SynchronizeResult


@dataclass(frozen=True, slots=True)
class _Sequence:
    first: int
    last: int
    previous: int | None
    received: int


class DepthContinuity:
    def __init__(
        self,
        market: Market,
        *,
        capacity: int,
        audit_observer: Callable[[QualityAudit, int | None], None] | None,
    ) -> None:
        if capacity < 2:
            raise ValueError("bootstrap buffer capacity must be at least two")
        self.market = market
        self.capacity = capacity
        self.audit_observer = audit_observer
        self._buffer: list[_Sequence] = []
        self.reliable_update_id: int | None = None
        self.bootstrap_buffer_overflowed = False
        self._warned = False
        self.audits: deque[QualityAudit] = deque(maxlen=256)

    @property
    def is_reliable(self) -> bool:
        return self.reliable_update_id is not None

    def _audit(self, kind: str, update: _Sequence, detail: str) -> None:
        audit = QualityAudit(kind, self.market, update.last, detail)
        self.audits.append(audit)
        if self.audit_observer is not None:
            self.audit_observer(audit, update.received)

    def offer(self, envelope: EventEnvelope) -> None:
        sequence = envelope.source_sequence
        first, last, previous = (sequence.get(key) for key in ("U", "u", "pu"))
        if not (
            isinstance(first, int)
            and not isinstance(first, bool)
            and isinstance(last, int)
            and not isinstance(last, bool)
            and 0 <= first <= last
            and (
                self.market == "spot"
                or (isinstance(previous, int) and not isinstance(previous, bool) and previous >= 0)
            )
        ):
            # Compatibility for older callers constructing envelopes without the
            # extractor's metadata. Live valid envelopes take the allocation-free path.
            parsed = depth_update_from_envelope(envelope)
            if parsed.symbol != envelope.symbol:
                raise OrderBookDataError("depth payload identity differs from envelope")
            first, last, previous = (
                parsed.first_update_id,
                parsed.final_update_id,
                parsed.previous_final_update_id,
            )
        update = _Sequence(
            first,
            last,
            previous if isinstance(previous, int) else None,
            envelope.receive_time_utc_ns,
        )
        if self.is_reliable:
            self._apply(update)
            return
        if self.bootstrap_buffer_overflowed:
            return
        if len(self._buffer) >= self.capacity:
            self._buffer.clear()
            self.bootstrap_buffer_overflowed = True
            self._audit("bootstrap_buffer_overflow", update, "bounded sequence buffer exhausted")
            return
        self._buffer.append(update)
        if not self._warned and len(self._buffer) >= max(1, int(self.capacity * 0.75)):
            self._warned = True
            self._audit("bootstrap_buffer_near_capacity", update, "sequence buffer near capacity")

    def invalidate(self, received_at: int) -> None:
        current = self.reliable_update_id or 0
        self.reliable_update_id = None
        self._buffer.clear()
        self._audit(
            "sequence_gap",
            _Sequence(current, current, None, received_at),
            "malformed_depth_cannot_establish_continuity",
        )

    def _apply(self, update: _Sequence) -> bool:
        current = self.reliable_update_id
        if current is None:
            raise RuntimeError("no snapshot-backed depth sequence")
        if update.last <= current:
            self._audit("duplicate_or_stale_depth", update, "update already covered")
            return True
        reason = (
            "spot_first_update_id_exceeds_local_plus_one"
            if self.market == "spot" and update.first > current + 1
            else "usdm_pu_does_not_equal_previous_u"
            if self.market != "spot" and update.previous != current
            else None
        )
        if reason is not None:
            self.reliable_update_id = None
            self._buffer = [update]
            self._audit("sequence_gap", update, reason)
            return False
        self.reliable_update_id = update.last
        return True

    def synchronize(self, envelope: EventEnvelope) -> SynchronizeResult:
        if "malformed" in envelope.capture_flags:
            raise OrderBookDataError("malformed depth snapshot")
        # Snapshot parsing is infrequent and validates its existing provenance.
        snapshot = snapshot_from_envelope(envelope)
        if not self._buffer:
            return SynchronizeResult.NEED_MORE_EVENTS
        target = snapshot.last_update_id + (1 if self.market == "spot" else 0)
        candidates = [update for update in self._buffer if update.last >= target]
        if not candidates:
            self._buffer.clear()
            return SynchronizeResult.NEED_MORE_EVENTS
        if not candidates[0].first <= target <= candidates[0].last:
            return SynchronizeResult.SNAPSHOT_TOO_OLD
        self._buffer = []
        self._warned = self.bootstrap_buffer_overflowed = False
        self.reliable_update_id = candidates[0].last
        for index, update in enumerate(candidates[1:], start=1):
            if not self._apply(update):
                self._buffer.extend(candidates[index + 1 :])
                return SynchronizeResult.SEQUENCE_GAP
        return SynchronizeResult.SYNCHRONIZED
