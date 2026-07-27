"""市场级深度再同步的证据与协调。

DepthResyncCoordinator 是唯一的决策点,将生命周期违背(意外断开、计划轮换、
服务端关机、序列缺口或 bootstrap 缓冲区溢出)转换为幂等的 snapshot 触发恢复周期。
Spot 和 USD-M 各自拥有隔离的协调器,因此一个市场的 resync 永不会停滞另一个。

关键不变量:
- 在活跃 resync 周期内,request() 是幂等的:仅首次调用递增 failure_count
  并创建 Catalog 证据。
- complete() 记录 snapshot 来源、恢复的 update ID,并重置活跃请求。
- prepare_restart() 是 resync 循环到会话重启的交接点;它清除 asyncio 事件,
  使下一周期可重新触发。
- RLock 保护 _active 和 _failure_count 免受并发观察和生命周期回调竞争。
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from hashlib import sha256
from threading import RLock
from uuid import uuid4

from ..domain.event import EventEnvelope
from ..storage.catalog import Catalog


@dataclass(frozen=True, slots=True)
class ResyncRequest:
    reason: str
    gap_started_at_utc_ns: int
    original_connection_id: str | None
    failure_count: int


class DepthResyncCoordinator:
    """Coordinate one market's bounded capture-session restart."""

    def __init__(self, *, market: str, catalog: Catalog) -> None:
        self.market = market
        self.catalog = catalog
        self.requested = asyncio.Event()
        self._lock = RLock()
        self._last_connection_id: str | None = None
        self._active: ResyncRequest | None = None
        self._failure_count = 0

    def observe_depth(self, envelope: EventEnvelope) -> None:
        with self._lock:
            self._last_connection_id = envelope.connection_id

    def request(self, reason: str, occurred_at_utc_ns: int | None = None) -> None:
        if not reason:
            raise ValueError("depth resync reason must be non-empty")
        with self._lock:
            if self._active is None:
                self._failure_count += 1
                self._active = ResyncRequest(
                    reason=reason,
                    gap_started_at_utc_ns=occurred_at_utc_ns or time.time_ns(),
                    original_connection_id=self._last_connection_id,
                    failure_count=self._failure_count,
                )
                request = self._active
                self.catalog.record_operational_event(
                    event_id=f"depth-resync-requested:{self.market}:{uuid4()}",
                    event_type="DEPTH_RESYNC_REQUESTED",
                    occurred_at_utc_ns=request.gap_started_at_utc_ns,
                    evidence={
                        "market": self.market,
                        "reason": request.reason,
                        "gap_started_at_utc_ns": request.gap_started_at_utc_ns,
                        "interval_classification": "UNRELIABLE",
                        "original_connection_id": request.original_connection_id,
                        "failure_count": request.failure_count,
                    },
                )
            self.requested.set()

    def complete(self, snapshot: EventEnvelope, recovered_update_id: int) -> None:
        with self._lock:
            request = self._active
            if request is None:
                return
            completed_at = time.time_ns()
            self.catalog.record_operational_event(
                event_id=f"depth-resync-completed:{self.market}:{uuid4()}",
                event_type="DEPTH_RESYNC_COMPLETED",
                occurred_at_utc_ns=completed_at,
                evidence={
                    "market": self.market,
                    "reason": request.reason,
                    "gap_started_at_utc_ns": request.gap_started_at_utc_ns,
                    "gap_ended_at_utc_ns": completed_at,
                    "interval_classification": "UNRELIABLE",
                    "original_connection_id": request.original_connection_id,
                    "new_connection_id": self._last_connection_id,
                    "snapshot_provenance": {
                        "payload_encoding": snapshot.payload_encoding,
                        "raw_payload_sha256": sha256(
                            snapshot.raw_payload
                        ).hexdigest(),
                        "source_sequence": dict(snapshot.source_sequence),
                    },
                    "recovered_update_id": recovered_update_id,
                    "failure_count": request.failure_count,
                },
            )
            self._active = None

    def prepare_restart(self) -> ResyncRequest | None:
        with self._lock:
            request = self._active
            self.requested.clear()
            return request

    @property
    def active(self) -> ResyncRequest | None:
        with self._lock:
            return self._active
