"""Failure-isolated Spot public side-data tasks."""

from __future__ import annotations

import asyncio
import json
import logging

from binance_common.errors import RateLimitBanError, TooManyRequestsError

from ..binance.spot.exchange_info import (
    SpotExchangeInfoApi,
    capture_spot_exchange_info,
)
from ..binance.spot.rate_limit import shared_spot_ip_rate_limiter
from ..spool.stream import StreamSpool
from .usdm_side_data import SideDataStats


class SpotExchangeInfoPoller:
    """Stateless REST poller: retryable by the supervisor (no transport).

    Each request mints its own connection_id; there is no WebSocket
    continuity to protect, so ``terminal_on_failure`` stays False.
    """

    terminal_on_failure = False

    def __init__(
        self,
        *,
        interval_seconds: float,
        spool: StreamSpool,
        stats: SideDataStats,
        collector_instance_id: str,
        collector_version: str,
        rest_api: SpotExchangeInfoApi | None,
        timeout_ms: int,
        logger: logging.Logger,
    ) -> None:
        self.interval_seconds = interval_seconds
        self.spool = spool
        self.stats = stats
        self.collector_instance_id = collector_instance_id
        self.collector_version = collector_version
        self.rest_api = rest_api
        self.timeout_ms = timeout_ms
        self.logger = logger

    async def run(self, stop: asyncio.Event) -> None:
        try:
            while not stop.is_set():
                try:
                    limiter = shared_spot_ip_rate_limiter()
                    await limiter.acquire_weight(weight=20)
                    async with limiter.request_slot():
                        envelope = await asyncio.to_thread(
                            capture_spot_exchange_info,
                            rest_api=self.rest_api,
                            collector_instance_id=self.collector_instance_id,
                            collector_version=self.collector_version,
                            timeout_ms=self.timeout_ms,
                        )
                    provenance = json.loads(envelope.raw_payload)
                    await limiter.observe_success_weight(
                        weight=20,
                        headers=provenance["response"]["headers"],
                    )
                    self.spool.enqueue(envelope)
                    await asyncio.to_thread(self.spool.drain_all)
                    self.stats.accepted += 1
                    self.stats.observe_success()
                except (RateLimitBanError, TooManyRequestsError) as exc:
                    status = 418 if isinstance(exc, RateLimitBanError) else 429
                    limiter = shared_spot_ip_rate_limiter()
                    await limiter.observe_weight_rejection(
                        status=status,
                        weight=20,
                        headers={},
                        body_text=str(exc),
                    )
                    self.stats.observe_failure(type(exc).__name__)
                except (OSError, RuntimeError, TimeoutError, ValueError) as exc:
                    self.stats.observe_failure(type(exc).__name__)
                    self.logger.warning(
                        "Spot exchangeInfo failed without stopping core L2",
                        extra={"error_type": type(exc).__name__},
                    )
                try:
                    await asyncio.wait_for(
                        stop.wait(), timeout=self.interval_seconds
                    )
                except TimeoutError:
                    continue
        finally:
            await asyncio.to_thread(self.spool.close_and_seal)
