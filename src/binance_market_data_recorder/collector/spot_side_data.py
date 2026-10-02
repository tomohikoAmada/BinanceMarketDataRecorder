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
from ..binance.spot.rate_limit import SpotIpRateLimiter, shared_spot_ip_rate_limiter
from ..binance.websocket_common import run_owned_blocking_call
from ..domain.event import EventEnvelope
from ..domain.product import product_log_fields
from ..logging import log_event
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

                    async def capture(
                        rate_limiter: SpotIpRateLimiter = limiter,
                    ) -> EventEnvelope | None:
                        await rate_limiter.acquire_weight(weight=20)
                        async with rate_limiter.request_slot():
                            if stop.is_set():
                                return None
                            try:
                                return await run_owned_blocking_call(
                                    capture_spot_exchange_info,
                                    symbol=self.spool.symbol,
                                    rest_api=self.rest_api,
                                    collector_instance_id=self.collector_instance_id,
                                    collector_version=self.collector_version,
                                    timeout_ms=self.timeout_ms,
                                )
                            except (RateLimitBanError, TooManyRequestsError) as exc:
                                status = 418 if isinstance(exc, RateLimitBanError) else 429
                                await rate_limiter.observe_weight_rejection(
                                    status=status, weight=20, headers={}, body_text=str(exc),
                                )
                                raise

                    request_task = asyncio.create_task(capture())
                    stop_task = asyncio.create_task(stop.wait())
                    try:
                        done, _pending = await asyncio.wait(
                            {request_task, stop_task}, return_when=asyncio.FIRST_COMPLETED,
                        )
                        if stop_task in done and stop.is_set():
                            request_task.cancel()
                            # Cancellation waits for an already-started SDK worker;
                            # no slot/worker can outlive the spool/Catalog shutdown.
                            (outcome,) = await asyncio.gather(request_task, return_exceptions=True)
                            if isinstance(outcome, BaseException) and not isinstance(
                                outcome, asyncio.CancelledError
                            ):
                                raise outcome
                            break
                        envelope = await request_task
                    finally:
                        for task in (request_task, stop_task):
                            if not task.done():
                                task.cancel()
                        await asyncio.gather(request_task, stop_task, return_exceptions=True)
                    if envelope is None:
                        break
                    provenance = json.loads(envelope.raw_payload)
                    await limiter.observe_success_weight(
                        weight=20,
                        headers=provenance["response"]["headers"],
                    )
                    self.spool.enqueue(envelope)
                    await run_owned_blocking_call(self.spool.drain_all)
                    await run_owned_blocking_call(self.spool.sync)
                    self.stats.accepted += 1
                    self.stats.observe_success()
                except (RateLimitBanError, TooManyRequestsError) as exc:
                    status = 418 if isinstance(exc, RateLimitBanError) else 429
                    self.stats.observe_failure(type(exc).__name__)
                    log_event(
                        self.logger,
                        logging.WARNING,
                        "spot_exchange_info_rate_limited",
                        "Spot exchangeInfo request was rate limited",
                        **product_log_fields(
                            "spot", self.spool.symbol, stream="exchange_info"
                        ),
                        http_status=status,
                    )
                except (OSError, RuntimeError, TimeoutError, ValueError) as exc:
                    self.stats.observe_failure(type(exc).__name__)
                    log_event(
                        self.logger,
                        logging.WARNING,
                        "spot_exchange_info_failed",
                        "Spot exchangeInfo failed without stopping core L2",
                        **product_log_fields(
                            "spot", self.spool.symbol, stream="exchange_info"
                        ),
                        error_type=type(exc).__name__,
                    )
                try:
                    await asyncio.wait_for(
                        stop.wait(), timeout=self.interval_seconds
                    )
                except TimeoutError:
                    continue
        finally:
            await run_owned_blocking_call(self.spool.close_and_seal)
