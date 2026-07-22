from __future__ import annotations

import pytest

from binance_market_data_recorder.spool.queue import BoundedEventQueue, IngressQueueFull
from tests.factories import event


def test_queue_overload_is_explicit_and_never_silent() -> None:
    queue = BoundedEventQueue(1)
    queue.put_nowait(event(1))
    with pytest.raises(IngressQueueFull, match="capacity 1 exhausted"):
        queue.put_nowait(event(2))
    assert queue.depth == 1
    assert queue.get_nowait() == event(1)
    assert queue.get_nowait() is None
