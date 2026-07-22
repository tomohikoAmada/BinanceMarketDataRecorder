from __future__ import annotations

import platform
from threading import Event, Timer

import pytest

from binance_market_data_recorder.storage.macos import DiskArbitrationAdapter


@pytest.mark.skipif(platform.system() != "Darwin", reason="macOS-only platform proof")
def test_disk_arbitration_session_callbacks_and_startup_inventory() -> None:
    adapter = DiskArbitrationAdapter(startup_window_seconds=0.1)
    capability = adapter.capability()
    callback = adapter.callback_probe(duration_seconds=0.1)
    inventory = adapter.inventory()

    assert capability == {
        "available": True,
        "framework": "DiskArbitration",
        "binding": "PyObjC",
    }
    assert callback["session_created"] is True
    assert callback["appeared_callback_registered"] is True
    assert callback["disappeared_callback_registered"] is True
    assert isinstance(callback["startup_objects_observed"], int)
    assert callback["startup_objects_observed"] > 0
    assert callback["filesystem_mutated"] is False
    assert all(volume.internal is False for volume in inventory)
    assert all(volume.volume_uuid for volume in inventory)


@pytest.mark.skipif(platform.system() != "Darwin", reason="macOS-only platform proof")
def test_disk_arbitration_lifecycle_listener_registers_all_callbacks() -> None:
    stop = Event()
    timer = Timer(0.15, stop.set)
    timer.start()
    try:
        DiskArbitrationAdapter().observe(lambda _event: None, stop)
    finally:
        timer.cancel()
    assert stop.is_set()
