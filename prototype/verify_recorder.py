"""Verify C1 against the frozen Python reader and independently inspect SQLite state."""

import asyncio
import base64
import json
import os
import signal
import sqlite3
import sys
from bisect import bisect_left
from functools import partial
from http.server import ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Thread

from verify import (
    BINARY,
    FIXTURES,
    WIRE,
    Response,
    verify_baseline,
    ws_body,
)
from websockets.asyncio.server import serve


def depth_frame(depth, index, frame_size):
    if index == 1:
        return b'{"stream":'
    return (
        ws_body(depth)
        .replace(
            b'"U":157,"u":160', f'"U":{157 + index * 4},"u":{160 + index * 4}'.encode()
        )
        .ljust(frame_size, b" ")
    )


def check_output(root, depth, frame_size, directory, report, offered):
    from binance_market_data_recorder.normalize.raw import (
        iter_source_records,
        load_source_chunks,
    )
    from binance_market_data_recorder.storage.layout import StorageLayout

    from binance_market_data_recorder.storage.catalog import Catalog

    assert not list((root / "data").rglob("*.partial"))
    layout = StorageLayout.from_root(root)
    ws_count = rest_count = 0
    with Catalog(layout.catalog, read_only=True) as catalog:
        chunks = load_source_chunks(layout=layout, catalog=catalog)
        assert len(chunks) == report["sealed"]
        for chunk in sorted(
            chunks, key=lambda chunk: chunk.manifest["created_at_utc_ns"]
        ):
            seen = 0
            last = 0
            for item in iter_source_records(chunk):
                event = item.envelope
                assert event.receive_time_utc_ns > 0
                assert event.receive_monotonic_ns >= last
                last = event.receive_monotonic_ns
                assert (
                    event.collector_instance_id
                    in chunk.manifest["collector_instance_ids"]
                )
                assert event.connection_id in chunk.manifest["connection_ids"]
                if event.module == "binance.public.websocket.v1":
                    expected = depth_frame(depth, ws_count, frame_size)
                    assert event.raw_payload == expected
                    assert ("malformed" in event.capture_flags) == (ws_count == 1)
                    if ws_count != 1:
                        assert event.source_sequence["U"] == 157 + ws_count * 4
                        assert event.source_sequence["u"] == 160 + ws_count * 4
                    ws_count += 1
                else:
                    response = json.loads(event.raw_payload)["response"]
                    assert response["valid"] and response["status"] == 200
                    assert base64.b64decode(response["raw_body_base64"]) == directory
                    rest_count += 1
                seen += 1
            assert seen == chunk.manifest["record_count"] > 0
    with sqlite3.connect(layout.catalog) as db:
        assert db.execute(
            "SELECT state, count(*) FROM chunks GROUP BY state"
        ).fetchall() == [("SEALED", report["sealed"])]
        assert db.execute("SELECT sum(record_count) FROM chunks").fetchone() == (
            report["records"],
        )
        assert db.execute("SELECT count(*) FROM chunk_transitions").fetchone() == (
            3 * report["sealed"],
        )
        assert db.execute("SELECT event_type FROM operational_events").fetchall() == [
            ("PROCESS_START",)
        ]
    assert ws_count == offered
    assert rest_count >= 1
    assert ws_count + rest_count == report["records"]
    return {"ws_records": ws_count, "rest_records": rest_count}


async def scenario(
    root,
    count,
    frame_size,
    *,
    rate=0,
    duration=0,
    rotation=None,
    interrupt=False,
    fail_seal=False,
):
    depth = (FIXTURES / "spot/diff_depth.json").read_bytes()
    directory = (FIXTURES / "p2/spot-directory.json").read_bytes()
    served = 0
    done = asyncio.Event()

    async def publish(peer):
        nonlocal served
        assert peer.request.path == f"/stream?streams={WIRE}"
        loop = asyncio.get_running_loop()
        start = loop.time()
        for i in range(count):
            if rate:
                await asyncio.sleep(max(0, start + i / rate - loop.time()))
            await peer.send(depth_frame(depth, i, frame_size))
            served += 1
        done.set()
        await peer.wait_closed()

    with ThreadingHTTPServer(
        ("127.0.0.1", 0), partial(Response, directory, 200)
    ) as http:
        thread = Thread(target=http.serve_forever, kwargs={"poll_interval": 0.05})
        thread.start()
        try:
            async with serve(
                publish, "127.0.0.1", 0, compression=None, ping_interval=None
            ) as ws:
                args = [
                    BINARY,
                    "record",
                    str(ws.sockets[0].getsockname()[1]),
                    str(http.server_port),
                    str(count + int(interrupt)),
                    root,
                    "1000",
                ]
                if rotation:
                    args.extend(map(str, rotation))
                proc = await asyncio.create_subprocess_exec(
                    *args,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                try:
                    if interrupt:
                        await asyncio.wait_for(done.wait(), timeout=15)
                        await asyncio.sleep(0.15)
                        if fail_seal:
                            sealed = root / "data/sealed"
                            sealed.rename(sealed.with_name("sealed-before-fault"))
                            sealed.write_text("injected artifact path I/O failure")
                        proc.send_signal(signal.SIGTERM)
                    stdout, stderr = await asyncio.wait_for(
                        proc.communicate(), duration + 90
                    )
                finally:
                    if proc.returncode is None:
                        proc.kill()
                        await proc.wait()
        finally:
            http.shutdown()
            thread.join()
    if fail_seal:
        assert proc.returncode == 1 and b"create artifact failed" in stderr, stderr
        assert list((root / "data/active").glob("*.partial"))
        assert not list((root / "data/manifests").glob("*.manifest.json"))
        with sqlite3.connect(root / "state/catalog.sqlite") as db:
            states = {state for (state,) in db.execute("SELECT state FROM chunks")}
        assert "SEALING" in states and "SEALED" not in states
        from recover_raw import recover

        sealed = root / "data/sealed"
        sealed.unlink()
        sealed.with_name("sealed-before-fault").rename(sealed)
        with sqlite3.connect(root / "state/catalog.sqlite") as db:
            (active_rest,) = db.execute(
                "SELECT partial_path FROM chunks WHERE state='ACTIVE'"
            ).fetchone()
        with (root / active_rest).open("ab") as partial_file:
            partial_file.write(b"\x00\x00")
        actions = recover(root)
        assert any(action["action"] == "tail_truncated" for action in actions)
        manifests = [
            json.loads(path.read_bytes())
            for path in (root / "data/manifests").glob("*.json")
        ]
        (recovered,) = [manifest for manifest in manifests if manifest["recovered"]]
        assert recovered["record_count"] == 1 and not recovered["complete"]
        assert actions
        assert not list((root / "data/active").glob("*.partial"))
        recover(root)
        assert len(list((root / "data/manifests").glob("*.json"))) == len(manifests)
        artifact = root / recovered["relative_path"]
        withheld = artifact.with_suffix(".withheld")
        artifact.rename(withheld)

        async def run_recovery():
            child = await asyncio.create_subprocess_exec(
                sys.executable,
                Path(__file__).with_name("recover_raw.py"),
                root,
                env={**os.environ, "PYTHONPATH": str(FIXTURES.parents[2] / "src")},
                stdout=asyncio.subprocess.PIPE,
            )
            output, _ = await child.communicate()
            return child.returncode, json.loads(output)

        try:
            status, failure = await run_recovery()
            assert status == 1 and any(
                action["action"] == "reconcile_failed" for action in failure
            )
        finally:
            withheld.rename(artifact)
        status, _ = await run_recovery()
        assert status == 0
        return {
            "failed_as_expected": True,
            "retained_active": True,
            "recovery": actions,
        }
    assert proc.returncode == 0, stderr.decode()
    assert served == count
    report = json.loads(stdout)
    report["samples"] = [json.loads(line) for line in stderr.splitlines()]
    report.update(check_output(root, depth, frame_size, directory, report, count))
    assert report["interrupted"] is interrupt
    assert report["stop_reason"] == ("SIGNAL" if interrupt else "INPUT_COMPLETE")
    assert report["peak_queue_records"] <= 8192
    assert report["peak_queue_payload_bytes"] <= 64 * 1024**2
    return report


async def main():
    seconds = int(os.environ.get("SBMDR_LONG_SECONDS", "0"))
    with TemporaryDirectory(
        prefix="sbmdr-c1-", dir=os.environ.get("SBMDR_SCRATCH")
    ) as tmp:
        root = Path(tmp)
        if seconds:
            report = await scenario(
                root / "long", seconds * 10 + 1, 8192, rate=10, duration=seconds
            )
            assert report["wall_seconds"] >= seconds
            assert report["rotations"]["time"] >= 3
            assert report["max_durability_ns"] <= 1_250_000_000
            assert report["peak_rss_bytes"] <= 768 * 1024**2
            assert report["receive_to_append_p99_ms"] <= 100
            samples = report["samples"]
            timeline = [sample["elapsed_seconds"] for sample in samples]
            windows = [
                100
                * (sample["cpu_seconds"] - samples[index]["cpu_seconds"])
                / (sample["elapsed_seconds"] - timeline[index])
                for sample in samples
                if sample["elapsed_seconds"] >= 61
                for index in [bisect_left(timeline, sample["elapsed_seconds"] - 60) - 1]
                if index >= 0
            ]
            report["max_60s_cpu_percent"] = max(windows)
            assert report["max_60s_cpu_percent"] <= 120
            assert report["max_status_cpu_percent"] <= 200
            print(json.dumps(report, indent=2))
        else:
            timed = await scenario(
                root / "timed",
                25,
                2048,
                rate=10,
                rotation=(500, 128 * 1024**2),
                interrupt=True,
            )
            assert timed["rotations"]["time"] >= 2
            sized = await scenario(root / "sized", 132, 1024**2)
            assert sized["rotations"]["size"] >= 1
            fault = await scenario(
                root / "fault", 3, 2048, rate=10, interrupt=True, fail_seal=True
            )
            print(
                json.dumps(
                    {"time_and_signal": timed, "size": sized, "seal_fault": fault},
                    indent=2,
                )
            )


if __name__ == "__main__":
    verify_baseline()
    asyncio.run(main())
