"""C2 real loopback: snapshot epochs, Spot U/u, USD-M pu, and staggered REST."""

import asyncio
import base64
import json
import sqlite3
from collections import Counter
from functools import partial
from http.server import ThreadingHTTPServer
from itertools import pairwise
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Thread
from time import monotonic
from urllib.parse import urlsplit

from verify import BINARY, FIXTURES, Response, verify_baseline
from websockets.asyncio.server import serve


async def until(predicate, timeout=12):
    deadline = monotonic() + timeout
    while not predicate():
        assert monotonic() < deadline, "fixture condition timed out"
        await asyncio.sleep(0.02)


async def exercise(root: Path, market: str):
    usdm = market == "usdm"
    calls = []
    snapshots = 0
    completed = 0
    connections = 0
    directory = (FIXTURES / "p2/spot-directory.json").read_bytes()
    expected_bodies = set()

    class CoreResponse(Response):
        protocol_version = "HTTP/1.1"

        def do_GET(self):
            nonlocal snapshots, completed
            path = urlsplit(self.path).path
            calls.append((path, monotonic(), self.client_address))
            self.status = 200
            if path.endswith("/exchangeInfo"):
                self.payload = directory
            elif path.endswith("/depth"):
                snapshots += 1
                if not usdm and snapshots <= 2:
                    self.status = 429 if snapshots == 1 else 418
                    self.payload = b'{"code":-1003}'
                else:
                    index = snapshots - (0 if usdm else 2) - 1
                    last = [104, 112 if usdm else 121, 201][min(index, 2)]
                    self.payload = json.dumps(
                        {"lastUpdateId": last, "bids": [], "asks": []}
                    ).encode()
            elif path.endswith("/openInterest"):
                self.payload = b'{"symbol":"BTCUSDT","openInterest":"12","time":1}'
            else:
                # Deliberately old and legitimate empty buckets: retain, never fast retry.
                self.payload = (
                    b"[]"
                    if path.endswith("PositionRatio")
                    else b'[{"symbol":"BTCUSDT","timestamp":1}]'
                )
            expected_bodies.add(self.payload)
            if self.status == 429:
                self.send_response(self.status)
                self.send_header("Retry-After", "1")
                self.send_header("Content-Length", str(len(self.payload) + 100))
                self.send_header("Connection", "close")
                self.end_headers()
                self.wfile.write(self.payload)
                self.close_connection = True
            else:
                self.send_response(self.status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(self.payload)))
                self.end_headers()
                self.wfile.write(self.payload)
            if path.endswith("/depth") and self.status == 200:
                completed += 1

        def end_headers(self):
            if self.status == 418:
                self.send_header("Retry-After", "1")
            super().end_headers()

    async def publish(peer):
        nonlocal connections
        connections += 1
        expected_path = (
            "/public" if usdm else ""
        ) + "/stream?streams=btcusdt@depth@100ms"
        assert peer.request.path == expected_path

        async def send(first, last, previous):
            await peer.send(
                json.dumps(
                    {
                        "stream": "btcusdt@depth@100ms",
                        "data": {
                            "e": "depthUpdate",
                            "E": 1000,
                            "s": "BTCUSDT",
                            "U": first,
                            "u": last,
                            "pu": previous,
                            "b": [],
                            "a": [],
                        },
                    }
                )
            )

        if connections == 1:
            await send(100, 102, 99)
            await send(103, 104, 102)
            await send(105, 107, 104)
            await until(lambda: completed >= 1)
            await asyncio.sleep(0.2)
            await send(108, 110, 107)
            await send(108, 110, 107)
            await send(111 if usdm else 120, 113 if usdm else 122, 999)
            await until(lambda: completed >= 2)
            await asyncio.sleep(0.2)
            await send(114 if usdm else 123, 116 if usdm else 125, 113 if usdm else 122)
            await asyncio.sleep(0.1)
            peer.transport.abort()
        else:
            await send(200, 202, 199)
            await until(lambda: completed >= 3)
            await asyncio.sleep(0.2)
            await send(203, 205, 202)
            if usdm:
                await until(
                    lambda: (
                        all(
                            n >= 2
                            for p, n in Counter(
                                p
                                for p, _, _ in calls
                                if "/futures/data/" in p or p.endswith("/openInterest")
                            ).items()
                        )
                        and len(
                            {
                                p
                                for p, _, _ in calls
                                if "/futures/data/" in p or p.endswith("/openInterest")
                            }
                        )
                        == 5
                    )
                )
            await send(206, 208, 205)
            await peer.wait_closed()

    with ThreadingHTTPServer(
        ("127.0.0.1", 0), partial(CoreResponse, directory, 200)
    ) as http:
        thread = Thread(target=http.serve_forever, kwargs={"poll_interval": 0.05})
        thread.start()
        try:
            async with serve(publish, "127.0.0.1", 0, compression=None) as ws:
                proc = await asyncio.create_subprocess_exec(
                    BINARY,
                    "record",
                    str(ws.sockets[0].getsockname()[1]),
                    str(http.server_port),
                    "10",
                    root,
                    "1800",
                    "900000",
                    "134217728",
                    "0",
                    market,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                try:
                    stdout, stderr = await asyncio.wait_for(proc.communicate(), 30)
                finally:
                    if proc.returncode is None:
                        proc.kill()
                        await proc.wait()
        finally:
            http.shutdown()
            thread.join()
    assert proc.returncode == 0, stderr.decode()
    assert connections == 2
    report = json.loads(stdout)
    assert report["stop_reason"] == "INPUT_COMPLETE"
    from binance_market_data_recorder.normalize.raw import (
        iter_source_records,
        load_source_chunks,
    )
    from binance_market_data_recorder.storage.catalog import Catalog
    from binance_market_data_recorder.storage.layout import StorageLayout

    layout = StorageLayout.from_root(root)
    with Catalog(layout.catalog, read_only=True) as catalog:
        chunks = load_source_chunks(layout=layout, catalog=catalog)
        records = [r.envelope for c in chunks for r in iter_source_records(c)]
    assert len(records) == report["records"]
    ws_records = sorted(
        [r for r in records if r.module == "binance.public.websocket.v1"],
        key=lambda r: r.receive_monotonic_ns,
    )
    assert len(ws_records) == 10
    assert (
        all(
            r.market == ("um_shared" if r.symbol == "__GLOBAL__" else "um_perpetual")
            for r in records
        )
        if usdm
        else all(r.market == "spot" for r in records)
    )
    assert "depth_stale" in ws_records[4].capture_flags, [
        (dict(r.source_sequence), list(r.capture_flags)) for r in ws_records
    ]
    assert "sequence_gap" in ws_records[5].capture_flags
    assert "sequence_gap" in ws_records[7].capture_flags
    assert "depth_bridge" in ws_records[-1].capture_flags
    assert (
        ws_records[-1].source_sequence["depthSnapshotEpoch"]
        > ws_records[0].source_sequence["depthSnapshotEpoch"]
    )
    rest = [r for r in records if r.module == "binance.public.rest.v1"]
    for r in rest:
        body = json.loads(r.raw_payload)
        assert base64.b64decode(body["response"]["raw_body_base64"]) in expected_bodies
        assert body["venue_market"] == market
        assert body["request"]["request_time_utc_ns"] <= r.receive_time_utc_ns
    snapshots_read = [
        r
        for r in rest
        if r.source_sequence["sourceStream"] == "depth_snapshot"
        and "lastUpdateId" in r.source_sequence
    ]
    assert len(snapshots_read) == 3
    assert all("depth_bridge" in r.capture_flags for r in snapshots_read)
    if usdm:
        polls = [
            (p, t)
            for p, t, _ in calls
            if "/futures/data/" in p or p.endswith("/openInterest")
        ]
        counts = Counter(p for p, _ in polls)
        assert len(counts) == 5 and set(counts.values()) == {2}
        first = [next(t for p, t in polls if p == name) for name in counts]
        assert all(b - a >= 0.20 for a, b in pairwise(first))
        assert len({address for _, _, address in calls}) < len(calls)
    else:
        depth_calls = [t for p, t, _ in calls if p.endswith("/depth")]
        assert depth_calls[1] - depth_calls[0] >= 0.99
        assert depth_calls[2] - depth_calls[1] >= 0.99
    with sqlite3.connect(layout.catalog) as db:
        assert db.execute("select sum(record_count) from chunks").fetchone() == (
            len(records),
        )
    return {
        "market": market,
        "records": len(records),
        "snapshots_bridged": 3,
        "http_requests": len(calls),
        "rest_sources": sorted({r.source_sequence["sourceStream"] for r in rest}),
        "status": "PASS",
    }


if __name__ == "__main__":
    verify_baseline()
    with TemporaryDirectory(prefix="sbmdr-core-") as tmp:
        print(
            json.dumps(
                [
                    asyncio.run(exercise(Path(tmp) / market, market))
                    for market in ("spot", "usdm")
                ],
                indent=2,
            )
        )
