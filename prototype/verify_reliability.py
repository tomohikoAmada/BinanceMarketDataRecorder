"""Exercise connection loss and HTTP cooldown through the recorder entry point."""

import asyncio
import json
import signal
import socket
import sqlite3
from functools import partial
from http.server import ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Thread
from time import monotonic

from verify import BINARY, FIXTURES, Response, verify_baseline
from verify_recorder import depth_frame
from websockets.asyncio.server import serve


async def verify_reconnect(root: Path) -> None:
    attempts = []
    connections = 0
    http_connections = set()
    depth = (FIXTURES / "spot/diff_depth.json").read_bytes()
    directory = (FIXTURES / "p2/spot-directory.json").read_bytes()

    class LimitedResponse(Response):
        protocol_version = "HTTP/1.1"

        def do_GET(self):
            attempts.append(monotonic())
            http_connections.add(self.client_address)
            index = len(attempts)
            self.status = {1: 429, 2: 418, 3: 429, 4: 418}.get(index, 200)
            self.payload = directory if self.status == 200 else b'{"code":-1003}'
            if index in {3, 4}:
                self.send_response(self.status)
                self.send_header("Content-Length", str(len(self.payload) + 100))
                self.send_header("Connection", "close")
                self.end_headers()
                self.wfile.write(self.payload)
                self.close_connection = True
            else:
                super().do_GET()

        def end_headers(self):
            if self.status in {418, 429}:
                self.send_header("Retry-After", "2")
            super().end_headers()

    async def publish(peer):
        nonlocal connections
        connections += 1
        if connections == 1:
            await peer.send(depth_frame(depth, 0, 256))
            await asyncio.sleep(0.05)
            peer.transport.abort()
            return
        if connections <= 7:
            peer.transport.abort()
            return
        await peer.send(depth_frame(depth, 2, 256))
        await asyncio.sleep(1.5)
        await peer.send(depth_frame(depth, 3, 256))
        await peer.wait_closed()

    with ThreadingHTTPServer(
        ("127.0.0.1", 0), partial(LimitedResponse, directory, 200)
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
                    "3",
                    root,
                    "100",
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                try:
                    stdout, stderr = await asyncio.wait_for(proc.communicate(), 40)
                finally:
                    if proc.returncode is None:
                        proc.kill()
                        await proc.wait()
        finally:
            http.shutdown()
            thread.join()
    assert proc.returncode == 0, stderr.decode()
    assert b"WS_RECONNECT" in stderr and b"REST_COOLDOWN" in stderr
    assert connections == 8 and len(attempts) >= 6
    assert all(attempts[i + 1] - attempts[i] >= 1.99 for i in range(4))
    assert len(http_connections) < len(attempts)
    events = [json.loads(line) for line in stderr.splitlines()]
    assert [
        e["status"] for e in events if e.get("event") == "REST_TRANSPORT_FAILURE"
    ] == [429, 418]
    manifests = [
        json.loads(path.read_bytes())
        for path in (root / "data/manifests").glob("*.json")
    ]
    (ws_manifest,) = [m for m in manifests if m["symbol"] == "BTCUSDT"]
    assert ws_manifest["record_count"] == 3
    assert len(ws_manifest["connection_ids"]) == 2
    assert ws_manifest["gap"] and not ws_manifest["complete"]
    (rest_manifest,) = [m for m in manifests if m["symbol"] == "__GLOBAL__"]
    assert rest_manifest["record_count"] == len(attempts) - 2
    assert json.loads(stdout)["records"] == sum(m["record_count"] for m in manifests)
    with sqlite3.connect(root / "state/catalog.sqlite") as db:
        assert db.execute(
            "SELECT count(*) FROM chunks WHERE state='SEALED'"
        ).fetchone() == (2,)


async def verify_shutdown_in_backoff(root: Path) -> None:
    async def reject(peer):
        peer.transport.abort()

    with socket.socket() as unavailable:
        unavailable.bind(("127.0.0.1", 0))
        await shutdown_recorder(root, reject, str(unavailable.getsockname()[1]))


async def shutdown_recorder(root: Path, reject, http_port: str) -> None:
    async with serve(reject, "127.0.0.1", 0) as ws:
        port = str(ws.sockets[0].getsockname()[1])
        proc = await asyncio.create_subprocess_exec(
            BINARY,
            "record",
            port,
            http_port,
            "1",
            root,
            "1000",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            while True:
                line = await asyncio.wait_for(proc.stderr.readline(), 8)
                assert line
                if json.loads(line).get("event") == "WS_RECONNECT":
                    break
            proc.send_signal(signal.SIGTERM)
            # The recorder drains a started HTTP transfer, which has a 10 s timeout.
            stdout, stderr = await asyncio.wait_for(proc.communicate(), 12)
            assert proc.returncode == 0, stderr.decode()
            assert json.loads(stdout)["stop_reason"] == "SIGNAL"
        finally:
            if proc.returncode is None:
                proc.kill()
                await proc.wait()


if __name__ == "__main__":
    verify_baseline()
    with TemporaryDirectory(prefix="sbmdr-reliability-") as scratch:
        asyncio.run(verify_reconnect(Path(scratch) / "reconnect"))
        asyncio.run(verify_shutdown_in_backoff(Path(scratch) / "shutdown"))
