"""Drive the C0 binary with loopback fixtures and the frozen Python Raw reader."""

import asyncio
import base64
import json
import os
import subprocess
import sys
from functools import partial
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Thread

from websockets.asyncio.server import serve

BINARY, BASELINE = (Path(arg).resolve() for arg in sys.argv[1:3])
COMMIT = "38acb1d0ee1ed7c5343024aead45d3a6a3f70a85"
sys.path.insert(0, str(BASELINE / "src"))
from binance_market_data_recorder.normalize.parser import parse_envelope
from binance_market_data_recorder.spool.format import (
    FRAME_PREFIX,
    decode_chunk_header,
    decode_envelope,
    scan_chunk,
)

FIXTURES = BASELINE / "tests/fixtures/binance"
WIRE = "btcusdt@depth@100ms"
LARGE_SIZE = 17_817_762
OVERSIZE = 24 * 1024**2
BURST_RECORDS = 8192
BURST_FRAME_BYTES = 8192


def records(path):
    result = scan_chunk(path)
    assert result.is_clean, result
    with path.open("rb") as source:
        decode_chunk_header(source)
        while prefix := source.read(FRAME_PREFIX.size):
            length, *_ = FRAME_PREFIX.unpack(prefix)
            yield decode_envelope(source.read(length))


class Response(BaseHTTPRequestHandler):
    def __init__(self, payload, status, *args):
        self.payload = payload
        self.status = status
        super().__init__(*args)

    def do_GET(self):
        if self.path != "/api/v3/exchangeInfo":
            self.send_error(404)
            return
        self.send_response(self.status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(self.payload)))
        self.end_headers()
        try:
            self.wfile.write(self.payload)
        except (BrokenPipeError, ConnectionResetError):
            pass  # The over-limit client terminates the transfer.

    def log_message(self, *_args):
        pass


def ws_body(depth):
    return b'{"stream":"' + WIRE.encode() + b'","data":' + depth.strip() + b"}"


async def run_case(root, name, frames, rest, *, pause=0, status=200, rejected=False):
    async def publish(peer):
        assert peer.request.path == f"/stream?streams={WIRE}"
        for frame in frames:
            await peer.send(frame)
        await peer.wait_closed()

    output = root / name
    with ThreadingHTTPServer(("127.0.0.1", 0), partial(Response, rest, status)) as http:
        thread = Thread(target=http.serve_forever, kwargs={"poll_interval": 0.05})
        thread.start()
        try:
            async with serve(publish, "127.0.0.1", 0, compression=None) as ws:
                proc = await asyncio.create_subprocess_exec(
                    BINARY,
                    str(ws.sockets[0].getsockname()[1]),
                    str(http.server_port),
                    str(len(frames)),
                    output,
                    str(pause),
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                try:
                    stdout, stderr = await asyncio.wait_for(
                        proc.communicate(), timeout=60
                    )
                except TimeoutError:
                    proc.kill()
                    await proc.wait()
                    raise
        finally:
            http.shutdown()
            thread.join()

    if rejected:
        assert proc.returncode == 1 and b"exceeded the body limit" in stderr, stderr
        assert not list(output.glob("*.partial"))
        return {"case": name, "rejected_http_bytes": len(rest)}

    assert proc.returncode == 0, stderr
    stats = json.loads(stdout)
    if frames:
        for expected, record in zip(
            frames, records(output / "ws.bmdr.partial"), strict=True
        ):
            assert record.raw_payload == expected
            assert record.receive_time_utc_ns > 0 and record.receive_monotonic_ns > 0
            malformed = expected == b'{"stream":'
            assert ("malformed" in record.capture_flags) == malformed
            if not malformed:
                depth = json.loads(expected)["data"]
                assert record.exchange_event_time == depth["E"]
                assert record.source_sequence == {
                    "U": depth["U"],
                    "u": depth["u"],
                    "sourceStream": "diff_depth",
                    "sourceFamily": "S02",
                    "wireStream": WIRE,
                }
            if name == "ordinary":
                assert parse_envelope(record)[0].valid == (not malformed)

    (record,) = records(output / "rest.bmdr.partial")
    provenance = json.loads(record.raw_payload)
    response = provenance["response"]
    assert record.source_sequence == {
        "sourceStream": "exchange_info",
        "sourceFamily": "S01",
    }
    assert response["status"] == status
    assert response["valid"] == (status == 200)
    assert response["receive_monotonic_ns"] == record.receive_monotonic_ns
    assert provenance["request"]["request_monotonic_ns"] <= record.receive_monotonic_ns
    assert response["headers"]["content-type"] == "application/json"
    assert response["receive_time_utc_ns"] == record.receive_time_utc_ns
    assert base64.b64decode(response["raw_body_base64"], validate=True) == rest
    assert "model" not in response
    if name in {"ordinary", "http-error"}:
        assert parse_envelope(record)[0].valid == (status == 200)
    assert stats["records"] == len(frames) + 1
    assert stats["peak_queue_records"] <= 8192
    assert stats["peak_queue_payload_bytes"] <= 64 * 1024**2
    return {
        "case": name,
        "ws_input_bytes": sum(map(len, frames)),
        "http_input_bytes": len(rest),
        **stats,
    }


def verify_baseline():
    source = subprocess.check_output(
        ["git", "-C", BASELINE, "rev-parse", "HEAD"], text=True
    )
    assert source.strip() == COMMIT
    assert not subprocess.check_output(
        [
            "git",
            "-C",
            BASELINE,
            "status",
            "--porcelain",
            "--",
            "src",
            "tests/fixtures",
            "tests/golden",
        ]
    )
    subprocess.run(
        [BINARY, "--golden", BASELINE / "tests/golden/raw_chunk_v1.json"], check=True
    )


async def main():
    depth = (FIXTURES / "spot/diff_depth.json").read_bytes()
    directory = (FIXTURES / "p2/spot-directory.json").read_bytes()
    large = directory.ljust(LARGE_SIZE, b" ")
    frames = tuple(
        ws_body(depth)
        .replace(b'"U":157,"u":160', f'"U":{157 + i * 4},"u":{160 + i * 4}'.encode())
        .ljust(BURST_FRAME_BYTES, b" ")
        for i in range(BURST_RECORDS)
    )
    with TemporaryDirectory(
        prefix="sbmdr-c0-", dir=os.environ.get("SBMDR_SCRATCH")
    ) as tmp:
        root = Path(tmp)
        results = [
            await run_case(
                root, "ordinary", [ws_body(depth), b'{"stream":'], directory
            ),
            await run_case(root, "http-error", [], b'{"code":-1}', status=503),
            await run_case(root, "large", [], large),
            await run_case(root, "burst", frames, large, pause=250),
            await run_case(
                root, "oversize", [], directory.ljust(OVERSIZE, b" "), rejected=True
            ),
        ]
    print(
        json.dumps(
            {"python_commit": COMMIT, "raw_golden": "PASS", "results": results},
            indent=2,
        )
    )


if __name__ == "__main__":
    verify_baseline()
    asyncio.run(main())
