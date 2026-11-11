"""Configured-mode async sealer backpressure reproduction (F1/F2)."""

import asyncio
import json
import os
import signal
import sqlite3
import sys
from functools import partial
from http.server import ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Thread
from urllib.parse import parse_qs, urlsplit

from verify import BINARY, Response, verify_baseline
from websockets.asyncio.server import serve

SHIM = sys.argv[3]
SYMBOLS = [
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "BNBUSDT",
    "ZECUSDT",
    "XRPUSDT",
    "NEARUSDT",
    "UNIUSDT",
    "DOGEUSDT",
    "LINKUSDT",
]
TOTAL = 16000
RATE = 2000
PAD = "x" * 8000


def body(stream, seq):
    symbol, kind = stream.split("@", 1)
    data = {
        "s": symbol.upper(),
        "E": 1000 + seq,
        "_pad": PAD,
    }
    if kind == "depth@100ms":
        data.update(e="depthUpdate", U=seq, u=seq, b=[], a=[])
        if symbol.startswith("btc") or symbol.startswith("eth"):
            pass
    elif kind == "bookTicker":
        data.update(e="bookTicker", u=seq, b="1", B="1", a="2", A="1")
    elif kind == "aggTrade":
        data.update(
            e="aggTrade", a=seq, f=seq, l=seq, p="1", q="1", T=seq, m=False, M=True
        )
    elif kind == "markPrice@1s":
        data.update(e="markPriceUpdate", p="1", i="1", P="1", r="0", T=seq)
    elif kind == "forceOrder":
        data.update(e="forceOrder", o={"s": symbol.upper(), "T": seq})
    else:
        raise AssertionError(kind)
    return json.dumps({"stream": stream, "data": data}, separators=(",", ":")).encode()


async def exercise(root, market, expect_wait, fault=None):
    from binance_market_data_recorder.storage.catalog import Catalog
    from binance_market_data_recorder.storage.layout import ensure_storage_layout

    layout = ensure_storage_layout(root)
    with Catalog(layout.catalog):
        pass
    config = root / "config.json"
    config.write_text(json.dumps({"spot": SYMBOLS, "usdm": SYMBOLS}))

    class Handler(Response):
        def do_GET(self):
            if self.path.endswith("/depth"):
                body_data = {"lastUpdateId": 101, "bids": [], "asks": []}
            else:
                body_data = {
                    "symbols": [{"symbol": s, "status": "TRADING"} for s in SYMBOLS],
                    "rateLimits": [],
                }
            payload = json.dumps(body_data).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    holder = {}
    fd_samples = []

    async def sample_fds(proc):
        while proc.returncode is None:
            try:
                fd_samples.append(len(os.listdir(f"/proc/{proc.pid}/fd")))
            except (FileNotFoundError, ProcessLookupError):
                break
            await asyncio.sleep(0.2)

    async def publish(peer):
        streams = parse_qs(urlsplit(peer.request.path).query)["streams"][0].split("/")
        counters = {stream: 1 for stream in streams}
        start = asyncio.get_running_loop().time()
        index = 0
        try:
            for index in range(TOTAL):
                stream = streams[index % len(streams)]
                seq = counters[stream]
                counters[stream] += 1
                if stream.split("@", 1)[1] == "depth@100ms" and market == "usdm":
                    data = json.loads(body(stream, seq))
                    data["data"]["pu"] = max(1, seq - 1)
                    await peer.send(
                        json.dumps(data, separators=(",", ":")).encode()
                    )
                else:
                    await peer.send(body(stream, seq))
                due = start + (index + 1) / RATE
                await asyncio.sleep(
                    max(0, due - asyncio.get_running_loop().time())
                )
            await asyncio.sleep(1)
            holder["proc"].send_signal(signal.SIGTERM)
            await peer.wait_closed()
        except Exception:
            if holder.get("proc") and holder["proc"].returncode is None:
                holder["proc"].send_signal(signal.SIGTERM)
            raise

    http = ThreadingHTTPServer(("127.0.0.1", 0), partial(Handler, b"", 200))
    thread = Thread(target=http.serve_forever, daemon=True)
    thread.start()
    try:
        async with serve(publish, "127.0.0.1", 0, compression=None) as ws:
            proc = await asyncio.create_subprocess_exec(
                BINARY,
                "record",
                str(ws.sockets[0].getsockname()[1]),
                str(http.server_port),
                "9223372036854775807",
                root,
                "300000",
                "2000",
                "1048576",
                "0",
                market,
                config,
                env={
                    **os.environ,
                    "LD_PRELOAD": SHIM,
                    "SBMDR_IO_FAULT": fault or "stalled_seal",
                },
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            holder["proc"] = proc
            sampler = asyncio.create_task(sample_fds(proc))
            try:
                stdout, stderr = await asyncio.wait_for(proc.communicate(), 90)
            finally:
                sampler.cancel()
                if proc.returncode is None:
                    proc.kill()
                    await proc.wait()
    finally:
        http.shutdown()
        thread.join()
    if fault:
        assert proc.returncode != 0, (proc.returncode, stderr.decode())
        text = (stdout + stderr).decode(errors="replace").lower()
        assert any(word in text for word in ("seal", "sync", "i/o", "input/output")), text
        return {
            "market": market,
            "fault": fault,
            "returncode": proc.returncode,
            "error_visible": True,
        }
    assert proc.returncode == 0, (proc.returncode, stderr.decode())
    report = json.loads(stdout)
    with sqlite3.connect(f"file:{layout.catalog}?mode=ro", uri=True) as db:
        catalog_records = db.execute("SELECT coalesce(sum(record_count),0) FROM chunks").fetchone()[0]
    assert catalog_records == report["records"], (catalog_records, report["records"])
    assert report["dropped_total"] == 0
    assert report["seal_queue_peak"] >= 64, report["seal_queue_peak"]
    wait = report["seal_enqueue_wait_ns_total"]
    if expect_wait:
        assert wait > 0, report
    if "--expect-fixed" in sys.argv:
        assert wait < 1_000_000, report
    return {
        "market": market,
        "records": report["records"],
        "sealed": report["sealed"],
        "queue_peak": report["seal_queue_peak"],
        "queue_depth_final": report["seal_queue_depth"],
        "enqueue_wait_ns": wait,
        "enqueue_wait_max_ns": report["seal_enqueue_wait_max_ns"],
        "pending_raw_bytes": report["seal_pending_raw_bytes"],
        "catalog_wait_ns": report["catalog_wait_ns_total"],
        "catalog_wait_max_ns": report["catalog_wait_max_ns"],
        "max_durability_ns": report["max_durability_ns"],
        "peak_rss_bytes": report["peak_rss_bytes"],
        "wall_seconds": report["wall_seconds"],
        "open_files_peak": max(fd_samples, default=0),
    }


if __name__ == "__main__":
    verify_baseline()
    expect_wait = "--expect-wait" in sys.argv
    fault = (
        "fsync_eio"
        if "--seal-fault" in sys.argv
        else "syncfs_eio"
        if "--syncfs-fault" in sys.argv
        else None
    )
    results = []
    with TemporaryDirectory(prefix="sbmdr-fault-async-backpressure-") as tmp:
        markets = ("spot",) if fault else ("spot", "usdm")
        for market in markets:
            results.append(
                asyncio.run(exercise(Path(tmp) / market, market, expect_wait, fault))
            )
    print(json.dumps(results, indent=2))
