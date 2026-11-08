"""A slow many-file time seal must not monopolize the sole writer during ingress."""

import asyncio
import json
import os
import sqlite3
import sys
from functools import partial
from http.server import ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Thread

from verify import BINARY, Response, verify_baseline
from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

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
    "BTCUSDC",
    "ETHUSDC",
]
TOTAL = 30000
RATE = 3000


def wire(index):
    symbol = SYMBOLS[index % len(SYMBOLS)]
    kind = (index // len(SYMBOLS)) % 3
    cycle = index // (len(SYMBOLS) * 3)
    data = {"s": symbol, "E": index + 1000, "_test_id": index}
    if kind == 0:
        stream = "depth@100ms"
        data.update(e="depthUpdate", U=100 + cycle, u=100 + cycle, b=[], a=[])
    elif kind == 1:
        stream = "bookTicker"
        data.update(e="bookTicker", u=100 + cycle, b="1", B="1", a="2", A="1")
    else:
        stream = "aggTrade"
        data.update(
            e="aggTrade",
            a=cycle,
            f=cycle,
            l=cycle,
            p="1",
            q="1",
            T=index + 999,
            m=False,
            M=True,
        )
    return json.dumps(
        {"stream": symbol.lower() + "@" + stream, "data": data}, separators=(",", ":")
    ).encode()


async def exercise(root, expect_overload, readable_overload=False):
    from binance_market_data_recorder.storage.catalog import Catalog
    from binance_market_data_recorder.storage.layout import ensure_storage_layout

    layout = ensure_storage_layout(root)
    # Match the production install path: full Python Catalog precedes the C++ writer.
    with Catalog(layout.catalog):
        pass
    config = root / "config.json"
    config.write_text(json.dumps({"spot": SYMBOLS, "usdm": SYMBOLS}))

    class Handler(Response):
        def do_GET(self):
            body = json.dumps(
                {"lastUpdateId": 101, "bids": [], "asks": []}
                if "/depth" in self.path
                else {
                    "symbols": [{"symbol": s, "status": "TRADING"} for s in SYMBOLS],
                    "rateLimits": [],
                }
            ).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    async def publish(peer):
        start = asyncio.get_running_loop().time()
        try:
            for batch in range(0, TOTAL, 30):
                for index in range(batch, min(batch + 30, TOTAL)):
                    await peer.send(wire(index))
                due = start + (batch + 30) / RATE
                await asyncio.sleep(max(0, due - asyncio.get_running_loop().time()))
            await peer.wait_closed()
        except ConnectionClosed:
            if not expect_overload:
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
                str(TOTAL),
                root,
                "300000",
                "5000",
                "134217728",
                "0",
                "spot",
                config,
                env={
                    **os.environ,
                    "LD_PRELOAD": SHIM,
                    "SBMDR_IO_FAULT": "stalled_seal"
                    if readable_overload
                    else "slow_seal",
                },
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                stdout, stderr = await asyncio.wait_for(proc.communicate(), 90)
            finally:
                if proc.returncode is None:
                    proc.kill()
                    await proc.wait()
    finally:
        http.shutdown()
        thread.join()
    if expect_overload:
        assert proc.returncode != 0 and b"INGRESS_OVERLOAD" in stderr, (
            proc.returncode,
            stderr.decode(),
        )
        if not readable_overload:
            print("old writer many-file rotation reproduces INGRESS_OVERLOAD: PASS")
            return
    else:
        assert proc.returncode == 0, (proc.returncode, stderr.decode())
        report = json.loads(stdout)
        assert report["rotations"].get("time", 0) >= 2
    from binance_market_data_recorder.normalize.raw import (
        iter_source_records,
        load_source_chunks,
    )

    with Catalog(layout.catalog, read_only=True) as catalog:
        chunks = load_source_chunks(layout=layout, catalog=catalog)
        seen = set()
        records = 0
        for chunk in chunks:
            for record in iter_source_records(chunk):
                records += 1
                e = record.envelope
                if e.module == "binance.public.websocket.v1":
                    index = json.loads(e.raw_payload)["data"]["_test_id"]
                    assert index not in seen and e.raw_payload == wire(index)
                    seen.add(index)
    if readable_overload:
        with sqlite3.connect(f"file:{layout.catalog}?mode=ro", uri=True) as db:
            row = db.execute(
                "SELECT evidence_json,market,symbol,stream,gap_id FROM operational_events WHERE event_type='INGRESS_OVERLOAD'"
            ).fetchone()
            count = db.execute("SELECT sum(record_count) FROM chunks").fetchone()[0]
        assert row is not None and all(value is None for value in row[1:])
        evidence = json.loads(row[0])
        assert (
            evidence["market"] == "spot"
            and evidence["symbol"] == "__GLOBAL__"
            and evidence["stream"] == "__ALL_WS__"
        )
        assert any(c.manifest["gap"] for c in chunks)
        assert (
            0 < len(seen) < TOTAL and seen == set(range(len(seen))) and records == count
        )
        print(
            "overload evidence, retained accepted prefix and frozen Catalog compatibility: PASS",
            len(seen),
        )
        return
    assert seen == set(range(TOTAL)) and records == report["records"]
    print(
        json.dumps(
            {
                "status": "PASS",
                "ws_records": len(seen),
                "chunks": len(chunks),
                "rotations": report["rotations"],
                "peak_queue_records": report["peak_queue_records"],
            }
        )
    )


if __name__ == "__main__":
    verify_baseline()
    with TemporaryDirectory(prefix="sbmdr-fault-rotation-") as tmp:
        stalled = "--stall-one" in sys.argv
        asyncio.run(
            exercise(Path(tmp), "--expect-overload" in sys.argv or stalled, stalled)
        )
