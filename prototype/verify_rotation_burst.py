"""Writer stalls must be absorbed by the configured queue, and full queues must drop honestly."""

import asyncio
import json
import os
import signal
import sqlite3
import sys
from collections import Counter, defaultdict
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
STREAMS = ("depth@100ms", "bookTicker", "aggTrade")
RECORD_STREAMS = ("diff_depth", "book_ticker", "agg_trade")
DROP_SYMBOLS = SYMBOLS[:3]
TOTAL = 30000
RATE = 3000


def wire(index, symbols=SYMBOLS):
    stride = len(symbols) * len(STREAMS)
    symbol = symbols[index % len(symbols)]
    kind = (index // len(symbols)) % len(STREAMS)
    cycle = index // stride
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


def expected_key(index, symbols=SYMBOLS):
    return (
        symbols[index % len(symbols)],
        RECORD_STREAMS[(index // len(symbols)) % len(RECORD_STREAMS)],
    )


async def exercise(root, mode):
    from binance_market_data_recorder.storage.catalog import Catalog
    from binance_market_data_recorder.storage.layout import ensure_storage_layout

    drop_path = mode == "drop"
    stalled = mode in {"stall", "drop"}
    symbols = DROP_SYMBOLS if drop_path else SYMBOLS
    stride = len(symbols) * len(STREAMS)
    test_queue_records = 1024 if drop_path else None
    snapshot_requests = Counter()
    depth_u = {}

    layout = ensure_storage_layout(root)
    # Match the production install path: full Python Catalog precedes the C++ writer.
    with Catalog(layout.catalog):
        pass
    config = root / "config.json"
    config.write_text(json.dumps({"spot": symbols, "usdm": symbols}))

    class Handler(Response):
        def do_GET(self):
            if "/depth" in self.path:
                from urllib.parse import parse_qs, urlsplit

                symbol = parse_qs(urlsplit(self.path).query)["symbol"][0]
                snapshot_requests[symbol] += 1
                # One update behind the latest stream so the bridge resumes immediately.
                last = max(101, depth_u.get(symbol, 101) - 1)
                body = {"lastUpdateId": last, "bids": [], "asks": []}
            else:
                body = {
                    "symbols": [{"symbol": s, "status": "TRADING"} for s in symbols],
                    "rateLimits": [],
                }
            payload = json.dumps(body).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    holder = {}

    async def publish(peer):
        start = asyncio.get_running_loop().time()
        try:
            for batch in range(0, TOTAL, 30):
                for index in range(batch, min(batch + 30, TOTAL)):
                    kind = (index // len(symbols)) % len(STREAMS)
                    if kind == 0:
                        cycle = index // stride
                        depth_u[symbols[index % len(symbols)]] = 100 + cycle
                    await peer.send(wire(index, symbols))
                due = start + (batch + 30) / RATE
                await asyncio.sleep(max(0, due - asyncio.get_running_loop().time()))
            if drop_path:
                # Let the recorder consume the final TCP bytes before stopping it.
                await asyncio.sleep(1.0)
                holder["proc"].send_signal(signal.SIGTERM)
            await peer.wait_closed()
        except ConnectionClosed:
            pass

    http = ThreadingHTTPServer(("127.0.0.1", 0), partial(Handler, b"", 200))
    thread = Thread(target=http.serve_forever, daemon=True)
    thread.start()
    try:
        async with serve(publish, "127.0.0.1", 0, compression=None) as ws:
            arguments = [
                BINARY,
                "record",
                str(ws.sockets[0].getsockname()[1]),
                str(http.server_port),
                "9223372036854775807" if drop_path else str(TOTAL),
                root,
                "300000",
                "5000",
                "134217728",
                "0",
                "spot",
                config,
            ]
            if test_queue_records is not None:
                arguments.append(str(test_queue_records))
            proc = await asyncio.create_subprocess_exec(
                *arguments,
                env={
                    **os.environ,
                    "LD_PRELOAD": SHIM,
                    "SBMDR_IO_FAULT": "stalled_seal" if stalled else "slow_seal",
                },
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            holder["proc"] = proc
            try:
                stdout, stderr = await asyncio.wait_for(proc.communicate(), 120)
            finally:
                if proc.returncode is None:
                    proc.kill()
                    await proc.wait()
    finally:
        http.shutdown()
        thread.join()
    assert proc.returncode == 0, (proc.returncode, stderr.decode())
    report = json.loads(stdout)
    assert report["dropped_total"] == report["ws_dropped_total"] + report["rest_dropped_total"]
    if mode == "rotation":
        assert report["rotations"].get("time", 0) >= 2
    if mode == "stall":
        assert report["dropped_total"] == 0
    if drop_path:
        assert report["rest_dropped_total"] == 0
        assert report["ws_dropped_total"] > 0

    from binance_market_data_recorder.normalize.raw import (
        iter_source_records,
        load_source_chunks,
    )

    with Catalog(layout.catalog, read_only=True) as catalog:
        chunks = load_source_chunks(layout=layout, catalog=catalog)
        ws_records = []
        seen = set()
        record_count = 0
        for chunk in chunks:
            for record in iter_source_records(chunk):
                record_count += 1
                e = record.envelope
                if e.module == "binance.public.websocket.v1":
                    payload = json.loads(e.raw_payload)
                    index = payload["data"]["_test_id"]
                    assert index not in seen
                    seen.add(index)
                    assert e.raw_payload == wire(index, symbols)
                    assert (e.symbol, e.stream) == expected_key(index, symbols)
                    ws_records.append(
                        {
                            "index": index,
                            "symbol": e.symbol,
                            "stream": e.stream,
                            "flags": set(e.capture_flags),
                            "sequence": e.source_sequence,
                            "utc": e.receive_time_utc_ns,
                        }
                    )
    assert record_count == report["records"]

    if drop_path:
        assert len(ws_records) + report["ws_dropped_total"] == TOTAL
        events = []
        with sqlite3.connect(f"file:{layout.catalog}?mode=ro", uri=True) as db:
            rows = db.execute(
                "SELECT evidence_json,market,symbol,stream,gap_id FROM operational_events "
                "WHERE event_type='INGRESS_DROP'"
            ).fetchall()
        assert rows and all(all(value is None for value in row[1:]) for row in rows)
        for row in rows:
            evidence = json.loads(row[0])
            assert evidence["market"] == "spot"
            assert evidence["first_drop_utc_ns"] <= evidence["last_drop_utc_ns"]
            events.append(evidence)
        assert sum(event["dropped"] for event in events) == report["ws_dropped_total"]

        by_stream = defaultdict(list)
        for record in ws_records:
            by_stream[(record["symbol"], record["stream"])].append(record)
        affected = set()
        for key, records in by_stream.items():
            records.sort(key=lambda record: record["index"])
            for previous, current in zip(records, records[1:]):
                missing = (current["index"] - previous["index"]) // stride - 1
                if not missing:
                    assert "ingress_drop" not in current["flags"]
                    continue
                affected.add(key)
                assert "ingress_drop" in current["flags"], (key, previous, current)
                assert "sequence_gap" in current["flags"]
                assert current["sequence"]["ingressDropped"] == missing, (
                    key,
                    missing,
                    current["sequence"],
                )
        assert affected
        assert len(events) == sum(
            1
            for records in by_stream.values()
            for record in records
            if "ingress_drop" in record["flags"]
        )

        # Every affected symbol must ask for a fresh depth snapshot and re-bridge.
        dropped_depth = {symbol for symbol, stream in affected if stream == "diff_depth"}
        assert dropped_depth
        last_drop = {
            symbol: max(
                record["utc"]
                for record in ws_records
                if record["symbol"] == symbol
                and record["stream"] == "diff_depth"
                and "ingress_drop" in record["flags"]
            )
            for symbol in dropped_depth
        }
        for symbol in dropped_depth:
            assert snapshot_requests[symbol] >= 2, (symbol, snapshot_requests)
            assert any(
                record["symbol"] == symbol
                and record["stream"] == "diff_depth"
                and record["utc"] > last_drop[symbol]
                and "depth_bridge" in record["flags"]
                for record in ws_records
            ), (symbol, last_drop[symbol])
        print(
            json.dumps(
                {
                    "status": "PASS",
                    "mode": mode,
                    "ws_records": len(ws_records),
                    "dropped": report["ws_dropped_total"],
                    "streams_with_drops": len(affected),
                    "catalog_events": len(events),
                    "snapshot_requests": dict(snapshot_requests),
                    "chunks": len(chunks),
                    "rotations": report["rotations"],
                    "peak_queue_records": report["peak_queue_records"],
                }
            )
        )
        return

    assert {record["index"] for record in ws_records} == set(range(TOTAL))
    assert report["dropped_total"] == 0
    print(
        json.dumps(
            {
                "status": "PASS",
                "mode": mode,
                "ws_records": len(ws_records),
                "chunks": len(chunks),
                "rotations": report["rotations"],
                "peak_queue_records": report["peak_queue_records"],
            }
        )
    )


if __name__ == "__main__":
    verify_baseline()
    selected = "drop" if "--drop-path" in sys.argv else "stall" if "--stall-one" in sys.argv else "rotation"
    with TemporaryDirectory(prefix="sbmdr-fault-rotation-") as tmp:
        asyncio.run(exercise(Path(tmp), selected))
