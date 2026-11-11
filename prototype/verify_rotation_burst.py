"""Writer stalls must be absorbed by the configured queue, and full queues must drop honestly."""

import asyncio
import json
import os
import signal
import sqlite3
import sys
import time
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
TAIL_TOTAL = 6000
RECONNECT_AFTER = 9000
RATE = 3000


def wire(index, symbols=SYMBOLS):
    stride = len(symbols) * len(STREAMS)
    symbol = symbols[index % len(symbols)]
    kind = (index // len(symbols)) % len(STREAMS)
    cycle = index // stride
    data = {"s": symbol, "E": index + 1000, "_test_id": index}
    if kind == 0:
        stream = "depth@100ms"
        # Wide overlapping ranges keep the bridge fixture deterministic while the
        # queue backs up: any snapshot near the current cycle is covered by the
        # next update, while the dedicated depth-edge unit test covers exact U/u
        # and pu sequencing.
        data.update(
            e="depthUpdate",
            U=100 + max(0, cycle - 1000),
            u=100 + cycle + 1000,
            b=[],
            a=[],
        )
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
    tail_path = mode == "tail"
    rest_path = mode == "rest"
    reconnect_path = mode == "reconnect"
    renew_path = mode == "renew"
    congested = drop_path or tail_path or rest_path or reconnect_path
    stalled = mode == "stall"
    symbols = DROP_SYMBOLS if congested else SYMBOLS
    if renew_path:
        # One symbol keeps the Spot depth weight budget (250 per snapshot,
        # 2000 per 60 s) out of the way while renewals are compressed to 2 s.
        symbols = ["BTCUSDT"]
    if reconnect_path:
        # A reconnect plus the drop marker can request two snapshot epochs per
        # symbol; two symbols stay inside the 2000/60 s Spot depth budget.
        symbols = DROP_SYMBOLS[:2]
    stride = len(symbols) * len(STREAMS)
    test_queue_records = 1024 if congested else None
    total = TAIL_TOTAL if tail_path else TOTAL
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
                if rest_path and snapshot_requests[symbol] == 1:
                    # Hold the first snapshot response until the bounded queue is full.
                    time.sleep(0.5)
                # Use the last sent update ID. Any in-flight event at or below it is
                # stale for Spot, so the next produced update bridges deterministically.
                last = max(101, depth_u.get(symbol, 101))
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
    connections = {"count": 0, "next": 0}

    def stop_recorder():
        if holder["proc"].returncode is None:
            holder["proc"].send_signal(signal.SIGTERM)

    def send_index(index):
        kind = (index // len(symbols)) % len(STREAMS)
        if kind == 0:
            depth_u[symbols[index % len(symbols)]] = 100 + index // stride
        return wire(index, symbols)

    async def publish(peer):
        if reconnect_path or renew_path:
            connections["count"] += 1
            start = asyncio.get_running_loop().time()
            sent = 0
            try:
                while connections["next"] < TOTAL:
                    index = connections["next"]
                    connections["next"] += 1
                    await peer.send(send_index(index))
                    sent += 1
                    if (
                        reconnect_path
                        and connections["next"] >= RECONNECT_AFTER
                        and connections["count"] == 1
                    ):
                        await peer.close()
                        return
                    due = start + sent / RATE
                    await asyncio.sleep(max(0, due - asyncio.get_running_loop().time()))
                await asyncio.sleep(1.0)
                stop_recorder()
                await peer.wait_closed()
            except ConnectionClosed:
                pass
            return
        start = asyncio.get_running_loop().time()
        try:
            for batch in range(0, total, 30):
                for index in range(batch, min(batch + 30, total)):
                    await peer.send(send_index(index))
                due = start + (batch + 30) / RATE
                await asyncio.sleep(max(0, due - asyncio.get_running_loop().time()))
            if congested:
                # Tail drops must survive the stop with no later frame to carry them.
                await asyncio.sleep(0.2 if tail_path else 1.0)
                stop_recorder()
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
                "9223372036854775807" if congested else str(TOTAL),
                root,
                "300000",
                "5000",
                "134217728",
                "3500" if congested else "0",
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
                    **({"SBMDR_WS_RENEW_SECONDS": "2"} if renew_path else {}),
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
    if congested:
        assert report["ws_dropped_total"] > 0
    if drop_path:
        assert report["rest_dropped_total"] == 0
    if rest_path:
        assert report["rest_dropped_total"] > 0

    from binance_market_data_recorder.normalize.raw import (
        iter_source_records,
        load_source_chunks,
    )

    with Catalog(layout.catalog, read_only=True) as catalog:
        chunks = load_source_chunks(layout=layout, catalog=catalog)
        ws_records = []
        rest_records = []
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
                            "connection_id": e.connection_id,
                        }
                    )
                elif e.module == "binance.public.rest.v1":
                    rest_records.append(
                        {
                            "symbol": e.symbol,
                            "stream": e.source_sequence.get("sourceStream"),
                            "flags": set(e.capture_flags),
                            "sequence": e.source_sequence,
                            "utc": e.receive_time_utc_ns,
                            "connection_id": e.connection_id,
                        }
                    )
    assert record_count == report["records"]

    if renew_path:
        assert connections["count"] >= 3, connections
        status_rows = [
            json.loads(line) for line in stderr.decode().splitlines() if line.strip()
        ]
        renewals = [
            row
            for row in status_rows
            if row.get("event") == "WS_RECONNECT" and row.get("reason") == "23h_renewal"
        ]
        assert len(renewals) >= 2, renewals
        assert all(
            row["group"] == "spot-public" and row["accepted"] > 0 for row in renewals
        ), renewals
        assert not any(
            row.get("event") == "WS_RECONNECT" and row.get("reason") == "transport_failure"
            for row in status_rows
        ), status_rows
        assert len(ws_records) > TOTAL // 2, (len(ws_records), TOTAL)
        assert report["dropped_total"] == 0
        by_connection = defaultdict(set)
        latest_utc = {}
        for record in ws_records:
            connection = record["connection_id"]
            by_connection[(record["symbol"], record["stream"])].add(connection)
            latest_utc[connection] = max(
                latest_utc.get(connection, 0), record["utc"]
            )
        first_connection = min(
            latest_utc, key=lambda connection: latest_utc[connection]
        )
        for symbol in symbols:
            key = (symbol, "diff_depth")
            assert len(by_connection[key]) >= 2, (key, by_connection[key])
            assert any(
                record["symbol"] == symbol
                and record["stream"] == "diff_depth"
                and record["connection_id"] != first_connection
                and "depth_bridge" in record["flags"]
                for record in ws_records
            ), (symbol, first_connection)
        print(
            json.dumps(
                {
                    "status": "PASS",
                    "mode": mode,
                    "ws_records": len(ws_records),
                    "connections": connections["count"],
                    "renewals": len(renewals),
                    "renewal_groups": sorted({row["group"] for row in renewals}),
                    "chunks": len(chunks),
                }
            )
        )
        return

    if drop_path or reconnect_path:
        assert len(ws_records) + report["ws_dropped_total"] <= total
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
        assert len([event for event in events if not event.get("tail")]) == sum(
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
        result = {
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
            "reconnect_events": max(0, connections["count"] - 1),
        }
        if reconnect_path:
            assert connections["count"] >= 2
            by_connection = defaultdict(set)
            for record in ws_records:
                by_connection[(record["symbol"], record["stream"])].add(
                    record["connection_id"]
                )
            for symbol in dropped_depth:
                key = (symbol, "diff_depth")
                assert len(by_connection[key]) >= 2, (key, by_connection[key])
                latest = max(
                    by_connection[key],
                    key=lambda connection: max(
                        r["utc"] for r in ws_records if r["connection_id"] == connection
                    ),
                )
                assert any(
                    r["symbol"] == symbol
                    and r["stream"] == "diff_depth"
                    and r["connection_id"] == latest
                    and "depth_bridge" in r["flags"]
                    for r in ws_records
                ), (symbol, latest)
            first_connection = min(
                {record["connection_id"] for record in ws_records},
                key=lambda connection: min(
                    record["utc"]
                    for record in ws_records
                    if record["connection_id"] == connection
                ),
            )
            assert any(
                "ingress_drop" in record["flags"]
                and record["connection_id"] != first_connection
                for record in ws_records
            ), first_connection
        print(json.dumps(result))
        return

    if tail_path:
        assert len(ws_records) + report["ws_dropped_total"] == TAIL_TOTAL
        assert not any("ingress_drop" in record["flags"] for record in ws_records)
        events = []
        with sqlite3.connect(f"file:{layout.catalog}?mode=ro", uri=True) as db:
            rows = db.execute(
                "SELECT evidence_json,market,symbol,stream,gap_id FROM operational_events "
                "WHERE event_type='INGRESS_DROP'"
            ).fetchall()
        assert rows and all(all(value is None for value in row[1:]) for row in rows)
        for row in rows:
            evidence = json.loads(row[0])
            assert evidence["market"] == "spot" and evidence["tail"] is True
            assert evidence["first_drop_utc_ns"] <= evidence["last_drop_utc_ns"]
            events.append(evidence)
        assert sum(event["dropped"] for event in events) == report["ws_dropped_total"]
        assert any(event["stream"] == "diff_depth" for event in events)
        tailed = [
            chunk.manifest
            for chunk in chunks
            if "ingress_drop" in chunk.manifest.get("capture_flags", [])
        ]
        assert tailed, "tail gap was not attached to any manifest"
        assert any(not chunk.manifest.get("complete", True) for chunk in chunks)
        print(
            json.dumps(
                {
                    "status": "PASS",
                    "mode": mode,
                    "ws_records": len(ws_records),
                    "dropped": report["ws_dropped_total"],
                    "tail_events": len(events),
                    "tail_streams": sorted({e["stream"] for e in events}),
                    "chunks": len(chunks),
                    "rotations": report["rotations"],
                    "peak_queue_records": report["peak_queue_records"],
                }
            )
        )
        return

    if rest_path:
        events = []
        with sqlite3.connect(f"file:{layout.catalog}?mode=ro", uri=True) as db:
            rows = db.execute(
                "SELECT evidence_json,market,symbol,stream,gap_id FROM operational_events "
                "WHERE event_type='INGRESS_DROP'"
            ).fetchall()
        assert rows and all(all(value is None for value in row[1:]) for row in rows)
        for row in rows:
            events.append(json.loads(row[0]))
        ws_events = sum(event["dropped"] for event in events if not event["rest"])
        rest_events = sum(event["dropped"] for event in events if event["rest"])
        assert ws_events == report["ws_dropped_total"], (ws_events, report)
        assert rest_events == report["rest_dropped_total"], (rest_events, report)
        marked_rest = [record for record in rest_records if "ingress_drop" in record["flags"]]
        assert marked_rest and all(
            record["stream"] == "depth_snapshot" for record in marked_rest
        )
        assert sum(
            record["sequence"]["ingressDropped"] for record in marked_rest
        ) == rest_events
        for symbol in sorted({record["symbol"] for record in marked_rest}):
            assert snapshot_requests[symbol] >= 1, (symbol, snapshot_requests)
            dropped_at = max(
                record["utc"]
                for record in marked_rest
                if record["symbol"] == symbol
            )
            assert any(
                record["symbol"] == symbol
                and record["stream"] == "diff_depth"
                and record["utc"] > dropped_at
                and "depth_bridge" in record["flags"]
                for record in ws_records
            ), (symbol, dropped_at)
        print(
            json.dumps(
                {
                    "status": "PASS",
                    "mode": mode,
                    "ws_records": len(ws_records),
                    "ws_dropped": report["ws_dropped_total"],
                    "rest_dropped": report["rest_dropped_total"],
                    "catalog_ws_events": ws_events,
                    "catalog_rest_events": rest_events,
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
    selected = (
        "drop"
        if "--drop-path" in sys.argv
        else "tail"
        if "--tail-path" in sys.argv
        else "rest"
        if "--rest-path" in sys.argv
        else "reconnect"
        if "--reconnect-path" in sys.argv
        else "renew"
        if "--renew-path" in sys.argv
        else "stall"
        if "--stall-one" in sys.argv
        else "rotation"
    )
    with TemporaryDirectory(prefix="sbmdr-fault-rotation-") as tmp:
        asyncio.run(exercise(Path(tmp), selected))
