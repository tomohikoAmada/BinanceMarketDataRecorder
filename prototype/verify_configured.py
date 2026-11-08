"""Multi-symbol combined routes and independent Raw identities, using C3 entry point."""

import asyncio
import json
import signal
from functools import partial
from http.server import ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Thread
from urllib.parse import parse_qs, urlsplit

from verify import BINARY, verify_baseline
from verify_core import until
from websockets.asyncio.server import serve

SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"]


async def exercise(root, market, config):
    delivered = set()
    snapshot_symbols = set()

    class HTTP(ThreadingHTTPServer):
        pass

    from verify import Response

    class Handler(Response):
        def do_GET(self):
            parsed = urlsplit(self.path)
            if parsed.path.endswith("/depth"):
                snapshot_symbols.add(parse_qs(parsed.query)["symbol"][0])
                payload = b'{"lastUpdateId":101,"bids":[],"asks":[]}'
            else:
                payload = json.dumps(
                    {
                        "symbols": [
                            {"symbol": s, "status": "TRADING"} for s in SYMBOLS
                        ],
                        "rateLimits": [],
                    }
                ).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    async def publish(peer):
        streams = parse_qs(urlsplit(peer.request.path).query)["streams"][0].split("/")
        for stream in streams:
            symbol, kind = stream.split("@", 1)
            data = {
                "e": "depthUpdate"
                if kind == "depth@100ms"
                else "aggTrade"
                if kind == "aggTrade"
                else "markPriceUpdate"
                if kind == "markPrice@1s"
                else "forceOrder"
                if kind == "forceOrder"
                else "bookTicker",
                "s": symbol.upper(),
                "E": 1000,
                "U": 100,
                "u": 102,
                "pu": 99,
                "a": 7,
                "b": [],
                "o": {"s": symbol.upper()},
            }
            if kind == "depth@100ms":
                data["a"] = []
            await peer.send(json.dumps({"stream": stream, "data": data}))
            delivered.add(stream)
        await peer.wait_closed()

    with HTTP(("127.0.0.1", 0), partial(Handler, b"", 200)) as http:
        thread = Thread(target=http.serve_forever, kwargs={"poll_interval": 0.05})
        thread.start()
        try:
            async with serve(publish, "127.0.0.1", 0, compression=None) as ws:
                proc = await asyncio.create_subprocess_exec(
                    BINARY,
                    "record",
                    str(ws.sockets[0].getsockname()[1]),
                    str(http.server_port),
                    "100000",
                    root,
                    "6000",
                    "900000",
                    "134217728",
                    "0",
                    market,
                    config,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                try:
                    await until(
                        lambda: (
                            len(delivered)
                            == len(SYMBOLS) * (5 if market == "usdm" else 3)
                            and snapshot_symbols == set(SYMBOLS)
                        )
                    )
                    await asyncio.sleep(0.3)
                    proc.send_signal(signal.SIGTERM)
                    stdout, stderr = await asyncio.wait_for(proc.communicate(), 12)
                except AssertionError as error:
                    if proc.returncode is None:
                        proc.send_signal(signal.SIGTERM)
                    output, errors = await asyncio.wait_for(proc.communicate(), 12)
                    raise AssertionError(
                        {
                            "delivered": sorted(delivered),
                            "snapshots": sorted(snapshot_symbols),
                            "stderr": errors.decode(),
                            "stdout": output.decode(),
                        }
                    ) from error
                finally:
                    if proc.returncode is None:
                        proc.kill()
                        await proc.wait()
        finally:
            http.shutdown()
            thread.join()
    assert proc.returncode == 0, stderr.decode()
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
    assert len(records) == json.loads(stdout)["records"]
    ws_records = [r for r in records if r.module == "binance.public.websocket.v1"]
    assert len(ws_records) == len(delivered)
    assert {r.symbol for r in ws_records} == set(SYMBOLS)
    assert len({(r.symbol, r.stream) for r in ws_records}) == len(delivered)
    assert len({r.connection_id for r in ws_records}) == (2 if market == "usdm" else 1)
    assert all(
        not r.capture_flags or "malformed" not in r.capture_flags for r in ws_records
    )
    snapshots = [
        r for r in records if r.source_sequence.get("sourceStream") == "depth_snapshot"
    ]
    assert {r.symbol for r in snapshots} == set(SYMBOLS)
    assert all("depth_bridge" in r.capture_flags for r in snapshots)
    print(market, len(records), "PASS")


if __name__ == "__main__":
    verify_baseline()
    with TemporaryDirectory(prefix="sbmdr-configured-") as tmp:
        config = Path(tmp) / "config.json"
        config.write_text(json.dumps({"spot": SYMBOLS, "usdm": SYMBOLS}))
        for market in ("spot", "usdm"):
            asyncio.run(exercise(Path(tmp) / market, market, config))
