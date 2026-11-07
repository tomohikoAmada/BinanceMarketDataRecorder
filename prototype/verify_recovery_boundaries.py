"""Inject Linux syscalls at actual C++ Raw/seal/Catalog boundaries, then recover."""

import asyncio
import json
import os
import socket
import subprocess
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

from verify import BINARY, FIXTURES, verify_baseline
from verify_recorder import depth_frame
from websockets.asyncio.server import serve

SHIM = sys.argv[3]


async def exercise(root, fault):
    body = depth_frame((FIXTURES / "spot/diff_depth.json").read_bytes(), 0, 8192)

    async def publish(peer):
        await peer.send(body)
        if fault == "torn_frame":
            await asyncio.sleep(1.1)  # First frame is covered by the normal data fsync.
            await peer.send(body)
        await peer.wait_closed()

    with socket.socket() as unavailable:
        unavailable.bind(("127.0.0.1", 0))
        async with serve(publish, "127.0.0.1", 0, compression=None) as ws:
            proc = await asyncio.create_subprocess_exec(
                BINARY,
                "record",
                str(ws.sockets[0].getsockname()[1]),
                str(unavailable.getsockname()[1]),
                "2" if fault == "torn_frame" else "1",
                root,
                "1000",
                env={**os.environ, "LD_PRELOAD": SHIM, "SBMDR_IO_FAULT": fault},
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                stdout, stderr = await asyncio.wait_for(proc.communicate(), 15)
            finally:
                if proc.returncode is None:
                    proc.kill()
                    await proc.wait()
    expected = (
        0
        if fault in {"short", "eintr"}
        else 1
        if fault in {"enospc", "fsync_eio"}
        else 86
    )
    assert proc.returncode == expected, (fault, proc.returncode, stderr.decode())
    if expected == 0:
        assert json.loads(stdout)["records"] == 1
    recovery = []
    for _ in range(2):
        child = await asyncio.to_thread(
            subprocess.run,
            [sys.executable, Path(__file__).with_name("recover_raw.py"), root],
            env={**os.environ, "PYTHONPATH": str(FIXTURES.parents[2] / "src")},
            capture_output=True,
            check=False,
        )
        assert child.returncode == 0, (
            fault,
            child.stdout.decode(),
            child.stderr.decode(),
        )
        recovery.append(json.loads(child.stdout))
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
    assert len(records) == (0 if fault == "enospc" else 1), (
        fault,
        len(records),
        recovery,
    )
    assert all(r.raw_payload == body for r in records)
    if expected != 0:
        assert all(
            not c.manifest["complete"] for c in chunks if c.manifest["recovered"]
        )
    assert not list((root / "data/active").glob("*.partial"))
    return {
        "fault": fault,
        "exit": proc.returncode,
        "records_read": len(records),
        "recovery": recovery,
        "status": "PASS",
    }


if __name__ == "__main__":
    verify_baseline()
    with TemporaryDirectory(prefix="sbmdr-fault-") as tmp:
        cases = (
            "eintr",
            "short",
            "enospc",
            "fsync_eio",
            "after_raw_fsync",
            "torn_frame",
            "before_sealed_rename",
            "after_sealed_rename",
            "after_manifest_rename",
            "after_catalog_commit",
        )
        print(
            json.dumps(
                [asyncio.run(exercise(Path(tmp) / case, case)) for case in cases],
                indent=2,
            )
        )
