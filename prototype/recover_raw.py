"""Recover an offline S-BMDR root using the frozen Python Raw lifecycle."""

import argparse
import fcntl
import json
from dataclasses import asdict
from pathlib import Path

from binance_market_data_recorder.spool.format import scan_chunk
from binance_market_data_recorder.spool.recovery import _quarantine, recover_storage
from binance_market_data_recorder.spool.seal import seal_partial
from binance_market_data_recorder.storage.catalog.types import ChunkState
from binance_market_data_recorder.storage.layout import ensure_storage_layout

from binance_market_data_recorder.storage.catalog import Catalog


def recover(root: Path) -> list[dict[str, object]]:
    if not (root / "state/c1.lock").is_file():
        raise ValueError("expected an existing S-BMDR recorder root")
    with (root / "state/c1.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        layout = ensure_storage_layout(root)
        with Catalog(layout.catalog) as catalog:
            actions = [
                asdict(action)
                for action in recover_storage(layout=layout, catalog=catalog)
            ]
            for path in sorted(layout.active.glob("*.bmdr.partial")):
                scan = scan_chunk(path)
                if not scan.is_clean or scan.header is None:
                    raise RuntimeError(f"unresolved partial {path.name}")
                if scan.statistics.record_count == 0:
                    chunk_id = str(scan.header.chunk_id)
                    catalog.transition(
                        chunk_id,
                        ChunkState.QUARANTINED,
                        idempotency_key=f"empty-partial:{chunk_id}",
                        evidence={"reason": "zero_complete_frames"},
                    )
                    actions.append(
                        asdict(
                            _quarantine(
                                path,
                                layout=layout,
                                catalog=catalog,
                                reason="zero_complete_frames",
                            )
                        )
                    )
                    continue
                manifest = seal_partial(
                    path,
                    layout=layout,
                    catalog=catalog,
                    forced_flags=frozenset({"sequence_gap"}),
                )
                actions.append(
                    {
                        "path": path.name,
                        "action": "sealed_after_stop",
                        "chunk_id": manifest["chunk_id"],
                    }
                )
            return actions


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    actions = recover(parser.parse_args().root)
    print(json.dumps(actions, indent=2))
    return int(any(action["action"] == "reconcile_failed" for action in actions))


if __name__ == "__main__":
    raise SystemExit(main())
