"""Read-only cloud diagnostic: installed unbounded history vs in-memory Q1 source.

Run locally with Python 3.12. No deployment, production-data access or service
mutation; remote interpreters have bytecode writes disabled. Each mode uses a
separate process. This synthetic duplicate burst is not a live-capacity verdict.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path

REMOTE = r'''
import datetime, hashlib, json, platform, subprocess, sys, time, tracemalloc, types
from pathlib import Path
from binance_market_data_recorder.orderbook.model import BookSnapshot, DepthUpdate
from binance_market_data_recorder.orderbook.reconstructor import LocalBookReconstructor

def service_state():
    return {command: subprocess.run(
        ['systemctl', command, 'binance-market-data-recorder.service'],
        capture_output=True, text=True, check=False,
    ).stdout.strip() for command in ['is-active', 'is-enabled']}

before = service_state()
if before != {'is-active': 'inactive', 'is-enabled': 'disabled'}:
    raise RuntimeError('diagnostic requires the recorded stopped/disabled checkpoint')
if MODE == 'bounded':
    module = types.ModuleType('binance_market_data_recorder.orderbook.q1_memory_probe')
    module.__package__ = 'binance_market_data_recorder.orderbook'
    sys.modules[module.__name__] = module
    exec(compile(CANDIDATE_SOURCE, '<q1-in-memory-source>', 'exec'), module.__dict__)
    LocalBookReconstructor = module.LocalBookReconstructor

observed = 0
def observe(audit, timestamp):
    global observed
    observed += 1

options = {'audit_history_limit': 256} if MODE == 'bounded' else {}
book = LocalBookReconstructor('spot', audit_observer=observe, **options)
initial = DepthUpdate('spot', 'BTCUSDT', 161, 170, None, (), (), 170)
book.offer(initial)
book.synchronize(BookSnapshot('spot', 'BTCUSDT', 160, (('99', '1'),), (('101', '1'),)))
duplicate = DepthUpdate('spot', 'BTCUSDT', 170, 170, None, (), (), 170)
tracemalloc.start()
started = time.monotonic()
samples = []
for count in range(1, 100001):
    book.offer(duplicate)
    if count in {10000, 50000, 100000}:
        current, peak = tracemalloc.get_traced_memory()
        rss = next(line for line in Path('/proc/self/status').read_text().splitlines()
                   if line.startswith('VmRSS:'))
        samples.append({'events': count, 'retained_audits': len(book.audits),
                        'observer_calls': observed, 'traced_current_bytes': current,
                        'traced_peak_bytes': peak, 'rss_bytes': int(rss.split()[1]) * 1024})
book_hash = book.book.logical_hash()
book.offer(DepthUpdate('spot', 'BTCUSDT', 180, 180, None, (), (), 180))
gap_count = len(book.unreliable_intervals)
after = service_state()
if after != before:
    raise RuntimeError('service checkpoint changed during diagnostic')
print(json.dumps({'mode': MODE, 'host': platform.node(), 'machine': platform.machine(),
                  'python': platform.python_version(),
                  'retrieved_at_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
                  'service_before': before, 'service_after': after, 'samples': samples,
                  'elapsed_seconds_with_tracemalloc': time.monotonic() - started,
                  'book_hash': book_hash, 'gap_count': gap_count,
                  'observer_calls_after_gap': observed}, sort_keys=True))
'''


def main() -> None:
    repository = Path(__file__).resolve().parents[2]
    source_path = repository / "src/binance_market_data_recorder/orderbook/reconstructor.py"
    source = source_path.read_text()
    results = []
    for mode in ("installed", "bounded"):
        program = f"MODE = {mode!r}\nCANDIDATE_SOURCE = {source!r}\n" + REMOTE
        response = subprocess.run(
            ["ssh", "-T", "-o", "BatchMode=yes", "-o", "ConnectTimeout=8",
             "greencloud-tokyo-01", "/opt/binance-market-data-recorder/venv/bin/python -B -"],
            input=program, capture_output=True, text=True, timeout=60, check=True,
        )
        results.append(json.loads(response.stdout))
    print(json.dumps({"candidate_source_sha256": hashlib.sha256(source.encode()).hexdigest(),
                      "scope": "synthetic_cloud_diagnostic_not_live_capacity", "results": results},
                     indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
