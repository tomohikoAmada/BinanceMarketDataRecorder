"""Install only the uniquely named, runtime-only P4 units; retain other units."""

import json
import subprocess
import sys
from pathlib import Path

base = "/opt/binance-market-data-recorder/p4-20261101-btc-c"
root = "/var/lib/binance-market-data-recorder/p4-20261101-btc-c"
storage_id = json.loads(Path(sys.argv[1]).read_text())["storage_id"]
common = f"""User=bmdr
Group=bmdr
WorkingDirectory={root}
UMask=0027
NoNewPrivileges=true
Environment=PYTHONUNBUFFERED=1
Environment=BINANCE_MARKET_RECORDER_GIT_COMMIT=8bdae2005b0311dac4615bcecffd56c8a3cb211c
"""
cli = f"{base}/venv/bin/python -m binance_market_data_recorder --config {base}/recorder.toml"
units = {
    "binance-market-data-p4c-btc.service": f"""[Unit]
Description=Recorder P4 BTC full scope
After=network-online.target
[Service]
Type=simple
MemoryAccounting=yes
CPUAccounting=yes
{common}ExecStart={cli} _service run
Restart=no
TimeoutStopSec=180
KillSignal=SIGTERM
""",
    "binance-market-data-p4c-archive.service": f"""[Unit]
Description=Recorder P4 normal archive drain
[Service]
Type=oneshot
{common}ExecStart={cli} archive drain --storage-id {storage_id} \\
 --max-runtime-seconds 50 --max-files 1000
TimeoutStartSec=90
""",
    "binance-market-data-p4c-archive.timer": """[Unit]
Description=Recorder P4 normal archive timer
[Timer]
OnActiveSec=60s
OnUnitActiveSec=60s
AccuracySec=1s
Unit=binance-market-data-p4c-archive.service
""",
    "binance-market-data-p4c-prep-stop.service": """[Unit]
Description=Recorder P4 preparation deadline
[Service]
Type=oneshot
ExecStart=/usr/bin/systemctl stop binance-market-data-p4c-btc.service
""",
    "binance-market-data-p4c-prep-stop.timer": """[Unit]
Description=Recorder P4 fixed 900 second preparation cap
[Timer]
OnActiveSec=900s
AccuracySec=1s
Unit=binance-market-data-p4c-prep-stop.service
""",
}
for name, body in units.items():
    with (Path("/run/systemd/system") / name).open("x") as handle:
        handle.write(body)
    with (Path(root) / "evidence" / name).open("x") as handle:
        handle.write(body)
subprocess.run(["systemctl", "daemon-reload"], check=True)
subprocess.run(
    [
        "systemctl",
        "start",
        "binance-market-data-p4c-prep-stop.timer",
        "binance-market-data-p4c-archive.timer",
        "binance-market-data-p4c-btc.service",
    ],
    check=True,
)
subprocess.run(
    [
        "systemctl",
        "show",
        "binance-market-data-p4c-btc.service",
        "-p",
        "MainPID",
        "-p",
        "InvocationID",
        "-p",
        "ActiveState",
    ],
    check=True,
)
