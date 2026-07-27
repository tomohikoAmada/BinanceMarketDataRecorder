# Ubuntu ARM64 / RK3588 operations

Status: M20 Developer Preview / Soak Candidate. This is not Production Ready.
The 72-hour and 168-hour soaks have not run.

## Fixed layout

```text
source checkout  /home/orangepi/BinanceMarketDataRecorder
production venv  /opt/binance-market-data-recorder/venv
configuration    /etc/binance-market-data-recorder/recorder.toml
internal data    /var/lib/binance-market-data-recorder
unit             binance-market-data-recorder.service
logs             journald
```

The checkout is not a data directory. The Collector runs as the User/Group
selected at install time, never root. Do not replace Ubuntu's
`/usr/bin/python3`; provide a separate Python `>=3.12,<3.13`.

## Build and first installation

Build in the existing checkout and install the final Wheel, not an editable
checkout:

```bash
python3.12 -m build --no-isolation
sudo install -d -m 0755 /opt/binance-market-data-recorder
sudo python3.12 -m venv /opt/binance-market-data-recorder/venv
sudo /opt/binance-market-data-recorder/venv/bin/python -m pip install \
  dist/binance_market_data_recorder-0.1.0a1-py3-none-any.whl
sudo install -d -o orangepi -g orangepi -m 0750 \
  /var/lib/binance-market-data-recorder
sudo install -d -o root -g orangepi -m 0750 \
  /etc/binance-market-data-recorder
```

Create `/etc/binance-market-data-recorder/recorder.toml` as root, group-readable
by the service group, mode `0640`:

```toml
[recorder]
data_root = "/var/lib/binance-market-data-recorder"
network_proxy_mode = "explicit"
network_proxy_url = "http://127.0.0.1:7890"
ingress_queue_capacity = 262144
log_level = "INFO"
```

This loopback URL contains no credentials. Never add proxy username/password,
API keys, account fields, controller secrets, node authentication, or
subscription URLs.

The RK3588 acceptance host uses the bounded `262144` capacity for both the
WebSocket receipt queues and Raw spool ingress queues. The generic default
remains `8192`. Raw time rotation is deterministically phase-staggered by
market/stream so that eMMC compression and fsync work does not start for every
stream in the same instant. Queue depth, resident memory, seal latency, and
overflow faults remain M21 soak observations; increasing the bound is not a
claim of unlimited buffering or zero interruption.

Validate before installation:

```bash
/opt/binance-market-data-recorder/venv/bin/binance-market-recorder \
  --config /etc/binance-market-data-recorder/recorder.toml doctor
/opt/binance-market-data-recorder/venv/bin/binance-market-recorder \
  --config /etc/binance-market-data-recorder/recorder.toml config show
```

Install and start:

```bash
sudo /opt/binance-market-data-recorder/venv/bin/binance-market-recorder \
  --config /etc/binance-market-data-recorder/recorder.toml \
  systemd install --user orangepi --group orangepi
sudo /opt/binance-market-data-recorder/venv/bin/binance-market-recorder \
  --config /etc/binance-market-data-recorder/recorder.toml systemd start
/opt/binance-market-data-recorder/venv/bin/binance-market-recorder \
  --config /etc/binance-market-data-recorder/recorder.toml systemd status
```

The unit has `After` and `Wants` for `network-online.target` and
`mihomo.service`, not `Requires`. Proxy process restarts therefore become
visible reconnect/resync events rather than coupling service liveness to
Mihomo's unit state.

## Proxy operation

Three modes are exact:

| Mode | Behavior |
| --- | --- |
| `direct` | WebSocket, urllib, SDK, and Historical all ignore shell proxy variables |
| `environment` | standard proxy variables and `no_proxy`; intended for interactive use |
| `explicit` | one validated unauthenticated HTTP(S) URL for every production exit |

Production systemd uses `explicit`; it does not inherit SSH shell proxy
variables. `status`, `doctor`, and `config show` reveal only mode, scheme,
loopback, and port. Do not automate `mihomo-select`; online acceptance uses the
operator's already-selected fixed node. Do not enable TUN, redirect/TProxy,
iptables, nftables, or policy routing for this service.

## Status, logs, stop, and restart

```bash
systemctl status binance-market-data-recorder.service
journalctl -u binance-market-data-recorder.service --since today
/opt/binance-market-data-recorder/venv/bin/binance-market-recorder \
  --config /etc/binance-market-data-recorder/recorder.toml status
sudo systemctl stop binance-market-data-recorder.service
sudo systemctl start binance-market-data-recorder.service
sudo systemctl restart binance-market-data-recorder.service
```

SIGTERM must reach `STOPPED` after draining and sealing within
`TimeoutStopSec=90s`. A stale PID/heartbeat is never reported as healthy.

## Safe update and rollback

Linux blue/green deployment is not certified in M20. Use a controlled,
gap-explicit update:

1. Save the current Wheel and configuration version.
2. Stop the unit; verify `STOPPED` and sealed manifests.
3. Install the new final Wheel into the production venv.
4. Run `doctor`, `config show`, and import smoke.
5. Run `systemd install` again (idempotent unit refresh), then start.
6. Require both markets READY and verify new Raw/Catalog evidence.

Rollback repeats the stop/seal sequence, installs the saved prior Wheel,
refreshes the unit, and starts. Never delete or edit
`/var/lib/binance-market-data-recorder`.

## Already-mounted external archive directory

The OS/operator mounts the filesystem first. Recorder performs no mount,
unmount, format, repair, repartition, or udev action.

```bash
findmnt --json
lsblk --json
binance-market-recorder --config /etc/binance-market-data-recorder/recorder.toml \
  storage list
binance-market-recorder --config /etc/binance-market-data-recorder/recorder.toml \
  storage inspect /media/orangepi/archive/Recorder
binance-market-recorder --config /etc/binance-market-data-recorder/recorder.toml \
  storage register /media/orangepi/archive/Recorder
```

Registration requires an existing subdirectory, external/hotplug block-device
evidence, a reliable filesystem UUID, writable capability, marker, and
write/fsync/rename/readback probe. The external directory receives only sealed
archive transactions; Active Raw always stays internal.

If the filesystem disappears, collection continues, the archive attempt is
reported failed/absent, and the internal source is retained. M20 has no trusted
udisks eject backend: `storage eject` reports `MANUAL_ACTION_REQUIRED` and does
not claim safe removal. Stop/finish archive work and use trusted OS tooling.

## Soak plan (not executed in M20)

M21 owns both runs:

- 7-day/168-hour: repeated connection rotation, proxy restart, service restart,
  resource trends, gaps/resyncs, seal/catalog consistency, and archive backlog.
- 30-day operational observation: disk growth/forecast, journal retention,
  external disappearance/reinsertion, update/rollback drills, and alert review.

The separate 72-hour gate must also be recorded. Until those results exist the
platform remains a Soak Candidate and must not be described as Production
Ready or zero-interruption.
