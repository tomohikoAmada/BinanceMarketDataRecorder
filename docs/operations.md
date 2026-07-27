# Operations

Ubuntu ARM64/RK3588 systemd, explicit proxy, update/rollback, mounted external
directory, and M21 soak procedures are in
[`ubuntu_rk3588_operations.md`](ubuntu_rk3588_operations.md). Ubuntu is an M20
Developer Preview / Soak Candidate, not Production Ready.

## Proxy status

`config show`, `doctor`, and `status` expose only proxy mode, scheme, loopback,
and port. They never expose the configured URL. `direct` ignores the shell,
`environment` honors standard variables plus `no_proxy`, and `explicit` uses
one credential-free HTTP(S) proxy for all production network exits. systemd
production uses TOML `explicit`, never an SSH environment.

## Side-data and backfill status

Runtime market state includes each side task's `status`, `enabled`, `running`,
`attempts`, `accepted`, `failures`, `consecutive_failures`,
`last_success_at_utc_ns`, `last_error_type`, and `next_retry_at_utc_ns`.
Retrying/stale enabled tasks make network status `DEGRADED`; core collection
continues.

Always run `backfill plan` before `backfill run` and review estimated bytes and
URLs. Imports use concurrency one, `.partial` files, official checksums and
atomic revision commits. Funding-rate archives are planned monthly because that
is the verified official layout; other partial months use daily files.
Normalization streams fixed-size Arrow batches instead of retaining an entire
CSV in memory. `backfill verify` rereads immutable ZIP hashes and verifies
Parquet readability and lineage metadata. A 404 is a recorded gap, not empty
data.

Each limited-retention USD-M 5-minute dataset has an independent durable
Cursor. Restart catches up from the next unpersisted period in bounded pages.
`EMPTY_RESPONSE` and request/fsync failures keep the Cursor stationary. Monitor
`consecutive_failures` and explicit unrecoverable-gap events; no process can
recover a period after Binance removes it from the public retention window.

## Status and reports

All commands return structured JSON:

```bash
binance-market-recorder doctor
binance-market-recorder status
binance-market-recorder report daily --date YYYY-MM-DD
binance-market-recorder storage forecast
binance-market-recorder archive status
```

`status` is evidence-based. A missing, dead, future-dated, or stale heartbeat
cannot produce `RUNNING`.

## LaunchAgent lifecycle

```bash
binance-market-recorder launchd install \
  --label "$AUTHOR_CONTROLLED_LABEL" \
  --author-controls-namespace
binance-market-recorder launchd start
binance-market-recorder launchd status
binance-market-recorder launchd stop
binance-market-recorder launchd uninstall
```

The service runs only in the logged-in user session and needs no root access.
SIGTERM drains queues and seals active Raw. launchd restarts unsuccessful
exits. The single-process lock prevents two service owners from sharing one
data root.

Uninstall never removes market data or the Catalog. Confirm the configured
data root separately before removing any code environment.

## systemd lifecycle

Use an explicit config on every management command:

```bash
sudo binance-market-recorder --config /etc/binance-market-data-recorder/recorder.toml \
  systemd install --user orangepi --group orangepi
sudo binance-market-recorder --config /etc/binance-market-data-recorder/recorder.toml \
  systemd start
binance-market-recorder --config /etc/binance-market-data-recorder/recorder.toml \
  systemd status
```

Stop/restart/uninstall are idempotent. SIGTERM drains/seals; uninstall retains
configuration and all data. Logs are in journald.

## External storage

Only an explicit project subdirectory may be registered:

```bash
binance-market-recorder storage list
binance-market-recorder storage inspect /Volumes/Disk/Chosen/Recorder
binance-market-recorder storage register /Volumes/Disk/Chosen/Recorder
binance-market-recorder storage status
binance-market-recorder archive retry
binance-market-recorder archive verify <storage-id>
```

To remove a disk, stop new allocation and request non-forced macOS
unmount/eject:

```bash
binance-market-recorder storage eject <storage-id>
```

Do not unplug until the command confirms both system operations. `BUSY`,
refusal, timeout, or disappearance is not safe-to-remove confirmation.
External absence does not stop internal capture.
Linux M20 performs no automatic eject; it reports manual action without
`SAFE_TO_REMOVE`.

## Sleep and resource operation

Laptop sleep is a known gap source. `prevent_sleep=true` uses a service-scoped
`caffeinate` assertion and never changes permanent power settings. It does not
promise closed-lid capture. Review daily reconnect, sequence-gap, resync,
oldest-unarchived, queue, file-handle, memory, and disk/backlog metrics.

The 40%, 15%, and emergency alerts are operational boundaries, not capacity
targets. Attach and verify archive storage before the internal spool
approaches the hard reserve.

## Recovery

- A killed Collector is restarted by launchd; startup scans `.partial` files
  and truncates only to the last complete frame.
- A gap makes the local book unreliable until a public snapshot resynchronizes
  it; the incomplete interval remains visible.
- Archive copy/checksum/Catalog failures are retryable and never authorize
  deletion of an unverified source.
- A reinserted registered disk is resolved by volume UUID and marker, not by a
  fixed `/Volumes/<name>` assumption.

See [data and storage](data_and_storage.md) for artifact guarantees and
[known limitations](known_limitations.md) before operating the preview.
