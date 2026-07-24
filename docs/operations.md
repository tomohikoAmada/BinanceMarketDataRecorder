# Operations

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
