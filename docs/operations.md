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

## M21.4 production deployment experience

M21.4 deployment established several operational practices that supplement
`ubuntu_rk3588_operations.md`:

### Wheel identity verification

The production artifact identity is determined by immutable Wheel SHA-256,
`direct_url.json` matching, non-editable install confirmation, and static
file verification inside the installed `dist-info`. The CLI `--version`
output contains a Git suffix that may be affected by the runtime working
directory; it is a display convenience, not the authoritative identity.
Always run production CLI checks from `/tmp` to avoid CWD contamination.

### Deployed RECORD SHA

The SHA-256 of the entire installed `RECORD` file is environment-specific
and cannot serve as a cross-machine fixed identity gate. It is preserved as
installation evidence only.

### Stop/seal/offline Wheel install

The production venv was updated with the service stopped and all active
Raw sealed. The new Wheel was installed with pip, the systemd unit refreshed,
and the service restarted. Both markets were confirmed READY with orderbooks
synchronized before the deployment was considered complete.

### Rollback Wheel

The prior Wheel was preserved. Rollback follows the same stop/seal/offline
install sequence but reinstalls the saved prior artifact.

### Canonical Installed Identity Gate

After installation, a static audit verified: Wheel file SHA, direct_url.json,
non-editable state, module_file path, dist-info path, RECORD file hashes,
and production Python/CLI resolution. This gate confirms the deployed artifact
is the intended build before any production validation window starts.

### Production code revision vs documentation revision

The production code commit (`cf1e749c...` for M21.4) and the repository
documentation revision are tracked separately. Documentation-only commits
merged to `main` after the production deployment do not change the production
Wheel, collector version, or running code. Do not conflate a documentation
merge commit with a production code change.

### 2h/12h/24h/72h/168h T0 independence

Each validation window has its own T0, Target, and evidence root. A prior
window's PASS does not automatically start the next window. Each window
must be explicitly created with its own T0 anchor and continuous observation.
The 24h window passed on process stability with corrective and contract
forensic confirmation; **the formal 72h window FAILED on data integrity
(reconnect boundaries seal without gap evidence; see
`docs/milestone_evidence/M21.4-72h-failure-and-reconnect-integrity.md`)**.
The 168h window remains pending and is never started automatically. A new
artifact must restart the full chain (2h→12h→24h→72h→168h) after review and
deployment.

### Formal evidence collection rules (24h corrective lessons, mandatory for 72h/168h)

The 24h corrective review and Backpressure contract forensic review
established binding rules for every future formal window:

1. **Journal boundaries derive from the formal T0/Target UTC nanoseconds, but
   journald filtering is not nanosecond-exact.** `journalctl`/systemd time
   parsing and the journal `__REALTIME_TIMESTAMP` field have **microsecond**
   resolution, not nanosecond. Derive explicit UTC timestamps from the
   nanosecond T0/Target and pass them as:
   - explicit RFC3339 UTC timestamps with microsecond precision, e.g.
     `2026-08-05T15:09:30.200566Z`
     (`journalctl --since "2026-08-05 15:09:30.200566Z"`), or
   - the `@<Unix-seconds>` syntax that the project has verified read-only on
     the actual target systemd version; do not assume fractional `@` forms
     are accepted without that read-only test.
   Never use bare local-time strings such as `--since "2026-08-05 15:09:30"`
   without a timezone suffix; journalctl interprets them in local time and
   the formal boundary shifts (the 24h export shifted 8 hours). Do not claim
   journal filtering itself is nanosecond-exact.
2. **Forbidden**: timezone-ambiguous since/until strings in any formal
   export.
3. **Export structured formats and verify bounds independently**: export with
   `--output=json` / `--output=json-seq` / `--output=export` so every record
   carries `__REALTIME_TIMESTAMP` (UTC microseconds); after each export,
   verify the first and last records' `__REALTIME_TIMESTAMP` against the
   derived UTC microsecond bounds (T0/Target nanoseconds truncated to
   microseconds). Boundary enforcement happens at journald's documented
   microsecond resolution.
4. **Boundary ambiguity**: if a record's timestamp falls in the boundary
   microsecond and its inside/outside classification cannot be decided at
   microsecond resolution, record `BOUNDARY_PRECISION_AMBIGUOUS` instead of
   guessing. Soak samples and observations carry their own nanosecond fields
   and remain strictly filtered by the nanosecond T0/Target.
5. **Formal samples/observations** must be strictly filtered by the T0/Target
   nanosecond bounds; post-Target data must never be mixed into formal
   statistics (it may be reported separately as post-window current state).
6. **Catalog events must be queried from Catalog**: `STREAM_DISCONTINUITY_*`
   and other operational events are written only to Catalog, never to the
   journal. Journal string counts are invalid evidence for them.
7. **Raw gaps must be read from Raw** (EventEnvelope `capture_flags`) and
   **manifest status from manifests** (`gap`/`complete`). Each layer has its
   own evidence responsibility.
8. **Save both raw text and parsed JSON** for every observation round; a
   serialization failure must not discard the raw command output
   (12h timer-parsing lesson).
9. **Never overwrite original evidence**: corrective reviews and forensic
   reviews must create their own independent directories under the run root
   and leave all original files byte-identical.
10. **Distinguish recovery events precisely**: `queue_backpressure_recovered`
    (queue below low_watermark) is not stream recovery completion; the
    completion boundary is new connection + first-new `sequence_gap` persisted
    + Raw sync + Catalog COMPLETED.
11. **Saturation semantics**: the 30 s backpressure budget is accumulated
    saturation time above low_watermark, and the timeout raises only when a
    later put re-encounters a full queue. A long started→timeout span is not
    a continuous full-queue span; record it as accumulated saturation.
