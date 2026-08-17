# macOS Operations Contract

## Deployment model

macOS Apple Silicon is the development/local profile, not the primary future
production authority. The implemented service is a user LaunchAgent on macOS
Apple Silicon, Python 3.12, requiring the user to be logged in. Installation does not require root
and does not silently install a LaunchDaemon. Future launchd, service, log, and
configuration identifiers derive from `BinanceMarketDataRecorder`, never a
consumer name or a namespace implying Binance ownership. The final reverse-DNS
namespace must be controlled by the project author and is intentionally not
guessed in M0.2. Logs and structured state live under
`~/Library/Application Support/BinanceMarketDataRecorder/`, not the repository
or `/Users/amada/Documents/Development/Crypto`.

M14 provides install, uninstall, start, stop, and status scripts; crash restart;
SIGTERM sealing; configuration permission checks; and a single-service-process
lock compatible with ADR-0018 supervised overlap. ADR-0019 is authoritative.

## Volume discovery

macOS normally mounts volumes. The Recorder's macOS adapter uses Disk
Arbitration capabilities to:

- perform a startup inventory;
- observe media/volume appearance and disappearance;
- obtain volume UUID, name, filesystem type, mount state/mountpoint and
  read-only state;
- observe/request unmount and eject;
- re-resolve the current mountpoint by UUID after reinsertion.

ADR-0014 selects exact-pinned PyObjC 12.2.1 after an unprivileged capability
test proved session creation, startup delivery, and appeared/disappeared
callback bridging. The adapter also registers description-change callbacks on
its Core Foundation run loop. There is no polling fallback. Fixed
`/Volumes/<name>` identity is forbidden.

Unregistered volumes are display-only. The service writes only inside an
explicitly registered folder and places the marker there, never at disk root.
No format, partition, mount-mode, filesystem repair, or SMART operation is
performed.

## Required CLI surface

By M9/M12 the `binance-market-recorder` CLI includes:

```text
binance-market-recorder storage list
binance-market-recorder storage inspect <path>
binance-market-recorder storage register <folder-path>
binance-market-recorder storage unregister <storage-id>
binance-market-recorder storage status
binance-market-recorder storage eject <storage-id>
binance-market-recorder storage forecast
binance-market-recorder archive status
binance-market-recorder archive retry
binance-market-recorder archive verify <storage-id>
```

Registration is an explicit user action. Unregistering stops future use and
does not erase archived data.

M9 provides `storage list`, `inspect`, `register`, `unregister`, and `status`.
List/inspect are read-only. Register/status capability probes write only
temporary Recorder-owned files in the selected/registered directory. M10 provides `archive status`,
`archive retry [--storage-id ID]`, and `archive verify <storage-id>`. Retry
advances one oldest eligible transaction; it requires exactly one READY target
unless an ID is specified. Verify performs complete artifact and manifest
readback. Neither command mounts, repairs, formats, or ejects a volume.

M11 provides `storage forecast`; it persists a capacity observation for
internal storage and each accessible registered target, then reports
independent threshold state, robust rates and UTC ETAs. It never mounts a
missing target.

M12 provides `storage eject <storage-id> [--timeout-seconds N]`. Exit zero
means Disk Arbitration confirmed both unmount and eject. Structured `BUSY`,
`EJECT_REFUSED`, `FORCED_REMOVAL`, and error results are nonzero and never mean
safe-to-remove.

## Eject protocol

An eject request sets `EJECT_PENDING`, prevents new archive allocation, and
requires current work to be completed through the existing idempotent retry
path. Recorder fsyncs its archive directories and Catalog, closes its handles,
then asks Disk Arbitration to unmount and eject with default non-forced
options. Immediate requests are refused as `BUSY` while any archive transaction
is nonterminal; Recorder does not kill that worker or discard its temp evidence.
“Safe to remove”/“可以拔出” is printed only after both system callbacks succeed.
Busy, timeout, unmount-only, or system-refused eject is reported without data
deletion. Forced removal is not success, preserves internal source, and
reconciles on verified reinsertion. ADR-0017 is authoritative.

## Power and sleep

Mac sleep suspends user processes and network capture; closed-lid continuity is
not promised. M14 subscribes to `NSWorkspaceWillSleepNotification` and
`NSWorkspaceDidWakeNotification`, immediately records sleep start, and commits
a marked gap at wake. It also compares wall/monotonic heartbeat deltas and
labels that fallback as a suspend/clock inference.

`prevent_sleep=false` is the default. When enabled, the service owns
`/usr/bin/caffeinate -i -w <service-pid>` and releases it on shutdown; a crash
also ends the assertion with that PID. This only prevents user-idle system
sleep while policy permits. Recorder never calls `pmset`, changes persistent
settings, or promises continuity through lid-close, explicit sleep, shutdown,
battery exhaustion, or platform policy.

## Blue/green and connection rotation

ADR-0018 planned deploys run versioned old/candidate instances with independent
connections for one market. Candidate readiness requires current connections
and durably written events for all three core streams, a durably written public
REST snapshot, and synchronized market-specific order-book reconstruction.
After readiness, both instances must write fresh events before old shutdown.

Raw overlap carries deployment ID, role, reason, instance/version, connection,
and source provenance; a failed or unready candidate leaves old active. Reverse
rollback uses the same gate. Scheduled rotation invokes it at 23 h 40 min,
while the existing 23 h 50 min per-stream reconnect is a marked fallback.
launchd ownership and instance locks must accommodate only this explicit
supervised overlap.

## LaunchAgent installation and control

No service label is guessed. Set `AUTHOR_CONTROLLED_LABEL` to a reverse-DNS
label in a namespace the project author actually controls; it must end in
`.BinanceMarketDataRecorder`. Installation requires an explicit attestation and
refuses namespaces that imply Binance ownership, root, placeholder namespaces,
insecure config/data permissions, and an existing different data-root
registration.

```bash
export AUTHOR_CONTROLLED_LABEL='replace-this-with-an-author-owned-label'
scripts/install-launchagent \
  --label "$AUTHOR_CONTROLLED_LABEL" \
  --author-controls-namespace
scripts/start-recorder
scripts/status-recorder
scripts/stop-recorder
scripts/uninstall-launchagent
```

The literal placeholder above is intentionally invalid; substitute the real
author-owned reverse-DNS label. For a nondefault configuration, set
`BINANCE_MARKET_RECORDER_CONFIG_FILE` for every wrapper command.

The generated mode-0600 plist lives in `~/Library/LaunchAgents/`, runs in
`gui/<uid>`, starts at login, restarts only unsuccessful exits, throttles crash
loops, allows 60 seconds for SIGTERM, uses umask 077, and redirects stdout and
stderr under the internal `logs/` directory. `stop` bootouts the job for the
current login session; `start` bootstraps or kickstarts it; `uninstall` removes
only its selected plist and Recorder install metadata. Machine restart resumes
only after the user logs in.

The future local archive client is intended to run on macOS, Linux, and Windows.
This document covers only the implemented macOS volume/eject adapter; it does
not claim that the portable remote archive client exists.
