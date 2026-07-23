# macOS Operations Contract

## Deployment model

The certified V1 service is a user LaunchAgent on macOS Apple Silicon, Python
3.12, requiring the user to be logged in. Installation does not require root
and does not silently install a LaunchDaemon. Future launchd, service, log, and
configuration identifiers derive from `BinanceMarketDataRecorder`, never a
consumer name or a namespace implying Binance ownership. The final reverse-DNS
namespace must be controlled by the project author and is intentionally not
guessed in M0.2. Logs and structured state live under
`~/Library/Application Support/BinanceMarketDataRecorder/`, not the repository
or `/Users/amada/Documents/Development/Crypto`.

M14 will provide install, uninstall, start, stop, and status scripts; automatic
restart; SIGTERM sealing; configuration permission checks; and a single-instance
lock. M0 provides no runnable service.

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
not promised. The service detects wall-clock versus monotonic discontinuities
where possible and records a marked gap across sleep/wake. An opt-in
prevent-sleep mode may use a scoped system assertion while recording; it does
not permanently change the user's power settings and cannot promise closed-lid
operation.

## Blue/green and connection rotation

Planned deploys run versioned old/candidate instances with independent
connections. Candidate must be healthy and order-book synchronized before old
stops. Raw overlap is allowed and tagged; a failed candidate leaves old active.
The same handoff is used for planned 24-hour stream rotation. launchd ownership
and instance locks must accommodate only this explicit supervised overlap.
