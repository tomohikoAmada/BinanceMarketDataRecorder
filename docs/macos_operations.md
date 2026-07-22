# macOS Operations Contract

## Deployment model

The certified V1 service is a user LaunchAgent on macOS Apple Silicon, Python
3.12, requiring the user to be logged in. Installation does not require root
and does not silently install a LaunchDaemon. Logs and structured state live
under the application support root, not the repository.

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

PyObjC or a minimal native helper may be selected only after M9 capability
tests. A polling fallback cannot be silently treated as equivalent if it misses
required lifecycle semantics. Fixed `/Volumes/<name>` identity is forbidden.

Unregistered volumes are display-only. The service writes only inside an
explicitly registered folder and places the marker there, never at disk root.
No format, partition, mount-mode, filesystem repair, or SMART operation is
performed.

## Required CLI surface

By M9/M12 the CLI includes:

```text
recorder storage list
recorder storage inspect <path>
recorder storage register <folder-path>
recorder storage unregister <storage-id>
recorder storage status
recorder storage eject <storage-id>
recorder storage forecast
recorder archive status
recorder archive retry
recorder archive verify <storage-id>
```

Registration is an explicit user action. Unregistering stops future use and
does not erase archived data.

## Eject protocol

An eject request sets `EJECT_PENDING`, prevents new archive allocation, waits
for or safely cancels current work, completes/rolls back the archive transaction,
fsyncs, closes handles, then asks Disk Arbitration to unmount/eject. “Safe to
remove” is printed only after system success. Busy or system-refused eject is
reported without data deletion. Forced removal preserves internal source and
reconciles temp/Catalog state on reinsertion.

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
