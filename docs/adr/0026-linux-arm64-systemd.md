# ADR-0026: Ubuntu ARM64 paths, systemd, and mounted-directory storage

Status: accepted for M20 Developer Preview / Soak Candidate.

## Context

The original service and storage adapters were intentionally macOS-specific:
LaunchAgent, NSWorkspace/caffeinate, Disk Arbitration, `/Volumes`, and the
Application Support default. M20 must run the same Recorder and byte contracts
on Ubuntu ARM64/RK3588 without changing the macOS behavior, replacing the
system Python, or granting the Collector root privileges.

## Decision

- Python remains `>=3.12,<3.13`. Ubuntu may install an independently built or
  vendor-provided Python 3.12, but never replace `/usr/bin/python3`.
- Interactive Linux data defaults to
  `~/.local/share/BinanceMarketDataRecorder`. The system service uses
  `/var/lib/binance-market-data-recorder`; configuration uses
  `/etc/binance-market-data-recorder/recorder.toml`.
- `pyobjc` remains guarded by Darwin package markers. Linux chooses a no-op
  platform sleep observer while retaining wall/monotonic discontinuity gap
  detection. `prevent_sleep=true` remains macOS-only and fails visibly on
  Linux.
- `binance-market-recorder systemd` installs and controls
  `binance-market-data-recorder.service`. Install requires explicit non-root
  User and Group. The unit wants and starts after `network-online.target` and
  `mihomo.service`, but does not require Mihomo. It uses
  `Restart=on-failure`, bounded `RestartSec`, `UMask=0027`, SIGTERM, and a
  90-second graceful-stop window. Standard error/stdout flow to journald.
- The unit carries no shell proxy variables. Production proxy behavior comes
  only from the explicit TOML configuration. Uninstall removes the managed
  unit but no data or configuration.
- Linux volume discovery reads `/proc/self/mountinfo` and corroborates it with
  `findmnt --json` and `lsblk --json`. Only already-mounted block filesystems
  with external/removable/hotplug evidence are candidates. Source device,
  mountpoint, filesystem type, filesystem UUID when available, capacity,
  writability, marker, and `storage_id` remain visible.
- A reliable filesystem UUID is required to register. The Recorder never
  mounts, unmounts, formats, repairs, repartitions, creates udev rules, or
  changes firewall/routing state. Without a reliable automatic eject backend,
  Linux returns `MANUAL_ACTION_REQUIRED` and never claims `SAFE_TO_REMOVE`.

The internal root remains the only active ingestion target. A missing external
directory does not stop collection. Archive failure preserves the internal
sealed source.

M20 does not certify Linux blue/green deployment. Updates use a stop/seal,
Wheel replacement, daemon-reload, start, readiness-check sequence; rollback
installs the previous Wheel and restarts. Long-run blue/green and 72/168-hour
proof are deferred.

## Consequences

macOS LaunchAgent, Disk Arbitration, and caffeinate tests remain supported and
run as simulated regressions on Linux; real macOS platform proof is still
separate. Ubuntu ARM64 is a Developer Preview / Soak Candidate, not Production
Ready. The unit may start before Mihomo becomes healthy because proxy recovery
belongs to the Recorder's visible reconnect/resync lifecycle.

## Rollback

Stop the unit and allow SIGTERM sealing, disable/remove only the managed unit,
install the prior Wheel into the production venv, restore the previous
configuration, daemon-reload, and start. Do not remove `/var/lib` or alter any
mounted filesystem.
