# ADR-0019: User LaunchAgent and power-aware service lifecycle

- Status: Accepted
- Date: 2026-07-23
- Milestone: M14

## Context

V1 must run continuously on macOS Apple Silicon while the user is logged in,
restart after a process crash, seal Raw on SIGTERM, prevent accidental
multi-process writers, expose honest local status, and mark sleep/wake
continuity risk. It must not install a root LaunchDaemon, change permanent
power settings, imply Binance ownership through a service label, or promise
capture while a MacBook is asleep or closed.

The installed macOS 26.5 SDK and local system manuals establish the platform
semantics used here:

- `launchctl bootstrap`, `bootout`, `kickstart`, `kill`, and `print` operate on
  the logged-in `gui/<uid>` domain;
- `launchd.plist(5)` requires a unique `Label`, supports
  `ProgramArguments`, `RunAtLoad`, `KeepAlive`, `ThrottleInterval`,
  `ExitTimeOut`, `StandardOutPath`, `StandardErrorPath`, `ProcessType`, and
  `Umask`;
- AppKit exports `NSWorkspaceWillSleepNotification` and
  `NSWorkspaceDidWakeNotification`; and
- `/usr/bin/caffeinate` supports an idle-sleep assertion tied to a process by
  `-i -w <pid>`.

ADR-0007 requires an author-controlled service namespace but intentionally does
not guess it. No verified author domain is present in the repository.

## Decision

### Service identity and installation

The project does not ship a guessed fixed reverse-DNS label. Installation
requires `--label` plus `--author-controls-namespace`. The label must:

- be a reverse-DNS name with at least three components;
- end in `.BinanceMarketDataRecorder`;
- not use a namespace that implies Binance organization ownership; and
- not contain placeholder components such as `example`, `invalid`,
  `localhost`, or `changeme`.

The acknowledgement is an explicit operator assertion; Recorder cannot prove
domain ownership. The selected label and plist path are stored in
`state/launchagent.json`. A different label cannot take over a data root while
that registration exists. No default silently claims another party's namespace.

Installation generates
`~/Library/LaunchAgents/<author-label>.plist`, mode `0600`, and bootstraps it
only into `gui/<uid>`. Root is refused. The plist executes the absolute Python
interpreter with:

```text
python -m binance_market_data_recorder _service run
```

It freezes the effective credential-free Recorder configuration into approved
environment keys, optionally passes the secure config file, injects Git
revision metadata, uses the internal data root as working directory, and sends
stdout/stderr to `logs/launchd.stdout.log` and
`logs/launchd.stderr.log`.

`RunAtLoad=true` starts after user login. `KeepAlive.SuccessfulExit=false`
restarts an unsuccessful/crashed process without restarting an intentional
zero exit. `ThrottleInterval=10` bounds crash loops; `ExitTimeOut=60` gives
SIGTERM sealing time; `ProcessType=Standard` avoids falsely classifying the
recorder as interactive; and `Umask=077` protects runtime files. A successful
install is atomic with respect to newly created plist/bootstrap: bootstrap
failure removes the new plist. Start uses `bootstrap` when unloaded and
`kickstart` when loaded. Stop uses `bootout`; uninstall bootouts then removes
only the selected plist and Recorder metadata.

The wrapper scripts under `scripts/` call these same CLI operations. They do
not use legacy `launchctl load/unload`, sudo, a LaunchDaemon, or shell parsing
of service state.

### Runtime ownership and shutdown

The service holds a nonblocking `flock` on `state/service.lock` for its complete
lifetime. A second operating-system process for the same internal root exits
without opening Collectors. ADR-0018 old/candidate overlap remains valid
because both Collector instances are supervised inside the single lock-owning
service process.

Startup runs M3 Raw recovery before opening network collectors, creates
independent Spot and USD-M workers, and records `SERVICE_STARTED`. One market
failure remains isolated. If no core market worker remains, the process records
`SERVICE_FAILED` and exits nonzero so launchd may restart it.

SIGTERM and SIGINT set the common graceful-stop event. Each Collector drains and
seals its spools and commits metrics/reports before the service records
`SERVICE_STOPPED`, writes final state, releases power resources, and unlocks.
SIGKILL cannot run cleanup; the kernel releases the lock and the next launchd
instance performs M3 recovery.

`state/service_state.json` is an atomic, fsynced `service-state.v1` heartbeat
containing PID, service/collector identity, market readiness, network state,
runtime resource evidence, power state, recovery count, and last sleep gap.
The CLI reports `RUNNING` only when the schema is valid, PID exists, and
heartbeat is fresh. A stale or malformed file never fabricates health.

### Sleep and power

A private PyObjC run-loop thread subscribes to the two official NSWorkspace
sleep notifications. `will_sleep` immediately records `SYSTEM_SLEEP_BEGIN`;
`did_wake` records a `SYSTEM_SLEEP_GAP` interval. A second detector compares
UTC wall-clock and monotonic deltas each heartbeat. A sufficiently large
wall-only discontinuity records the same explicit gap contract, labelled as a
clock/suspend inference rather than certain sleep. Overlapping evidence is
deduplicated.

`prevent_sleep=false` is the default. When explicitly enabled, Recorder starts
`/usr/bin/caffeinate -i -w <service-pid>`. This is a scoped user-idle system
sleep assertion only. Graceful shutdown terminates it; after a crash, `-w`
ends when the service PID disappears and launchd's process-group cleanup is an
additional boundary. Recorder never calls `pmset`, modifies persistent power
preferences, or claims that this defeats lid-close, explicit sleep, battery
exhaustion, shutdown, or platform policy.

## Consequences

- The service starts automatically only after the user logs in; it is not a
  pre-login/root daemon.
- Crash restart is launchd-controlled and throttled. A complete machine reboot
  still requires the next user login.
- Configuration files and data/log/state directories must be owned by the
  current user with no group/other access before installation.
- Platform process integration is testable without Binance by using local fake
  collectors; online capture and long soak remain explicit M17 evidence.
- The already resolved Cocoa wheel becomes a direct project dependency because
  NSWorkspace is now a project API; the certified resolved wheel set is
  unchanged. launchctl, plistlib, flock, and caffeinate use Python/macOS
  facilities already present.

## Alternatives rejected

- Guess a label such as an invented author domain: violates ADR-0007.
- Use a Binance-owned-looking reverse-DNS root: falsely implies official
  ownership.
- Install a root LaunchDaemon: exceeds V1 privilege and logged-in-user scope.
- Use only a PID file: stale PID files and PID reuse are weaker than a
  kernel-held lock.
- Set `KeepAlive=true`: would also restart intentional successful exits and can
  produce an avoidable restart loop.
- Use `pmset` or permanent sleep changes: mutates user/system policy.
- Promise closed-lid continuity: macOS hardware and power policy do not support
  that guarantee.

## Rollback

Run the uninstall operation for the recorded label, which bootouts the user
service and removes only its plist and `launchagent.json`. If running manually,
send SIGTERM and wait for `STOPPED`/sealed Raw before reverting M14 code. Stop
and remove the scoped caffeinate child if present. Preserve
`service_state.json`, service/sleep operational events, Raw, Catalog, metrics,
manifests, archive registrations, and every data artifact. Rollback never
changes power settings because M14 never persists any.
