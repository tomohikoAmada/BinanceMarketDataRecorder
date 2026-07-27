# M20 RK3588 deployment evidence

Status: **RK3588 SHORT-TERM VALIDATION COMPLETE — GITHUB HANDOFF PENDING**

Platform classification: Ubuntu ARM64/RK3588 **Developer Preview / Soak
Candidate**, not Production Ready. Neither the 72-hour nor 168-hour soak was
run.

## Immutable baseline

| Item | Observation |
| --- | --- |
| Hostname | `opihome` |
| LAN IP | `192.168.0.118` |
| Architecture | `aarch64` |
| OS / kernel | Ubuntu Server 22.04; `6.1.43-rockchip-rk3588` |
| Repository | `/home/orangepi/BinanceMarketDataRecorder` |
| Baseline SHA | `43428a1b0784344c81b2ae0debf563dc30cdc13e` |
| Branch | `feat/m20-ubuntu-arm64-rk3588` |
| Python | locally isolated CPython `3.12.13`; system Python unchanged |
| CPU / memory | 8-core RK3588 (4x Cortex-A55, 4x Cortex-A76); 7.7 GiB |
| Internal filesystem | root ext4, approximately 56 GiB total / 51 GiB free at baseline |
| Mihomo | `1.19.29`, systemd active/enabled; configuration and secrets not read |

M19.2 was confirmed in remote `main`. HTTPS Git public read succeeded; the
initial push dry-run reported missing Git credentials without displaying a
token, private key, or credential.

## Build and offline gates

The final source tree on CPython 3.12.13 produced:

- default pytest: `435 passed, 28 skipped, 1 deselected, 5 warnings in
  32.76s`;
- explicit stress: `1 passed, 463 deselected in 197.53s`;
- Ruff: passed;
- strict MyPy: no issues in 174 source files;
- M0 standalone contracts: passed;
- Go `raw-chunk.v1` golden verifier: passed;
- Wheel and sdist build with `--no-isolation`: passed;
- `git diff --check`: passed.

The skips are explicit online opt-ins or real macOS platform tests. M20's
required public online checks were performed separately on this host; real
macOS execution remains unexecuted.

A clean Wheel-only venv imported native aarch64 installations of
`pyarrow==25.0.0`, `zstandard`, `google-crc32c`, `cbor2`,
`websockets==15.0.1`, and the official Spot/USD-M SDKs. `pyobjc` was absent.
The exact final-commit Wheel repetition is recorded during final deployment.

## Proxy and public network evidence

No API key was configured. No account/order endpoint or trading action was
used.

With every upper- and lower-case proxy environment variable removed, direct
mode behaved as a real direct transport:

- Spot REST depth failed with OS `Network unreachable`;
- Spot `exchangeInfo` and USD-M REST failed with the official SDK network
  error;
- Spot and USD-M WebSocket handshakes timed out and instrumentation confirmed
  `proxy=None`.

The failures are expected on this host's network and prove direct mode did not
fall back to the shell proxy. With explicit mode and the unauthenticated
loopback Mihomo listener:

- Spot REST depth and `exchangeInfo` returned HTTP 200;
- USD-M REST depth returned HTTP 200;
- Spot and USD-M WebSockets each received a public frame with the explicit
  proxy argument;
- a small Spot daily 1-minute Historical file for 2024-01-01 imported 1,440
  rows and verification returned one verified import.

Status exposed only `proxy_mode=explicit`, `proxy_scheme=http`,
`proxy_loopback=true`, and `proxy_port=7890`. The raw URL was not emitted in
service status, Catalog events, manifests, or ordinary logs.

## systemd and Linux paths

The final Wheel, not an editable checkout, was installed under
`/opt/binance-market-data-recorder/venv`. Configuration is root-owned,
group-readable mode `0640` under `/etc/binance-market-data-recorder`; internal
data is owned by the non-root Collector user under
`/var/lib/binance-market-data-recorder`.

The managed unit:

- is enabled and runs as the configured `orangepi` User/Group;
- uses `After` and `Wants` for `network-online.target` and `mihomo.service`,
  never `Requires`;
- uses `Restart=on-failure`, bounded `RestartSec=10s`, `UMask=0027`, SIGTERM,
  and a 90-second stop window;
- contains no proxy environment variable and opens no listener;
- passed `systemd-analyze verify`;
- produced `changed=true` then `changed=false` on repeated install.

An initial real systemd 249 rejection showed that `WorkingDirectory` does not
accept Exec-style quotes. The renderer was corrected to systemd path escaping
and covered by a static regression test.

Lifecycle evidence after the live interval:

- SIGTERM stop recorded `shutdown_reason=SIGTERM`, returned normally, changed
  Catalog from 21 ACTIVE / 631 SEALED to 0 ACTIVE / 668 SEALED;
- start recovered/audited the existing 668 sealed chunks and both markets
  returned READY under PID 24848;
- restart changed PID 24848 to 24906 and both core markets returned READY;
- two uninstalls reported `unit_removed=true` then `false`;
- before/after uninstall the Catalog SHA-256
  `23cc4463389a1843399cc5f1524cb956909359a0202fced30781a6062509447a`,
  file count 1,435, and directory byte count 38,985,576 were identical;
- uninstall left the unit absent/inactive and did not remove data.

## Live concurrent collection

The production configuration uses a bounded
`ingress_queue_capacity=262144` for both WebSocket receipt and Raw spool
queues. Raw time seals are deterministically phase-staggered per market/stream.
The generic default remains 8,192.

Real eMMC seal pressure first exposed a USD-M bookTicker receipt overflow.
The service failed closed, recorded the terminal failure, exited nonzero, and
was restarted by systemd; it did not silently drop. Review found that the
configured capacity had reached only the spool layer and that all streams
sealed together. Both causes were corrected before starting the final
candidate at `2026-07-27T10:26:10Z`.

All six five-minute USD-M public statistics routes accepted a completed period
after their start boundary was moved one additional closed period inside the
published rolling retention window. Spot and USD-M core streams reached READY,
REST snapshots succeeded, Raw chunks sealed, and Catalog reconciliation
remained available.

At `2026-07-27T10:56:51Z`, after 30 minutes 41 seconds:

- PID 23454 was unchanged and systemd `NRestarts=0`;
- Spot and USD-M were both READY with synchronized order books and all three
  core streams connected;
- all six five-minute statistics kinds had accepted seven observations;
- Catalog contained 631 SEALED and 21 ACTIVE chunks;
- the candidate interval added only its `SERVICE_STARTED` operational event:
  no terminal failure, queue overflow, gap, or resync occurred;
- RSS was 222,142,464 bytes (peak equal), after a rise during startup and a
  long plateau around 211 MiB; this is only short-term evidence.

The stored aggregate network status at the final instant was `DEGRADED` even
though both core markets were READY. Review found that hourly
`exchangeInfo`/`fundingInfo` tasks used a fixed 900-second stale threshold
before their next scheduled poll. The final code uses each task's expected
interval plus the 900-second failure grace and adds a regression test. An
immediate failure such as `RETRYING` still degrades status. This status-only
fix changes no captured data.

## Mihomo fault injection

Exactly one operator-authorized `systemctl restart mihomo` was executed at
`2026-07-27T10:04:14Z`; no node selection or Mihomo configuration was changed.
All eight active public WebSockets disconnected visibly. Both Spot and USD-M
recorded `DEPTH_RESYNC_REQUESTED`, fetched new REST snapshots, then recorded
`DEPTH_RESYNC_COMPLETED` with new connection IDs.

The simultaneous reconnect/seal workload also triggered the bounded USD-M
overflow described above. It became a visible terminal fault and systemd
process restart (`NRestarts=1`), followed by both markets returning to READY.
Those immutable observed Catalog records predate the final evidence-field
change and are not rewritten. The final code makes future gap/resync events
classify the bounded interval as `UNRELIABLE` and record both its start and
completion; no claim is made that the interrupted interval is complete.

## Mounted storage

Real `findmnt` nesting initially caused the internal eMMC root device to be
misclassified as hotplug external media. Recursive mount parsing plus backing
device lineage exclusion corrected the result to
`external_volume_count=0`. Offline fixtures cover nested mounts, registration,
disappearance, and internal-source retention.

No physical external disk was attached, so no real external filesystem
registration or sudden removal was performed. Recorder did not mount, unmount,
format, repair, partition, or create udev rules. With no trusted udisks
capability, safe removal remains report-only.

## Explicit non-evidence

- No real macOS M20 execution was possible on the RK3588; simulated macOS
  direct/launchd regressions are part of the offline suite.
- No physical external storage exercise was possible.
- No TUN, redirect, TProxy, iptables/nftables, firewall, routing, Mihomo
  configuration, node selection, subscription/auth/controller secret, API
  key, account endpoint, order, or trade was accessed or changed.
- No disk was formatted, repaired, partitioned, mounted, or unmounted.
- No 72-hour or 168-hour run was performed.
- No Production Ready or zero-interruption conclusion is permitted.
