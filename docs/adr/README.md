# Architecture Decision Records

Current and historical decisions:

- [ADR-0001: Independent stateful Recorder repository](0001-independent-recorder-repository.md)
- [ADR-0002: Framed raw chunk format](0002-framed-raw-chunk-format.md)
- [ADR-0003: Registered-directory external archive](0003-registered-directory-archive.md)
- [ADR-0004: Clock and deterministic replay semantics](0004-clock-and-replay-semantics.md)
- [ADR-0005: Defer Binance transports until capability evidence — superseded](0005-binance-transport-evidence-gate.md)
- [ADR-0006: Project identity and workspace — superseded](ADR-0006-project-identity-and-workspace.md)
- [ADR-0007: Binance-scoped project identity](ADR-0007-binance-scoped-project-identity.md)
- [ADR-0008: Official modular SDKs for public REST snapshots](0008-official-sdk-rest-transport.md)
- [ADR-0009: Generic WebSocket client for Binance market streams](0009-websocket-transport.md)
- [ADR-0010: Raw chunk v1 byte format and crash lifecycle](0010-raw-chunk-v1-byte-format.md)
- [ADR-0011: Market-specific order-book reconstruction and checkpoints](0011-orderbook-reconstruction-and-checkpoints.md)
- [ADR-0012: USD-M side-data isolation and semantics](0012-usdm-side-data-isolation-and-semantics.md)
- [ADR-0013: Idempotent operational metrics and UTC daily reports](0013-idempotent-operational-metrics-and-daily-reports.md)
- [ADR-0014: macOS volume discovery and registered-directory readiness](0014-macos-volume-discovery-and-registration.md)
- [ADR-0015: Crash-reconcilable archive transaction](0015-crash-reconcilable-archive-transaction.md)
- [ADR-0016: Capacity forecast and emergency reserve](0016-capacity-forecast-and-emergency-reserve.md)
- [ADR-0017: Non-forced macOS safe eject](0017-non-forced-macos-safe-eject.md)
- [ADR-0018: Readiness-gated blue/green Collector handoff](0018-readiness-gated-blue-green-handoff.md)
- [ADR-0019: User LaunchAgent and power-aware service lifecycle](0019-user-launchagent-and-power-lifecycle.md)
- [ADR-0020: Content-addressed normalized Parquet datasets](0020-content-addressed-normalized-parquet.md)
- [ADR-0021: Deterministic replay and generic consumer boundary](0021-deterministic-replay-and-consumer-boundary.md)

ADRs are immutable after acceptance. Superseding decisions add a new ADR and
link both records.
