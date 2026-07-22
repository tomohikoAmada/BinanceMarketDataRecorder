# ADR-0001: Independent stateful Recorder repository

- Status: Accepted
- Date: 2026-07-22

## Context

The audited Alpha101Crypto repository is a compute/research engine. Its frozen
contracts assign it normalized Parquet, PIT instruments/universes, Alpha DSL,
factor diagnostics, strategy targets, execution simulation, USD-M ledger,
backtest orchestration, reports, a local API, and frontend. It explicitly is not
a trading bot and does not own live L2 capture.

Recorder requires long-lived network sessions, append-only recovery, a Catalog,
volume lifecycle handling, archive transactions, capacity emergency behavior,
blue/green deploys, and launchd operations. Those responsibilities have a
different failure domain and release cadence from research compute.

## Decision

Create Alpha101CryptoRecorder as an independent Git repository and stateful
service. It does not import or modify Alpha101Crypto. The only supported future
dependency is:

```text
Recorder output contract -> Alpha101Crypto consumer adapter
```

Recorder cannot depend on Alpha DSL, factor, strategy, BacktestRunner, account
Ledger, UI/API, or execution modules. Alpha101Crypto cannot control Recorder
internals or require knowledge of external-volume mountpoints.

## Consequences

- Each project can evolve and deploy independently.
- Storage/recovery tests do not contaminate research code.
- A versioned M16 consumer contract and example are required before integration.
- Some schema concepts may look similar; sharing happens by documented data
  format, not source imports.

## Alternatives rejected

- Add Collector modules inside Alpha101Crypto: mixes stateful infrastructure
  with compute and conflicts with its module ownership.
- Make Recorder a library controlled by BacktestRunner: reverses dependency and
  compromises continuous independent capture.
- Modify Alpha101Crypto during M0: unnecessary and violates repository scope.

## Rollback

Before production data, archive this repository and revoke the decision with a
new ADR. After data exists, repository deletion is not rollback; consumers must
retain a compatible reader/export and immutable data provenance.
