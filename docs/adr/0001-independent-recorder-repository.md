# ADR-0001: Independent stateful market-data Recorder repository

- Status: Accepted
- Date: 2026-07-22

M0.2 scope note: ADR-0007 freezes this independent repository as a
Binance-specific, unofficial market-data Recorder. Independence from consumers
remains unchanged; multi-exchange scope is not implied.

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

Create an independently versioned market-data Recorder Git repository and
stateful service. It does not import or modify consumer projects. The only
supported future dependency direction is:

```text
Recorder generic output/replay contract -> arbitrary consumer adapter
```

Recorder cannot depend on any consumer's DSL, factor, strategy, backtest,
account ledger, UI/API, monitoring, or execution modules. No consumer can
control Recorder internals or require knowledge of external-volume mountpoints.
Alpha101Crypto is the historical audit object and one possible ordinary
consumer, not the target of a specialized contract.

## Consequences

- Each project can evolve and deploy independently.
- Storage/recovery tests do not contaminate research code.
- A versioned generic M16 consumer contract and independent example are
  required; any named consumer validation is optional.
- Some schema concepts may look similar; sharing happens by documented data
  format, not source imports.

## Alternatives rejected

- Add Collector modules inside Alpha101Crypto: mixes stateful infrastructure
  with compute and conflicts with its module ownership.
- Make Recorder a library controlled by any BacktestRunner: reverses dependency
  and compromises continuous independent capture.
- Modify Alpha101Crypto during M0: unnecessary and violates repository scope.

## Rollback

Before production data, archive this repository and revoke the decision with a
new ADR. After data exists, repository deletion is not rollback; consumers must
retain a compatible reader/export and immutable data provenance.
