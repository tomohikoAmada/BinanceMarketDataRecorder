# Developer Guide

This guide maps the implementation merged in PR #78 to the code and tests.
For deployment status, read [current production state](CURRENT_PRODUCTION_STATE.md).

## Environment and checks

Python `>=3.12,<3.13`; distribution `binance-market-data-recorder`;
package `binance_market_data_recorder`; CLI `binance-market-recorder`.

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'
python -m pytest -q
python -m ruff check .
python -m mypy
python tests/verify_m0_contracts.py
go run tools/verify_raw_chunk_golden.go
```

Platform runtime and CI dependency locks are in `requirements/`.
`.github/workflows/ci.yml` runs Ubuntu x86_64 and macOS Python 3.12, package
builds, fresh-wheel smoke checks, and deployment-identity fixtures. Online
smoke is separate. In a sibling checkout, M0's documented CI override is
`CI=true M0_CONTRACT_ROOT=<absolute-checkout-path>`; normal canonical-checkout
checks need no override. Use a temporary data root for tests.

The exact merged code passed 1,838 offline tests (24 skipped, 4 deselected),
Ruff, strict mypy, M0, Go Raw golden, locked build and clean-wheel checks.
[CI run 36742042348](https://github.com/tomohikoAmada/BinanceMarketDataRecorder/actions/runs/36742042348)
passed on both platforms. Those are recorded prior code gates; this documentation
change does not claim a new full-suite run.

## Code map

Paths below are relative to `src/binance_market_data_recorder/`.

| Area | Entry points | Responsibility |
|---|---|---|
| Configuration and CLI | `config.py`, `paths.py`, `cli.py` | Product selection, profiles, path validation, command routing |
| Service assembly | `service/runtime.py`, `service/state.py` | One process, configured collectors, lifecycle and service state |
| Public transport/schema | `binance/spot/`, `binance/usdm/`, `network/` | Market-specific schemas, SDK REST and WebSocket transport, proxy policy |
| Collection | `collector/spot.py`, `collector/usdm.py`, `collector/resync.py`, `collector/supervisor.py` | Receive evidence, bounded ingress, core reconnect/resync and handoff |
| Auxiliary capture | `collector/spot_side_data.py`, `collector/usdm_side_data.py` | Product-specific and shared public metadata/periodic datasets |
| Raw persistence | `spool/` | CBOR frames, CRC32C, writes, rotation, compression, seal/recovery |
| Durable metadata | `storage/catalog.py`, `storage/layout.py`, `spool/seal.py` | SQLite lifecycle, paths, manifests and archive state |
| Order books | `orderbook/` | Spot `U/u`, USD-M `U/u/pu`, bootstrap and checkpoints |
| Archive | `archive/manager.py`, `archive/remote_*.py`, `archive/archive_set.py` | Verified copy/retirement, receipts, remote transfer and custody |
| Derived data | `normalize/`, `replay/`, `backfill/` | Parquet, deterministic consumer replay, official historical imports |
| Operations | `metrics/`, `storage/forecast.py`, `service/launchd.py`, `service/systemd.py` | Reports, capacity and platform service control |
| Acceptance | `service/acceptance.py`, `service/acceptance_v5_*.py` | Historical schema routing and V5 evidence |

## Runtime and storage

`ProductKey = (market, symbol)`. A single process assembles one Collector per
configured product. Empty markets create no collectors or network traffic.
Shared USD-M REST/global side data is not duplicated for every product.

Capture produces original bytes plus provenance, then writes framed Raw to
`data/active/*.partial`. Sealing publishes compressed immutable Raw, a manifest,
and Catalog lifecycle transitions. Normal live-owned clean writers use M23.4's
incremental evidence; crash/recovered/unknown partials retain full scan authority.
Archive verifies its copy before source retirement. Normalization and replay
consume the published data; consumer projects do not become Recorder dependencies.

StorageLayout derives `data/active`, `data/sealed`, `data/manifests`,
`data/checkpoints`, `data/quarantine`, and `state/catalog.sqlite` below the
configured root. Do not confuse `data/active` with a top-level `active/` folder.

Service collection uses the internal `_service run` entry point managed by
launchd/systemd; there is no public `run` command. The global `--config` option
precedes the subcommand. For Linux commands under `runuser`, use an accessible
working directory such as the service data root, rather than `/root`.

## V5 acceptance flow

```text
exact identity -> stopped baseline + independent verification
  -> Recorder start + fresh readiness
  -> timed online stage -> immutable target
  -> operator stop/disable + archive drain/quiescence
  -> terminal full audit + independent LIVE Raw verification
  -> final publication -> completed historical verification
```

| Module | Owns |
|---|---|
| `acceptance_v5_delta.py` | Three durable cursors, bounded SQLite snapshot pages and causal companions |
| `acceptance_v5_online.py` | Predecessor binding, T0, samples, continuation replay and target |
| `acceptance_v5_reconnect.py` | Reconnect/discontinuity continuity |
| `acceptance_v5_corpus.py` | Baseline migration, stopped certificate, Catalog backup, private manifest freeze |
| `acceptance_v5_audit.py` | Deterministic full control/Raw/archive qualification |
| `acceptance_v5_raw.py` | Cancellable worker and forward-progress watchdog |
| `acceptance_v5_io.py` | Canonical immutable documents and bounded chained shards |
| `acceptance_v5_finalize.py` | Baseline/finalize orchestration and independent reconstruction |

The first 2h stage consumes a passing baseline `audit-root.json`; later stages
consume the previous V5 `stage-final.json`. Baseline high-water cursors and
continuation state let ordinary samples inspect changes without rereading all
historical manifests. Prepublication verification still reads LIVE Raw.
Completed verification uses frozen controls/proofs, so later authorized archive
retirement does not invalidate completed evidence. A producer summary alone is
not sufficient for publication or eligibility.

Limits: 300s samples, 600s evidence gap, 240s delta work budget, 900s global
recoverable-readiness episode, 256-row pages and 256 pending causal references.
Full audit has 60s progress publication and a separate 900s no-progress watchdog;
there is no total audit-duration cap. Audit duration earns no Formal credit.
A legitimate active partial is expected online; stopped quiescence requires zero.
Read the [full V5 contract](acceptance_evidence_v5.md) before changing these paths.

CLI reference (placeholders describe the interface, not a scheduled live run):

```text
deployment acceptance identity --expected-source-git-sha <sha> --evidence-root <root>
deployment acceptance baseline --evidence-root <root>
deployment acceptance verify --baseline --evidence-root <root>
deployment acceptance readiness --identity-evidence <identity.json> --evidence-root <root>
deployment acceptance stage --stage 2h --previous-evidence <baseline/audit-root.json> --evidence-root <parent>
deployment acceptance finalize --evidence-root <stage-root>
deployment acceptance verify --evidence-root <stage-root>
```

## Focused tests and remaining work

V5 fixtures are in `tests/v5_support.py`; unit tests are
`tests/unit/test_acceptance_v5_*.py`; integration tests cover finalize,
adversarial corruption and actual ArchiveManager retirement. Readiness tests,
Raw golden verification, and historical acceptance compatibility accompany them.
For a V5 change, a useful focused selection is:

```bash
python -m pytest -q tests/unit/test_acceptance_v5_*.py tests/integration/test_acceptance_v5_*.py
```

The implementation is merged and reviewed. Production qualification is paused
before baseline completion and before Formal T0. Full-corpus audit practicality
remains an open P2; partial progress does not establish complete integrity or
terminal performance. Use [handoff](PROJECT_HANDOFF.md) for the next task.
