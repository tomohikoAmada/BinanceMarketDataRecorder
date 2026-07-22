# Alpha101Crypto Repository Audit

- Audit date: 2026-07-22
- Mode: read-only
- Repository: `/Users/amada/Documents/Development/Alpha101/Alpha101Crypto`
- Audited commit: `22f9dae2955acc2d9d32e68b9d565bbfb185c8a5`
- Branch at audit: `codex/frontend-api-doc` tracking
  `origin/codex/frontend-api-doc`

## Worktree preservation

The research repository already contained unrelated modified/untracked frontend
files at audit time (`frontend/package.json`, styles/views, visualization
components/scripts/utilities). Recorder M0 did not modify, stage, test, or clean
them. This baseline prevents later work from mistaking them for Recorder edits.

No `AGENTS.md` was present within the audited Alpha101 tree. The required
Recorder `AGENTS.md` was therefore bootstrapped as part of M0.

## Required files reviewed

- `README.md`
- `docs/project_contract.md`
- `docs/architecture.md`
- `docs/module_boundaries.md`
- `pyproject.toml`
- `python/crypto101/exchange/http.py`
- `python/crypto101/exchange/capabilities.py`
- `python/crypto101/exchange/snapshot_contract.py`
- `python/crypto101/exchange/snapshots.py`
- `python/crypto101/data/archive.py`
- `python/crypto101/data/normalization.py`
- `python/crypto101/data/depth_schema.py`
- `python/crypto101/data/order_book_schema.py`
- `python/crypto101/data/agg_trades.py`
- `python/crypto101/data/futures.py`
- `python/crypto101/store/binance.py`

The full set was inspected as source; large modules were navigated by their
public types/functions and relevant ownership/normalization/storage behavior.

## Findings

Alpha101Crypto's contract consistently identifies it as a Binance crypto
compute and research engine, not a live capture service or trading bot. It owns:

- migrated C++/Alpha DSL matrix compute (`S x T`);
- normalized canonical Parquet and public archive helpers;
- PIT instruments, universes, `tradable_mask`, and context matrices;
- factor diagnostics, target generation, simulated execution, USD-M ledger,
  BacktestRunner and reports;
- a local research FastAPI/frontend surface.

Its Binance modules are research-oriented:

- `exchange` provides a small REST protocol/client plus persisted exchange and
  account snapshot contracts/capability resolution. Some existing research
  paths explicitly support signed/account snapshots, which are forbidden in
  Recorder and will not be reused.
- `data` downloads public archives, normalizes trades/depth/futures side data,
  and provides quality schemas. It does not implement Recorder's durable raw
  byte envelope, long-running connection lifecycle, spool recovery, or archive
  transaction.
- `store.BinanceDataStore` queries normalized partitioned Parquet through
  DuckDB. It is a plausible future consumer shape, not a Recorder state store.
- Its optional `exchange` dependency names match the two official modular SDK
  candidates, but are unpinned and provide no Recorder capability evidence.

## Boundary conclusion

An independent, generically named Recorder repository does not conflict with
Alpha101Crypto's current contracts. It removes live, stateful,
platform-specific reliability work from the research release/failure domain.
Alpha101Crypto is only one possible external consumer. If optionally validated,
its adapter may translate generic normalized/manifests/replay contracts to
`BinanceDataStore`-style reads; the Recorder cannot expose mountpoints, import
research code, or specialize core contracts for that adapter.

Reusing code by cross-repository import is rejected. If a generic schema is
eventually shared, it must be published/versioned as a data contract or neutral
package through a separate decision.

## M0 independence evidence

- Recorder was initialized at the original distinct M0 path and its complete
  Git repository was later moved intact to
  `/Users/amada/Documents/Development/Crypto/CryptoMarketDataRecorder` by M0.1.
- No files under Alpha101Crypto were written by M0.
- Recorder contracts exclude all research, strategy, account, execution, and
  UI ownership.
- ADR-0001 makes dependency direction one-way.
