# Summary

- recover Spot and USD-M depth capture after lifecycle breaks, gaps, and
  bootstrap overflow;
- fail the service visibly when a core market terminates, while sealing the
  healthy market and preserving Catalog evidence;
- restart isolated side-data attempts with bounded backoff and expose their
  health in service status;
- add Spot exchange information and six documented USD-M five-minute public
  statistics;
- add a checksum-verified, idempotent historical importer for official
  `data.binance.vision` archives;
- add offline macOS CI, data-coverage documentation, ADRs, and a Spot bootstrap
  comparison tool.

# Safety boundaries

- public, unsigned market-data interfaces only;
- no credentials, account access, orders, trading, strategies, models, or
  backtests;
- no historical L2 fabrication and no silent mixing of historical archive time
  with live receive-clock replay;
- the Spot bootstrap documentation conflict remains open in R-034.

# Validation

- `python3.12 -m pytest -q`
- `python3.12 -m pytest -o addopts='' -m stress -q`
- explicit unsigned public M19 online probe
- `python3.12 -m ruff check .`
- `python3.12 -m mypy`
- `python3.12 tests/verify_m0_contracts.py`
- `go run tools/verify_raw_chunk_golden.go`
- `python3.12 -m build --no-isolation`
- clean virtualenv wheel install and CLI smoke
- `git diff --check`

# Remaining risks

- 72-hour and 168-hour continuous reliability acceptance has not been run;
- the official Spot local-order-book prose and official example behavior still
  disagree about the first bootstrap target;
- online smoke is deliberately opt-in and CI remains offline by default.

## M19.1 follow-up

- fixes product-specific official archive filenames and adds opt-in URL/file
  validation;
- fails immediately when any core collector returns before global stop;
- awaits USD-M side-data shutdown before Catalog close;
- adds durable per-kind 5-minute Cursors with bounded catch-up and explicit
  empty/gap semantics;
- streams historical CSV into 50,000-row Parquet batches;
- validates Range resume, ZIP/manifest/checksum, and Parquet lineage;
- records the post-bridge reliable book update ID;
- fixes the existing macOS CI workflow's pre-job context validation error.
