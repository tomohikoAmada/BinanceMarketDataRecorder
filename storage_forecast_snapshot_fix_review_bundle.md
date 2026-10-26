# Storage forecast snapshot fix review bundle

## Live authority

Remote authority was rechecked after `git fetch origin` and after the final
push.

```text
REMOTE_MAIN_SHA=138e3557d19b966f04da9f22cfda96144e86aa7e
REMOTE_MAIN_TREE=f21736d5e253385383c0a265bd508a981b907d8e
REMOTE_FIRST_CANDIDATE_SHA=cdeb895f55b85a8d2db63e8a1d85f3abc56e46ad
REMOTE_FIRST_CANDIDATE_TREE=b321276aa5195f2a0ad5727a603b6c879a1ecc5a
REMOTE_FINAL_CANDIDATE_SHA=7fa4ecc5720c3c33beabdf135dd39bfe4f3da0b5
```

`origin/main` and the first candidate matched the supplied authority before
editing. The final remote branch check returned:

```text
138e3557d19b966f04da9f22cfda96144e86aa7e  refs/heads/main
7fa4ecc5720c3c33beabdf135dd39bfe4f3da0b5  refs/heads/perf/bounded-storage-forecast
```

## Initial local state and safe branch switch

```text
INITIAL_LOCAL_BRANCH=docs/hardening-handoff-refresh
INITIAL_LOCAL_HEAD=7610113821832171c971c8c43c29938919a1f524
INITIAL_LOCAL_SUBJECT=docs: refresh current state and project handoff
INITIAL_LOCAL_WORKTREE_CLEAN=yes
```

`git status --porcelain=v1` was empty before switching. `git fetch origin`
completed normally with no authority changes.

The local candidate branch already existed. It was verified before switching:

```text
LOCAL_CANDIDATE_SHA=cdeb895f55b85a8d2db63e8a1d85f3abc56e46ad
ORIGIN_CANDIDATE_SHA=cdeb895f55b85a8d2db63e8a1d85f3abc56e46ad
LOCAL_CANDIDATE_TREE=b321276aa5195f2a0ad5727a603b6c879a1ecc5a
LOCAL_CANDIDATE_PARENT=138e3557d19b966f04da9f22cfda96144e86aa7e
LOCAL_REMOTE_DIVERGENCE=0 0
```

Git initially reported the candidate branch in the clean duplicate worktree
`/private/tmp/bmdr-bounded-storage-forecast`. That worktree was safely
detached at the same candidate SHA with ordinary `git switch --detach`; it was
not deleted. The requested repository path was then switched with ordinary
`git switch perf/bounded-storage-forecast`.

Required state before editing:

```text
CURRENT_BRANCH_BEFORE_EDIT=perf/bounded-storage-forecast
HEAD_BEFORE_EDIT=cdeb895f55b85a8d2db63e8a1d85f3abc56e46ad
ORIGIN_CANDIDATE_BEFORE_EDIT=cdeb895f55b85a8d2db63e8a1d85f3abc56e46ad
HEAD_PARENT_BEFORE_EDIT=138e3557d19b966f04da9f22cfda96144e86aa7e
WORKTREE_BEFORE_EDIT_CLEAN=yes
```

The old `docs/hardening-handoff-refresh` branch and all other branches were
preserved.

## Final identities and history

```text
BASE_MAIN_SHA=138e3557d19b966f04da9f22cfda96144e86aa7e
BASE_MAIN_TREE=f21736d5e253385383c0a265bd508a981b907d8e
FIRST_CANDIDATE_SHA=cdeb895f55b85a8d2db63e8a1d85f3abc56e46ad
FIRST_CANDIDATE_TREE=b321276aa5195f2a0ad5727a603b6c879a1ecc5a
SECOND_COMMIT_SHA=7fa4ecc5720c3c33beabdf135dd39bfe4f3da0b5
FINAL_HEAD_SHA=7fa4ecc5720c3c33beabdf135dd39bfe4f3da0b5
FINAL_HEAD_TREE=a4a1f4a145ca6aef98b61b82b47321484e84803b
SECOND_COMMIT_PARENT=cdeb895f55b85a8d2db63e8a1d85f3abc56e46ad
SECOND_COMMIT_SUBJECT=fix: pin storage forecast read snapshot
```

`git log --oneline origin/main..HEAD`:

```text
7fa4ecc (HEAD -> perf/bounded-storage-forecast, origin/perf/bounded-storage-forecast) fix: pin storage forecast read snapshot
cdeb895 perf: bound storage forecast history reads
```

## Second-commit diff

Second-commit diff stat:

```text
src/binance_market_data_recorder/storage/catalog.py       |  77 +++++++
src/binance_market_data_recorder/storage/forecast.py      |  13 +-
tests/unit/test_storage_forecast.py                      | 223 ++++++++++++++++++---
3 files changed, 279 insertions(+), 34 deletions(-)
```

The production diff adds the explicit Catalog domain read-set below and
replaces the four independently committed forecast reads with one tuple
acquisition. The test diff adapts the bounded-read probe and adds the
deterministic writer/snapshot and rollback tests.

## Cumulative branch diff

```text
src/binance_market_data_recorder/storage/catalog.py       | 149 +++++
src/binance_market_data_recorder/storage/forecast.py      |  31 +-
tests/unit/test_storage_forecast.py                      | 648 +++++++++++++++++++++
3 files changed, 811 insertions(+), 17 deletions(-)
```

Cumulative changed files are exactly:

```text
src/binance_market_data_recorder/storage/catalog.py
src/binance_market_data_recorder/storage/forecast.py
tests/unit/test_storage_forecast.py
```

No schema file, migration, index, Raw/writer/archive/order-book/BBO/network,
metrics, JSON, Decimal, cadence, threshold, or capacity-safety file changed.

## Final Catalog forecast read-set implementation

`Catalog.space_forecast_read_set(scope_id, start_exclusive_utc_ns,
end_inclusive_utc_ns)` now performs the following while holding
`Catalog._lock`:

```python
with self._lock:
    self._connection.execute("BEGIN")
    try:
        sample_rows = self._connection.execute(
            """
            SELECT * FROM storage_space_samples
            WHERE scope_id = ?
              AND observed_at_utc_ns > ?
              AND observed_at_utc_ns <= ?
            ORDER BY observed_at_utc_ns, sample_id
            """,
            (scope_id, start_exclusive_utc_ns, end_inclusive_utc_ns),
        ).fetchall()
        predecessor_row = self._connection.execute(
            """
            SELECT * FROM storage_space_samples
            WHERE scope_id = ? AND observed_at_utc_ns <= ?
            ORDER BY observed_at_utc_ns DESC, sample_id DESC
            LIMIT 1
            """,
            (scope_id, start_exclusive_utc_ns),
        ).fetchone()
        current_row = self._connection.execute(
            """
            SELECT * FROM storage_space_samples
            WHERE scope_id = ? AND observed_at_utc_ns <= ?
            ORDER BY observed_at_utc_ns DESC, sample_id ASC
            LIMIT 1
            """,
            (scope_id, end_inclusive_utc_ns),
        ).fetchone()
        count_row = self._connection.execute(
            """
            SELECT COUNT(*) AS sample_count
            FROM storage_space_samples
            WHERE scope_id = ?
            """,
            (scope_id,),
        ).fetchone()
        if count_row is None:
            raise CatalogStateError("storage space sample count is unavailable")
        read_set = (
            [dict(row) for row in sample_rows],
            dict(predecessor_row) if predecessor_row is not None else None,
            dict(current_row) if current_row is not None else None,
            int(count_row["sample_count"]),
        )
    except BaseException:
        if self._connection.in_transaction:
            with suppress(sqlite3.Error):
                self._connection.execute("ROLLBACK")
        raise
    else:
        self._connection.execute("COMMIT")
return read_set
```

The first bounded `SELECT` is the first read after deferred `BEGIN`, pinning
the WAL snapshot. All four results are materialized before the method returns;
the transaction is committed before `StorageForecaster` performs any Python
window, median, ETA, or capacity work. Existing individual Catalog helpers
remain available and their accepted ordering semantics are unchanged.

## Final forecast acquisition

The final `StorageForecaster.forecast()` acquisition is:

```python
now = self.utc_clock_ns() if now_utc_ns is None else now_utc_ns
max_window_seconds = max(WINDOWS_SECONDS.values())
max_cutoff = now - max_window_seconds * 1_000_000_000
(
    samples,
    predecessor,
    current,
    sample_count,
) = self.catalog.space_forecast_read_set(
    scope_id,
    start_exclusive_utc_ns=max_cutoff,
    end_inclusive_utc_ns=now,
)
if current is None:
    return {
        "scope_id": scope_id,
        "status": "INSUFFICIENT_DATA",
        "sample_count": 0,
    }
rate_samples = ([predecessor] if predecessor is not None else []) + samples
```

Python rate-window selection, median, ETA, capacity decision, archive backlog
and oldest-unarchived handling remain outside the SQLite transaction. Successful
forecasts use the read-set `sample_count`, preserving all historical rows
including future rows; no-current results still expose `sample_count=0`.

## New deterministic concurrency and snapshot tests

New tests in `tests/unit/test_storage_forecast.py`:

```text
test_forecast_read_set_pins_snapshot_and_allows_writer_commit
test_forecast_read_set_rolls_back_after_read_failure
```

The concurrency test uses two real `Catalog` instances on one SQLite path.
Generation G contains only a predecessor at the seven-day cutoff and a current
row at `now`. The reader trace callback recognizes the predecessor `SELECT`,
which can only be reached after the bounded first `SELECT` has completed. It
sets a `threading.Event`; the writer then inserts a row at `now - 1h` and
returns from its normal committed `record_space_sample()` transaction. The
reader trace callback waits for that commit event before allowing the
predecessor query to continue.

The test asserts all of the following:

```text
writer insertion returned True;
writer thread completed before reader forecast returned;
reader bounded rows contain only generation-g current;
reader predecessor is generation-g-predecessor;
reader current is generation-g-current;
reader read-set count is 2;
reader forecast equals the pre-write full-history reference;
post-write full-history count is 3;
post-write selected rate differs from the pre-write rate;
reader connection is not in_transaction;
trace is BEGIN, bounded SELECT, predecessor SELECT, current SELECT,
COUNT SELECT, COMMIT;
trace contains no BEGIN IMMEDIATE.
```

There is no `sleep()` and no retry loop. The writer commit is proven while the
reader callback is paused, so a `BEGIN IMMEDIATE` implementation would fail
the writer-commit-before-reader-completion assertion rather than pass by
serializing the writer.

The rollback test installs a SQLite authorizer that denies reads of
`storage_space_samples` after `BEGIN`. It asserts the exception propagates,
the trace contains `BEGIN` and `ROLLBACK`, and
`catalog._connection.in_transaction` is `False`.

## Transaction and writer evidence

The successful trace assertions observed the essential sequence:

```text
BEGIN
SELECT bounded rows: observed_at_utc_ns > cutoff AND <= now
  ORDER BY observed_at_utc_ns, sample_id
SELECT predecessor: observed_at_utc_ns <= cutoff
  ORDER BY observed_at_utc_ns DESC, sample_id DESC LIMIT 1
SELECT current: observed_at_utc_ns <= now
  ORDER BY observed_at_utc_ns DESC, sample_id ASC LIMIT 1
SELECT COUNT(*) AS sample_count for scope
COMMIT
```

The reader `in_transaction` flag was false after success. The test's writer
event was set only after the writer's committed `record_space_sample()` call
returned, and the reader callback observed that event before the remaining
snapshot reads proceeded. The final captured read-set remained generation G;
the independent post-commit full-history read saw generation G+1.

Failure cleanup evidence was also green: injected read denial propagated and
rollback left the connection out of transaction.

## Validation

Final full offline suite:

```text
python3.12 -m pytest -q
1453 passed, 24 skipped, 4 deselected, 13 warnings in 119.04s
```

The 24 skips are existing online/public-API and preview gates. The explicitly
named unit command passed:

```text
python3.12 -m pytest -q tests/unit/test_storage_forecast.py \
  tests/unit/test_vps_capacity_profile.py tests/unit/test_metrics.py \
  tests/unit/test_service_runtime.py
62 passed in 4.21s
```

Directly relevant existing Catalog/WAL tests passed:

```text
python3.12 -m pytest -q tests/integration/test_remote_delete.py \
  tests/integration/test_multiprocess_catalog.py \
  tests/unit/test_catalog_snapshot.py \
  tests/fault_injection/test_catalog_snapshot.py
58 passed, 8 warnings in 5.37s
```

Static and whitespace checks:

```text
python3.12 -m ruff check \
  src/binance_market_data_recorder/storage/catalog.py \
  src/binance_market_data_recorder/storage/forecast.py \
  tests/unit/test_storage_forecast.py
All checks passed!

python3.12 -m mypy \
  src/binance_market_data_recorder/storage/catalog.py \
  src/binance_market_data_recorder/storage/forecast.py
Success: no issues found in 2 source files

git diff --check
PASS
```

## Bounded-path sanity benchmark

A small temporary Catalog benchmark seeded 721 hourly rows across 30 days.
The read-set returned 168 bounded rate rows plus one predecessor, while the
historical count remained 721. Twenty forecast calls produced:

```text
history_rows=721
bounded_rate_rows=168
predecessor_rows=1
forecast_median_ms=0.492
forecast_max_ms=0.689
```

The accepted bounded materialization mechanism remains in place; no heavyweight
benchmark or additional optimization was run.

## Push and final status

```text
REMOTE_BRANCH_PUSHED=yes
PUSH=cdeb895..7fa4ecc  perf/bounded-storage-forecast -> perf/bounded-storage-forecast
FORCE_PUSH_USED=NO
PR_CREATED=NO
MERGED=NO
BRANCH_DELETED=NO
CI_WAITED_FOR=NO
```

Final local status before adding this bundle was clean and tracking the remote:

```text
## perf/bounded-storage-forecast...origin/perf/bounded-storage-forecast
```

The bundle itself is intentionally untracked and is not part of either
commit. No live VPS A/B, BBO work, soak, or additional optimization was run.
