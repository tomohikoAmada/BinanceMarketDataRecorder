# MS1 Acceptance — Multi-symbol durable identity foundation

## Result and authority

MS1 is **IMPLEMENTED / LOCALLY VALIDATED / CANDIDATE COMMIT** on the MacBook
development repository. The candidate was created from the exact requested
base `c421605e302d2ad46acdb2466627f64644181c9a`, whose root tree was
`a521dd61f8a090b4930cce5254985383f8893a3f`. No VPS, deployment, pull request,
merge, or live Binance traffic was used.

The candidate SHA is reported by the implementation handoff rather than
embedded here because the acceptance record is part of the candidate commit.

## Scope and compatibility

The durable discontinuity identity is now `(market, symbol, stream)`; lifecycle
event matching additionally retains `gap_id` as the per-discontinuity key.
The durable symbol-specific side-data cursor identity is `(kind, symbol)`.
Historical single-symbol Catalog rows migrate to `symbol=BTCUSDT` under the
existing initialization transaction. The migration is atomic, idempotent,
restart-safe, preserves IDs/timestamps/reasons/evidence, retains distinct
historical records without merging, and fails closed for malformed or partial
identity schemas. Global side-data remains global and is not duplicated per
symbol.

Normal new discontinuity, reconnect, recovery, readiness, REST side-data, and
symbol-specific cursor APIs require an explicit symbol. The current runtime
assembly remains the existing single-symbol `BTCUSDT` profile and no runtime
fan-out or readiness-policy change was made.

Raw v1 bytes, payload schemas, historical Raw data, archive manifests, and the
external Contracts repository are unchanged.

## Focused evidence

The dedicated `tests/integration/test_ms1_multisymbol_durable_identity.py`
covers:

- legacy Spot and USD-M Catalog migration, preserved evidence, idempotent
  reopen, malformed/partial fail-closed behavior, and restart preservation;
- independent open/complete/query behavior for every frozen target symbol in
  both Spot and USD-M, including BTC/ETH non-interference;
- independent `(kind, symbol)` cursor positions across every frozen target
  symbol and preservation across reopen;
- unchanged global-side-data cursor semantics; and
- explicit-symbol API enforcement.

## Validation record

The command results in the final handoff are authoritative. The validation
matrix is:

```text
focused MS1 and related reconnect/side-data suites: PASS
full offline pytest: PASS
ruff: PASS
mypy: PASS
compileall: PASS
git diff --check: PASS
online Binance traffic: NOT RUN
VPS/deployment: NOT RUN
Contracts repository: NOT MODIFIED
```

The four pre-existing untracked review bundles in the repository worktree were
preserved and are outside the MS1 candidate.
