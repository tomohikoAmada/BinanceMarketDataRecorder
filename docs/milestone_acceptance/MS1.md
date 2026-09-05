# MS1 Acceptance — Multi-symbol durable identity foundation

## Result and authority

MS1 is **IMPLEMENTED / LOCALLY VALIDATED / FINAL NARROW-FIX CANDIDATE** on the
MacBook development repository. The original candidate was created from the
exact requested base `c421605e302d2ad46acdb2466627f64644181c9a`, whose root
tree was `a521dd61f8a090b4930cce5254985383f8893a3f`. An independently adjudicated
narrow follow-up corrects historical identity-migration ordering and legacy
authority-digest compatibility. A final narrow follow-up makes genuinely fresh
identity initialization restart-safe without redesigning MS1. No VPS,
deployment, pull request, merge, or live Binance traffic was used.

The candidate SHAs are reported by the implementation handoff rather than
embedded here because this acceptance record is part of those commits.

## Scope and compatibility

The durable discontinuity identity is now `(market, symbol, stream)`; lifecycle
event matching additionally retains `gap_id` as the per-discontinuity key.
The durable symbol-specific side-data cursor identity is `(kind, symbol)`.
The identity-schema decision now precedes ordinary idempotent initialization.
The supported pre-M19 shape (`operational_events=legacy`,
`side_data_cursors=absent`) and exact-c421 shape (both tables legacy) migrate
in one `BEGIN IMMEDIATE` transaction. Historical single-symbol operational
events and any c421 cursor rows migrate to `symbol=BTCUSDT`; the pre-M19 cursor
history is correctly treated as empty. A fresh identity schema is created only
after the fresh-state decision. Its current `operational_events`, current
`side_data_cursors`, and discontinuity identity index share one explicit
`BEGIN IMMEDIATE` transaction and one commit boundary before ordinary Catalog
initialization. A failure before that commit leaves both identity tables absent
and no index or migration artifacts; a failure after that commit can reopen the
coherent current identity schema and safely finish ordinary idempotent
initialization. Already-current schemas reopen without a rewrite, and arbitrary
asymmetric/malformed/partial states—including current/current with a missing
identity index—fail closed.

Historical `chunk_transitions` SEALING evidence is not rewritten by the MS1
migration. A symbol-less pre-MS1 `seal_intent` stays exactly persisted as it
was reviewed, so `legacy-reconnect-classification.v3` continues to hash the
original intent document. The explicit legacy recovery/read boundary derives
`symbol=BTCUSDT` only in memory. New runtime-produced intents continue to
require an explicit symbol. Global side-data remains global and is not
duplicated per symbol.

Normal new discontinuity, reconnect, recovery, readiness, REST side-data, and
symbol-specific cursor APIs require an explicit symbol. The current runtime
assembly remains the existing single-symbol `BTCUSDT` profile and no runtime
fan-out or readiness-policy change was made.

Raw v1 bytes, payload schemas, historical Raw data, archive manifests, and the
external Contracts repository are unchanged.

## Focused evidence

The dedicated `tests/integration/test_ms1_multisymbol_durable_identity.py`
covers:

- the real pre-M19 operational-events-only schema and the exact-c421
  two-legacy-table schema, including row preservation, empty pre-M19 cursor
  history, integrity, and idempotent reopen;
- fresh-schema creation, already-current reopen, and malformed/unsupported
  partial-schema fail-closed behavior;
- deterministic fresh-identity rollback after operational-event creation,
  after cursor-table creation, before index creation, and after index creation
  but before commit, followed by successful retry from exact absent/absent/no-
  index durable state;
- restart completion after the fresh identity transaction commits but before
  ordinary Catalog initialization begins;
- deterministic rollback and successful retry at six migration checkpoints:
  after rename, during row copy, after legacy-table drop, before index, after
  index, and immediately before commit, for both supported legacy shapes;
- independent open/complete/query behavior for every frozen target symbol in
  both Spot and USD-M, including BTC/ETH non-interference;
- independent `(kind, symbol)` cursor positions across every frozen target
  symbol and preservation across reopen;
- unchanged global-side-data cursor semantics; and
- explicit-symbol API enforcement.

`tests/integration/test_orphan_extension_intent.py` additionally covers a
pre-MS1 ambiguous reconnect candidate with a symbol-less persisted intent and
a non-empty v3 operator authority. The digest is computed against the exact
pre-MS1 intent; after migration, preflight and repeated recovery keep the
authority valid and the operator classification eligible without changing the
persisted SEALING JSON. Its companion wrong-digest case remains `STALE` and
fails closed after migration and reopen.

## Validation record

The command results in the final handoff are authoritative. The validation
matrix is:

```text
focused fresh/legacy identity, corruption, v3 authority, discontinuity, and
cursor suites: PASS (41 passed)
full offline pytest: PASS (1507 passed, 24 online/preview skips, 4 stress deselected)
ruff: PASS
mypy: PASS
compileall: PASS
M0 contract verifier: PASS
Raw v1 Go golden verifier: PASS
CLI version check: PASS
git diff --check: PASS
online Binance traffic: NOT RUN
VPS/deployment: NOT RUN
Contracts repository: NOT MODIFIED
```

The six pre-existing untracked review artifacts in the repository worktree
were preserved and are outside all MS1 commits.
