# MS2 — Configurable Product Runtime acceptance

- Date: 2026-09-09
- Status: **CLOSED — implementation and offline acceptance**
- Delivery: dedicated branch `feat/ms2-configurable-products`; independent PR
  review/merge remains required.
- MS3: **NEXT**, after independent MS2 review/merge and separate authorization.
- MS4: **PLANNED**
- FORMAL_M22_9=NOT_STARTED
- PRODUCTION_READY=NO
- CURRENT_MAIN_DEPLOYED=NO

## Authority and scope

PR #53 was verified MERGED (2026-09-09T00:01:57Z). The implementation base is
`42ba52ba328fa991a04e5f21ca7f397d58c895ce`, tree
`37b9c46dd160001a384358a7352baaf7b12c20b4`. At that base ADR-0032 was
Accepted target; MS2–MS4 not implemented, and ADR-0031 was superseded.

The last behavior authority before MS2 remains
`d38180074b5f76ab6b7778eea7fc505160c671ae`, tree
`95f16f05b30b7db23e43ebb6439ed0d055081902`. All five descendants before
MS2 were verified documentation-only:

- `34250e5661ecc45da8173a15502361fb6716709a`
- `07401e1f7b30efb0d026bab33d653bc0cfd753d4`
- `b34c9718b5f4386186254f036b37c7b9e713cf16`
- `085769bab9bd556237cb17806210f48def3ece0e`
- `42ba52ba328fa991a04e5f21ca7f397d58c895ce`

MS1 acceptance was read and retained unchanged. Before publishing this branch,
remote inspection found only origin/main (and origin/HEAD), no open PRs, and
unchanged origin/main. Six pre-existing untracked review artifacts were
preserved outside the MS2 commit.

The commit containing this record is the MS2 candidate; its exact commit/tree
and PR URL are recorded in the delivery report rather than self-referenced here.

## Implemented behavior

Configuration preserves field presence. Neither selection field supplied means
the legacy BTCUSDT Spot plus USD-M profile. Either supplied means explicit
topology: canonical uppercase supplied lists, omitted sibling empty, both empty
invalid. Canonical list order is preserved in public config; ProductKey iteration
is sorted deterministically. Validation is local, rejects duplicate/unsafe
identities, and performs no exchange discovery or network preflight.

An immutable Recorder-local ProductKey keys one Collector per configured
(market, symbol). CollectorSettings require explicit symbols. Core WS routes,
parsers, REST requests, provenance, envelopes, spools, readiness, resync and
product side-data use that identity. Wrong s/ps payloads fail validation;
existing malformed Raw evidence remains recoverable and cannot satisfy core
readiness. The BTC-only guards in the existing orderbook model/reconstructor
were direct non-BTC bootstrap blockers and were removed without changing book
algorithms.

Spot retains the existing shared public-IP limiter and snapshot singleflight
now distinguishes products. Runtime owns one USD-M request lock and one cooldown
for the configured USD-M products. Product managers and the conditional
process-global manager receive the same objects. Global scope owns only
funding_info/exchange_info; it exists only with configured USD-M products and an
enabled global kind. The legacy BTCUSDT global sentinel remains durable data
identity only and never adds a product. Empty markets construct no collection
REST clients, WS tasks, collectors or side managers.

Service state exposes products by market and symbol, canonical configured lists,
expected/ready counts and exact-topology readiness. VPS readiness receives the
expected keys from loaded CLI config and independently checks the product set
and individual evidence, alongside existing deployment/process/heartbeat/
Catalog/capacity checks. Recoverable readiness/resync/queue state is product-local;
terminal core failures remain process-fatal. Hard reserve records only configured
products times core streams using existing MS1 gap_id matching.

Configuration changes apply on normal restart. Removed products' historical Raw,
Catalog, manifests, archive and continuity remain intact. MS1 cursor identity
(kind, symbol), gap identity, migration authority, Raw v1, manifest/archive formats,
Catalog schema, external Contracts and Projection are unchanged.

## Offline acceptance evidence

Environment: macOS 26.6.2 (25G83), Apple Silicon arm64, Python 3.12.9.
Commands used the isolated .venv-ms2 Python environment, installed editable
with development dependencies. All filesystem/transport integration fixtures
use test roots and deterministic local fakes.

Focused command:

```bash
python3.12 -m pytest -q \
  tests/unit/test_ms2_configurable_products.py \
  tests/unit/test_usdm_side_data_rest.py \
  tests/unit/test_usdm_shared_rest_gate.py \
  tests/unit/test_service_runtime.py \
  tests/unit/test_vps_service_readiness.py \
  tests/integration/test_spot_collector.py \
  tests/integration/test_usdm_collector.py \
  tests/integration/test_usdm_side_data_integration.py \
  tests/integration/test_ms1_multisymbol_durable_identity.py
```

Result: **215 passed in 8.83s**. The final full suite includes subsequent minor
config-order and shutdown-report logging changes.

The dedicated 75-case MS2 suite covers presence/default semantics, exact assembly,
non-BTC routing/parsing/REST identity, shared limiter/singleflight, shared USD-M
object identities and observed 418/429 blocking across core/product/global paths,
conditional global ownership and sentinel separation, zero-market fake
construction, missing/extra/unready/identity-mismatched readiness, product-local
resync/queue state, hard-reserve exact coverage and persisted gap preservation.
Real ServiceRuntime with fake transports reaches ready and seals correctly
identified chunks for Spot-only, USD-M-only and mixed non-BTC topologies.
Wrong-symbol malformed frames cannot satisfy readiness.

Existing regression suites retain explicit BTC fixtures. Spot Collector integration
also covers ETH/SOL; USD-M side REST tests cover BTC/ETH/SOL and all six five-minute
families. MS1 migration, gap and cursor tests, terminal service failure, reconnect,
backpressure, recovery and signal shutdown remain in the full offline suite.

| Command | Result |
| --- | --- |
| python3.12 -m pytest -q | PASS: 1606 passed, 24 skipped, 4 deselected; 13 existing fork deprecation warnings |
| python3.12 -m ruff check . | PASS |
| python3.12 -m mypy | PASS: 249 source files |
| python3.12 -m compileall src | PASS |
| python3.12 tests/verify_m0_contracts.py | PASS |
| go run tools/verify_raw_chunk_golden.go | PASS: unchanged Raw v1 golden |
| git diff --check | PASS |

Online/preview tests were skipped by their opt-in gates; stress tests were
deselected. Live Binance, VPS/platform deployment tests, long stress/burn-in,
72h/168h qualification and manual GitHub CI were not run, as instructed.
No external volume was used. Automatic CI, if started on push, is human-supervised
and is neither awaited nor manipulated by this delivery.

## Remaining limitations

No new blocking offline failure is known. Existing process-memory-only cooldown
across restart, conservative typed/no-header 418 fallback, repeated-cancellation
supplemental test-strength backlog, RSS WATCH/not-proven-leak, open R-034 and
unresolved formal capacity runway remain as recorded in known_limitations.md.
Offline tests do not establish live product availability or production capacity.

Next action: **INDEPENDENT_MS2_PR_REVIEW**.
