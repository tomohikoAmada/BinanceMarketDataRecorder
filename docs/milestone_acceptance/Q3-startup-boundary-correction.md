# Q3 — Sparse capture startup boundary

October2,2026. Continue through reviewed eligible Formal2h, then stop before12h.
All14 flags, four ProductKeys and the growing corpus remain unchanged.

## Scope and retained failure

605bc16's nonformal catch-up failed `unmarked_reconnect` on the initial ETHUSDT
liquidation connection. Independent original-chain replay and minimal reproduction
locate a pre-T0 connection whose first payload arrives after T0, compared against
a prior-process baseline tail. Later network drops are separate. Original bundle
SHA8fb18b3427dc68581a455dfda910a67e0e38adfc90fae0fd749f1d56b59c7129,
diagnosis SHA2f3846fcd38a5436308d78cd0ce9457b0e2f657335f647033b68a5f2c1f40508.

[ADR-0038](../adr/0038-durable-capture-startup-boundary.md) adds the authentic
zero-frame initial boundary before any socket and retains initially forced flags
in durable SEALING authority. No acceptance classifier or network intent is
relaxed. Existing incomplete pre-SEALING headers remain retained and block an
attempt's zero-partials gate. No corpus reset, Raw deletion or scope reduction.

## Verification and next gates

129 selected startup/transport/network/legacy/crash tests PASS7.95s; final full
1991 PASS168.15s,24 online SKIP,5 stress deselected,13 existing fork warnings.
Strict mypy287, Ruff, M0, Go golden and diff check PASS. Independent core/source
review ACCEPT,119 selected tests PASS7.25s; extended94-test review including
stress PASS10.87s. Two existing fixture assumptions exposed by startup scheduling
were made explicit: stale USD-M snapshot input follows actual persisted depth,
retaining exact2-request readiness; ordinary idle rotation may split global-stop
capture, but EVERY resulting chunk must remain complete/gap-free with original
payload/order/no-discontinuity assertions. The final small fixture delta passed
13 focused tests4.02s; exact-source CI runs the complete final tree.
Final aggregate local/offline review ACCEPT binds source3d05282/treebc1a71a,
all21 changed paths and the three operator helpers; report SHA
f567246a874815f85703e09481daa359b5d013f29bb40d3ccd6f59f499315cfe.
Exact-source CI37027577424 FAILED Profile D on both platforms; affected cloud
run retained265 PASS/1 FAIL/1 stress deselected. Neither run qualifies release.
The first drain hook could block an empty idle check before the42-socket barrier,
leaving the first receipt queued and preventing the third receive. The fixture
now blocks only a real admitted Raw batch and explicitly proves an initial empty
check cannot select that gate. Queue sizes, all42-stream/sibling/backpressure,
payload/count/completeness assertions and watchdogs remain unchanged. The full
production-path module passes7 tests1.58s locally; new exact-source CI/cloud
checks and independent review of this test-only correction remain required.
At that initial review checkpoint deployed source remained605bc16; Recorder inactive AND disabled,
1182 chunks/transactions LOCAL_DELETED, Catalogok and0partials at last check.
Archive/normal OS authority is active. No Formal T0 or credit exists.

Next: freeze reviewed source, exact-source CI and clean locked wheel, affected
cloud tests and stopped canonical deployment. Record a new exact engineering
predecessor, actual all-enabled normal/missed/catch-up observations, resources
and cumulative audit/capacity forecasts. Then the authoritative post-warm-up
baseline under a finite quiet window, strict readiness and one Formal2h,
followed by both full LIVE passes, completed verification and independent
eligibility review. Stop before12h.

Prepared cloud evidence root:
`/srv/recorder-data/recorder-archive/evidence/Q3-startup-boundary-20261002-mfJDVr8h`.
Operator baseline/warmup helpers only rebind this root. The30s sampler checks
freshness using a current timestamp after reading service state, avoiding a
false future heartbeat caused by its earlier resource-query timestamp. Actual
future/stale checks and all auxiliary predicates remain unchanged. Local cache:
`/Users/amada/Library/Caches/BinanceMarketDataRecorder/q3-startup-boundary-20261002`.

## Exact replacement release and stopped deployment

Frozen source4e1cf320e0eec3eb69880744dd24609fa85df35b,
treeafd041f64633f3f6e1d7a037e08ed1672aca3a9c. Independent fixture supplement
ACCEPT,94 PASS11.69s (stress included), no file changes/network; report SHA
325054df3dd57a9a5682fbefd3059da3f353b64c06d579c21cab0243dc8c280a.
Exact CI37031972497 PASS both: macOS1991/24skips/5deselected287.15s;
Ubuntu1987/28skips/5deselected371.63s; all lint/type/M0/Go/build/install steps PASS.
Cloud affected266 PASS/1stress deselected54.62s. An earlier command with incorrect
test paths collected zero tests and remains retained separately; it is not PASS.
Clean wheel505280B SHA2905d30e24ddd53708f50318e4dac74f8c6f183a7fb012c089a7cb25841e83e8,
121 package Python files match clean source;28 exact dependencies/pip check and
isolated CLI version/doctor/status PASS. Source archive SHA
4c1030518f813567963eb2bf7471b74d6a80ffe9a672ef870629853fd1ea6c72.

Stopped deployment script independent static ACCEPT SHA4fa11304…; pin helper
SHAebe6da40…; report SHAc8c44142a526701bc102f65731e3b00dab89915d2e6b697fdd230bfc5f0b21c9.
New release `/opt/binance-market-data-recorder/release-4e1cf32-20r7YLY4`;
canonical venv/config/unit/data/archive remain. Prior605bc16 venv retained at
`/opt/binance-market-data-recorder/venv-custody-q3-startup-605bc16-ggMjcbMJ/venv`.
Actual root and bmdr deployment verification PASS:130 RECORD,28 exact dependencies,
4859 protected files/407 directories, root-controlled canonical venv/identity.
Identity SHA1be38a7d12082e17cf532b887d3f346fea9a436529bd7a9bba26c9681060f477.
All14 flags/four products/config/lock/unit hashes unchanged. Recorder remains
inactive AND disabled,MainPID0/Resultsuccess/NRestarts0;1182LOCAL_DELETED/0partials.
Archive enabled/active finite recurrence verified after deployment, then paused
for new engineering baseline. OS authority restored; no Formal T0/credit.

New exact engineering baseline plus completed verification is executing on1182
retained chunks, unit `binance-recorder-q3-startup-engineering-baseline.service`,
invocationab29cc66759242708c270a965ac38ca1. Both LIVE/publication/verification are
required before PASS. This engineering predecessor cannot replace the required
authoritative post-warm-up baseline. Actual warm-up/forecasts/readiness and Q4
remain pending; stop before12h.
