# Q3 — Empty auxiliary responses and bounded recovery

October2,2026. Current task: reviewed eligible Formal2h, then stop before12h.
All14 auxiliary flags, four ProductKeys, growing corpus and Raw/Catalog/archive
contracts remain unchanged. This is one Q3 correction, not a new milestone or
Formal attempt. Exact reviewed605bc16 is installed/verified; its engineering baseline and
fresh readiness pass. Nonformal cloud warm-up is running; remaining gates pending. Source48ca920 remains inert custody.

## Defect and narrow correction

The preceding engineering baseline/completed verification passed on637 chunks;
[retirement correction](Q3-raw-retirement-correction.md) retains exact evidence.
After start, the strict auxiliary pre-start gate found two taker-volume owners
RETRYING. Actual retained responses contain permitted leading overlap but no
requested period; first empty response is persisted and cursor stays unchanged.
Repeated empty responses reuse a fixed event ID with different observed times,
which Catalog correctly rejects. Independent old-source reproduction confirms
RuntimeError followed by CatalogStateError.697 captured chunks are now fully
archived, Recorder inactive/disabled, Catalogok,0partials, no Formal T0/credit.

Each empty-response event now binds the existing unique REST connection ID.
Each retention-gap observation binds its one captured observed timestamp into
both ID and occurred-time. The latter was an independently reproduced existing
P2 with the same mechanism; it is not claimed as the fresh-cloud cause. Both
changes preserve prior events, every response Raw, genuine Catalog conflicts,
strict cursor monotonicity and fsync-before-cursor. No schema/reader migration,
request/timing/retention change, retry framework or silent event overwrite.
Repeated same-time exact gap identity remains legitimately idempotent.

A dedicated EmptySideDataResponse(RuntimeError) exposes the exact empty-period
cause in existing public diagnostic state. Generic RuntimeError is not sufficient
evidence of this condition. No forward fill, cursor advance or claimed missing
period completeness is added.

## Proportionate supplementary coverage policy

Pre-start and target remain strict: all26 enabled/running owners RUNNING,22 REST
contexts with successful fresh captures,2 fresh connected mark streams,2 connected
liquidation streams and12 correctly symbol-bound caught-up five-minute cursors.

Only explicit during-observation checks may allow a five-minute REST owner
RETRYING for EmptySideDataResponse, with prior real success and the unchanged
configured interval+900s freshness/cursor-lag checks. Missing success, old/future
capture or cursor, wrong identity, terminal/STALE/stopped owner, disconnected WS,
CatalogStateError and other generic failures still BLOCK. The output retains
original status/error and explicitly lists recovering_contexts; recovery is not
represented as complete period data. Each stage must end with strict coverage.

The nonformal helper uses strict engineering-start before/after checks; subsequent
observations explicitly allow only this bounded condition. Completion requires
zero delta/causal backlog and no recovering context before or after the last
observation. The30s sampler preserves these same diagnostic records; it neither
changes Recorder nor grants credit. Observer300/600/240s,7/8MiB,7200s warm-up limit,
actual missed550/catch-up300, both LIVE audits and completed verification remain.

## Candidate evidence and pending gates

Final six-path runtime/tests/operator manifest SHA-256:
6d4e32e26257e9c60e15435eac3ed460ab2a712bb1e0145c7e7b43b88e87e7e1.
Source collector SHA0d93fe0a7685dad9661bae09c3b3c41b965a024bb8c23f124ce7585a0993ca5f.
94 focused regressions PASS1.29s; final full1970 PASS161.77s,24 online SKIP,
5 stress deselected,13 existing fork warnings. Ruff/strict mypy286,M0,Go golden
and diff-check PASS. Independent final GPT-6.1 Sol xhigh local/offline review ACCEPT; scope
P0/P1/P2/P3=0, independent56 selected tests PASS1.09s. Earlier six-path review ACCEPT
is superseded by this additional minimal gap correction; no stale review claims
bind changed bytes. Before the final typed policy,1948 tests passed; final source
full checks PASS; exact-source CI remains required. Online/stress capture evidence remains
explicitly bound to prior unchanged transport/writer inputs.

New prepared evidence root:
/srv/recorder-data/recorder-archive/evidence/Q3-empty-recovery-20261002-59G89csa.
Helpers80b81a90… andd8f4ce0a… bind this root; source/release build not yet deployed.
NEXT: exact final review/checks/CI, one clean frozen release, stopped redeploy,
new engineering predecessor, real nonformal cloud gates/forecasts, authoritative
post-warm-up baseline/quiet window, one Formal2h plus complete eligibility review.
12h/24h remain NOT_STARTED; PRODUCTION_READY=NO.


Final independent record SHAead5b28330128f6c0f71b20ee504a1e0158733e0c94d48ade2128440ff2d97d6;
gap closure probe SHA579056dcb907c44d914cd66aa0aa73f6ddf19cb3986f409a7140382302a1996e.
Same-ns gap replay is exact/idempotent; +1ns creates a separate observed fact;
modified same-ID evidence still fails Catalog identity. Three empty requests
reach REST with unchanged cursor; subsequent real requested data advances only
after persistence. No new source finding remains; historical full-audit scalability
P2 is separate and OPEN. Exact-source CI/build/redeploy/cloud gates remain.


## Exact release and stopped redeploy

Source605bc169cb9d9bbc886b01c548a10c16d35b67be, treeb10fea2beac6eb8adf52dfa4f7f0ff2445fa5d9a;
[exact-source CI37002357094](https://github.com/tomohikoAmada/BinanceMarketDataRecorder/actions/runs/37002357094)
SUCCESS on Ubuntu/macOS. One clean Git-archive wheel504417B, SHA
690327fc0b6c8d241d7d95a806d0b9c34cb665ece26c214074ae65e03a5a336f.
Source archive SHA0e150e8bee633287be590bdac9fa955cf91d545fdc39c9742c95abde92b0c8f9;
Linux lock44cd3733… unchanged. Packaged Python source equals clean archive.
Noneditable clean installation,28 exact runtime distributions, pip check and
isolated doctor/status PASS. Cloud98 changed capture/gate/integration tests
PASS8.65s. Existing unchanged V5/public/stress results retain original bindings.

Stopped deployment preserved source48 venv in
/opt/binance-market-data-recorder/venv-custody-q3-empty-recovery-20261002-48ca920.
New root-controlled release /opt/binance-market-data-recorder/release-605bc16-aiW0UvIt;
canonical venv/writer/archive/config/unit remain. Exact deployment identity
1ecd42ca420e7a9d74f69e9618c0e9fb0f7a3be06d41364391769f805dde4458 VERIFIED,
including130 package RECORDs,28/28 locked dependencies and protected installation.
All14 actual flags and four selected ProductKeys rechecked true; no corpus reset.
Frozen reviewed release SHA2077d5413bae96d7b6e429a953f1094f1898fe438a280df6a13741b90aeb2a35.

Engineering full baseline/completed verify over697 retained archived chunks PASS,
no blockers, both LIVE passes. Root157e01d3dda930d2e5e889bb770fd2fd55111f9b9f80d8b8e6ba60bfe1c98795.
Baseline subprocess wall envelope670s and completed verify12s; unit CPU687.857s,
memory peak307.9MiB including cache, swap peak0. Archive authority restored with
actual automatic recurrence. Recorder enabled/running for NONFORMAL only; core
READY and strict26-context auxiliary gate PASS before warm-up.
No Formal T0/credit. New baseline is needed to bind this exact deployment, not
a rerun of the unchanged source48 proof. Afterwards: actual nonformal gates,
forecasts and authoritative post-warm-up baseline. Stop before12h.


Independent completed engineering review ACCEPT, SHA
cfd81f2f867a046bfa9eed623ba1f247b3e2c9db662a616b5857f945305e295c.
Frozen-control reconstruction PASS0.811s with Raw calls0/production opens0.
697 manifests/transactions,25 shards/11857 records and exact deployment bindings
agree; this grants no Formal credit and does not replace the post-warm-up baseline.


## Subsequent actual cloud gate — not accepted

Fresh strict26-context pre-start PASS and all four products READY. Actual
complete observations: start18.463s, immediate11.448s, normal43.464s,
missed550 cadence82.028s; all below240s. Catch-up-1 fails unmarked_reconnect;
no completion or target,0 Formal credit. Bundle SHA
8fb18b3427dc68581a455dfda910a67e0e38adfc90fae0fd749f1d56b59c7129.
Independent online chain reconstruction localizes one finding at transition7723,
um_perpetual:ETHUSDT:liquidation. Prior baseline chunk5d08586e-7ae2-4198-acf5-be3122899e18
last receive1790938867199992725; first current chunk444ad598-6067-41d1-9214-a3ce0be9b376
first receive1790943882125252977 (12:24:42.125UTC). Current collector identity
0371e595-8245-4b5b-94de-7c2d20f6b1a0:um_perpetual:ETHUSDT matches the frozen
service instance, started1790942976692667264 before T0. Liquidation connected
with attempts1/failures0 throughout, but had no message at T0. Later real
12:31 network reconnects are separate and do not cause this finding.

Stop/disable and verified archive drain complete:1182 chunks/transactions all
LOCAL_DELETED,4762557 frames,238437460 stored bytes,3624723275 uncompressed
bytes, Catalogok,0 active partials. Archive timer enabled/active with actual
finite recurrence; OS authority restored. RecorderResultsuccess/NRestarts0.
Resource monitor stopped;90 records/3492808B SHA
de80d755a0adac77a282195e940f3500aa7d054512bde02f7be523c772a273c2.
Actual capture remains in this same growing corpus. Source605bc16 stays installed;
no unchanged repeat or Formal start. Next is a minimal startup-boundary correction
and independent review preserving real unmarked-reconnect detection and legacy
replay, then affected release and cloud gates. Q3 remains PARTIAL_NOT_ACCEPTED.
