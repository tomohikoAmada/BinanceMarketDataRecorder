# Q3 — Empty auxiliary responses and bounded recovery

October2,2026. Current task: reviewed eligible Formal2h, then stop before12h.
All14 auxiliary flags, four ProductKeys, growing corpus and Raw/Catalog/archive
contracts remain unchanged. This is one Q3 correction, not a new milestone or
Formal attempt. Current deployed source48ca920 is stopped; replacement pending.

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
