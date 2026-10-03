# Q3 — Private discontinuity index encoding

October2 UTC / October3 Shanghai,2026. Q3 remains PARTIAL_NOT_ACCEPTED;
Formal2h has not started, credit0,12H_STARTED=NO,PRODUCTION_READY=NO.

## Actual failure and cause

Installed4e1cf32 engineering attempt2 on1182 retained chunks failed16:40:53 UTC,
after its16:31:34 start, before publishing audit-root or completed verification.
Unit `binance-recorder-q3-startup-engineering-baseline-2.service`, invocation
ec9cbd60f5d04a3480090d1ed1415a99, exit2. CPU562.412s, memory peak323919872B
including cache. Original result/log/control files remain retained under
`/srv/recorder-data/recorder-archive/evidence/Q3-startup-boundary-20261002-mfJDVr8h`.
Failure log SHA5b74fbe4a458f2eee38e1102ec62c5e8386263dc1b8f6ac1aa08d4902aa1a3de.
No failure is converted into a PASS and no failed prefix is reused under a new identity.

The last durable checkpoint has18920 records/39 shards and operational event8.
Event10/11 are a real network STARTED/COMPLETED pair, distinct from the earlier
sparse initial-connection failure. `_operational_events` passed this array of
9-key mappings to `canonical_json`, whose public document contract calls
`dict(document)`. That raises `ValueError: dictionary update sequence element
#0 has length 9; 2 is required`. Earlier697-chunk PASS contained no such pair.
This is an audit implementation error, not evidence of corrupt Raw or hardware
insufficiency; no data loss is inferred.

## Minimal correction and verification

Only serialize the private SQLite `gaps.pair` value as deterministic UTF-8 JSON
array, matching its existing `json.loads` consumer. Keep the authoritative
mapping encoder, Raw/Catalog/proof formats, gap classification, readiness,
batch/time/cadence limits and both LIVE passes unchanged. No new framework,
fallback, schema or ADR is needed for this private scratch representation.

Real sealed Raw + Catalog full-baseline integration cases exercise an open gap,
a completed gap and malformed generation authority. Both valid cases must
publish without findings and reconstruct through LIVE and frozen-control
verification; malformed authority must remain FAIL. All cases retain zero
Formal duration credit. Before the fix the open/completed cases both reproduced
the exact ValueError. Retained local log:
`/Users/amada/Library/Caches/BinanceMarketDataRecorder/q3-startup-boundary-20261002/discontinuity-array-before-fix.log`.
After correction56 affected tests PASS7.49s, then all6 finalize-module cases
including malformed PASS1.28s and the full affected57-test selection PASS7.32s.
Full offline1994 PASS164.75s,24 online SKIP,5 stress deselected;13 existing
fork warnings. Ruff, strict mypy287, M0, Go golden and diff check PASS.
Independent GPT-6.1 Sol xhigh LOCAL/OFFLINE source-only ACCEPT, no open findings;
60 independent tests PASS5.71s, zero network/SSH/production operations/repository
edits. Twelve real scratch-index traversals separately confirm exact array1/2,
malformed FAIL, LIVE/historical reconstruction and original-method ValueError.
Review identity `/private/var/tmp/bmdr-startup-ordinary-review-kbJkqHnV/discontinuity-array-review-identity.json`,
SHA92e0233ae052b0d5a1b60645f344ca874f30bea1281adf3e971549d45609aa6b.
Exact runtime/test diff SHA55d82ae9575fe7d1a0a2bf161331a14fb1655df261fd2d6ff625487d2514d04b.
The five startup runtime files remain unchanged; prior source ACCEPT is reused.
At that source-review checkpoint, replacement CI/wheel/cloud/deployment/baseline
gates were pending. The October3 release checkpoint below supersedes this status.

Recorder remains inactive AND disabled,1182 chunks/transactions LOCAL_DELETED,
Catalogok/0partials/backlog. Archive authority is paused for the failed audit;
normal OS authority restored. No live monitor or Formal T0 exists. All14 flags,
four products and the growing corpus stay unchanged. Preserve unique archive,
old metadata and both failed attempts. Future archive pauses stop the timer
first and wait for the current worker to finish, preserving strict quiescence.

Next at that checkpoint: independent review, freeze/push one exact replacement,
dual-platform CI, clean locked wheel/affected cloud tests, stopped deployment and fresh
engineering baseline/both LIVE/completed verification. Then actual warm-up/
convergence/resource forecasts, finite-window authoritative post-warm-up baseline,
strict readiness and reviewed eligible Formal2h. Stop before12h.


## Exact CI continuation — October3

Frozen6c157fc1b3101330a559ce3f630e9b520be66067/tree92646570ad4c267fbd59b446a40bda501626e7ff
CI37038494890 did not qualify release. Ubuntu1989 PASS/1 FAIL/28 skips/5 deselected:
`test_prior_backpressure_success_cannot_mask_later_session_restart_timeout`
assumed one final-session manifest; actual capture had a complete normally rotated
prefix plus the incomplete reconnect boundary. The fixture now checks every
complete prefix, the final incomplete reconnect marker, total count and unchanged
exact original payload/order/single STARTED/failed persisted-boundary authority.
Explicit normal/rotation cases exercise the real writer deadline after admission.
63 related ingress/replay tests PASS10.56s,1 stress deselected; Ruff/mypy287/diff PASS.
Runtime package files are unchanged from the independently accepted6c157fc scope.

macOS was cancelled at the existing45-minute job limit, last quiet progress28%.
The log does not establish the blocked test or root cause. CI now requests a
120-second faulthandler thread dump for a long test, with no skipping, termination,
budget expansion or result relaxation. Diagnose an actual repeated block from
its trace; do not call this diagnostic setting a runtime fix. Retained logs:
`/Users/amada/Library/Caches/BinanceMarketDataRecorder/q3-startup-boundary-20261002/ci-37038494890-failed.log`
and `ci-37038494890-macos.log` in that same cache. Independent GPT-6.1 Sol xhigh LOCAL/OFFLINE fixture/diagnostic supplement ACCEPT,
no findings;55 PASS9.78s/1 stress deselected plus22 PASS5.91s/1 local-server
deselected, no network/SSH/repository edits. Exact two-file diff SHA
02f8832f50981cf3503edf596bdf580fd363bdac9b9eab11d9aea131948e31a6. Report SHA
279fe55db3700811c494ed0ef9840a5b1db2ffbb34eb70f4af573d7039f0af71;
identity SHA54f5b810b79772c5d021ea477edd3dd4c98e4c966a18c8ff32dd4eb80c7a1633.
New exact-source CI must actually PASS before release; cf3909e9 subsequently passed below.

Cloud read-only check October3 confirms the same boot, inactive AND disabled
Recorder/MainPID0/Resultsuccess/NRestarts0;1182 chunks and archive transactions
LOCAL_DELETED, Catalogok,0partials. Archive timer/worker remain paused; OS update
authority restored. No Formal T0, target or credit exists. The owner has changed
the continuation to hourly quota/work checks; it resumes only idle work when
quota permits and does not intervene in work already advancing. Keep only main and
preserve unrelated bundles/stash/worktrees.


Prepared retained replacement evidence root:
`/srv/recorder-data/recorder-archive/evidence/Q3-discontinuity-array-20261003-ik5UgdqY`;
staging `/var/tmp/binance-recorder-q3-array-admission-iZYQlFLk`. This is a new
control/build scope, not a data reset. Q is root:bmdr0750; native warmup/Formal
children bmdr0750. No replacement wheel is installed and no live collection
started. Four operator helpers have independently confirmed literal root rebind
only, with previous startup/endpoint ACCEPT reused. Exact helper inputs are in
local cache `array-release-operator-inputs/operator-helper-inputs.json`.

## Verified replacement preparation — October3,09:10 Shanghai

Frozen runtime candidate cf3909e9ee6da3862c03b4bef609cc4a4a049cc7,
tree2ac585d0f397abc6e9f0e588aab81b14d423e11e, passed exact
[CI37083270715](https://github.com/tomohikoAmada/BinanceMarketDataRecorder/actions/runs/37083270715).
macOS1995 PASS/24 SKIP/5 deselected258.17s; Ubuntu1991 PASS/28 SKIP/5 deselected374.37s.
Ruff/mypy/M0/Go/build/locked-dependency/clean-wheel CLI checks PASS. The older
macOS timeout remains unresolved historical evidence; the new passing run does
not establish its cause or convert it into a PASS.

Cloud admission completed with168 affected tests PASS/1 stress deselected128.37s,
unit invocationc02bd56b7c4b4a578c6e1d48efabbc78, exit0/Resultsuccess.
Clean noneditable wheel505407 bytes, SHA
7e15bbd9e9d84f92868481bc0cbd995248c4d139cb10c7ddca949f15cb78f82b;
121 package Python files match the exact source archive, all28 locked dependencies
match and pip-check/CLI smoke PASS. Lock SHA remains44cd373324f2af5f2682851996bc59a16199c65f8de9e98089131e1c67d6f335;
source archive SHAa5c8121975dc9be156300fb4a35764bf017e11136a2fe133a3e0f17134dbb9ce.
Native evidence is `array-clean-wheel.json` and `cloud-array-admission.log` under
the prepared root. This admission uses isolated staging, not the production venv.

The stopped canonical deployment script has independent GPT-6.1 Sol xhigh
LOCAL/OFFLINE static ACCEPT, SHA7ae281f35c4b892044fb876b24522a8a7ad75c1c2e4f7fa1b7e0698116998e18;
report SHA71c0d65674cd1c744a44f021d0c7277b10976fda397f2ec416c5be9a8d026a2e,
identity SHAb482523c514fad7f33bbf4b81248a400ae46526b2b4ee3541df59a510b7f522f.
It stops the archive timer first and waits for actual worker success with a
bounded read-only query/deadline; no worker SIGTERM, deletion or Recorder start.
Six independent mocked idle/deadline cases PASS. Static ACCEPT grants no actual
deployment or Formal eligibility.

Fresh read-only SSH confirms the unchanged4e1cf32 installed identity, Recorder
inactive AND disabled/MainPID0/Resultsuccess/NRestarts0, same boot, all1182 chunks
and archive transactions LOCAL_DELETED, Catalogok,0partials. Archive timer remains
inactive/enabled and worker inactive/Resultsuccess; no new baseline or native
stage-start/target/final exists. Next is actual stopped replacement installation,
fresh engineering baseline/both LIVE/completed verify, then warm-up and the
remaining Q3 gates. Q4 Formal2h NOT_STARTED; duration credit0; stop before12h.


## Actual stopped deployment and new engineering run

Reviewed deployer executed once as invocation420230b74af04240bd93cc0afe98c4dd,
exit0/Resultsuccess. Canonical release `/opt/binance-market-data-recorder/release-cf3909e-UmHzaUrB`;
prior4e1cf32 venv retained inert at `/opt/binance-market-data-recorder/venv-custody-q3-array-4e1cf32-Sw6lnM1j/venv`.
Actual bmdr deployment verification VERIFIED with identity
eacada170e9a3b113c9cc9b1166a63cb5d71a35da758ec2f997e8eb5f2ae54cc,
130 package RECORD/28 exact dependencies/4859 protected files/407 directories.
Config/lock/unit hashes and all14 flags/four products remain unchanged. Recorder
inactive AND disabled/MainPID0/Resultsuccess/NRestarts0. Archive timer restored
enabled/active with finite future deadline, then paused timer-first while its
worker finished successfully; no worker signal. Actual automatic recurrence must
still be observed after the engineering audit before warm-up.

Fresh full engineering baseline started on1182 retained chunks under
`binance-recorder-q3-array-engineering-baseline.service`, invocation
553523c11dc1486a8a8e92a3a8b96f85. Current status RUNNING, not PASS; both full LIVE
passes and completed verification remain mandatory. Native audit has a900s
no-forward-progress watchdog and no total-duration cap. Economical read-only
Luna monitor is active. No Formal T0/target/final or credit exists.


## Completed engineering predecessor and live NONFORMAL warm-up

The corrected1182-chunk engineering baseline published root
422b14368787746f41be8447c421810ee42fffe07915561a25c98cefdb30ad1e,
COMPLETE/PASS_CANDIDATE/0 blockers/0credit, with20118 records/42 shards. Both full
LIVE passes completed before root publication; completed CLI verification matched
the exact same root. Counts:1182 manifest/chunk/archive_transaction/raw_location,
9456 chunk_transition,5910 archive_event,24 operational_event. Actual unit wall
01:15:52.140–01:34:12.147 UTC, baseline18m05s plus verify15s; CPU1109.493s,
328MiB cgroup including cache, swap0. Old failures remain retained. Frozen control
bundle5506190B SHA3d7d39d35b3908e412a9881a85167c7552331991e34471eef0790a1ecfda356d
was transferred for independent LOCAL/OFFLINE reconstruction; review is running.
This is an engineering predecessor, not the post-warm-up authoritative baseline.

Archive resumed and actual automatic trigger01:40:04 UTC/successful exit01:40:13
with finite future deadline PASS. Recorder enabled/running only for NONFORMAL
preparation, PID37218/invocation756c109fa6ae4a128048e8683387c42f. Actual strict
core4+aux26 readiness passed; all14 flags/four products unchanged. Warm-up unit
invocationf1c8e255a89d4a5b85fc65e191599e91 and30s resource sampler
823b24e87a3a477aba0aff86685d5b18 started; engineering-start/immediate observations
completed without blockers, immediate16.150s total. Remaining real normal/missed/
catch-up/forecast/post-warm-up gates remain. Warm-up exit always stop/disables
Recorder; native diagnostic deadline7200s/Runtime7800s, no automatic retry.
No Formal target/credit; Q3 PARTIAL_NOT_ACCEPTED.

A minimal finite-window restoration helper was independently ACCEPTed locally,
SHA58af69f97937e399fbd7e020ab11685378d89cab534cc2daa3279d17a10642e2,
report SHA855ea39d69c572f1649e6f92be3121a0b7d040827517b8bd30ced74bf0e8de84,
identity SHA88b9007f6e920643706a9ed8560bf4950836f5b2fdb31ce623838b2544ce1d83.
It restores only the five approved runtime OS maintenance exclusions, checks
normal enabled/active authority, and never controls Recorder or grants credit.
Seven pure mocks/AST PASS. Controlled exit restores and proves RESTORED before
cancelling the expiry timer, retaining protection across a connection/quota loss.
The finite window/timer is NOT active yet; its length must bind actual measured
complete audits and new ingestion forecasts. No persistent masks/automatic extension.
