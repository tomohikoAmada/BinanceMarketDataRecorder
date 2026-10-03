# Post-Q4 Spot snapshot idle correction

Updated October 3, 2026. Owner scope: fix the macOS CI hang and assess whether
the corrected deployment needs another Formal2h. No VPS deployment, collection,
new T0 or automation resume is performed in this scope.

## Diagnosis and minimal correction

Post-closeout pushes `cdc616e` and `0fc4f1b` contain the same source/tests/workflow
as frozen `cf3909e9`. Their Ubuntu jobs pass, but the macOS pytest jobs reach the
45-minute job limit and are cancelled:

- [CI 37105529235](https://github.com/tomohikoAmada/BinanceMarketDataRecorder/actions/runs/37105529235).
- [CI 37105593686, macOS job](https://github.com/tomohikoAmada/BinanceMarketDataRecorder/actions/runs/37105593686/job/111153348403).

The latter diagnostic stack names
`test_bootstrap_buffer_overflow_restarts_connections_and_snapshot`,
`SpotCollector._capture_snapshot` and `SpotSnapshotRequester.wait_for_idle`.
A completed worker can remain in the single-flight registry while its removal
callback is queued. Gathering already-completed workers can return without
yielding; the old loop repeatedly observes the same entries and starves the
callback and the rest of the event loop. This is a production restart/shutdown
path defect, not evidence of an undersized VPS or a slow whole test suite.

The correction snapshots `(key, task)` pairs, waits for those exact workers and
then calls the existing identity-checked removal helper itself. The callback
remains idempotent, and an older worker cannot remove a newer same-key worker.
Request shielding, single-flight ownership, rate limiting, caller exception
delivery and existing idle-waiter cancellation semantics are preserved. No new
runtime timeout, sleep, retry, dependency or acceptance policy is introduced.

Source `23ea5c40541b93563f5bda414c6691ba44af5b04`, tree
`8e038727b4260cacb7aa3fa151d6143f624ed515`.
Reviewed source/test patch SHA-256:
`de945b6d28423b72111c90a4de4d40b47f6fe9541d6ae327cc4cba0a8ecaf09e`.

## Validation

- New deterministic regression fails the old method with an external 10-second
  deadline; parent result is `TimeoutExpired`, 1 failed in 10.19s.
- Corrected success, worker-error and cancelled-caller regressions pass. They
  use actual capture, a fixture REST response and an injected limiter scheduling
  boundary, without mutating the registry. A parent-process deadline bounds
  future failures because an asyncio timeout cannot stop event-loop starvation.
- Local Python3.12.9/macOS: full offline suite 1998 PASS, 24 skipped, 5 stress
  deselected, 172.94s. Public online tests are opt-in and were not run. The 13
  existing multiprocessing/fork deprecation warnings remain visible.
- Focused requester/collector/mutation tests: 27 PASS. Full Ruff, strict mypy,
  M0, Go Raw golden and `git diff --check` PASS.
- Local sdist/wheel build PASS with the CI-pinned setuptools75.8.0/build1.3.0/
  wheel0.45.1 in a separate cache environment; all121 packaged Python source
  files match the checkout. Diagnostic wheel SHA-256
  `f5e990e8387b29f103204e5720bc2617130a0f4f007f707b6a4f2096e62241b6`.
  This is not a frozen/deployed production replacement artifact.
- Independent GPT-6.1 Sol xhigh LOCAL/OFFLINE source/tests review ACCEPT, zero
  actionable findings: 32 focused tests, Ruff/mypy/diff checks PASS; old-method
  negative control hits10.007s external deadline. Additional concurrent drains,
  newer same-key workers, worker cancellation and idle-waiter cancellation PASS.
  Temporary fixtures independently confirm both2h/12h reject a changed identity.
  Reviewed source SHA `3da4cfba3389cd8d885ee2921269efb7b37b4dbbce191c136fe3e56a3e599d68`;
  tests SHA `e86237dfcb1ad7c33af1bc4ba272b6ed8beb21d47c319e8d82ed48404caf1709`.
- [Exact-source CI37130186751](https://github.com/tomohikoAmada/BinanceMarketDataRecorder/actions/runs/37130186751)
  BOTH PASS for23ea5c4: macOS1998/24 skipped,196.07s; Ubuntu1994/28 skipped,
  374.20s; five stress tests deselected on each. Ruff/mypy/M0/Go/build/clean-wheel
  smoke PASS on both; exact Linux production dependency-identity check PASS.
  CI completes14:44:01UTC. Cached result SHA-256
  `f1d9ff777073d6014912df8829cbb4c0ee3bababc41a21bd153ab8f3ba78f295`.

Original CI log archives remain in
`/Users/amada/Library/Caches/BinanceMarketDataRecorder/`:
37105529235 SHA `9557550f07762e1656e9f55b4d6f05e72baa2139f4a65cb6d3c2243063151ed2`;
37105593686 SHA `6d226ae71d5d6a8a4e8cf601db22b5eb903070b299d14bfcca5fe0db47284854`.
The independent review performs no network/SSH/service operations or production
data opens. This correction performs no VPS operations.

## Formal2h disposition and next gates

The accepted [original Q4 record](Q4-formal2h-20261003.md) remains genuine for
installed `cf3909e9`/wheel7e15bbd9/identityeacada17:7200.002178843s, both complete
LIVE terminal passes, completed verification and independent review. That run
does not exhibit this hang; its evidence is not rewritten as a failure.
Its earlier exact-source CI37083270715 genuinely passed both platforms, while
the later intermittent failures show that one pass was insufficient to discover
this race. Original Raw/archive/failed evidence remains retained.

**A corrected deployment needs its own Formal2h before Formal12h.** The change
touches production startup/resync/shutdown work, and a replacement wheel/source
changes deployment identity. The [current plan](../milestone_plan.md) requires
one frozen artifact/profile across2h+12h+24h, and
`acceptance_v5_online.predecessor_reference` rejects another deployment identity
before accepting either the baseline or a stage predecessor. Do not transfer
the original2h credit to the corrected artifact, forge the old identity, or
relax that check. A short recovery test is useful but is not Formal2h credit.

Before a future authorized cloud continuation:

1. Freeze the reviewed source and one locked replacement wheel; complete affected
   cloud offline tests and stopped canonical deployment/identity verification.
2. Preserve the existing growing corpus/archive and old evidence. Recheck actual
   capacity/audit windows on that larger corpus; R-078 remains OPEN.
3. Repeat affected Spot bootstrap-overflow/resync and graceful-stop/cancellation
   gates, all-enabled NONFORMAL warm-up, fresh identity-bound stopped baseline
   with both LIVE passes/completed verification, and strictcore4/aux26 readiness.
4. Perform one new Formal2h under the corrected frozen identity, all14 flags/four
   products, followed by both full LIVE terminal passes, completed verification
   and independent acceptance. Only then consider Formal12h.

The original fix/analysis-only request did not start cloud work. The owner
subsequently explicitly authorized continuation through a new reviewed VPS2h
and reactivated hourly `vps-2h` on October3. New evidence root:
`/srv/recorder-data/recorder-archive/evidence/Q4-spot-idle-20261003-Xo1tH7WN`. Installed old cf3909e9 remains stoppedANDdisabled. Corrected cloud
admission attempt1 retained232 PASS/1 failure: source archive has no .git marker
required by repository-location fixture. Isolated checkout metadata repair and
unchanged affected-gate attempt2 are in progress. No replacement/newT0/credit yet;
old2h evidence remains genuine and cannot transfer across deployment identities.

## Corrected cloud release gates, owner continuation October3

Hourly current-chat `vps-2h` ACTIVE; historical automations remain paused.
Exact source23ea5c4/tree8e038727, archive332233388e07ed9beb59df391956cf5da22f33037f5996358067341f74bc3bd0.
Cloud affected attempt1 retained232 PASS/1 fixture FAIL177.58s: an archive has no
.git. Independently reproduced FAIL without.git/PASS after isolated git init.
Attempt2 identical tests233 PASS/1stress deselected172.38s; wheel build and
pip check PASS. Doctor then correctly refused smoke data inside the new checkout
workspace. A separate mktemp smoke directory fixes only that environment:
CLI version/doctor/status and exact clean-wheel checks PASS; native cloud-smoke
unit exit0. Doctor's x86_64 preview warning is retained, not platform certification.
No production source/test/assertion/config change. Failed logs are retained.

Frozen wheelbb7e357edfb4a20b09bcda64f5df18bcf59f096bdd290b7ba8010bb4645e52a0,
505506B;121 Python files match exact archive, all28 locked production dependencies
match Linux lock44cd3733…f335. Clean receipt SHA2cf920f905528a0e8a10d055b6c494065b6c54f3ecd226b6a39e8dfd7a2201ef.
Independent GPT-6.1 Sol xhigh LOCAL/OFFLINE helper rebind ACCEPT: eight helpers
match prior reviewed bytes after only nine evidence-root substitutions; six
Python/Bash syntax and39 auxiliary/path tests PASS. Manifest
e292787ba34e9f0b81b4ff2dad86f1d70a50b0dc84129fc8475753560d380982.
New deployment script5f66f31057e951f937b6bac188597695014b9631a2bdee1eb0ee13e1ce33e48e
retains old stopped/disabled, archive natural-drain, canonical locked identity
verification and recoverable old venv custody; independent LOCAL/OFFLINE
ACCEPT, no actionable findings. Reviewer corroborated actual cloud logs and exact
receipt: admission7cf518ad…d8044/continuation7e6c1cfa…0fb8c. Production deployment
is now started as a native oneshot; engineering/warm-up/Formal not yet started.
Docs2cacae6 exact CI37132398767 success; these docs are not replacement wheels.

## Actual corrected deployment and engineering checkpoint

Actual stopped deployment PASS at15:24UTC (native invocation4cca3edf…f6aad1);
canonical source23ea5c4/tree8e038727/wheelbb7e357e…52a0, identity
0ea93b2c15e35e949e3bd5dc4cb1c8cee96aa5544f5d51e6ace33ba3d965fbee.
Noneditable130 RECORD files/all28 locked dependencies/root-controlled canonical
venv/config/unit VERIFIED as bmdr. Old cf3909e9 venv retained in
`/opt/binance-market-data-recorder/venv-custody-q4-spot-idle-cf3909e9-j6lzEApv/venv`;
old identity retained in the new evidence scope. Archive restored, then its timer
stopped and existing worker waited to natural success before engineering audit.
Recorder remains inactiveANDdisabled/MainPID0/NRestarts0, all3868 chunks+archive
transactions LOCAL_DELETED/Catalogok/0partials; retained7612527frames/5842258936
uncompressedB. Rootfree29398269952B/archivefree2101791215616B after new build/custody.
Engineering baseline is RUNNING since15:26:10UTC, unit
`binance-recorder-q4-spot-idle-engineering-baseline.service`, invocation
5b66d9c272e846c8b165199bee434710. Both LIVE/completed verify still pending.
Native progress watchdog remains authoritative; economical GPT-6 Luna Max
read-only monitoring active. Warm-up/Formal NOT_STARTED, new Formal credit0.

## Completed engineering review and actual NONFORMAL preparation

Engineering rootd983a3bb6c8879fa1215a98ea0609088df60500df9b4f7b134041ff16189c996,
65834 records/138shards, both LIVE and completed verification PASS. Native journal
wall2055.550707s/CPU2029.937691s/cgroup502411264B/swap0. Independent guarded
LOCAL/OFFLINE reconstruction5.520s ACCEPT, exact source/identity/authority/proofs,
zero Raw/network/production opens/outside-private writes. Bundle17938937B
SHA3d9428d78cdf4cc2d2e2c226964b3bb410a71df7c805e9c224bb598ea8bafbbd;
[review](../reviews/2026-10-04-spot-idle-engineering-independent-review.md), report
63839ec374ae8ce0bb55d730d9e7df58e96ac9c41dbf1cc6d5cb329daa3136ba.
This baseline is an engineering predecessor, not the authoritative post-warm-up
root. Old gap metadata/687 flagged-incomplete manifests/32 zero-frame chunks/
eight paired arrays remain visible; no perfect historical capture claim.

Archive actual automatic trigger16:31:44UTC/natural worker success/finite future
deadline PASS. Recorder explicitly started16:28UTC for NONFORMAL preparation,
PID62625/invocation5125a437bdc9451a824937fbcfda3b16/NRestarts0. Strict core4 and
aux26/all14/four products PASS. Actual native warm-up starts16:33:27UTC, invocation
32732ddea5834e32b83fc3eacb7b72fd; start29.479943884s/immediate17.034715997s,
no blockers/pending/recovering. Normal/missed/catch-up remain; no FormalT0/credit.
Native7800s startup timeout/inner7200s diagnostic BOOTTIME deadline/ExecStopPost
always stops+disables Recorder. Read-only30s sampler e7323ef6…ce49052 active.
