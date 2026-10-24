# R2 bounded stream-close diagnostics

Current: **IMPLEMENTED / LOCAL QA COMPLETE / INDEPENDENT REVIEW ACCEPTED / EXACT DUAL CI PASSED / DEPLOYED /30MIN OBSERVATION COMPLETE / SEPARATE2H COMPLETE / FINAL REVIEW ACTIVE**.
Base is the exact released sourcec3686dd76406c8d483b23eb10417349e5a1ffecd,
not main's unrelated R3 code. New canonical20e57/wheeldd2c/identity9b8ce821 deployed;
config5b73/BTC+ETH/all14 unchanged. Both30min and separate2h observations completed; actual2h final review active, no formal T0.

N2 completed its own86400.144191527s. Final fixed monitoring2861/2880
(99.3402777778%) is below the owner99.5% threshold:17 book_ticker disconnect
samples and2 OI network errors. This does not establish the VPS hardware maximum.
Native full new-data double LIVE/completed immutable verification returned
62687921 saved frames100%; final independent controls are reviewed separately.
The recorded exception class alone cannot identify the initiating transport cause.

- [x] Add one shared field extractor to the two existing Spot/USD-M unexpected
  disconnect logs: received/sent Close code and bounded reason, their order,
  opening/established phase, connection age at log time, immediate cause class
  and OS errno. Missing frames remain null. Age includes cleanup; it is not an
  exact disconnect timestamp or outage duration. Do not infer a unique cause
  from a code alone.
- [x] Keep retry/backoff, ping, routes, queues, raw bytes, freshness/cursors,
  archive authority and stop behavior unchanged. No new service or dependency.
  Reasons are bounded; URL-bearing reasons are redacted. No exception-message,
  headers, proxy URL or raw payload dump.
- [x] Focused30 offline cases PASS, Ruff PASS and mypy310 files PASS. Tests cover
  both real reconnect log callsites while preserving exact raw-payload/gap checks,
  opening failure, sent/received/order/null, OS causes and bounded/redacted reason.
- [x] Full local suite:2146PASS/24SKIP/5DESELECTED plus one M0 workspace-path
  failure because the worktree override was omitted. Original failed log retained.
  Existing CI=true/M0_CONTRACT_ROOT mode affected-only recheck1PASS; no source
  change and no claim of one clean2147-test run. All2147 selected offline tests
  are covered across full run and affected recheck.
- [x] Independent small offline faults10PASS through both real collector callsites:
  actual websockets15.0.1 peer close/echo ordering, unframed EOF, delayed cleanup,
  opening failure/privacy and exact synthetic raw-byte preservation. Code-review
  artifacts55b7cca5/resultcc93f898/manifest99f39af4 are frozen; all manifest
  members and six exact input bytes checked. No stability/root-cause/capacity credit.
- [x] Independent code review ACCEPT for diagnostics only, no actionable findings.
  [Report](../reviews/2026-10-24-r2-stream-close-diagnostics-report.md).
- [x] Exact-head dualCI40005007941 BOTH SUCCESS: both actual checkout20e57f3;
  macOS2147PASS/24SKIP/5DESELECTED, Ubuntu2143PASS/28SKIP/5DESELECTED;
  Ruff/mypy310/M0/Go/build/clean wheel CLI allPASS. PR85 actually merged73fa05fb
  at October23 19:54:13UTC. Production release remains exact20e57, not whole mainR3.
- [x] Isolated source729ae411/cloud83PASS/full actual supply-chain acceptedd8105977.
- [x] Actual stopped deployment and complete independent reconstructionb329e35e
  accepted; all20 commands success/installed124/locked28/config unchanged/old venv
  preserved/normal archive restored. Recorder inactive AND disabled/PID0/NRestarts0.
- [x] Separate short1800s controls finally ACCEPTa0b92c29/33 isolated cases;
  genuine Linux import-clock smoke/copy97a0a632 passed.
- [x] Single nativec344d006 dispatched23:52:40UTC, initial activating/start;
  normal OS/archive owners remain enabled, zero formal/capacity duration.
- [x] Actual copy/Linux smoke/request binding independently ACCEPT061ce832;
  only initial native state is accepted, no completed observation.
- [x] Actual30min observation and normal cleanup independently ACCEPT40593125/
  result2ef6c70e/manifest0ad6e075;352 publications and bounded journal rebuilt.
  No unexpected WebSocket close was reproduced; one auxiliary REST warning
  recovered. This cannot explain the old WebSocket interruption or prove a fix.
- [x] Separate2h exact delta564086 ACCEPT7eedc9a8/8 isolated lease cases;
  actual copy2f72c6c0 and initial nativee33cf116/dispatch27270710 bound.
- [x] Actual physical/initial effective binding independently ACCEPT99b539a0/
  result077ae5b9/manifest5ba6baac; no Recorder health or completed-observation claim.
- [x] Once-checkpoint new2h metadatae805efd9/result81918b64 COMPLETE1413rows,
  normal cleanup357cd170 and manager completion; no WS close reproduced.
- [ ] Independent actual2h reconstruction active LOCAL/OFFLINE; all3 network
  warnings/39 typed empty warnings and diagnostic gates stay honest. No new24h
  or proven stability repair; no extra VPS/read/Raw audit or old review.
- [ ] Choose any stability repair from actual evidence and qualify it, then a
  fresh N2 own24h/full data/restored acceptance before N4/8. Do not transfer
  N1/old-source duration or accept the failed monitor from a successful data audit.

This is a diagnostic change, not a demonstrated stability fix. Capacity/headroom
and initiating transport cause remain unresolved;48h PENDING/R078 OPEN.
All existing data, failed evidence, venvs and worktrees remain preserved.

API semantics follow the pinned [websockets15.0.1 exception documentation](https://websockets.readthedocs.io/en/15.0.1/reference/exceptions.html):
received and sent frames are separate, and order describes their sequence.
An abnormal close or an incomplete close handshake does not by itself identify
whether exchange, transport or local scheduling initiated the interruption.
