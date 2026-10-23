# R2 bounded stream-close diagnostics

Current: **IMPLEMENTED / LOCAL QA COMPLETE / INDEPENDENT REVIEW ACCEPTED / EXACT DUAL CI PASSED / NOT DEPLOYED**.
Base is the exact released sourcec3686dd76406c8d483b23eb10417349e5a1ffecd,
not main's unrelated R3 code. No new wheel, deployment, identity, window or T0.

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
- [x] New isolated release source archive729ae411 and seven-file helper design
  prepared; independent review active, no physical copy/cloud tests yet.
- [ ] Only after release qualification: stopped deployment and a bounded actual
  diagnostic collection to obtain the missing close/transport evidence.
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
