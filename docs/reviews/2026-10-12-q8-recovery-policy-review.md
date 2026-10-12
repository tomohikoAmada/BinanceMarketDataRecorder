# Q8 sampled recovery policy correction review

Verdict: **ACCEPT exact private implementation**, not an accepted twelve-hour run.
Reviewer: independently authorized GPT-6.1 Sol xhigh, LOCAL/OFFLINE/read-only;
no VPS/network/Raw/production connections. Production source/wheel unchanged.

## Exact evidence and finding

Full failure export has50 verified members; manifest
`f4ae0578195e101f3ac9b4f874165d6da9acb3824ab33ec7248ef34fce6c1c41`.
The1384-row replay reproduces `core_heartbeat_recovery_budget_exhausted` on the
already recovered final row. Three observed intervals total89.970415412s; a60s
cumulative rule terminated the run at41490.050143783s. No own43200 target/full
terminal audit/saved-frame denominator exists. Closed partial1381/1384 monitor
PASS is an availability metric. Old failure/restoration and every Raw/archive
member remain retained; there is no retroactive PASS.

The same-read state shows three temporary book_ticker connection losses, all
with fresh heartbeat and the same invocation/PID/boot. Recorder logs reconnect
in0.609251s,1.625364s,4.280392s nearby; these are log intervals, not readiness or
exact outage measurements. Missing close-code/reason prevents definitive
Binance-versus-route-versus-client attribution. RSSmax357634048B,
host availablemin4506341376B, cgroup swap0, host swap counters unchanged and
disk above bounds do not establish a hardware bottleneck. No CPU throttling was
observed in the retained resource interval.

## Accepted minimal correction

[ADR-0042](../adr/0042-per-episode-core-recovery-in-owner-tests.md) keeps60s per
observed episode with strict fresh same-incarnation recovery, removes cumulative
termination, separates adaptive5s diagnostics from fixed30s availability rows,
and retains all hard gates/full saved-data checks. Two independently discovered
timing defects were corrected before acceptance:

- A65s delayed sample originally caused immediate catch-up writes and an invalid
  monitoring stream. It now writes at65/90/120/150s in the toy180s run, discloses
  skipped slots, retains undefined/INVALID availability and still dispatches audit.
- A recovery probe155→185s, or a regular closing observation150→180.5s, must not
  extend that toy180s target. Both retain the late observation and fail before
  owner-target or terminal launch.

Independent ValueError/KeyError/TypeError/OSError monitor-evaluation cases retain
INVALID/BLOCK/no percentage and still dispatch the full saved-data auditor after
a valid own-duration strict target. There is no fabricated monitoring PASS.

Accepted controller SHA
`0e5fa1e8a1046059a87fa224d16108ac4f001f3d9fc7656ccbd2647022b58043`;
helper SHA `dfb252a63fcb5e511e1cfb967bee6609f97c8b1f2936c91eee78d3c03db4f844`.
Review report SHA
`8fb4979fcf4580a8578781708d0e7a65cd9d1b22ad44f7ce0869ffffb0d613e3`;
output manifest SHA
`94ac0797e4634936664d34f5d3c80b90c3d23b762b61c957870ec6c60de21b75`.
Private report `/private/var/tmp/q8-recovery-review-j38i_h6a/REPORT.md`.

## Validation and limits

Independent12 unit assertions+23 diagnostic+15 integration+7 timing/evidence
cases PASS. Repo focused28 tests/Ruff/mypy PASS. Exact final23+15 fixtures PASS
again on actual VPS Ubuntu/Python3.12.3 in
`/var/tmp/bmdr-q8-recovery-final-fixtures-20261012-w1zha7gl`, service/socket mocks
only. Complete copy/readback/stdout controls are LOCAL
`/private/var/tmp/bmdr-q8-recovery-fix-20261012-9kqvnuov`.

No production release rebuild, real window, T0 or twelve-hour credit results
from these fixtures. Fresh bound scope/capacity/native start qualification and
full data/restored closeout are required. PRODUCTION_READY=NO/R-078 OPEN;
canonicalV5 final/predecessor and24h/48h eligibility remain false.
