# Q5 running error review — October 4, 2026

Read-only inspection at 05:32–05:36 UTC / 13:32–13:36 Shanghai. This answers
the owner's request to identify other errors during the current Formal12h.
It does not alter the running service, configuration, controls or acceptance policy.

## Scope and evidence

Sole stage `12h-1c68e5eaf9724cfbb26e4f5c904786f6`, T0
01:53:49.145836970 UTC; target 13:53:49 UTC / 21:53:49 Shanghai.
Evidence root:
`/srv/recorder-data/recorder-archive/evidence/Q5-formal12h-attempt3-20261004-6wnh0ts_`.
Read the Recorder, archive, native observer and resource-sampler journals since
T0; the monitoring JSONL; stage samples; and a query-only SQLite connection.
No full Raw scan or terminal audit was launched.

Current Recorder PID86133/NRestarts0, observer PID86184 and sampler PID86182
remain active. Recorder remains enabled. Config SHA
`5b73db1b6ba6ac8cc7ab84cbcf0c7b688187b316644bef3df44bbb42f9283419`
and deployment identity remain unchanged.

## Runtime errors and data impact

The selected journals contain exactly two structured WARNING events, both
`usdm_side_rest_failed` / `NetworkError` for BTCUSDT and ETHUSDT open_interest.
No additional WARNING/ERROR/CRITICAL was found. The separate journald-service
query returned no messages during this interval. The two missed observations
and verified recovery timestamps are documented in the
[coverage review](2026-10-04-data-coverage-gap-review.md#confirmed-q5-polling-misses).

At monitoring cutoff 05:33:15.714697 UTC, all438 **in-period** monitoring rows
have auxiliary PASS/core_ready=true, with no transient recorder-read failures.
The only nonzero auxiliary failure counters are those two OI counters, each1;
all inspected malformed counters are0. Four products are READY, with their
three core streams connected, synchronized books, and no product failure.

Catalog queries found zero operational events since T0, including no new
recorded stream discontinuity, terminal service failure or disk emergency stop;
zero new quarantined artifacts and zero new storage-alert events.
These observations do not certify absence of every inter-sample failure.

## Other visible warnings and pending work

**Capacity forecast:**419 of those438 monitoring rows report CRITICAL and19
report WARNING. The CRITICAL state is caused by the forecast hard-reserve ETA
being within72 hours, rather than actual free space crossing the14GiB threshold.
The state first changed to WARNING at13:23:44 Shanghai, briefly returned to
CRITICAL at13:31:15, then returned to WARNING at13:31:45.
Minimum sampled writer free space was29324390400B (about27.31GiB), above the
10GiB hard reserve; actual_hard_reserve_reached remained false. Archive free
space at the cutoff was2100616093696B. The alert warrants continued reserve
forecasting and fresh terminal-audit peak-space admission; it did not stop capture.
See the policy in `storage/capacity.py` and the forecast selection in
`storage/forecast.py`. Forecast status is distinct from a CRITICAL log event.

**Five pending delta observations:**source samples3/9/16/20/33 each retained
one SEALED transition as pending, with no Raw unit and no archive companion
in that frozen snapshot. All five next samples, approximately300 seconds later,
acknowledge the same transition with a Raw unit and archive companion;
delta_pending=false and blocking_findings=[] in each. This is consistent with
the archive advancing between the frozen snapshot and Raw qualification.
The observer preserved pending work instead of advancing an unverified cursor.
The evidence establishes recovery by the next observation, not the exact
duration of the temporary state. See `service/acceptance_v5_raw.py:qualify_unit`.

Samples0–43 inspected as a corpus were all complete with no blocking findings.
The subsequent latest sample44 (05:33:59.925346 UTC) is also complete,
delta_pending=false, blocking_findings=[], with no integrity findings or
COPYING/VERIFYING/VERIFIED/LOCAL_DELETE_PENDING backlog. Its SHA-256 is
`f8a0f1616ac6f9ea7583aa2d81765a6b0da56b63a285fcd7e15197d5e7584212`.

## Archive and qualification limits

The inspected195 completed archive-drain results all ended BACKLOG_EMPTY,
with zero failed transactions and zero remaining backlog. Maximum drain wall
time was5.998240124 seconds. A subsequent Catalog snapshot had3890 archive
transactions created since T0, all LOCAL_DELETED, with none having attempt_count
greater than1 or a retained last_error. This confirms the inspected archive
lifecycle results; full terminal readback audits remain pending.

The two failed preparation attempts occurred before the sole T0 and grant no
Formal credit. They are not additional errors within this running12h interval.
Q5 remains RUNNING. Actual target, strict endpoint, full terminal audits,
completed verification and independent restored closeout are still required.
No every-event completeness or final12h acceptance is claimed by this review.

## Updated inspection — 08:07 UTC / 16:07 Shanghai

Read-only journal, monitoring, Catalog and native-stage inspection now finds
four logical anomalies, represented by six WARNING logs; no ERROR/CRITICAL
log entries in the selected units. Compared with the earlier checkpoint,
two USD-M book_ticker unexpected disconnects are new:

| Product/stream | Failure time, Shanghai | Recorded gap start | First-new-frame gap endpoint | Recorded gap duration |
| --- | --- | --- | --- | --- |
| BTCUSDT book_ticker |14:22:40.666751|14:22:40.667262215|14:22:41.242948434|0.575686219s|
| ETHUSDT book_ticker |15:58:05.284276|15:58:05.284489055|15:58:06.702539822|1.418050767s|

Both disconnects log ConnectionClosedError and one recovery WARNING. Catalog
retains STARTED/COMPLETED pairs with unexpected_disconnect, UNRELIABLE,
sequence_gap and historical_continuity_restored=false. BTC gap ID
`134de05a-5c80-42ed-a05c-ab6f9904f8ca`; ETH gap ID
`0eeb6985-4c44-4107-860e-9086611153b1`. COMPLETED records the first new-generation
frame after durable sync; it does not backfill missing historical updates.
The durations above are the recorded gap bounds, not proof of exact exchange-event
loss duration or count. Underlying close cause is not established beyond the
logged exception. Both boundary records show queue_wait_count=0, so these
records do not establish receipt-queue saturation as the cause.

The two earlier OI NetworkErrors remain the only auxiliary failure counters,
each1; no further OI or typed-empty retries were found. All745 in-period
30s monitoring rows through16:07:06 Shanghai report PASS/core_ready=true,
with no recorder-read failures. All75 native samples0–74 are complete and have
blocking_findings=[]; latest sample74 at16:03:59.951052 has delta_pending=false,
integrity_findings=[] and zero observed archive backlog. Its SHA-256 is
`d4b70e329b4924123168719c5eb025a6fc6261da2493255ed4d2a8f381b40706`.
Nine samples have transient delta_pending, including new47/54/56/73;
the latest sample is caught up. These sampled PASS results do not erase the
two recorded WebSocket discontinuities or waive any frozen observed-check gate.

All330 inspected completed archive drains ended BACKLOG_EMPTY with zero failed
transactions. A Catalog snapshot had6574 new archive transactions since T0,
all LOCAL_DELETED, none retried or retaining last_error; zero new quarantines.
The only four new operational events are the two discontinuity pairs.
Recorder remains RUNNING/PID86133/NRestarts0 with four READY products; observer
and sampler remain active. A natural archive worker was running during inspection.

Current capacity state is CRITICAL by forecast, writer free29267361792B
(about27.26GiB), actual_hard_reserve_reached=false, forecast hard-reserve ETA
October6 08:46:13 UTC. This is a capacity warning, not a CRITICAL log failure.
Target and terminal artifacts are still absent; Q5 remains RUNNING with final
audits and independent closeout pending. No service restart or control change
was performed by this inspection.

## Updated inspection — 12:05 UTC / 20:05 Shanghai

Read-only checks completed20:05:42 Shanghai. The journals since T0 still contain
the same six WARNING logs for four logical anomalies: two OI NetworkErrors and
two book_ticker ConnectionClosedErrors with recovery warnings. No additional
WARNING/ERROR/CRITICAL log entry was found compared with16:07. Catalog still
contains only the same two completed discontinuity pairs since T0, zero new
quarantines, and no terminal failure or disk-emergency event.

All1221 in-period monitoring rows through20:05:36.333329 Shanghai report
PASS/core_ready=true with zero recorder-read failures. The only nonzero auxiliary
failure counters remain BTC/ETH OI, each1. Current Recorder PID86133/NRestarts0
and four READY products are unchanged; observer and sampler remain active.
All541 inspected completed archive drains report BACKLOG_EMPTY with zero failed
transactions. The Catalog snapshot contains10771 archive transactions created
since T0, all LOCAL_DELETED, with zero retries or retained last_error.

Latest native sample122 at20:03:59.990446 Shanghai has blocking_findings=[],
delta_pending=false, integrity_findings=[] and zero observed archive backlog.
SHA-256 `c14842038467c5f3e2b6dff6d5646876653bab1d7429fe3a1391c2101818e3cb`.
Current capacity forecast remains CRITICAL, with actual free29241167872B
(about27.23GiB), actual_hard_reserve_reached=false and forecast hard-reserve ETA
October6 14:44:04 UTC. This alert has not caused a stop. Stage-start is still the
only non-sample stage artifact; target/final are pending until the known target
and required terminal verification. No restart, new T0 or control mutation was made.

## Updated inspection — 13:13 UTC / 21:13 Shanghai

Read-only checks completed21:13:55 Shanghai. No new anomaly since20:05:
the selected journals still contain only six WARNING logs for the same two
OI NetworkErrors and two book_ticker disconnect/recovery pairs, with no
ERROR/CRITICAL log entry. Catalog retains the same two completed discontinuity
pairs, zero new quarantines, and no terminal failure or disk-emergency event.

All1357 in-period monitoring rows through21:13:45.113384 Shanghai report
PASS/core_ready=true with zero recorder-read failures. BTC/ETH OI each retain
failure count1; other auxiliary failure/malformed counters remain0.
Recorder PID86133/NRestarts0/four READY products and the active observer/sampler
are unchanged. All602 inspected completed archive drains ended BACKLOG_EMPTY
with zero failures. The Catalog snapshot contains11978 new archive transactions
since T0, all LOCAL_DELETED, with zero retries or retained last_error.

Latest native sample135 at21:08:59.999797 Shanghai has blocking_findings=[],
delta_pending=false, integrity_findings=[] and zero observed archive backlog;
SHA-256 `fb190709c8f79e233a08c82834ba6df38f12fb68428419370393afb5467557f5`.
Current capacity forecast remains CRITICAL, actual free29230686208B
(about27.22GiB), actual_hard_reserve_reached=false. Target/final remain absent;
the sole target is still21:53:49 Shanghai. This inspection grants no final12h
acceptance and made no production mutation.
