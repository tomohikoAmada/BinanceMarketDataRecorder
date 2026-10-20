# ADR-0048: Independent core recovery and auxiliary publication readiness

Date: 2026-10-19. Status: exact core/aux implementation, independent review, Linux qualification and dualCI remain accepted. The fresh24h failed October20 after38130.113168833s with a different heartbeat/writer-backpressure failure; no24h acceptance. Actual failed-run restoration is independently accepted; current source/metrics diagnosis is pending. See [current qualification](../milestone_acceptance/R2-owner24h-corefix-20261019.md).

The owner asked to diagnose the failed24h, make the smallest stability correction,
and start a separate new24h through complete saved-data and restored acceptance.
This decision applies prospectively. Failed run6bacd298 remains failed at
28295.688676650s; no old duration, window, target or data is transferred.

## Evidence and cause

The USD-M ETHUSDT book_ticker disconnected at09:00:42.704902UTC and reconnected
at09:00:44.956835UTC. The core was READY in the09:00:49.987 observation with a
post-fault heartbeat. BTCUSDT taker_buy_sell_volume_5m was RETRYING with
EmptySideDataResponse, last successful capture only46.086s earlier and a cursor
one five-minute period behind. The existing capture auxiliary gate was PASS;
its strict counterpart was BLOCK. CoreRecovery nevertheless required that strict
counterpart immediately and terminated the controller at09:00:50.231UTC.
SIGTERM09:00:52.620 followed this exception; the production side poller did not
stop the core by itself. Full current286 Recorder/15 capture journal rows and
10 diagnostic publications were independently reviewed without Raw reads.

Typed empty means no rows inside the requested interval. A full response may
contain only its allowed leading overlap; HTTP [] is not established here.
The official [taker endpoint](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Taker-BuySell-Volume)
describes five-minute periods, period-start timestamps and bounded historical
queries, but promises neither dense rows nor immediate publication latency.
We have not proved a request-boundary defect or the deeper transport closure
origin. Do not treat this like sparse OI and advance over unseen taker periods.

## Decision

CoreRecovery closes an observed core/heartbeat episode when core is READY,
the publication heartbeat is post-fault, and the **existing capture auxiliary
gate is PASS**. That existing gate already accepts a typed five-minute empty
retry only with enabled/running context, a previous accepted capture, recent
last success and a bounded, correctly scoped cursor. It continues rejecting
old/future/mismatched cursors, stale/absent captures, generic/schema/catalog
errors, failed/stopped contexts and disconnected WebSocket auxiliaries.

Do not add a new grace period, timeout, forward-fill, cursor skip, source flag,
or production deployment. The original first-bad60s bound, collection latency,
incarnation/boot/clock/resources/owned current+peak swap checks are unchanged.
Independent allowed publication waiting cannot cause an otherwise recovered
core to be shut down by a stricter unrelated intermediate predicate.

Strict warm-up, start and actual stop still require all26 contexts RUNNING and
core READY. ADR0046's minimum duration/original episode60s/absolute minimum+60s
finish constraints remain. Nominal monitor failures/probes retain their original
meaning. Both complete released LIVE passes, completed immutable verification,
strictly>99.9% complete saved-frame denominator, independent control review,
actual restore-beforecancel, normal archive/future finite monotonic deadline,
fresh VERIFY/stopped-disabled same-identity handoff and owner receipt remain
mandatory. A new scope/window/strict warm/T0 must earn its own86400s.

Production sourceca760c09/wheelc2eeb219/identity7276c9fa/config5b73 and all14/four
products remain frozen. R3 source in main is offline-only and is not deployed.
The accepted own2h and own12h remain valid; no unchanged release or old audits
are repeated. All failed data, prior software and unrelated edits are retained.
