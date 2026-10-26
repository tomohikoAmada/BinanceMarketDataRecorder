# ADR-0046: Owner minimum duration and strict actual stop

Status: future-only design concept independently accepted; exact implementation and new native qualification pending. October17,2026.

The owner asks for a repaired release to complete its own12h plus full saved-data and restored closeout. The old92099fbe run failed its explicitly fixed nominal endpoint and remains FAILED. Its elapsed43200.922028743s, failed observation and unqualified data are not rewritten or inherited. A reconnect log alone cannot prove READY at that endpoint.

For a future exclusive run,43200 seconds is the minimum qualified duration, with strict26/corePASS required at the actual stop. Only core/heartbeat recovery already allowed by the max60s-per-observed-episode policy may cross the minimum boundary. It retains the original first_bad deadline and fresh post-fault heartbeat, same incarnation, all resource/identity/clock checks and actual strictPASS. Healthy-core strict-only auxiliary failures remain fatal. A separate absolute minimum-target+60s bound prevents an unbounded finish. Existing runtime and quiet deadline are not extended.

The initial minimum-duration observation and every finish probe are retained separately; fixed30s nominal monitoring remains1440 slots, evaluated through nominal T0+43200 rather than the later strict stop. Publish both interval endpoints and honest outside/skipped/failed samples. No extra finish probe enters that denominator, no BLOCK becomes PASS, and no extra duration or next-stage credit is granted.

Forecast capture horizons and remaining time explicitly reserve the full60s worst case: prearm/launch45360 and43260, capture45060 and43260, prestart43860 and43260. Capacity floors and complete selected dualLIVE/completedverify/independent strict saved-frame criterion/restored handoff/receipt gates remain unchanged. Runtime source/wheel/config/deployment remain frozen. A genuinely changed production source would require its own release and duration.

This clarification is prospective under the owner's repair-and-new12h request, not a waiver for the old failed endpoint. No new run starts before exact private implementation/fixtures/independent bindings/current capacity and finite-window qualification. Stop before24h/48h.
