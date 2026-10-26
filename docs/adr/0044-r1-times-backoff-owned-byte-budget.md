# ADR-0044: R1 timestamps, capped backoff and owned-byte ingress

Date: 2026-10-14. Status: source correctness independently accepted; exact CI/release and new VPS qualification pending.

USD-M diff depth and bookTicker document `T` as transaction time; aggTrade
documents `T` as trade time. The old USD-M core extractor wrote all three into
the trade slot. Core capture now declares `binance.usdm.websocket.v2` and maps
the three streams separately. Event `E` remains event time. Required/invalid
timestamp handling retains exact Raw bytes and marks malformed metadata; invalid
USD-M core normalized rows have null exchange times rather than falling back
to legacy wrong-slot or out-of-int64 metadata.
millisecond metadata must fit the signed int64 normalized columns. Zero is a
present timestamp, not missing. The shared Contracts fixture is copied with
its original source URL/check date and SHA; there is no new runtime dependency.

Raw/EventEnvelope/chunk contracts remain v1. Existing Raw and normalized
artifacts are immutable. New Normalize builds use `normalized-dataset.v2` and
derive valid USD-M core `E`/`T` directly from the payload, including legacy
`binance.usdm.websocket.v1` Raw. Effective times also participate in logical
hashes. Dedup stays v2; physical stream schemas stay v1. v1 and v2 builds have
different identities/directories even with identical Raw sources. Replay's
default catalog selects v2; `ManifestCatalog(root,
dataset_version="normalized-dataset.v1")` explicitly opens unchanged legacy
builds with their original clock semantics. Catalogs do not mix versions or
silently rewrite old results. No historical production rederivation is part
of this repair or its new-data-only VPS qualification.

Reconnect and snapshot FullJitter delays now double only while below the
configured cap. Failure counts of 1025 and much larger do not evaluate an
overflowing exponent. Existing finite delays, jitter and stop behavior remain
unchanged; there is no new retry policy.

Each WebSocket collector keeps its existing receipt-count bound and adds a
64 MiB payload-byte budget. The charge survives dequeue and covers the entire
in-flight Raw writer batch, durable gap completion and envelope callbacks.
Exact item completion releases the original charge; failed/cancelled writers
do not advertise successful completion. Budget admission is atomic on the
event loop. Waiting producers use the existing timeouts, stop/writer-failure
checks and post-close boundary handoff; persistent saturation retains existing
Gap evidence rather than silently discarding frames. Stats publish queued,
owned and peak payload bytes. This is not a total-process RSS guarantee:
decoder/envelope overhead, the current receiving frame and transport buffers
are separate. The installed websockets assembler's high/low watermarks move
from 32/8 to 4/1; its 16 MiB message limit remains. No global coordinator,
configuration migration or new dependency is introduced.

Qualification includes persisted Raw → Normalize → Replay with distinct E/T,
zero/missing/invalid times, legacy artifact/hash preservation, extreme backoff,
weighted cancellation/writer failures and post-close handoff, legal large
frames with slow writers in both markets, and the real fragmented 16 MiB
transport path. Full offline/static/M0/Go gates, independent review, exact CI,
new locked wheel/cloud checks and canonical stopped deployment precede new
own-duration 2h and 12h tests. Old accepted durations remain attached to their
old production identity. All 14 auxiliary features, four products and prior
production data/evidence/venvs remain preserved.
