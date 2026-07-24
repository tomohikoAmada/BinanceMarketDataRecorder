# ADR-0022: Spot public REST rate-limit containment

- Status: Accepted for M17 short-term validation
- Date: 2026-07-24
- Supersedes: Spot depth-snapshot transport portion of ADR-0008

## Context

The pinned official `binance-sdk-spot==10.0.0` successfully parses public depth
snapshots and successful response headers. Its pinned official common transport
maps HTTP 429 and 418 to `TooManyRequestsError` and `RateLimitBanError`, but the
exception constructors retain only an error message and code. The response
headers—including `Retry-After`—are discarded before Recorder can observe
them. Therefore an SDK-only caller cannot meet the stricter M17 requirement to
preserve and obey the actual response boundary.

An M17 diagnostic also demonstrated the harm: repeated `limit=5000` requests
(documented weight 250) reached 429 and then 418. The ban end was not retained,
so this M17 continuation makes no live Spot REST call. That containment is
evidence, not proof that the prior ban has expired.

## Decision

Use a minimal Recorder-owned HTTPS wrapper for only the unsigned public
`GET https://api.binance.com/api/v3/depth` call. It:

- hard-codes HTTPS, the official host, exact path, `BTCUSDT`, no credentials,
  and no redirect/account/order fallback;
- uses Python's standard-library TLS/HTTP stack and preserves exact response
  bytes plus an allowlisted set of headers;
- records requested `limit`, its documented weight, UTC/monotonic timing,
  status, headers, exact-body base64, and transport provenance in the Raw
  snapshot envelope;
- defaults to `limit=1000` (weight 50), not `limit=5000` (weight 250).

One process-shared IP limiter governs every Spot REST caller. It provides
conservative weight pacing and one on-wire request slot. One snapshot requester
also coalesces concurrent work for the same market/symbol. A 429 blocks until
`Retry-After`; a 418 blocks all Spot REST until the supplied boundary or
message ban timestamp. Missing 429 evidence fails closed for one minute;
missing 418 evidence fails closed for 24 hours rather than probing.

HTTP 5xx and transport timeout/OSError use capped exponential **full jitter**.
They never form a tight loop. Cancellation shields the finite-time worker, then
`wait_for_idle` reclaims it before a capture session restarts or exits.
Successful response headers update limiter evidence. Mock/fault tests consume
no public API quota.

The bootstrap diff buffer is separately bounded under ADR-0011. Exhaustion
stops that capture session and restarts connections plus snapshot after
backoff; it does not issue repeated 5000-level snapshots to chase WebSocket
traffic.

## Consequences

Spot snapshot provenance changes from
`binance-spot-depth-snapshot-provenance.v1`/`binance.spot.rest.v1` to v2. Raw v1
records remain valid immutable evidence. USD-M and other public REST methods
remain on the official modular SDK under ADR-0008.

The process-local limiter cannot coordinate unrelated processes sharing the
same public IP. Normal service locking and blue/green single-process ownership
cover Recorder instances, but other applications remain an operational risk.
Long-run validation must freeze configuration and watch returned weight
headers.

## Alternatives rejected

- Keep using SDK exceptions: cannot preserve or obey discarded
  `Retry-After`.
- Parse only an exception string: 429 headers are authoritative and may be
  absent from text.
- Rapidly retry weight-250 snapshots: already demonstrated unsafe and can
  escalate to a ban.
- Add a broad HTTP framework: one frozen unsigned endpoint does not justify a
  new runtime dependency.

## Rollback

Disable Spot snapshot/readiness rather than returning to uncontained retries.
If a future pinned official SDK exposes error status, headers, exact body and
cancellation lifecycle, re-evaluate it in a new ADR and deterministic fault
suite. Preserve every Raw v1/v2 snapshot and limiter event.
