# ADR-0045: Sparse OI statistics use durable evaluated query windows

Status: accepted design; implementation and release qualification in progress.
Date: 2026-10-16 (Asia/Shanghai).

The October15 repaired-release12h attempt failed after6h37m. OI statistics
returned no requested rows; the generic dense-series cursor retried the same
start until freshness expired. A later recovered core connection could not
satisfy the unchanged strict auxiliary recovery check. The original run and
failure stay failed; this correction grants no retrospective duration.

The [official OI endpoint contract](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data#open-interest-statistics)
allows absent periods without subsequent backfill and defines timestamp as a
period end. Requiring an exact request-start row and continuous300000ms row
spacing was therefore an incorrect client invariant for this feed. The cause
of each original missing server response and transport closure is not proven.

Only `open_interest_statistics_5m` adopts these semantics:

- Preserve the entire official SDK model, exact timestamps and request bounds
  in Raw provenance. Do not round, forward-fill or invent a row for an absence.
- Query only closed aligned windows with API limit500. Fewer than500 returned
  rows establishes evaluation of that bounded interval, including a valid
  empty interval. Identity, field and response-range checks remain mandatory.
- A500-row response may be truncated. Persist it and incomplete-range evidence,
  keep the cursor and success unchanged, halve the per-poller window ceiling
  and retry through the existing bounded loop. Saturation of one window stays
  an explicit failure; no finer pagination or new service is introduced.
- Raw drain/fsync precedes symbol-scoped `SIDE_DATA_RANGE_EVALUATED`; an empty
  response also retains `SIDE_DATA_EMPTY_RESPONSE`. Both precede cursor and
  successful-poll publication. Evidence binds the real request connection,
  interval, record count and completeness. An evaluated interval does not
  establish exchange-event completeness or a record at every nominal period.
- For OI, `last_persisted_period_timestamp` is the start-label in milliseconds
  of the last fully evaluated closed **query window**. The end is derived from
  request bounds, never by relabelling the response's endtimestamp. Actual
  first/last response timestamps stay separate in source metadata.
- Each reconstructed poller first re-evaluates its existing cursor window.
  Legacy limited responses establish a row rather than complete-window
  coverage. This conservative boundary reread is retained as another genuine
  request observation, then normal cursor+five-minute progression resumes.

The other five periodic feeds retain their dense-series/empty-retry contract.
Storage failure, HTTP failure and incomplete responses cannot create cursor or
success progress. Existing strict startup/target/recovery, resource bounds,
all14 features/four products and complete saved-data audits remain unchanged.
No historical Raw or derived dataset is rewritten. This source correction
requires a new reviewed CI/wheel/deployment identity and its own2h then12h;
8f's accepted2h does not transfer to the corrected artifact.
