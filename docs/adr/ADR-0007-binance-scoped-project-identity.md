# ADR-0007: Binance-scoped project identity

- Status: Accepted
- Date: 2026-07-22
- Milestone: M0.2
- Supersedes: ADR-0006

## Context

M0.1 deliberately removed the original consumer-oriented name and selected the
intermediate identity `CryptoMarketDataRecorder`. That improved the consumer
boundary but described the Recorder as exchange-neutral infrastructure. The
actual V1 product is specifically responsible for Binance public market data:
BTCUSDT Spot, BTCUSDT USD-M perpetual, their L2/trades/book-ticker streams,
public snapshots, defined USD-M auxiliary data, immutable Raw, archive,
integrity verification, normalization, and replay.

Naming the source explicitly is more accurate than presenting speculative
multi-exchange scope. Reasonable Spot, USD-M, storage, archive, replay, and
metrics module boundaries remain necessary; a framework for hypothetical
exchanges does not.

## Decision

Freeze the final identity:

| Surface | Value |
| --- | --- |
| Display name | Binance Market Data Recorder |
| Repository directory | `BinanceMarketDataRecorder` |
| Workspace | `/Users/amada/Documents/Development/Crypto/BinanceMarketDataRecorder` |
| Python distribution | `binance-market-data-recorder` |
| Python import package | `binance_market_data_recorder` |
| Future CLI | `binance-market-recorder` |
| macOS application data | `~/Library/Application Support/BinanceMarketDataRecorder/` |

The project is an independent, unofficial project. It is not affiliated with,
maintained by, sponsored by, or endorsed by Binance. “Binance” in the name only
identifies the public data source and APIs the Recorder connects to.

Do not use Binance logos, official visual identity, or wording/identifiers that
imply ownership, certification, partnership, maintenance, sponsorship, or
endorsement. Future service, launchd, and publisher identifiers must use a
reverse-DNS namespace owned or controlled by the project author. Do not use a
Binance-owned-looking root under `.com`, `.org`, or `.io`. This ADR intentionally
does not guess the author's final namespace.

V1 is Binance-only. It supports Binance Spot and USD-M perpetual modules and
does not add abstractions or milestones for unplanned exchanges. A future
exchange requires a separate architecture review and compatibility plan; it is
not a V1 acceptance criterion.

Consumer contracts remain generic. Alpha101Crypto is one optional external
consumer example: Recorder never imports or modifies it, Raw/Catalog/Manifest/
Replay are not specialized for it, and it cannot control Recorder internals.
M16 remains the generic consumer data-contract milestone.

Move the same repository rather than create another one. Keeping `.git`
preserves the complete sequence of M0, M0.1, and M0.2 decisions without rewriting
or pretending the intermediate identity never existed. The original commits
remain directly auditable.

## Consequences

- M1 must implement exactly the distribution, import, CLI, and application-data
  identities above.
- Release-facing documentation must carry a prominent unofficial/no-affiliation/
  no-sponsorship/no-endorsement disclaimer.
- Official Binance documentation and SDKs remain authoritative for API behavior
  but confer no official status on this project.
- Ubuntu storage portability and arbitrary downstream consumers remain valid;
  multi-exchange product scope does not.
- ADR-0006 and M0.1 remain historical evidence and cannot be read as current
  identity decisions.

## Alternatives rejected

- Keep the intermediate exchange-neutral identity: inaccurately broadens the
  current product and encourages unnecessary abstraction.
- Revert to a consumer-specific name: incorrectly ties infrastructure to one
  research system.
- Create a new repository or rewrite history: loses transparent M0/M0.1
  provenance.
- Use branding that resembles an official Binance product: misleading and
  outside the independent-project contract.

## Rollback

Before M1 publishes identifiers, revert the single M0.2 commit and move the same
repository back only with an explicit new identity decision. Do not rewrite M0
or M0.1, create a divergent repository, or migrate/delete production data.
After package/service identifiers exist, any rename requires a new ADR and a
compatibility/migration plan.
