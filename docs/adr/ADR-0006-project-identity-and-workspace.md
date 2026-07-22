# ADR-0006: Project identity and workspace

- Status: Superseded by ADR-0007
- Date: 2026-07-22
- Milestone: M0.1
- Supersedes: the project-identity and consumer-specific wording in ADR-0001;
  ADR-0001's independent-repository decision remains accepted

This body is retained as historical M0.1 evidence. Its identity and
exchange-neutral conclusions are no longer current and must not guide M1 or
later work.

## Context

M0 created the repository with the display identity `Alpha101CryptoRecorder` at
`/Users/amada/Documents/Development/Alpha101/Alpha101CryptoRecorder`. That name
incorrectly implied that a general stateful market-data service belonged to, or
was specialized for, the separately audited Alpha101Crypto research project.
The M0 commit is
`1634b09e57d287eba82ef34f117b4657979cc38b`.

The service's capabilities—lossless public market-data capture, immutable Raw,
Catalog/manifests, replay, normalization, archive, and operational reporting—are
functional infrastructure usable by many research, backtest, and monitoring
consumers. Binance is merely the first exchange integration.

## Decision

Freeze the identity as:

| Surface | Value |
| --- | --- |
| Display name | Crypto Market Data Recorder |
| Repository directory | `CryptoMarketDataRecorder` |
| Workspace | `/Users/amada/Documents/Development/Crypto/CryptoMarketDataRecorder` |
| Python distribution | `crypto-market-data-recorder` |
| Python import package | `crypto_market_data_recorder` |
| Future CLI | `crypto-market-recorder` |
| macOS application data | `~/Library/Application Support/CryptoMarketDataRecorder/` |

Use functional and domain naming because the project records crypto market
data; it is not owned by a research model or consumer. Binance remains an
adapter because venue-specific endpoints, sequences, and schemas should not
become identities or assumptions of Raw framing, Catalog, archive, normalized
lineage, replay, or generic consumer contracts.

Python distribution names use hyphens by packaging convention and for readable
command/package metadata. Python import identifiers use underscores because
hyphens are not valid Python identifiers. The CLI uses an explicit functional
hyphenated command distinct from the import package.

Consumers cannot influence the core in reverse. A consumer may adapt generic
datasets/replay to its own matrices or models, but Recorder never imports its
modules, uses its identifiers as canonical core schema, or makes it a V1 gate.
Alpha101Crypto is at most one optional external example.

Move the existing repository rather than initialize a new one. This preserves
the complete `.git` object database, root M0 commit, acceptance evidence, and
future ability to audit the correction as a separate commit. No history is
rewritten: M0 remains reachable by its original SHA, and M0.1 records only the
workspace/identity/boundary correction.

## Consequences

- M1 must publish the exact distribution/import/CLI identities above.
- launchd labels, log/config/service identifiers must use the new functional
  identity when designed in later milestones.
- Current V1 remains Binance BTCUSDT Spot/USD-M, while exchange-neutral core
  contracts and adapter boundaries permit future venues without promising them
  in V1.
- Historical migration records may quote the former identity/path; no current
  project surface may use them.
- Production data remains outside both repository and workspace parent.

## Alternatives rejected

- Keep the former identity: falsely couples infrastructure to one consumer.
- Name the whole project after Binance: replaces consumer coupling with venue
  coupling and weakens adapter boundaries.
- Create a fresh repository at the new path: loses simple commit ancestry and
  makes M0 provenance harder to audit.
- Rewrite M0 history: destroys the immutable evidence the correction is meant
  to preserve.

## Rollback

Revert the M0.1 commit and move the same repository directory back only if no
later milestone depends on the frozen identifiers. Never create divergent Git
histories or migrate/delete production data as part of a source-workspace
rollback. After M1 publishes package/CLI identity, any further rename requires
a new ADR and compatibility/migration plan.
