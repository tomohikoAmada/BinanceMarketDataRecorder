# Offline Workspace

Status: approved future architecture; not implemented as a dedicated daemon or
physical disk.

The Offline Workspace is a local logical boundary for archive custody, derived
data, historical imports, Catalog recovery evidence, and discovery metadata.
It may span several physical devices and may use macOS, Linux, or Windows.
It is not one filesystem, one service, or one exclusive volume.

## Authority model

| Role | Contents | Authority | Rebuildability |
| --- | --- | --- | --- |
| Cold Archive | Recorder Raw and immutable manifests | Highest-value local market-data authority | Not derived; preserve artifacts |
| Normalized Dataset | Parquet and build manifests | Derived consumer input | Rebuildable from verified Raw/manifests |
| Historical Archive | Official Binance imports and provenance | Separate archive-source authority | Redownloadable/reverifiable while upstream exists |
| Catalog Backups | Post-session SQLite snapshots | Operational/control-plane recovery evidence | Reconstructability is not guaranteed from Raw alone |
| Archive Set metadata/index | Media and artifact discovery | Media-local metadata is durable; global index is convenience | Global index rebuildable from media |

## Cold Archive

The Cold Archive is the canonical long-term store for Recorder Raw and
manifests. Raw bytes remain exact, immutable after seal, and retain exchange,
receive, sequence, gap, and provenance evidence. It may span an Archive Set,
but each chunk remains whole on one physical medium. A verified local archive
copy can become the only copy after VPS deletion; it is not automatically a
redundant backup.

Media-local identity and manifests must remain interpretable without the local
workspace index. The global index may accelerate discovery but cannot be the
sole authority.

## Normalized Dataset

Normalized Parquet is a versioned, immutable, derived output for Replay,
analytics, Projection, and future visualization. It carries Raw lineage and
gap state but never becomes Raw authority. It is normally kept in the local
workspace and V1 does not require it to participate in the multi-media Archive
Set. A failed or incomplete source prevents publication of a complete build.

## Historical Archive

Historical imports from official Binance archive sources have separate source
revision, checksum, and archive-source clock semantics. They never masquerade
as live Recorder Raw and are never silently joined to receive-time data. V1
does not require Historical Archive artifacts to participate in the Archive Set.
They remain redownloadable and reverifiable only while the upstream source is
available.

## Catalog Backups

The VPS has one live Catalog. After a successful archive session, a
SQLite-supported consistent post-session snapshot is transferred to the local
workspace and verified. V1 retains at least `latest` and `previous`; unbounded
retention and automatic daily offsite snapshots are not required. A snapshot
failure is retryable and does not undo verified Raw archival or deletion.

Catalog backups preserve operational state such as lifecycle, archive, gap,
and recovery evidence that may not be reconstructable from Raw alone. They do
not contain the market-event corpus and never replace Raw or manifests.

## Archive Set metadata and index

`archive_set_id` identifies one logical collection. `storage_id` identifies one
physical HDD, SSD, or USB medium. Every medium carries its own Archive Set and
storage identity plus the artifacts/manifests it contains. A chunk is never
striped across media. The workspace index is rebuildable from that durable
media-local metadata.

## Execution roles

The live VPS profile performs capture, sealing, Catalog, recovery, status, and
remote export support. The local workspace performs heavy Normalize, Replay,
Historical Backfill, archive verification, and future visualization workloads.
These are execution roles within one Recorder distribution, not separate
repositories or microservices.

## Current implementation boundary

The current code has internal data roots, a local SQLite Catalog, local
registered-directory archive transactions, normalized data, Replay,
HistoricalImporter, the M22.1 read-only remote source kernel, and M22.2
Archive Set physical-media metadata with an explicit-path rebuildable workspace
index. M22.3 adds local fake/in-process receive, durable Raw and
`external-archive-manifest.v1` publication, Archive Set entry commit,
`remote-archive-receipt.v1`, and independent receipt revalidation on supported
Linux/macOS filesystems. The media-local Raw/manifest/entry/receipt chain is
authority; the workspace index is convenience discovery state and is never
sufficient by itself.

M22.4A implements the non-transport VPS-side receipt/source-bound pending
authorization and read-only recovery interpretation in the existing Catalog.
Actual remote transport/SSH and receipt exchange, VPS source deletion,
production terminal transition, and post-session Catalog snapshot transfer
are not implemented. Windows end-to-end receipt durability is not yet
supported. Archive Set membership is not a backup or redundancy guarantee.
