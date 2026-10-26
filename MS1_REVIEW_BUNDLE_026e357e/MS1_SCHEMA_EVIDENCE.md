# MS1 SQLite schema evidence

This evidence is extracted from the exact BASE and CANDIDATE trees. Line
numbers refer to `src/binance_market_data_recorder/storage/catalog.py` in the
respective tree.

## BEFORE — BASE DDL

From BASE `catalog.py` lines 423–435:

```sql
CREATE TABLE IF NOT EXISTS operational_events (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    occurred_at_utc_ns INTEGER NOT NULL,
    evidence_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS side_data_cursors (
    kind TEXT PRIMARY KEY,
    last_persisted_period_timestamp INTEGER NOT NULL,
    updated_at_utc_ns INTEGER NOT NULL,
    source_retention_window TEXT NOT NULL,
    retention_window_ms INTEGER NOT NULL
);
```

There was no MS1 discontinuity identity index or discontinuity-symbol CHECK
constraint in the BASE DDL. `side_data_cursors` was keyed only by `kind`.

## AFTER — CANDIDATE fresh-schema DDL

From CANDIDATE `catalog.py` lines 501–526:

```sql
CREATE TABLE IF NOT EXISTS operational_events (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    occurred_at_utc_ns INTEGER NOT NULL,
    evidence_json TEXT NOT NULL,
    market TEXT,
    symbol TEXT,
    stream TEXT,
    gap_id TEXT,
    CHECK (
        event_type NOT IN (
            'STREAM_DISCONTINUITY_STARTED',
            'STREAM_DISCONTINUITY_COMPLETED'
        )
        OR symbol IS NOT NULL
    )
);
CREATE TABLE IF NOT EXISTS side_data_cursors (
    kind TEXT NOT NULL,
    symbol TEXT NOT NULL,
    last_persisted_period_timestamp INTEGER NOT NULL,
    updated_at_utc_ns INTEGER NOT NULL,
    source_retention_window TEXT NOT NULL,
    retention_window_ms INTEGER NOT NULL,
    PRIMARY KEY(kind, symbol)
);
```

For a fresh Catalog, the MS1 identity index is created by the migration entry
point (CANDIDATE lines 709–721):

```sql
CREATE INDEX IF NOT EXISTS
operational_events_discontinuity_identity
ON operational_events(
    event_type, market, symbol, stream, gap_id
)
```

The index is deliberately non-unique and non-partial. The event table's
CHECK requires `symbol` for STARTED/COMPLETED discontinuity events; other
operational events may retain NULL identity columns.

## AFTER — CANDIDATE legacy migration rewrite DDL

Within the existing Catalog transaction, CANDIDATE lines 868–958 rename the
legacy tables, create the current tables, copy transformed rows, create the
discontinuity index, and drop the temporary tables. The exact table/index DDL
used by that rewrite is:

```sql
ALTER TABLE operational_events RENAME TO operational_events__ms1_legacy
```

```sql
CREATE TABLE operational_events (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    occurred_at_utc_ns INTEGER NOT NULL,
    evidence_json TEXT NOT NULL,
    market TEXT,
    symbol TEXT,
    stream TEXT,
    gap_id TEXT,
    CHECK (
        event_type NOT IN (
            'STREAM_DISCONTINUITY_STARTED',
            'STREAM_DISCONTINUITY_COMPLETED'
        )
        OR symbol IS NOT NULL
    )
)
```

```sql
INSERT INTO operational_events(
    event_id, event_type, occurred_at_utc_ns, evidence_json,
    market, symbol, stream, gap_id
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
```

```sql
DROP TABLE operational_events__ms1_legacy
```

```sql
CREATE INDEX operational_events_discontinuity_identity
ON operational_events(event_type, market, symbol, stream, gap_id)
```

```sql
ALTER TABLE side_data_cursors RENAME TO side_data_cursors__ms1_legacy
```

```sql
CREATE TABLE side_data_cursors (
    kind TEXT NOT NULL,
    symbol TEXT NOT NULL,
    last_persisted_period_timestamp INTEGER NOT NULL,
    updated_at_utc_ns INTEGER NOT NULL,
    source_retention_window TEXT NOT NULL,
    retention_window_ms INTEGER NOT NULL,
    PRIMARY KEY(kind, symbol)
)
```

```sql
INSERT INTO side_data_cursors(
    kind, symbol, last_persisted_period_timestamp,
    updated_at_utc_ns, source_retention_window,
    retention_window_ms
) VALUES (?, ?, ?, ?, ?, ?)
```

```sql
DROP TABLE side_data_cursors__ms1_legacy
```

The migration binds every legacy cursor to `BTCUSDT`. Legacy discontinuity
evidence is normalized to explicit symbol-aware columns and evidence; legacy
side-data evidence receives `BTCUSDT` when its symbol is absent. The migration
does not merge rows.

## Migration entry points and fail-closed checks

The writable constructor calls the migration after `_initialize()`:

```python
self._initialize()
self._migrate_symbol_identity_schema()
```

The migration starts with `_reject_identity_migration_artifacts()`, inspects
both table shapes, rejects mixed legacy/current states, and performs the
rewrite under `with self._transaction()`. It validates the current index
columns as exactly:

```text
("event_type", "market", "symbol", "stream", "gap_id")
```

It rejects malformed discontinuity/side-data evidence, malformed cursor
values, malformed SEALING intent identity, and leftover
`__ms1_legacy` artifacts. Exceptions roll back the transaction, leaving the
legacy schema/data intact. Read-only opening allows a complete legacy schema
to be observed but does not migrate it.
