# M19 independent code-review findings

Baseline: `c9d3c07b25f9688da9f55c6b12c8b4b2f42eeab4`.

| Item | Result | Baseline evidence | Regression evidence |
|---|---|---|---|
| A depth resync after lifecycle/gap | CONFIRMED | baseline `collector/spot.py:185,313` snapshot was one-shot; only overflow at line 112 restarted session | `test_m19_reliability.py` lifecycle matrix plus Spot integration overflow |
| B one dead core market | CONFIRMED | baseline `collector/supervisor.py:39-54` removed failed child and continued while another task lived | terminal supervisor/service tests and Catalog event |
| C USD-M overflow session restart | CONFIRMED | baseline `collector/usdm.py:194,257` used one snapshot/TaskGroup and no overflow observer | bounded overflow and USD-M lifecycle tests |
| D terminal side task | CONFIRMED | baseline `collector/usdm_side_data.py:183-203` stored failure and returned permanently | retry-attempt/stop-interruption tests |
| E top-level side status | CONFIRMED | baseline `service/runtime.py:222-247` exposed core readiness only | state now embeds required task fields and degrades stale/retrying state |
| F current RSS | CONFIRMED | baseline `service/runtime.py:93,279` emitted `ru_maxrss` as `rss_memory_bytes` | current/peak RSS semantic test |
| G GitHub Actions | CONFIRMED | no `.github/workflows` existed | offline macOS CI added by M19 |

A/B/C were all reproducible, so implementation proceeded. No finding required
a strategy, trading, account, GUI, or multi-service design.

## M19.1 second-review results

All nine reported blockers were independently reproduced or verified:

- archive products without an interval used an invalid ambiguous filename;
- a normally returning core collector was not terminal until all tasks ended;
- USD-M did not await side-data cleanup before closing downstream resources;
- six limited-retention statistics had no durable per-kind catch-up Cursor;
- historical CSV normalization accumulated the complete file in `rows`;
- Range resume did not validate `Content-Range` or recover safely from
  `200`/`416`/checksum failure;
- resync completion reported snapshot `lastUpdateId`, not the post-bridge
  reliable local-book ID;
- verification did not open and reconcile normalized Parquet lineage;
- Actions was enabled, but job-level use of unavailable `runner.temp` context
  caused workflow validation to fail before any job was created.

Regression tests now cover each path. R-034's production rule is unchanged.
