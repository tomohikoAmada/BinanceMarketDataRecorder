# Known Limitations History (Condensed)

> Historical limitation snapshots preserved from August–October 2026. For current limitations, see [`known_limitations.md`](known_limitations.md).

## Key Historical Operational & Architecture Limitations

1. **GreenCloud Ordinary KVM Shared-CPU Policy (`R-060`, `R-061`)**
   - The GreenCloud Tokyo VPS (`greencloud-tokyo-01`) runs under a shared-CPU KVM policy (Terms of Service: sustained average CPU ≤ 30%, bursts to 100% for 10 minutes per 24 hours).
   - Provider control-panel CPU percentages (~20% observed during early single-symbol runs) are coarse host-panel readings, not normalized Recorder benchmarks. Qualification must rely on process CPU seconds, queue high-watermarks, and continuity metrics.

2. **Whole-Corpus V1–V5 Audit Scalability (`R-074`, `R-078`)**
   - Prior to `ADR-0051`, V1–V4 `AcceptanceObserver` re-scanned all historical manifests on every sample (`R-074`, reaching ~4.14 GB RSS at 112,817 manifests), and V5 required a full-corpus baseline/terminal Raw decompression pass (`R-078`, taking >14 hours for 143,362 manifests).
   - `ADR-0051` superseded whole-corpus re-audits with incremental per-file verification, which is further streamlined in `M1`/`M2` of the current milestone plan.

3. **Artifact-Specific Long-Run Evidence (`R-035`, `R-063`)**
   - Pre-MS1 single-symbol (`BTCUSDT`) 24h/72h runs (`f659895`, `c421605e`) and bounded 4-product MS4/Q4 2h runs (`303e073e`, `23ea5c4`) apply only to their tested code and product scope; they do not transfer qualification credit to expanded multi-asset Spot/USD-M/COIN-M configurations.

4. **Platform & External-Media Boundaries (`R-017`, `R-031`, `R-041`)**
   - macOS sleep/lid-close interrupts live capture and records explicit clock/sleep gaps (`ADR-0019`).
   - Linux external storage discovery inspects already-mounted block filesystems (`mountinfo` + `findmnt` + `lsblk`) without auto-mounting, formatting, or claiming hardware `SAFE_TO_REMOVE`.
