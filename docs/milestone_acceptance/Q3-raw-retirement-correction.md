# Q3 — Raw retirement during online qualification

October2,2026. LOCAL_CORRECTION_REVIEWED; CLOUD_GATES_PENDING. The owner
resumes through completed VPS Formal2h and terminal acceptance, then stops
before12h. All14 public auxiliary flags and the existing four ProductKeys stay
enabled; old/new capture and failed attempt evidence remain retained.

The first all-enabled nonformal normal observation failed with Raw changed
during qualification. The exact VPS per-read interleaving was not recorded.
A local real ArchiveManager reproduction produces that same error when verified
source retirement unlinks an inode while its full Raw scan still holds a descriptor.
Device/inode/size/mtime and readable bytes stay unchanged; ctime and link count
change. This proves a code defect, not that it was the only cause of the cloud failure.

The correction validates stored/decompressed hashes, every frame CRC and all
manifest statistics before distinguishing a newly unlinked local inode. Only
otherwise exact identity, link count1→0 and absent no-follow pathname enter the
existing local-absence path. No captured retirement authority means pending with
no cursor advance; a later coherent frozen snapshot plus exact registered archive
proof may qualify it. No live Catalog requery, retry loop or wider budget is added.
Mutation/replacement/hardlink changes still fail; archive-copy deletion still fails.
Same-ctime-resolution retirement remains pending. Raw/Catalog/proof format and
old-policy replay/resume remain unchanged; no collection/schema/network behavior changes.

Frozen three-path input manifest SHA-256:
`e9a34bd98d9b338aa1257c906c049d22e5613721d4bd4afd3f15e706958ec929`.
Source SHA-256:24874be5b127538efa2802ef939ad1af615737802f9f80e7b77e30bba279f21c.
Independent GPT-6.1 Sol xhigh local read-only review ACCEPT, P0/P1/P2/P3=0.
Independent23 focused tests PASS2.62s and12 additional actual parallel archival/
adversarial probes PASS. No independent reviewer SSH or cloud mutation occurred.

Full offline suite1942 PASS,24 online SKIP,5 stress deselected,13 existing warnings,
165.62s. Ruff, strict mypy286, M0 contracts, Go Raw golden and diff-check PASS.
No identical passing checks are repeated without changed inputs. Cloud changed-
path tests, exact-source dual-platform CI, replacement clean locked wheel, actual
concurrent normal/missed/recovery gates, cumulative capacity/audit forecasts and
authoritative post-warm-up baseline are still required before Formal T0.

At resume SSH reached TCP22 but timed out before SSH banner. The owner separately
confirmed the same failure, then authorized/performed a host reboot and asked
to reconnect after five minutes. No reboot or administrative repair was performed
by the agent. Last confirmed Recorder was inactive/disabled and archive drain
succeeded. Post-reboot state and interpreter/dependency/identity must be freshly
verified; no Formal stage or credit exists.

NEXT: reconcile rebooted host, freeze exact corrected release, repeat affected
Q3 gates, then one Formal2h and its stopped full terminal/completed verification
and independent eligibility review. Stop before Formal12h.
