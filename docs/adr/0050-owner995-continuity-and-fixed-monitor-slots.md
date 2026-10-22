# ADR0050: owner995 continuity and fixed monitor slots

Status: accepted owner policy; exact N2 configuration, Linux QA, actual strict
warm/start and current formal prefix independently accepted. The native run is
in progress; full24h monitoring and saved-data/final restored acceptance remain
pending. See the [N2 actual acceptance](../milestone_acceptance/R2-capacity-n2-btc-eth-owner24h-20261022.md).

The owner explicitly chose monitor availability and complete saved-frame contract
conformance each >=99.5%, with separate denominators. Rare recoverable faults
should log/retry. For disk full, sustained write failure or inability to credibly
verify, the owner chose pause affected capture and notify.

The previous private controller could terminate a run for one60s recovery episode,
an unrelated auxiliary BLOCK when core recovered, or one BLOCK finish endpoint.
These quality vetoes did not implement the new aggregate policy. Its old monitor
also divided by actual rows; a missing30s slot could reduce the denominator.
Old N1 controls/evidence/results stay immutable; no retroactive code change.

For future capacity runs, the small `tools/qualification_owner_995.py` observer
records core episodes and5s recovery probes without a per-episode quality abort.
Keep the existing official auxiliary gate and its typed-empty recovery semantics;
strict auxiliary status is reported separately. This changes continuation, not
what an original gate PASS means. Warm-up still has its existing bounded strict
start checks. Own86400 minimum and one actual trusted finish observation remain
required; a bounded finish probe may remain BLOCK without falsifying strict PASS.
A timing-control failure preserves real duration/data for affected closeout,
rather than silently granting more duration or automatically repeating capture.

Every24h monitor has exactly2880 nominal30s slots. Missing/late slots fail once;
recovery/finish probes do not enlarge the denominator. Duplicate slots, mismatched
configuration/context maps, malformed timestamps or a UTC/BOOTTIME discontinuity
cannot establish a trustworthy percentage. Inclusive995/1000 uses integer math.

During capture, RSS/available memory/current and lifetime swap remain recorded
capacity diagnostics; they do not turn into a new unapproved data-quality vote.
Do not reset or repeatedly count a historical peak as a new fault. Retain fresh
filesystem reserve>10GiB and actual write-safety checks. Explicit terminal
service/product failure, actual hard reserve, lost identity/clock/publication or
failed evidence persistence remains critical, independently of the percentages.
Normal STARTING can honestly lack its first capacity census only in pre-T0 warm;
RUNNING/formal observations require complete safety evidence.

No new production daemon, queue, transport supervisor, automatic restart scheme
or increased Recorder StopSec is introduced. Production c368/wheel68460 remains
unchanged; only future private qualification controls use this policy. Final
capacity recommendations must report observed traffic/resource headroom, not
promise permanent error-free operation for arbitrary symbols.

Validation:30 focused tests, Ruff/mypy PASS. Independent policy review5e1938cb
reconstructed13 full86400 fake-clock controller cases,14 actual sample cases,
7 evidence faults,4 warm/resource/evidence boundaries and the actual failure
entrypoint's notification receipt. This is zero real-duration credit. A later
publication-byte retention supplement and exact N2 bindings are still required
before copying or executing future runtime controls.

October22 actual implementation checkpoint: single native067b/owner-startbf175
establishes the new own86400 clock. Independent39002-check report2609228c
reconstructed all493 current publication bytes and986 maps, including15 honest
pre-T0 STARTING records without initial write-safety publication, three recovered
core samples and14 strict-only auxiliary waits. This grants startup/current-prefix
credit only. Official459/462=99.350649% is an incomplete prefix; final fixed2880
monitor and saved-frame denominators remain pending.
