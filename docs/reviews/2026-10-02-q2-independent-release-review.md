# Q2 independent implementation review

Reviewer: owner-authorized GPT-6.1 Sol, xhigh, read-only local subagent.
Date: October 2, 2026. Verdict: **ACCEPT** after the correction below.
Open findings: **P0=0, P1=0, P2=0, P3=0**. One original P3 is resolved.
Release, deployment and cloud qualification remain separate gates.

| Reviewed authority | Exact value |
|---|---|
| Base | `d6f37576f5b044bf578501cba291fe889e45a919` |
| Candidate HEAD | `9e08c1d1f6434d4e9687d12163f47924456e8c09` |
| Candidate tree | `ed839e92fb8663bf26d684d7e9ffc04269c38435` |
| Additional runtime patch SHA-256 | `b689392a86358ce193a89f68a7bf9569eaf392e310fa34d919272217180df500` |
| Corrected `acceptance_v5_online.py` SHA-256 | `74057d5df680a27ccfdbfc2ab8569df24c8311b4a8b414987ebd844ce205fa56` |
| Inspected source/test working diff SHA-256 | `aa95c4d2f14ae27254995265c8086c59b47274f96e9eb4022ed597f9ad0edc4f` |

## Resolved finding and release-owner decision

At `acceptance_v5_online.py:365`, ordinary missed-cadence catch-up replaced an
over-budget entry with a pending descriptor but still appended its row and
companions. Measured delta totals were **7,342,821 and 7,344,242 bytes** against
the declared **7,340,032-byte** limit. Replay accepted those documents.

The reviewer classified this **P3, nonblocking**: the enforced 8 MiB document
bound remained intact, unqualified rows were not acknowledged, pending work
prevented terminal eligibility, and catch-up later drained. The release owner
chose to correct the literal bound before freezing, rather than redefine it.
This decision did not change the independent severity assessment.

The correction omits over-budget declared-policy entries without cursor advance;
frozen high-waters retain pending work. Independent replay enforces the cap.
Missing-policy legacy producer/replay behavior remains unchanged. Repeated
measured totals were **7,337,041 and 7,338,460 bytes**, with pending work retained
and subsequently drained. Finding **CLOSED**; no other actionable selected-scope
finding remained.

## Reviewed behavior

- Q1 supervises heartbeat failure/return through startup, recovery and running;
  drain failures propagate, primary causes survive peer cleanup, and final
  publication failure cannot bypass Catalog/lock cleanup.
- Normalized v2 identities include venue/market/symbol/stream and versioned build
  identity. Immutable v1 verification/replay retains its original path.
- Live diagnostic history is bounded without truncating callbacks or durable
  gap facts; offline reconstruction retains its unbounded default.
- Enabled auxiliary failure degrades aggregate health with core isolation.
- Raw optimization preserves complete validation/proofs. Frozen multi-page SQL,
  causal bounds, cancellation and legacy replay/resume remain intact.
- Q2 stress assertions retain ordering, generations, exact gap-ID/payload
  bindings and incomplete manifests. Extra empty markers require durable intent.
- The prepared Q3/Q4 procedure retains old custody, canonical paths, fresh
  registration/identity and finite maintenance gates, stopping before Q5. No
  concrete procedural hazard was found; reviewer executed no procedure.

## Independently executed checks

| Check | Result |
|---|---|
| Runtime/supervisor/normalization/history/cwd/archive/V5 focused suite | 125 PASS, 12.96s |
| Corrected V5 online/delta/throughput/legacy/finalization suite | 27 PASS, 6.00s |
| Complete old/new Raw proof dictionary differential | Equal across 128 sealed cases: empty/short, gap/overlap and seeded reconnect patterns |
| Injected actual STOPPED state-store publication failure | OSError propagated after both Collectors drained; Catalog closed, runtime stop references cleared, lock reacquired |
| Before/after byte measurements | Corrected strict admission and eventual catch-up confirmed |
| `git diff --check` | PASS |

Review was local and read-only: no SSH, network action, edits, commits or further
subagents. Existing CI/build/cloud results were read as recorded evidence, not
independently rerun. The correction makes the earlier wheel a **superseded
preliminary candidate**. Revised exact-source CI/build/wheel/identity gates remain
required. Q3 live memory/capacity, observations and audit costs remain unmeasured.
F4, historical R-078 and broader certification stay separate. No Formal duration
or production-ready status is granted by this review.
