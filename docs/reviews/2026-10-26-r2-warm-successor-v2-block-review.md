# V2 narrow component review: BLOCK

Exact4f73 candidate is BLOCKc3187d5f/resultabe9419e/manifestd57f165f. All120 complete frozen report members and manifest passed parent SHA/bytes.63+20+8 existing cases reproduce PASS and old six bypasses now reject; that does not erase the new findings.

P1: actual inner0/control/release followed by SIGKILL of the still-owned keeper returns keeper -9 but command_ok accepts. P2: failed inventory parsing loses original ps stdout/stderr and per-attempt evidence. The keeper-death counterexample leaves no live group and the sentinel is unaffected; it establishes a success-protocol gap, not dirty data. Malformed ps fails closed but loses evidence. All executions are LOCAL, with no Linux/runuser, deployment, new T0 or capacity credit.

A distinct V3 corrects only successful keeper protocol and raw inventory custody. Current V3 exact delta review is ACTIVE; original V2 and this BLOCK remain immutable. See [report](2026-10-26-r2-warm-successor-v2-block-review/REPORT.md), [result](2026-10-26-r2-warm-successor-v2-block-review/RESULT.json), [manifest](2026-10-26-r2-warm-successor-v2-block-review/MANIFEST.json) and [mirror map](2026-10-26-r2-warm-successor-v2-block-review/MIRROR.json).
