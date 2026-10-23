# Independent R2 stream-close diagnostics review

Verdict: **ACCEPT — diagnostic scope only.** No actionable findings were identified in the exact six production/test files listed in INPUTS.json. This acceptance is a source review and ten independent local faults; it is not a deployment, a stability repair, a capacity result, or inherited duration credit.

## Reviewed identity and authority

- Candidate: `/Users/amada/.codex/worktrees/r2-close-diagnostics/BinanceMarketDataRecorder`
- Branch: `codex/r2-stream-close-diagnostics-20261024`
- Base/HEAD: `c3686dd76406c8d483b23eb10417349e5a1ffecd`; six reviewed working-tree inputs, including the untracked new unit module.
- Applicable repository AGENTS.md was read, including data integrity, M20 proxy privacy, security, current-task priority, and offline test requirements. Its exact reviewed bytes are retained as AGENTS.reviewed.md. Parent's current authorization constrains this review to local source and synthetic offline evidence.
- No SSH/VPS/production/old Raw or deployed/frozen report-isolation tree was accessed. No candidate file was edited. All six input sizes and SHA-256 identities were rechecked at closeout and remain unchanged. Later parent documentation additions are visible in STATUS.after.txt and are outside this six-file verdict.

## Review conclusions

The only production edits import a shared helper and expand the existing Spot/USD-M unexpected-disconnect log events. They do not alter retry, queue, ping, endpoint, receive/persist, stop, or capture policy. Existing integration assertions for exact Raw payloads and gap lifecycle behavior remain present.

The helper reads the websockets15.0.1 `ConnectionClosed.rcvd`, `.sent`, and `.rcvd_then_sent` fields in their documented local directions. Missing frames retain null code/reason values, with no invented 1006 frame or server attribution. Sent-before-received remains false and receive-before-send true; a one-sided or unframed close keeps the order null. The inherited base exception also covers proper peer closes raised during collection.

The direct OSError errno or immediate explicit cause errno is recorded without traversing arbitrary chains. The immediate cause type is a class name; exception messages, exception repr/str, full transport/proxy URLs, handshake responses, and raw event bodies are not logged by this change. Close reasons are bounded to 128 characters, with any reason containing `://` replaced entirely by `[redacted URL]` before truncation. JSON formatting escapes close-reason control characters. These are protocol-close reasons, rather than general exception text.

The phase is opening until the opener context successfully yields, then established. Duration uses the same event-loop monotonic clock at log time. Both its explicit `connected_duration_seconds_at_log` name and helper docstring disclose that it includes context-manager cleanup, rather than asserting an exact failure time or outage duration. No additional diagnostic task, timer, retry, or network request is introduced.

## Independent verification

`OFFLINE_RUN.txt`: **10 passed in 0.92s** using Python3.12 and installed websockets15.0.1. `OFFLINE_EVIDENCE.json` binds candidate source import paths and individual observed fields; `test_independent_faults.py` preserves the complete independent test source.

- Both markets against genuine loopback websockets servers: peer1011 close and echoed sent frame/order, abrupt unframed transport EOF with absent-frame nulls, and an added context cleanup delay reflected honestly in duration. All six cases preserve the exact single synthetic Raw payload bytes after sealing.
- Both real collector callsites: an opening InvalidProxy error explicitly caused by ConnectionRefusedError111; opening phase/null duration, immediate cause type/errno, and absence of the injected proxy URL/private messages.
- Helper fault controls: a deeper OS cause plus separate exception context is not misrepresented as the immediate errno; a URL after the truncation boundary still redacts the complete close reason.

The parent already reported focused30PASS, RuffPASS and mypy310PASS; those are contextual parent evidence and were not independently rerun here. The parent full suite was still running and had an unresolved failure when this report was frozen. It remains a separate QA/release gate; this review does not claim full-suite success or authorize publication/deployment.

## Primary API evidence and limits

The official [websockets15.0.1 exception reference](https://websockets.readthedocs.io/en/15.0.1/reference/exceptions.html) was inspected during this review. It specifies close-frame direction and local ordering. Installed dependency source retained in INSTALLED_API_EVIDENCE.txt independently confirms those fields and that the asyncio recv implementation raises its close exception from its immediate receive exception.

Parent-reported N2 monitor2861/2880=99.3402778% remains below99.5%; 17 book_ticker and2 OI transient monitor failures have no uniquely established transport root cause. Native saved-frame result62687921/100% remains subject to its pending independent full controls. Neither operational result is independently reconstructed or newly accepted by this code review. New diagnostics can support later attribution; they do not establish that transient disconnects or OI behavior are repaired, permanent stability, capacity, or new2h/12h/24h credit.
