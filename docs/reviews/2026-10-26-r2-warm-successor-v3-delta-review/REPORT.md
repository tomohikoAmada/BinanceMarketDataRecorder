# Independent exact V3 narrow delta review

Decision: **ACCEPT for exact code/LOCAL component delta only**. No new blocking finding was established in the changed success protocol or inventory-custody paths. This accepts CODE_MANIFEST `b020d893bb4acb01aaa46b3272d4a15b043a483cddfba234715f9736ff3fb164` and its23 bound members for this limited scope. It does not authorize copy, ARM, launch or a new24h clock.

Candidate: `/private/var/tmp/bmdr-r2-warm-successor-v3-20261026-x7yfquh1`. Reviewed warm_readiness is `9305c447d8263a3a7a57a540fd129f1f9a974805c46766745f3e5855b9a79988`; owned_command is `ef2f8882cabbd6becca136b41075e117a28fb9f3c3ed7b8c721d60f283e757bd`. Controller `eb8f321a8c1433987f72e42d2dea1afc2ddc217a2e4e016ac4d5d3a2a9367c4f` and read-only warm_completion `f8c7fc8569d6a0b32cca4cb0e24a5b975e376be2a90f5b97872ce8c4e1d3287d` retain exact V2 bytes.

## Scope and provenance

All23 V3 inputs and12 referenced runtime files were read by full bytes and SHA/size checked. The six exact source maps still match source20e57. Prior V2 BLOCKd57f165f/reportc318/resultabe9 and its accepted parent all120 custody are retained as references; the old review, historical QA, source deployment and actual failed runs were not repeated.

AST comparison confirms warm_readiness changes only command_ok. Keeper main is AST-identical to V2. After removing only the inventory list initialization, extra _members evidence argument and returned inventory list field, run_owned is AST-identical to V2. The remaining owned change is the new raw-byte packing and bounded _members evidence function. Thus prior V2 positive conclusions about static/source pairing, current-publication rejection, original warm bounds and unchanged formal/stop/audit/restore code are reused, not newly expanded.

## V2 abnormal-keeper finding is corrected

command_ok now requires exact integer inner and keeper returncodes0, timed_out is False, lease_released is True, group_KILL_used is False, an explicit empty residual list, no cleanup error, the no-live cleanup state and a successful parse. It decodes the original SHA/size-bound control bytes and requires positive nonboolean integer PIDs and matching keeper/PGID/inner identities and control returncode0.

The provided11-case protocol regression independently reproduced PASS. This includes a real LOCAL inner success followed by actual release flush and SIGKILL of only its still-owned keeper. The returned receipt still truthfully records inner0, keeper-9 and no live group members, but V3 now rejects it. The unrelated sentinel remains unaffected. ABNORMAL_KEEPER_RECEIPT retains the exact full receipt/control bytes. Normal keeper release remains accepted as command execution only; bool-as-int fields, false release, intentional KILL and mismatched control identities reject.

The independent27-case delta probes additionally reject missing required protocol fields, integer substitutes for boolean flags, boolean/float PID fields, absent residual-list type, unresolved cleanup state, malformed/invalidUTF8/empty control structures and a changed control digest. Missing or invalid proof halts eligibility rather than supplying T0 credit. These are local predicate/fault-provider cases, not Linux process facts or arbitrary malicious-root resistance.

## V2 inventory evidence finding is corrected

_members now appends a proof record before actual I/O. Each record carries command, timeout, before/after monotonic timestamps, returncode or exception, explicit stdout/stderr availability, partial-by-timeout marker, lossless base64/length/SHA streams, parse_error and successful matching-member observations. All bounded attempts are returned in inventory_receipts on the owned command receipt, including the original failing attempt and later cleanup scans. Nonzero status is recorded before rejection; invalid encoding is not replacement-decoded. Timeout absence of a stream remains distinguishable from available empty bytes.

The provided three complete-owned failure scenarios independently reproduced PASS for invalidUTF8, timeout-prefix bytes and nonzero ps exit. Complete returned receipts were separately reconstructed and preserved under run/INDEPENDENT_FULL_INVENTORY_FAILURE_RECEIPTS.json; this extra reconstruction adds no new coverage count. The original failure stays visible while cleanup observations remain separate. Every encoded bytepack in these complete receipts, the abnormal keeper receipt and independent probe results was independently decoded and rehashed, with all lengths/SHA matching.

The independent bound probes establish that record128 can retain its full response and zombie distinction, while a129th attempt is refused before any new I/O and the list remains bounded. An exhausted nonpositive inventory budget likewise performs no I/O and retains a failure record. The source parser/cleanup success policy remains unchanged: malformed or failed observations are not interpreted as an empty group. A timeout with unavailable stdout and available empty stderr preserves both availability facts. No actual host encoding/ps-timeout fault is claimed; these responses are explicit providers around real local owned-command cleanup.

## Executed validation and limits

In the exclusive clone,63 full actual warm-prefix cases,11 keeper success/abnormal protocol cases and3 inventory evidence cases independently passed:77 provided V3 cases. The full-prefix cases execute the exact warm/start controller prefix through commit and exact source evaluator with explicit OS/static/clock providers; they are necessary compatibility coverage for changed command_ok. The27 independent delta probes also passed. This is104 LOCAL case executions in the current narrow review, not the parent's whole105-case suite and not all47 future design specifications. The unchanged20 boundary and8 broader owned cases are prior reviewed/reference coverage, not re-executed here. The extra three full-receipt reconstructions repeat evidence custody only.

Real subprocess tests are on macOS POSIX with an explicitly simulated BOOTTIME provider. Current scope remains processes retaining the newly leased session/group; arbitrary escaped descendants are not controlled or claimed. Exact Linux runuser/helper group retention, actual Linux process controls/clocks/imports and realistic scan/receipt latency remain mandatory future gates. A final read-only check of the known keeper/failure fixture groups found no live matching rows; it sent no signals and does not confer future ownership authority.

The105 parent LOCALPASS label was read as input provenance only. No new claim about a24h run, saved frames, CPU/network root cause, production source stability, N1 qualification, capacity boundary or N4 eligibility is made. Earlier V2 BLOCK and original failure evidence remain immutable.

## Required operational gates remain pending

This is code/LOCAL component acceptance only. The retained common constants point at failed P3 solely as OFFLINE references; scope/guard194e is CLOSED and forbidden for reuse or extension. No production/source/P3/runtime/W/repo-plan bytes were edited; no SSH/network, Raw/manifest payload, service action, copy/ARM/launch or newT0 occurred.

Before any operational capture, a new exact reader/control manifest must export and bind the budget, binding, first/full attestation, every end/start attempt/witness, owner-start and commit plus new owned-command proof. Actual Linux exact-source/helper/runuser retention/import/latency qualification and fresh stopped custody/current identity-config/resource admission/unique finite scope remain required. The future47/F47 gate is not discharged by component review. Only then can a new own86400 measurement begin under unchanged995/2880/minimum+60, full new saved-data dualLIVE/verify/independent acceptance and actual restored-before-cancel/handoff/receipt requirements. This result supplies none of that operational or duration credit.
