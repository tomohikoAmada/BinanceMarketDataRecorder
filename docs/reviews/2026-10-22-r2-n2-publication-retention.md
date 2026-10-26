# N2 original publication retention: independent narrow supplement

Verdict: **ACCEPT_UNBOUND_PUBLICATION_BINDING_LOCAL_OFFLINE**. No blocking finding in this two-field supplement. This supplements the frozen owner995 policy review without altering it or granting actual N2 execution credit. The N1 publication-retention limitation is not retroactively repaired or hidden.

Exact controller SHA256: `990a65f029f6befbeca75eb183d749f294b8a3dcaa3c9cb8ac16864b4f22d382`.

Reviewed snapshot: `candidate-final/`. Compared with the previously accepted controller cde4cf06, the only code change is two fields immediately before sample() returns: state_publication_base64 is standard base64 of the existing row.publication bytes; state_publication_bytes is their exact length. The original state_sha256 was already computed from those same already-read bytes. There is no second publication-path read or normalization. The existing1MiB raw bound, identity/write-safety checks, official/strict gate calls, role assignment, fixed2880 slots, resource semantics and finish control are unchanged. The owner995 module remains187224ed and the causal selector remains2ce6d581.

Because the retained fields belong to the same Observation dictionary, existing write_sample() JSONL flush/fsync preserves the exact source bytes alongside their gate/resource projection. Existing direct publish() of warm/start/finish/target rows also carries them. Sampling failures before completion still retain the original bounded .state diagnostic. A JSONL/control write failure still propagates as critical and retains the initiating observation; encoding is not an alternative success path.

Independent tests call the actual sample() and write_sample(), use the actual configured Spot+USD-M BTC+ETH auxiliary gate, persist/reload real local JSONL, strictly decode base64, verify byte length/SHA256, parse the original JSON, compare core/heartbeat/instance projections, and independently recompute the complete official and strict gate dictionaries at the recorded row timestamp. Eleven cases passed:

- healthy warm, honest STARTING/capacity-pending warm, nominal monitoring, core recovery probe and finish probe;
- typed EmptySideDataResponse recovery whose official result is PASS while strict result remains BLOCK;
- source path replaced after the one original read, proving saved bytes/instance/gates still bind that original read rather than the replacement path;
- noncanonical pretty UTF8 JSON and exact1MiB raw bytes, proving byte-preserving retention rather than parse/reserialize output and proving the cap applies before base64 expansion;
- raw1MiB+1 rejection with bounded diagnostic bytes/hash and no success JSONL;
- failed JSONL write that retains the original publication and propagates the initiating failure.

Five deliberate retention-record corruptions were independently rejected: invalid base64, Boolean byte length, incorrect byte length, wrong SHA256 and changed valid base64 content. These are review/reconstruction checks; the private collector is a writer of evidence, and the final independent reader must perform equivalent verification before trusting the embedded publication.

The one-read assertion measures sample() itself; the fixture's later deliberate inspection of the replaced path is not mistaken for a production read. Test inputs are synthetic local publication documents. No production Raw or service state was opened, no SSH/network/subprocess/service action occurred, and no real-time or saved-data credit is claimed.

Reproducible fixture: `publication_binding_faults.py [CANDIDATE_DIR] [FROZEN_SOURCE_SRC_DIR]`, run using the intended isolated Python with `-B`. It writes only synthetic temporary files beneath its own review directory and forbids external subprocesses. Fixture source and full output are hash-bound in FINAL_MANIFEST.json.

Before actual N2 binding/admission, account for the increased control-evidence storage on the archive filesystem, including5s recovery/finish probes and duplicated diagnostic/publication rows, not only2880 nominal rows. The parent's prospective control-evidence reserve and per-spool forecast have not been accepted by this supplement; they require the later exact common/policy/binding review. Current filesystem>10GiB and other trusted admission controls remain unchanged. Embedded publication bytes support independent gate reconstruction; they do not supply missing external OS syscall evidence or prove every Binance event was captured.
