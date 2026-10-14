# R1 Linux build/cloud helper independent design review

Verdict: **DESIGN_ACCEPT_BUILD_AND_ISOLATED_CLOUD_QUALIFICATION**. No actionable helper correction is required. Actual build/cloud execution remains UNRUN and exact dual CI is still a prerequisite. This result is neither a wheel/deployment result nor live-duration credit.

Reviewed LOCAL controls: `/private/var/tmp/bmdr-r1-release-20261014-rioikh96`. Frozen helper SHA-256 `ca70a8e2a9b108f798da9f055359cfe452ec8e85ccd017b8b664774fa59aa3ac`. Source commit `8fa54d18ac08f515362229d7f9c739b45767c2b7`. Frozen archive SHA-256 `bbf332e00b84ffdc5294b5b8e19c24207a5074d6648279b85a9ce543b3a74967`. Release-inputs SHA-256 `61b7855fbc944f080a4c2bdd9c13a4ecbec92a3e4bc4edf1b8f984400097e082`. The final readback of helper and inputs was unchanged.

## Source authority

I independently hashed the supplied gzip archive, validated its exact Git archive comment, member types, unique relative names and traversal exclusions, then compared all **588 files** with the exact Git commit's tree/blob contents. They match. All **122 Python source files** exactly match the immediately preceding accepted R1 source review; the only extra source package data member is `py.typed`. This is a binding to the new R1 source, not inherited acceptance from the old production identity. Full file inventory and Git-tree proof are retained locally.

The helper verifies the same archive hash/comment, requires an extracted source tree exactly matching the complete archive inventory, rejects symlinks and extra source files before testing, and persists the inventory with exclusive creation. After tests/build it rechecks the archive and every frozen source file. Wheel Python package entries must match the dynamic frozen expected set and each content hash, so additions/omissions/content drift fail rather than relying on a hardcoded source count. `py.typed` inclusion remains specified by the frozen setuptools package-data declaration; it is not a runtime implementation file.

## Isolation and failure behavior

Release and test-data arguments must already be canonical existing root-owned children of `/var/tmp/binance-recorder`, named separately `r1-release-*` and `r1-cloud-data-*`. The data child must be empty and distinct. Planned creation/readback requires both roots mode0700. Reserved prior release/wheel/log/venv/inventory/.git/temp/cache outputs are rejected, and no previous smoke-data child is reused. A future actual-copy gate must bind exact directory names, owners, modes and helper/archive hashes; those remote facts were not read by this reviewer.

The helper creates test temp/cache/data only under the sibling data child. Its source checkout is under the release child, with a private `.git` for workspace discovery, while persisted test Raw remains outside that source workspace. Dedicated TMPDIR, pytest basetemp, data-root and pip cache prevent tests or installs from using production storage. PYTHONDONTWRITEBYTECODE avoids source bytecode generation. Only the new private source, wheel directory and smoke venv are mutated; existing builder venv is reused read-only for tooling. No production service, config, data, archive, old evidence, or venv is installed over, deleted, moved or repaired.

`set -euo pipefail`, exclusive inventory/receipt writes, reserved-output admission, and noclobber log creation make a failed command stop before a clean receipt is published. A retry must preserve the failed child and use a fresh child; it must not override the existing failure. The final receipt is emitted only after selected tests, wheel build, locked clean install, pip check, CLI smoke, exact source/wheel Python comparison and installed runtime dependency verification pass.

## Meaningful gates

The isolated affected suite includes weighted queue/backoff and USD-M extractor boundaries, persisted Raw/Normalize/versioned Replay R1 tests, the parent-authorized real local fragmented transport fixture, reconnect/crash/startup/gap preservation, both-market overload/stop/cancellation/writer failures, normalization identity/side-data and Replay, service runtime, V5 complete audit/control verification, and selected-scope/recovery controls. This is reasonable coverage for the changed source and its capture/audit integration. Full unchanged engineering gates and both exact CI jobs precede this affected runtime qualification; the cloud helper need not duplicate every unrelated full-suite test.

The build uses the accepted frozen source and no-isolation builder tooling. The new smoke environment is created with copies, installs the exact hash-locked Linux x86_64 Python3.12 runtime dependencies and the local wheel without dependency resolution, runs pip check and offline version/doctor/status against fresh private data, then invokes the released exact installed-dependency verifier. It rejects missing/extra/wrong runtime distributions and editable/outside-venv runtime dependency authorities, and requires the recorder distribution separately. The clean receipt records source/archive/inventory/wheel/lock hashes, bytes, matched source count and full dependency evidence. Its `installed:false` field is no production-deployment credit, not a claim that no private smoke install occurred.

No reinstall of the production wheel or production source edit is part of this helper. The small shell orchestration and two inline Python validation/report blocks are appropriate; no larger release framework or new abstraction is necessary for this gate.

## Native execution design

The parent supplies a future Type=oneshot transient `binance-recorder-r1-cloud-20261014.service`, TimeoutStartSec3600, TimeoutStopSec600, UMask0077, WorkingDirectory at the fresh source directory, invoking `/bin/bash` with the exact helper and source/archive/data-root arguments. Type=oneshot makes the 3600s start bound cover the complete helper phase. A timeout/failed helper leaves only isolated qualification artifacts and grants no clean result; Stop600 bounds termination. The helper owns no production state needing a restore handler.

The retained argv is explicitly a template because concrete remote roots are not yet created/read back. Exact CI38502452866 **BOTH SUCCESS**, exclusive create/copy/readback and resolved exact argv/native properties are required before launch. The native command contains no production service/config action. This review accepts that bounded native design; it cannot verify an actual service invocation, execution identity, runtime resources, test count, clean wheel, or Linux result before execution.

## Independent checks and limits

`bash -n` passes. Both inline Python blocks parse under Python3.12. Supplied helper/archive/input hashes and all588 exact Git-file bindings pass. I did not run the helper, build, install, transport fixture, or any VPS/network action; only supplied LOCAL source/control files were read, and only this exclusive private report directory was written. An initial comparison included the non-Python `py.typed` marker in a Python-only manifest; the corrected like-for-like comparison matches all122 sources and retains the marker separately. This was a reviewer comparison boundary, not a candidate source mismatch.

Actual copy/admission/CI/runtime/cloud result and a new locked wheel must be read back and independently qualified later. Canonical stopped deployment and fresh same-release owner-policy2h/12h remain subsequent gates. No deployment, source-time transfer, old live duration, Formal target, saved-data result, canonical V5/24h/48h eligibility or production-ready credit is granted by this design acceptance.
