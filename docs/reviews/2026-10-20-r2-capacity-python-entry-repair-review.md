# Stage-copy package dependency failure and minimal completion repair

**ACCEPT for the reviewed repair and one finish-copy invocation; no remaining blocking finding.** LOCAL/OFFLINE only. No SSH, recopy, candidate main, ARM, launch, whole-capture/QA/CI rerun, or production Raw access occurred.

## Actual failure and review correction

The retained failed copy has empty stdout JSON (SHAe3b0c442…) and complete498-byte stderr **79885fa9bac22f13052445e3d3af6e55486fffeb5a0630bd2bec6a7676244f3e**. Its exact chain is system-Python stdin → copied common.exact() → newly added configured_cohort() → package import → ModuleNotFoundError:binance_market_data_recorder. The failure occurred after original payload copying/readback/fsync, before census/admission publication. The copy branch does not arm a window or start Recorder.

The earlier review missed this integration case: software QA used installed canonical Python and the AST fixtures bypassed the real system-Python package boundary. Original failure files are retained; this repair does not replace the failed attempt with success or invalidate the completed software QA.

## Minimal repair

Reviewed stage operator **8849f2a320069a7130a2dc1c13026233e2a48b15e044fe837563f1e09b8576ba**. Its outer remote interpreter, ARM target after the existing definition pin, and launch.py interpreter now use the already installed canonical Python. The pin and package-free restore/expiry interpreter remain system Python. All13 runtime members, common.pyc911, controls manifest634715ff, config/identity/deployment, accepted Linux QA and its fixture bytes remain unchanged. No production rebuild, source/CI/cloud rerun or new framework is needed.

The added finish-copy path requires the original scope/tmp root/mode identity, absence of copy-admission/window/T0/ARM-attempt markers, and exact bytes plus root0:bmdr/0640 attributes for **all17 original payloads**, including the original design review/result/manifest. It fsyncs existing directories and invokes the original stopped/exact/census/device/reserve/time admission path under canonical Python. Only the missing copy-admission is published exclusively. It neither recreates the scope nor rewrites runtime/review files. Its result identifies copy_action=finish-copy and runtime_files_rewritten=false. Original INDEPENDENT_DESIGN payload bytes remainb111/4719/e3ef; this repair report must not replace those original payloads before completion verification.

## Meaningful verification

All five fully rendered remote modes compile, including both copy actions; explicit __COPY_ACTION__ substitution leaves stat.S_IMODE intact. Exact interpreter argv checks pass. Fresh local3.12 isolated `-I -S` imports common and restore without the package, then genuinely reproduces the first configured_cohort package import failure. A separate fresh3.12 process with the accepted release package and private config/policy successfully derives2 products/14 contexts/22 spools. This local compatible environment is identified in its output; actual deployed canonical package availability is separately supported by the accepted Linux QA/prospective evidence, not a new VPS probe.

The exact finish-copy validation prefix was exercised against17 private payloads, substituting only the fixture path anchor and filesystem owner identity. It permits no write-open and preserves every byte. Existing admission/window/T0/ARM markers and changed runtime or review bytes are rejected. Real census/admission/canonical verification remain for the actual completion invocation; they are not mocked into execution credit. Fixture identity adjustment and all tests are retained.

## Qualification boundary

**Only one finish-copy execution is qualified** for this exact operator and existing affected scope. Do not run copy again. Success/current physical attributes/admission must then be independently read back before exact pending ARM/launch qualification. No ARM/window/capture/T0/24h/next-stage/production-ready credit exists. The original failed evidence and all earlier data/runtime/QA evidence stay preserved.
