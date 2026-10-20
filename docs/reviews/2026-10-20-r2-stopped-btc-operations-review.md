# Stopped BTC deployment operations design review

**ACCEPT, with the initial startup race fixed. No remaining blocking finding.** This is LOCAL/OFFLINE design/component qualification only. No SSH, native launch, production access, or candidate main execution occurred. Previously accepted cdb deployment helper, e2 config, actual isolated release and CI are referenced rather than reviewed again.

Reviewed exact controls:

- Copy `939748833810b0f2d520206a43930c375a3d48a26c596d07f3e793d912524cef`.
- Dispatcher **`d718badacf83d4600eb197a29f12fa63e3712787096910ae951ca384c7afc781`**.
- Reader `f1ea564d77ba5aa9a22170afe3484de2598568b658976311828ba2b5f6887a79`.
- Operations manifest `cd00b51047dc0977b8a55884258442a66ec528ee8a4fbc975d5369961cca9cdf`; deployment argv remains `7b258ae983c0508fb5e470a23b90bb12fb6b24e29d8d24dbbcfb303c27773293`.

The copy operator validates the complete local manifest and existing exact release scope, boot, old identity/config, receipt, pin and read-only same-inode exclusive service lock; after acquisition it requires Recorder stopped/disabled/PID0/NRestarts0. It copies only `stopped-deploy-btc.py` and `candidate-btc-recorder.toml`, with exclusive creation, full content rehash, root:988 / 0640 attributes, fsync and directory fsync. Existing payloads, canonical deployment output or log block reuse. It changes no canonical runtime and does not start capture.

The dispatcher requires the actual copy receipt and remote payload/receipt/pin/old identity/config bindings, rejects existing output/binding or existing native unit, and launches only the frozen system-python → definition pin → stopped deployment argv once. Type=oneshot has Start3600/Stop1500 bounds. A real invocation ID is required; a PID or default systemctl state alone supplies no execution credit. Server binding and local dispatch output use exclusive creation.

The original `cbbac361…` dispatcher could observe a real invocation after a very early failure and then reject PID0 before saving its binding. The minimal two-line correction preserves the known invocation in the exclusive server binding and marks it `EARLY_EXIT_NOT_PASS`. Healthy state only selects the provisional launched label; it never causes a retry. Exact old bytes are retained/reconstructed with their original SHA, plus the minimal diff.

The reader exports actual server binding, complete UNIT journal since dispatch and owned-invocation journal, all available deployment outputs/backups, current service states, config and identity bytes/hashes, all installed Python hashes, and pip inventory after a published verified result. Each local read output is timestamped and exclusive. A missing result is explicitly `NOT_YET_PUBLISHED`; this is a checkpoint, never successful deployment. Failed/unfinished exports remain useful evidence. The reader is an exporter: actual success still requires independent comparison of real result, identity/config, installed files/dependencies and owned native outcome.

All three operators and embedded remote scripts compile. Independent AST-selected component cases pass for delayed invocation, known early failure, unknown-invocation timeout, existing-unit and duplicate-binding rejection before launch, changed scope/prior-identity manifest rejection, and missing-result reporting. Synthetic tests use fake systemctl/native-run and a private q; no operator main or remote command was executed. See `CHECKS.json` and `component_checks.py`.

**Qualification stops at design/component ACCEPT.** Actual exclusive control-copy/readback remains pending, followed by exact pending native/read binding qualification. No deployment-execution, new canonical identity, capture, T0, 24h or production-ready credit is granted.
