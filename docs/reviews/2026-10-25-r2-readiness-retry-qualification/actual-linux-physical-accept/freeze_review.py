from pathlib import Path
import hashlib,json
R=Path(__file__).resolve().parent
def d(p):return {'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'bytes':p.stat().st_size}
reconstruction=json.loads((R/'RECONSTRUCTION.json').read_text());result=dict(reconstruction);result.update(findings=[],bindings={'operator_sha256':reconstruction['operator_sha256'],'controls_manifest_sha256':reconstruction['controls_manifest_sha256'],'runtime_members':reconstruction['current_runtime_members'],'scope':reconstruction['remote_scope'],'source':reconstruction['source'],'identity_sha256':reconstruction['identity_sha256']},current_qualification_state_eligible_after_parent_readback='CURRENT_LINUX_PHYSICAL_INDEPENDENTLY_ACCEPTED',pending_ARM_requires_actual_outcome=True,pending_ARM_bindings_sha256=d(R/'PENDING_ARM_BINDINGS.json')['sha256'],old_failed_duration_and_closed_guard_unchanged=True)
(R/'RESULT.json').write_text(json.dumps(result,sort_keys=True,indent=2)+'\n')
(R/'REPORT.md').write_text('''# Actual Linux classifier qualification and exclusive physical copy — ACCEPT

This review binds actual copy `3c09dd59f8691ce3b3de1d1f692cc61a057542dd17551e8c1882edc2c59bba00`, operator `f3b7b6bfc159b934408be326e6e157f031c3714b14fe8e09719c50dc06e603a9`, controls manifest `d3ed70e54b3e2fe1131f0fdffe7eaf1b7637aaafc673a188681dc38ad905b54f` and new scope `/srv/recorder-data/recorder-archive/evidence/R2-capacity-n2-readiness-retry-20261025-1s0kswlt`. It accepts only the already completed copy, genuine current Linux qualification, import/binding smoke and copy-time custody/admission. No duplicate copy, SSH, service action or tests were performed during this LOCAL/OFFLINE review.

The exact copy-program SHA `8d1d4841cfeb489d5f44b7c18d8caf421af9fd8833f37c6f7b2bed421f1d033c` independently reconstructs from the captured accepted binding and matches execution; returncode0, no error/retry and empty original stderr are retained. All27 observed software payload hashes, byte counts, owner0/group988/mode0640 match independently reconstructed local payload bytes. The scope is0:988/0750; archive TMP is999:988/0770. The exact14 runtime/control bindings match accepted design; software-copy publication precedes success output in the already reviewed program. Scope exists now and must never be copied again.

Actual canonical Linux qualification is accepted: platformlinux, `/opt/binance-market-data-recorder/venv/bin/python`, BOOTTIME1978521537836320 and corrected classifier621 bind the executed39 fixtures. The original6602-byte fixture result SHA `4b5ad6d4c8ae395f7520e54d2d48dbb59192dcc9de7228b15a5f7b8dfe3bcd81`, stdout summary and empty stderr round-trip exactly through all three base64/length/SHA packs. All39 unique named cases PASS, including the real failed-publication old-negative control, five recoverable readiness tags, true FAILED/write/disk guards and fixed2880/995/probe/missing tests. The inner fixture remains a synthetic clock test and truthfully labels its own real-duration credit false; the outer observed Linux environment qualifies its real execution, never86400 seconds.

Twelve actual clean import origins are exactly in this scope, package origin is the canonical installed venv, manifest14/source20e57/identity9b8 and four Spot+USD-M BTC/ETH products,26 contexts and42 spools match. Historical N1 sourcec368 is explicitly historical with zero inherited duration. No source build/cloud/CI QA or prior code review is repeated or granted new credit.

Copy-time Recorder is inactive AND disabled/PID0/NRestarts0/Resultsuccess. Fresh fail-closed inventory reports one directory, zero files/partials and no payload access. Current census/backlogs0 match the accepted stopped baseline. The complete numerical forecast independently reproduces from copy-time metadata/free-space observations: unarmed230400s exceeds required221626.19209639446s by8773.807903605542s, projected writer20,075,862,990B and archive1,957,747,707,088B exceed10GiB. Devices are separate as designed. These are observations and forecast at copy, not a running armed lease or permanent reserves.

After parent all-member readback, the actual-copy acceptance may bind CURRENT_LINUX_PHYSICAL_INDEPENDENTLY_ACCEPTED. Pending ARM argv is bound separately: sole new arm unit, oneshot/Start2400/Stop1500/UMask0027, whole-phase pin, archive TMP and system-Python failure restoration. Same-boot/kernel146/exact retained pending147 markers/packages, canonical verification, fresh zero-file custody/current resources and admission are still checked by the exact ARM path. The230400s window is a new finite window; it has no actual expiry until ARM succeeds. This report accepts no ARM/window, capture, T0,86400 duration, saved-frame denominator, capacity or stability improvement. Old failed23h43m, data/evidence/venvs and closed guards remain immutable. No language rewrite, CPU tuning or network/CPU causal conclusion belongs to this scope.
''')
items={str(p.relative_to(R)):d(p) for p in sorted(R.rglob('*')) if p.is_file() and p.name!='MANIFEST.json'}
(R/'MANIFEST.json').write_text(json.dumps({'kind':'INDEPENDENT_ACTUAL_CURRENT_LINUX39_AND_EXCLUSIVE_PHYSICAL_COPY_REVIEW','decision':'ACCEPT','accepted_copy_sha256':result['accepted_copy_sha256'],'controls_manifest_sha256':result['controls_manifest_sha256'],'members':items},sort_keys=True,indent=2)+'\n')
for n,v in items.items():assert d(R/n)==v
for p in R.rglob('*'):
 if p.is_file():p.chmod(0o444)
for p in sorted([p for p in R.rglob('*') if p.is_dir()],reverse=True):p.chmod(0o555)
R.chmod(0o555)
for n,v in items.items():assert d(R/n)==v
print(json.dumps({'directory':str(R),'member_count':len(items),'REPORT':d(R/'REPORT.md'),'RESULT':d(R/'RESULT.json'),'MANIFEST':d(R/'MANIFEST.json'),'readback':'PASS'},sort_keys=True))
