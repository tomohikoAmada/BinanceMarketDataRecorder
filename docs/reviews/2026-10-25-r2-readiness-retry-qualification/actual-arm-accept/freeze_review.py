from pathlib import Path
import hashlib,json
R=Path(__file__).resolve().parent
def d(p):return {'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'bytes':p.stat().st_size}
x=json.loads((R/'RECONSTRUCTION.json').read_text());x['limits']=['Accepts only the actual completed ARM and bindings at these original observations.','Five OS masked states are from the ACTIVE_VERIFIED publication; the launch path must recheck them live.','Native transient GC/not-found defaults are not actual runtime properties; initial effective properties and owned manager success bind outcome.','No source/design/code/Linux QA repeat, launch, capture, T0, duration, saved-data or capacity acceptance.','Fresh current inventory/window/reserve/admission remains required before sole launch.']
(R/'RESULT.json').write_text(json.dumps(x,sort_keys=True,indent=2)+'\n')
(R/'REPORT.md').write_text('''# Actual finite ARM outcome and binding — ACCEPT

This narrow LOCAL/OFFLINE review binds initial dispatch `25e1ae4c2a8171c4cd781d2ed241999d7c4ef87cf8a35b65bbe7b0523b724a24` and sole checkpoint read `3e86b3acba325123529f6aed0848fd73e99618a4f6521c65668868605721f0fe`, operatorf3b7 and controls manifestd3ed, for `/srv/recorder-data/recorder-archive/evidence/R2-capacity-n2-readiness-retry-20261025-1s0kswlt`. All13 original control byte lengths/base64/SHA/UTF-8 views and root0:988/0640 modes verify, with parseerrors0, returncode0, original stderr retained and no read retry. No extra status/SSH/Raw/service action or previous qualification tests occurred.

Actual ARM invocation `dfd67645b462452bbbfdc3b22ef030b8` initially ran as PID877050, activating-start. Its request exactly matches the already accepted pending argv. Initial effective properties independently bind Typeoneshot/Start40min/Stop25min/UMask0027, whole-phase canonical pin, system-Python failure restoration, accounting and control-group behavior. Seven owned same-boot journal rows include ACTIVE_VERIFIED and normal manager done at1792936189307067000ns. Later transient garbage collection and not-found/default properties are not execution failure or runtime evidence.

Window `194e50a2a58762a3862ef0414a1e573b36777d551bbbf4baafb14af6c3c3fc1f` is ACTUAL ACTIVE_VERIFIED on boot59bb. It armed at1792936179488935528ns; floor(armedUTCseconds)+230400 gives fixed expiry1793166579, **October28 05:49:39UTC**. Never extend, reuse or reARM this window. Fresh canonical5103ed98 verifies source20e57/wheeldd2c/identity9b8/config5b73, and production Stop90 is unchanged. The original five OS states are saved; all five actual ARM publication states are inactive/masked-runtime and the negative start probe is rejected. Exact kernel146/pending147 notice hashes/content/mtime/packages are retained identically before and after masking, with no reboot, marker change or scheduled shutdown.

Prearm9ba2196c and postarmc2d6282f are ADMITTED on the same boot and census/backlogs0. Postarm remaining230390.33156776428s exceeds required221626.19209639446s by8764.139471369825s; projected writer20,074,863,566B/archive1,957,747,576,016B exceed10GiB. Read-point fixed-deadline remaining230113.29197597504s also exceeds that requirement, but is not a future admission guarantee. The launch path must freshly recompute current limits and recheck OS masks/window/custody.

At the checkpoint Recorder is inactive AND disabled/PID0/NRestarts0; archive timer is enabled-active and latest owned workerdd4683e239e642b2b3a460d87042f157 has natural manager success and BACKLOG_EMPTY/failedtransactions0/backlogs0. Capture and terminal are not started; owner-start/capture-launch/restoration/cancellation/failure controls are absent. No new T0 or actual24h target exists yet. This accepts only the completed finite ARM and original bindings. All prior source/design/classifier/Linux-copy acceptance remains prior evidence, not retested or expanded. It supplies zero capture duration, saved-data, capacity, stability or historical-time credit.
''')
items={str(p.relative_to(R)):d(p) for p in sorted(R.rglob('*')) if p.is_file() and p.name!='MANIFEST.json'}
(R/'MANIFEST.json').write_text(json.dumps({'kind':'INDEPENDENT_ACTUAL_FINITE_ARM_OUTCOME_REVIEW','decision':'ACCEPT','accepted_arm_dispatch_sha256':x['accepted_arm_dispatch_sha256'],'accepted_arm_read_sha256':x['accepted_arm_read_sha256'],'window_sha256':x['window_sha256'],'members':items},sort_keys=True,indent=2)+'\n')
for n,v in items.items():assert d(R/n)==v
for p in R.rglob('*'):
 if p.is_file():p.chmod(0o444)
for p in sorted([p for p in R.rglob('*') if p.is_dir()],reverse=True):p.chmod(0o555)
R.chmod(0o555)
for n,v in items.items():assert d(R/n)==v
print(json.dumps({'directory':str(R),'member_count':len(items),'REPORT':d(R/'REPORT.md'),'RESULT':d(R/'RESULT.json'),'MANIFEST':d(R/'MANIFEST.json'),'readback':'PASS'},sort_keys=True))
