from pathlib import Path
import base64,datetime,hashlib,json
R=Path(__file__).resolve().parent;I=R/'inputs';S=R/'original-controls';S.mkdir()
DISPATCH='25e1ae4c2a8171c4cd781d2ed241999d7c4ef87cf8a35b65bbe7b0523b724a24';READ='3e86b3acba325123529f6aed0848fd73e99618a4f6521c65668868605721f0fe';OP='f3b7b6bfc159b934408be326e6e157f031c3714b14fe8e09719c50dc06e603a9';CTRL='d3ed70e54b3e2fe1131f0fdffe7eaf1b7637aaafc673a188681dc38ad905b54f';BOOT='59bb1735-ac48-408d-b3ec-bda79fc49b41';INV='dfd67645b462452bbbfdc3b22ef030b8';Q='/srv/recorder-data/recorder-archive/evidence/R2-capacity-n2-readiness-retry-20261025-1s0kswlt';PREFIX='binance-recorder-r2-capacity-n2-readiness-retry-20261025-1s0kswlt-';ARM=PREFIX+'arm.service';REC='binance-market-data-recorder.service';AT='binance-market-data-archive.timer';AS='binance-market-data-archive.service'
def sha(b):return hashlib.sha256(b).hexdigest()
def digest(p):return {'sha256':sha(p.read_bytes()),'bytes':p.stat().st_size}
afile=I/'actual-n2-stage-arm-1792936141508045000.json';rfile=I/'actual-n2-stage-read-1792936463800261000.json';assert sha(afile.read_bytes())==DISPATCH and sha(rfile.read_bytes())==READ
a=json.loads(afile.read_bytes());v=json.loads(rfile.read_bytes())
for p in [afile,rfile]:
 e=json.loads(p.with_suffix('.execution.json').read_bytes());assert e['returncode']==0 and e['error_type'] is None and e['remote_retry'] is False and e['operator_sha256']==OP and len(e['remote_sha256'])==64
 assert p.with_suffix('.stderr').read_bytes()==b''
assert v['scope']==Q and v['scope_exists'] and v['boot_id']==BOOT and v['parse_errors']=={} and len(v['files'])==13
controls={}
for name,item in v['files'].items():
 b=base64.b64decode(item['base64'],validate=True);assert sha(b)==item['sha256'] and len(b)==item['bytes'] and b.decode('utf-8')==item['content'] and (item['uid'],item['gid'],item['mode'])==(0,988,416),name
 assert Path(name).name==name;(S/name).write_bytes(b);controls[name]=json.loads(b)
assert v['files']['stage-controls-manifest.json']['sha256']==CTRL
assert v['files']['parent-tests.json']['sha256']=='4b5ad6d4c8ae395f7520e54d2d48dbb59192dcc9de7228b15a5f7b8dfe3bcd81' # binding only; do not rerun old QA
assert a['rc']==0 and INV in a['stderr'] and 'Running as unit:' in a['stderr']
prior=Path('/private/var/tmp/bmdr-r2-retry-actual-linux-physical-review-20261025-ufjx0ew5')
expected=json.loads((prior/'PENDING_ARM_BINDINGS.json').read_bytes());assert a['argv']==expected['argv']==controls['arm-native-attempt.json']['argv']
assert controls['arm-native-attempt.json']['status']=='ATTEMPT_NOT_PASS'
initial=dict(line.split('=',1) for line in a['effective_unit'].splitlines());assert (initial['Type'],initial['TimeoutStartUSec'],initial['TimeoutStopUSec'],initial['UMask'])==('oneshot','40min','25min','0027')
assert (initial['ActiveState'],initial['SubState'],initial['InvocationID'],initial['MainPID'],initial['NRestarts'])==('activating','start',INV,'877050','0')
assert initial['KillMode']=='control-group' and initial['CPUAccounting']==initial['MemoryAccounting']=='yes'
assert ('argv[]=/usr/bin/python3.12 '+Q+'/pin-unit-exec.py /opt/binance-market-data-recorder/venv/bin/python '+Q+'/arm-natural-wait.py') in initial['ExecStart']
assert ('argv[]=/usr/bin/python3.12 '+Q+'/restore.py capture-failure') in initial['ExecStopPost']
inv=a['fresh_active_inventory'];assert inv['walk_complete'] and inv['file_count']==inv['partial_count']==0 and not inv['entries'] and inv['raw_or_partial_bytes_opened'] is False
assert 0<=inv['ended_utc_ns']-inv['began_utc_ns']<30*10**9
assert (a['recorder']['ActiveState'],a['recorder']['UnitFileState'],a['recorder']['MainPID'],a['recorder']['NRestarts'])==('inactive','disabled','0','0')
w=controls['quiet-window-actual.json'];assert w['result']=='ACTIVE_VERIFIED' and w['boot_id']==BOOT and w['source_git_sha']=='20e57f30682e88b4090cc2f0da34aa26fca4de2e' and w['deployment_identity']=='9b8ce8219105fd7962cbf4831a99c9764c3c2889088bda7e73c834ff6bffc715' and w['formal_credit_seconds']==0
assert w['armed_at_utc_ns']==1792936179488935528 and w['finite_window_seconds']==230400 and w['deadline_epoch_seconds']==w['armed_at_utc_ns']//10**9+230400==1793166579
assert datetime.datetime.fromtimestamp(w['deadline_epoch_seconds'],datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')==w['deadline_utc']=='2026-10-28 05:49:39 UTC'
units=['apt-daily.timer','apt-daily-upgrade.timer','apt-daily.service','apt-daily-upgrade.service','unattended-upgrades.service'];assert set(w['original_states'])==set(w['masked_states'])==set(units)
for u in units:assert w['masked_states'][u]['ActiveState']=='inactive' and w['masked_states'][u]['UnitFileState']=='masked-runtime' and w['original_states'][u]['UnitFileState'] not in {'masked','masked-runtime'}
for u in ['apt-daily.timer','apt-daily-upgrade.timer','unattended-upgrades.service']:assert w['original_states'][u]['ActiveState']=='active' and w['original_states'][u]['UnitFileState']=='enabled'
assert w['negative_probe']['rc']!=0 and 'masked' in w['negative_probe']['stderr'] and 'apt-daily.service' in w['negative_probe']['stderr']
assert w['timer']['ActiveState']=='active' and w['timer']['NextElapseUSecRealtime']=='Wed 2026-10-28 05:49:39 UTC'
k=controls['kernel-update-pending.json'];km=w['masked_kernel_precondition'];assert {a:b for a,b in k.items() if a!='utc_ns'}=={a:b for a,b in km.items() if a!='utc_ns'}
assert k['result']=='KNOWN_KERNEL_ONLY_NOTICE_RETAINED' and k['boot_id']==BOOT and k['running_kernel']=='6.8.0-146-generic' and k['automatic_reboot_effective'] is False and k['scheduled_shutdown'] is False and k['reboot_performed'] is False and k['marker_modified'] is False
for name,h in [('/run/reboot-required','7f30167a2ab7158c539fa772a4b1eab5e374469b7c09a4abedeebe39bcf52cd4'),('/run/reboot-required.pkgs','72c621aa58304e95a9f418ac4ae3bd9a454735b0f9293e0541a7034e937328a2')]:assert sha(k['markers'][name]['text'].encode())==k['markers'][name]['sha256']==h and k['markers'][name]['mtime_ns']==1791958028494210381
assert k['installed_pending_packages']=='linux-base 4.5ubuntu9+24.04.2 install ok installed\nlinux-image-6.8.0-147-generic 6.8.0-147.147 install ok installed\n'
canon=controls['canonical-verification.json'];assert canon['status']=='VERIFIED' and canon['source_git_sha']==w['source_git_sha'] and canon['identity_sha256']==w['deployment_identity'] and canon['config_sha256']=='5b73db1b6ba6ac8cc7ab84cbcf0c7b688187b316644bef3df44bbb42f9283419' and canon['wheel_sha256']=='dd2cc06fd182001588b00e03f3407b94bbd648e44b14436d8933dd957e0bcb2c'
assert canon['systemd_effective']['timeout_stop_sec_usec']==90000000
for name in ['prearm-admission.json','postarm-admission.json']:
 ad=controls[name];assert ad['result']=='ADMITTED' and ad['boot_id']==BOOT and ad['remaining_seconds']>ad['required_seconds'] and ad['required_seconds']==221626.19209639446 and ad['margin_seconds']==ad['remaining_seconds']-ad['required_seconds']
 assert ad['census']['counts']==[191588,410907067,21751598870,313487954805,41554929] and ad['census']['backlog']==ad['census']['transaction_backlog']==0 and ad['writer_device']!=ad['audit_device'] and min(ad['projected_writer_free_bytes'],ad['projected_archive_free_bytes'])>10*1024**3
post=controls['postarm-admission.json'];assert post['remaining_seconds']==230390.33156776428 and post['projected_writer_free_bytes']==20074863566 and post['projected_archive_free_bytes']==1957747576016
assert post['utc_ns']>w['armed_at_utc_ns'] and post['utc_ns']<v['utc_ns']
states={name:dict(row.split('=',1) for row in body.splitlines()) for name,body in v['states'].items()}
assert states[REC]['ActiveState']=='inactive' and states[REC]['UnitFileState']=='disabled' and states[REC]['MainPID']==states[REC]['NRestarts']=='0'
assert states[AT]['ActiveState']=='active' and states[AT]['UnitFileState']=='enabled'
assert states[AS]['ActiveState']=='inactive' and states[AS]['MainPID']=='0' and states[AS]['Result']=='success'
assert states[PREFIX+'quiet-expiry.timer']['ActiveState']=='active' and states[PREFIX+'quiet-expiry.timer']['NextElapseUSecRealtime']=='Wed 2026-10-28 05:49:39 UTC'
assert states[ARM]['LoadState']=='not-found' and states[ARM]['MainPID']=='0' # GC, not outcome failure
for suffix in ['capture.service','terminal.service']:assert states[PREFIX+suffix]['LoadState']=='not-found' and states[PREFIX+suffix]['MainPID']=='0'
for name in ['owner-start.json','capture-launch.json','restoration.jsonl','expiry-cancellation.jsonl','capture-failure.json','terminal-failure.json']:assert name not in controls and name in v['missing_fixed_controls']
armrows=[json.loads(x) for x in v['unit_journals'][ARM].splitlines()];owned=[x for x in armrows if (x.get('INVOCATION_ID') or x.get('_SYSTEMD_INVOCATION_ID'))==INV]
assert len(owned)==7 and all(x['_BOOT_ID']==BOOT.replace('-','') for x in owned)
assert any(x.get('JOB_RESULT')=='done' for x in owned) and any('Deactivated successfully.' in x.get('MESSAGE','') for x in owned)
reported=json.loads(next(x['MESSAGE'] for x in owned if x.get('MESSAGE','').startswith('{')));assert reported=={'result':'ACTIVE_VERIFIED','deadline_utc':w['deadline_utc']}
done=next(x for x in owned if x.get('JOB_RESULT')=='done');assert int(done['__REALTIME_TIMESTAMP'])*1000>post['utc_ns']
assert all('failed' not in x.get('MESSAGE','').lower() for x in owned)
archive_inv=states[AS]['InvocationID'];archive_rows=[json.loads(x) for x in v['unit_journals'][AS].splitlines()];latest=[x for x in archive_rows if (x.get('INVOCATION_ID') or x.get('_SYSTEMD_INVOCATION_ID'))==archive_inv]
assert latest and all(x['_BOOT_ID']==BOOT.replace('-','') for x in latest) and any(x.get('JOB_RESULT')=='done' for x in latest) and any('Deactivated successfully.' in x.get('MESSAGE','') for x in latest)
drain=json.loads(next(x['MESSAGE'] for x in latest if x.get('MESSAGE','').startswith('{')));assert drain['command']=='archive.drain' and drain['failed_transactions']==drain['backlog_files_after']==drain['backlog_bytes_after']==0 and drain['exit_reason']=='BACKLOG_EMPTY'
(R/'owned-arm-journal.json').write_text(json.dumps(owned,sort_keys=True,indent=2)+'\n');(R/'latest-owned-archive-journal.json').write_text(json.dumps(latest,sort_keys=True,indent=2)+'\n')
references={str(prior/name):digest(prior/name) for name in ['REPORT.md','RESULT.json','MANIFEST.json','PENDING_ARM_BINDINGS.json']};(R/'PRIOR_ACCEPTANCE_REFERENCES.json').write_text(json.dumps(references,sort_keys=True,indent=2)+'\n')
remaining_read=w['deadline_epoch_seconds']-v['utc_ns']/1e9;assert remaining_read>post['required_seconds']
out={'decision':'ACCEPT','scope':'ACTUAL_ARM_OUTCOME_BINDING_ONLY','accepted_arm_dispatch_sha256':DISPATCH,'accepted_arm_read_sha256':READ,'operator_sha256':OP,'controls_manifest_sha256':CTRL,'remote_scope':Q,'original_controls_verified':13,'source':w['source_git_sha'],'identity_sha256':w['deployment_identity'],'config_sha256':canon['config_sha256'],'boot_id':BOOT,'arm_invocation':INV,'initial_arm_PID':877050,'initial_effective':{'Type':'oneshot','TimeoutStartUSec':'40min','TimeoutStopUSec':'25min','UMask':'0027','whole_phase_pin':True,'restore_ExecStopPost':True},'owned_arm_journal_rows':len(owned),'owned_manager_done_utc_ns':int(done['__REALTIME_TIMESTAMP'])*1000,'window_sha256':v['files']['quiet-window-actual.json']['sha256'],'window_result':'ACTIVE_VERIFIED','armed_at_utc_ns':w['armed_at_utc_ns'],'fixed_deadline_epoch_seconds':w['deadline_epoch_seconds'],'fixed_deadline_UTC':w['deadline_utc'],'window_seconds':230400,'never_extend_reuse_or_reARM':True,'canonical_sha256':v['files']['canonical-verification.json']['sha256'],'prearm_admission_sha256':v['files']['prearm-admission.json']['sha256'],'postarm_admission_sha256':v['files']['postarm-admission.json']['sha256'],'postarm_remaining_seconds':post['remaining_seconds'],'postarm_required_seconds':post['required_seconds'],'postarm_margin_seconds':post['margin_seconds'],'postarm_projected_writer_bytes':post['projected_writer_free_bytes'],'postarm_projected_archive_bytes':post['projected_archive_free_bytes'],'remaining_at_read_seconds':remaining_read,'original_5_OS_states_preserved_and_masked_at_ARM':True,'negative_probe_rejected':True,'kernel146_pending147_notice_unchanged':True,'normal_archive_enabled_active_latest_success_invocation':archive_inv,'recorder_inactive_AND_disabled_PID0_NRestarts0_at_read':True,'capture_T0_restoration_cancellation_failure_absent_at_read':True,'transient_GC_not_failure':True,'formal_credit_seconds':0,'data_or_capacity_credit':False,'source_design_code_Linux_tests_repeated':False,'review_network_Raw_service_actions':False,'future_launch_requires_fresh_live_admission':True,'findings':[]}
(R/'RECONSTRUCTION.json').write_text(json.dumps(out,sort_keys=True,indent=2)+'\n');print(json.dumps(out,sort_keys=True))
