from pathlib import Path
import ast,base64,hashlib,json,math
R=Path(__file__).resolve().parent;I=R/'inputs';P=Path('/private/var/tmp/bmdr-r2-n2-readiness-retry-20261025-1s0kswlt')
H='3c09dd59f8691ce3b3de1d1f692cc61a057542dd17551e8c1882edc2c59bba00';O='f3b7b6bfc159b934408be326e6e157f031c3714b14fe8e09719c50dc06e603a9';C='d3ed70e54b3e2fe1131f0fdffe7eaf1b7637aaafc673a188681dc38ad905b54f'
def sha(b):return hashlib.sha256(b).hexdigest()
def digest(p):return {'sha256':sha(p.read_bytes()),'bytes':p.stat().st_size}
raw=(I/'actual-n2-stage-copy-1792913610397799000.json').read_bytes();assert sha(raw)==H;v=json.loads(raw);ex=json.loads((I/'actual-n2-stage-copy-1792913610397799000.execution.json').read_bytes());assert (I/'actual-n2-stage-copy-1792913610397799000.stderr').read_bytes()==b'' and ex['returncode']==0 and ex['error_type'] is None and ex['remote_retry'] is False and ex['operator_sha256']==O
assert digest(I/'stage-operator.py')['sha256']==O and digest(I/'stage-controls-manifest.json')['sha256']==C
# Only isolated pure payload/code-generation definitions are evaluated, never main
# or any generated remote program. Inputs are LOCAL immutable controls.
tree=ast.parse((I/'stage-operator.py').read_bytes());g={'__file__':str(I/'stage-operator.py'),'Path':Path,'base64':base64,'hashlib':hashlib,'json':json}
exec(compile(ast.Module(body=[n for n in tree.body if isinstance(n,(ast.Assign,ast.FunctionDef)) and not(isinstance(n,ast.FunctionDef) and n.name=='main')],type_ignores=[]),'<isolated-actual-payload-reconstruction>','exec'),g)
g.update(P=P,RUNTIME=P/'runtime',MANIFEST=I/'stage-controls-manifest.json',BINDING=I/'stage-qualification.json')
m,runtime=g['load_design']();binding=json.loads((I/'stage-qualification.json').read_bytes());assert binding['state']=='DESIGN_ACCEPTED_COPY_ONLY'
payload=g['approved_payload'](m,runtime,'copy');assert len(payload)==27
program=g['build_remote']('copy');assert sha(program.encode())==ex['remote_sha256'];(R/'exact-copy-program.py').write_text(program)
S=R/'software-inputs';S.mkdir()
for name,data in payload.items():(S/name).write_bytes(data)
assert v['result']=='EXCLUSIVE_N2_CONTROLS_COPIED_NOT_ARMED' and v['scope']==m['scope'] and v['formal_credit_seconds']==0
assert set(v['physical'])==set(payload)
for name,data in payload.items():
 actual=v['physical'][name];assert actual=={'sha256':sha(data),'bytes':len(data),'uid':0,'gid':988,'mode':416},name
assert v['scope_stat']==[0,988,488] and v['tmp_stat']==[999,988,504]
inv=v['fresh_active_inventory'];assert inv['root']=='/var/lib/binance-market-data-recorder/data/active' and inv['walk_complete'] and inv['file_count']==inv['partial_count']==0 and inv['entries']==[] and inv['directories']==[inv['root']] and inv['raw_or_partial_bytes_opened'] is False
assert 0<inv['ended_utc_ns']-inv['began_utc_ns']<30*10**9
rec=v['recorder'];assert (rec['ActiveState'],rec['UnitFileState'],rec['MainPID'],rec['NRestarts'])==('inactive','disabled','0','0')
assert rec['Result']=='success'
qa=v['linux_classifier_qualification'];assert qa['result']=='GENUINE_LINUX_CLASSIFIER39_PASS' and qa['platform']=='linux' and qa['python_executable']=='/opt/binance-market-data-recorder/venv/bin/python' and qa['boottime_ns']==1978521537836320
assert qa['candidate_sha256']==m['members']['qualification_owner_995.py']=='621063b04ba21103206282519fa4b14731e599118a05f5a132c11b72abf3cdd2'
assert qa['fixture_result_sha256']=='4b5ad6d4c8ae395f7520e54d2d48dbb59192dcc9de7228b15a5f7b8dfe3bcd81' and qa['returncode']==0 and qa['error_type'] is None and qa['fixture_parse_error'] is None and qa['case_count']==39
assert qa['real_capture_duration_credit_ns']==0 and qa['production_or_raw_actions'] is False
packs=0
for name in ['fixture_result','stdout','stderr']:
 item=qa[name+'_raw'];data=base64.b64decode(item['base64'],validate=True);assert len(data)==item['bytes'] and sha(data)==item['sha256'];(R/('observed-linux-'+name+'.bin')).write_bytes(data);packs+=1
result=json.loads(base64.b64decode(qa['fixture_result_raw']['base64']));assert result['case_count']==39 and len(result['cases'])==39 and len({x['case'] for x in result['cases']})==39 and all(x['PASS'] for x in result['cases'])
assert result['result']=='LOCAL_CLASSIFIER_AND_FIXED_MONITOR_REGRESSION_PASS' and result['actual_trigger_reproduced_in_original'] and result['all_other_policy_AST_unchanged'] and result['candidate_sha256']==qa['candidate_sha256'] and result['synthetic_86400_seconds_only'] and result['formal_duration_credit_seconds']==0
summary=json.loads(base64.b64decode(qa['stdout_raw']['base64']));assert summary=={k:val for k,val in result.items() if k!='cases'} and base64.b64decode(qa['stderr_raw']['base64'])==b''
requiredcases=['normal_ready','FAILED_even_without_failure_text','service_failure','shutdown','actual_disk_reserve','writer_error','real_planned_rotation_trigger_stays_write_safe_but_not_ready','mixed_four_products_real_FAILED_not_masked_by_other_rotations','fake_own86400_complete2880_rotation_faults_remain_five_bad_probes_excluded','missing_nominal_stays_bad','late_and_missing_both_stay_bad','more_than_fourteen_bad_cannot_pass_995']
caseby={x['case']:x for x in result['cases']};assert all(n in caseby for n in requiredcases)
for reason in ['planned_rotation','unexpected_disconnect','server_shutdown','ingress_backpressure','sequence_gap']:assert caseby['recognized_connecting_'+reason]['expected_action']=='continue_quality_logging' and caseby['real_failed_even_with_'+reason]['expected_action']=='pause_untrusted_or_terminal'
smoke=v['linux_binding_smoke'];assert smoke['result']=='CURRENT_LINUX_CLEAN_IMPORT_AND_BINDING_SMOKE_PASS' and smoke['members']==m['members']
imports={name:m['scope']+'/'+name for name in m['members'] if name.endswith('.py') and name!='pin-unit-exec.py'};assert len(imports)==12 and smoke['imports']==imports
assert smoke['package_origin']=='/opt/binance-market-data-recorder/venv/lib/python3.12/site-packages/binance_market_data_recorder/__init__.py' and smoke['source']==m['source'] and smoke['identity_sha256']==m['actual_identity_sha256']
assert smoke['historical_n1_source']=='c3686dd76406c8d483b23eb10417349e5a1ffecd' and smoke['inherited_duration_ns']==smoke['formal_credit_seconds']==0 and smoke['boottime_ns']>qa['boottime_ns']
assert smoke['configured_products']==[['spot','BTCUSDT'],['spot','ETHUSDT'],['um_perpetual','BTCUSDT'],['um_perpetual','ETHUSDT']] and smoke['contexts']==26 and smoke['spools']==42
cs=v['census'];assert cs['counts']==m['expected_stopped_counts'] and cs['backlog']==cs['transaction_backlog']==0
policy=v['owner_predecessor'];assert policy==json.loads(payload['capacity-policy.json']) and policy['current_source_git_sha']==m['source'] and policy['current_identity_sha256']==m['actual_identity_sha256'] and policy['inherited_duration_ns']==policy['prior_stage_receipt']['inherited_duration_ns']==0 and policy['prior_stage_receipt']['historical_only']
ad=v['admission'];assert ad['result']=='ADMITTED' and ad['boot_id']=='59bb1735-ac48-408d-b3ec-bda79fc49b41' and ad['census']==cs and ad['remaining_seconds']==230400 and ad['required_seconds']==221626.19209639446 and ad['margin_seconds']==ad['remaining_seconds']-ad['required_seconds']
assert ad['writer_device']!=ad['audit_device'] and min(ad['projected_writer_free_bytes'],ad['projected_archive_free_bytes'])>10*1024**3
# Reconstruct only the already reviewed pure numerical forecast against the new
# copy-time actual figures; this is no live admission rerun.
common=ast.parse(payload['common.py']);node=next(n for n in common.body if isinstance(n,ast.FunctionDef) and n.name=='forecast')
class FakeQ:
 def __truediv__(self,n):return self
 def exists(self):return False
budget=policy['spool_decoded_budgets'];fg={'math':math,'Q':FakeQ(),'configured_cohort':lambda:{'products':smoke['configured_products'],'expected_spool_keys':set(budget),'spools':42},'predecessor':lambda:policy};exec(compile(ast.Module(body=[node],type_ignores=[]),'<isolated-copy-time-numerical-forecast>','exec'),fg)
f=fg['forecast'](cs['counts'],ad['metadata_bytes'],ad['catalog_bytes'],ad['writer_free_bytes'],ad['archive_free_bytes'],88560)
for name,val in f.items():assert ad[name]==val,name
assert ad['required_seconds']==1200+86460+900+900+1.25*f['estimated_full_dual_live_verify_seconds']+3600+180
assert ad['projected_writer_free_bytes']==20075862990 and ad['projected_archive_free_bytes']==1957747707088
# Pending ARM argv is generated/parsed only; actual acceptance must follow a
# genuine future ARM result and fresh live admission, never this static binding.
g['approved_payload']=lambda m,runtime,mode:payload
armprogram=g['build_remote']('arm');compile(armprogram,'<pending-arm-compile-only>','exec')
a=next(ast.literal_eval(n.value) for n in ast.parse(armprogram).body if isinstance(n,ast.Assign) and any(isinstance(x,ast.Name) and x.id=='a' for x in n.targets))
assert a[:4]==['systemd-run','--no-block','--unit','binance-recorder-r2-capacity-n2-readiness-retry-20261025-1s0kswlt-arm.service']
for value in ['--property=Type=oneshot','--property=TimeoutStartSec=2400','--property=TimeoutStopSec=1500','--property=UMask=0027','--property=ExecStopPost=/usr/bin/python3.12 '+m['scope']+'/restore.py capture-failure','--setenv=TMPDIR='+m['scope']+'/tmp','--setenv=SQLITE_TMPDIR='+m['scope']+'/tmp']:assert value in a
assert a[-4:]==['/usr/bin/python3.12',m['scope']+'/pin-unit-exec.py','/opt/binance-market-data-recorder/venv/bin/python',m['scope']+'/arm-natural-wait.py']
(R/'PENDING_ARM_BINDINGS.json').write_text(json.dumps({'argv':a,'argv_sha256':sha((json.dumps(a,sort_keys=True)+'\n').encode()),'scope':m['scope'],'finite_window_seconds':230400,'actual_ARM_accepted':False,'absolute_expiry_not_yet_exists':True,'kernel_precondition':'same boot59bb, running146, exact retained147 markers/packages, no scheduled reboot or package worker; checked again by exact arm.py','production_Stop90_unchanged':True},indent=2)+'\n')
output={'decision':'ACCEPT','scope':'ACTUAL_CURRENT_LINUX_CLASSIFIER39_IMPORT_SMOKE_PHYSICAL_COPY_AND_COPYTIME_ADMISSION_ONLY','accepted_copy_sha256':H,'controls_manifest_sha256':C,'operator_sha256':O,'remote_copy_program_sha256':ex['remote_sha256'],'remote_scope':m['scope'],'source':m['source'],'identity_sha256':m['actual_identity_sha256'],'current_runtime_members':m['members'],'payload_count':27,'all_observed_payload_SHA_bytes_uid0_gid988_mode0640_match':True,'scope_root0_gid988_mode0750':True,'tmp_uid999_gid988_mode0770':True,'real_Linux_classifier_cases':39,'real_Linux_imports':12,'raw_QA_packs_verified':packs,'synthetic_fixture_seconds_no_real_duration':True,'candidate_sha256':qa['candidate_sha256'],'fixture_result_sha256':qa['fixture_result_sha256'],'Linux_QA_boottime_ns':qa['boottime_ns'],'current_products':4,'contexts':26,'spools':42,'fresh_copy_inventory_files':0,'fresh_copy_inventory_partials':0,'copy_admission_remaining_seconds':ad['remaining_seconds'],'copy_admission_required_seconds':ad['required_seconds'],'copy_admission_margin_seconds':ad['margin_seconds'],'copy_projected_writer_bytes':ad['projected_writer_free_bytes'],'copy_projected_archive_bytes':ad['projected_archive_free_bytes'],'forecast_exact_reconstruction':True,'actual_ARM_or_window_accepted':False,'capture_or_T0_exists':False,'formal_credit_seconds':0,'saved_frame_or_capacity_credit':False,'historical_source_or_time_transfer':False,'review_SSH_Raw_service_action_or_test_rerun':False}
(R/'RECONSTRUCTION.json').write_text(json.dumps(output,indent=2,sort_keys=True)+'\n');print(json.dumps({k:v for k,v in output.items() if k!='current_runtime_members'},sort_keys=True))
