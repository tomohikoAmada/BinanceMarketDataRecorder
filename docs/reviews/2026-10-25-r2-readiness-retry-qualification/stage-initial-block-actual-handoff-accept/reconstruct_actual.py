from pathlib import Path
import ast,base64,collections,hashlib,json,math,shutil
R=Path(__file__).resolve().parent;I=R/'inputs';P=Path('/private/var/tmp/bmdr-r2-n2-readiness-retry-20261025-1s0kswlt')
H='e7dfc0f5aea159eb3fbe8b0d19b3a9c2b6f4fcb149c836eca72578ecf9401bd9'
def sha(b):return hashlib.sha256(b).hexdigest()
b=(I/'actual-current-preflight.json').read_bytes();assert sha(b)==H;v=json.loads(b);h=v['fresh_failed_attempt_handoff'];assert v['result']=='CURRENT_N2_METADATA_PREFLIGHT_NOT_ADMISSION'
for suffix in ['execution.json','stdout.txt','stderr.txt']:
 p=P/('preflight-1792912997523101000-'+suffix);shutil.copyfile(p,I/p.name)
e=json.loads((I/'preflight-1792912997523101000-execution.json').read_bytes());assert e['returncode']==0 and e['error_type'] is None and e['remote_retry'] is False
assert e['helper_sha256']=='abe667eca3ced0666dd4e2f54a9335218ca93b6f5cb512e01308300249ef8ef6'
assert (I/'preflight-1792912997523101000-stdout.txt').read_bytes()==b and (I/'preflight-1792912997523101000-stderr.txt').read_bytes()==b''
accepted=Path('/private/var/tmp/bmdr-r2-retry-preflight-lossless-review-20261025-qie5itpl/inputs/read_current_capacity.py')
t=ast.parse(accepted.read_bytes());remote=next(ast.literal_eval(n.value) for n in t.body if isinstance(n,ast.Assign) and any(isinstance(x,ast.Name) and x.id=='REMOTE' for x in n.targets));assert e['remote_sha256']==sha(remote.encode())
packcount=0
commands=[]
def check(node):
 global packcount
 if isinstance(node,dict):
  if set(node)=={'base64','bytes','sha256'}:
   raw=base64.b64decode(node['base64'],validate=True);assert len(raw)==node['bytes'] and sha(raw)==node['sha256'];packcount+=1
  if 'argv' in node and 'stdout_raw' in node:
   assert node['returncode']==0
   for key in ['stdout','stderr']:
    raw=base64.b64decode(node[key+'_raw']['base64'],validate=True);assert raw.decode('utf-8')==node[key] and node[key+'_decode_error'] is None
   commands.append(node)
  for value in node.values():check(value)
 elif isinstance(node,list):
  for value in node:check(value)
check(v)
assert v['source']=='20e57f30682e88b4090cc2f0da34aa26fca4de2e' and v['identity_sha256']=='9b8ce8219105fd7962cbf4831a99c9764c3c2889088bda7e73c834ff6bffc715' and v['config_sha256']=='5b73db1b6ba6ac8cc7ab84cbcf0c7b688187b316644bef3df44bbb42f9283419'
assert v['boot']=='59bb1735-ac48-408d-b3ec-bda79fc49b41' and v['deployment_result_sha256']=='e148f51a26aea4d0da81d96d9700900f3f4f171e8b1c0e1f8d9aa6c03f859a7f'
assert v['new_scope_created'] is False and v['proposed_scope_absent'] and v['read_only_metadata'] and v['formal_credit_seconds']==0 and v['raw_or_manifest_payload_opened'] is False
canonical=json.loads(h['canonical_verification']['stdout']);assert canonical==h['canonical_result'];assert canonical['status']=='VERIFIED' and canonical['config_sha256']==v['config_sha256'] and canonical['identity_sha256']==v['identity_sha256'] and canonical['source_git_sha']==v['source']
assert canonical['wheel_sha256']=='dd2cc06fd182001588b00e03f3407b94bbd648e44b14436d8933dd957e0bcb2c' and canonical['systemd_effective']['timeout_stop_sec_usec']==90000000
assert canonical['installed_dependencies']['exact_match'] and canonical['installed_dependencies']['installed_distribution_count']==canonical['installed_dependencies']['locked_distribution_count']==28
assert h['definition_pin_only_no_start'] and h['ended_within_bound'] and h['bounded_seconds']==180
rec=h['recorder'];assert rec==v['recorder'] and (rec['ActiveState'],rec['UnitFileState'],rec['MainPID'],rec['NRestarts'])==('inactive','disabled','0','0')
previous=Path('/private/var/tmp/bmdr-r2-instrumented-n2-failure-policy-review-20261025-MGvkiZG6')
for name,key,digest in [('original-restoration.jsonl','restoration','672d6fafa2277196bd72ae80721e3ab2cb0afcc440aaac5ddf3ff75f00b8a788'),('original-expiry-cancellation.jsonl','cancellation','9731ba3a0f01bfaa477af052af460f78a4c2edf7dae5618f1937e4343e4ed1d6')]:
 data=(previous/name).read_bytes();assert sha(data)==digest and json.loads(data)==h[key]
rest=h['restoration'];cancel=h['cancellation'];assert rest['result']=='RESTORED' and rest['errors']==[] and rest['boot_id']==v['boot'];assert rest['ended_at_utc_ns']==cancel['restoration_ended_at_utc_ns']<cancel['cancelled_at_utc_ns']
assert h['backlogs']==[0,0] and v['backlog']==v['transaction_backlog']==0 and h['startup_custody_gate'] and h['partial_count']==0 and h['partial_inventory']==[] and h['active_files_scanned_metadata_only']==0 and h['active_directories_scanned']==['/var/lib/binance-market-data-recorder/data/active'] and not h['partial_bytes_opened']
att=h['coherent_attempts'];assert len(att)==1 and att[0]['result']=='COHERENT_SNAPSHOT';att=att[0];assert len(att['command_observations'])==11
worker=h['archive_worker'];assert worker==att['worker_begin']==att['worker_end'] and (worker['ActiveState'],worker['MainPID'],worker['Result'])==('inactive','0','success') and worker['InvocationID']=='07dc122a931f4e1f939d28ea4872e2e4'
assert h['archive_worker_owned_journal']==att['owned_journal'] and h['recorder']==att['recorder'] and h['owners']==att['owners']
for command in att['command_observations']:
 if command['argv'][0]=='systemctl':
  parsed=dict(line.split('=',1) for line in command['stdout'].splitlines());unit=command['argv'][2]
  if unit=='binance-market-data-recorder.service':assert parsed==rec
  elif unit=='binance-market-data-archive.service':assert parsed==worker
  else:assert parsed==h['owners'][unit]
jr=[json.loads(x) for x in h['archive_worker_owned_journal']['stdout'].splitlines()];assert len(jr)==5
for row in jr:assert row['_BOOT_ID']==v['boot'].replace('-','') and (row.get('_SYSTEMD_INVOCATION_ID') or row.get('INVOCATION_ID'))==worker['InvocationID'] and int(row['__REALTIME_TIMESTAMP'])*1000>rest['ended_at_utc_ns']
assert any(r.get('JOB_RESULT')=='done' for r in jr) and any('Deactivated successfully.' in r['MESSAGE'] for r in jr)
drain=json.loads(next(r['MESSAGE'] for r in jr if r.get('_SYSTEMD_UNIT')=='binance-market-data-archive.service'));assert drain['command']=='archive.drain' and drain['exit_reason']=='BACKLOG_EMPTY' and drain['failed_transactions']==drain['backlog_files_after']==drain['backlog_bytes_after']==0
for u in ['apt-daily.timer','apt-daily-upgrade.timer','unattended-upgrades.service','binance-market-data-archive.timer']:assert h['owners'][u]['ActiveState']=='active' and h['owners'][u]['UnitFileState']=='enabled'
assert all(x['UnitFileState'] not in {'masked','masked-runtime'} for x in h['owners'].values())
f=h['future_deadline_check'];assert f==att['future_deadline_check'] and f['clock']=='CLOCK_MONOTONIC' and f['checked_at_utc_ns']==h['utc_ns'] and f['monotonic_us']<f['next_elapse_usec_monotonic']<(1<<64)-1 and f['remaining_us']==f['next_elapse_usec_monotonic']-f['monotonic_us']==25932545
elapsed=(h['utc_ns']-att['began_at_utc_ns'])/1e9;assert 0<elapsed<180
for name in ['/run/reboot-required','/run/reboot-required.pkgs']:
 marker=v['reboot_markers'][name];assert marker['exists'] and marker['uid']==0 and marker['mode']==420 and sha(marker['text'].encode())==marker['sha256']
assert v['kernel']['stdout']=='6.8.0-146-generic\n' and v['reboot_markers']['/run/systemd/shutdown/scheduled']['exists'] is False and v['ntp']['stdout'].strip()=='yes' and not v['package_audit']['stdout'].strip()
window=v['diagnostic_metadata_window'];rows=window['rows'];assert len(rows)==25639 and window['columns']==['chunk_id','state','sealed_path','record_count','uncompressed_bytes','stored_bytes','created_at_utc_ns'];assert (window['start_utc_ns'],window['end_utc_ns'])==(1792821038109362000,1792907346512229095)
ids=set();minutes=collections.defaultdict(lambda:[0,0,0,0])
for row in rows:
 assert row['chunk_id'] not in ids;ids.add(row['chunk_id']);assert window['start_utc_ns']<=row['created_at_utc_ns']<=window['end_utc_ns'] and row['state']=='LOCAL_DELETED'
 vals=[1,row['record_count'],row['stored_bytes'],row['uncompressed_bytes']];assert all(type(x)is int and x>=0 for x in vals)
 aggregate=minutes[row['created_at_utc_ns']//(60*10**9)]
 for i,value in enumerate(vals):aggregate[i]+=value
rates=[max(x[i] for x in minutes.values())/60 for i in range(4)];forecast=json.loads((I/'current-forecast.json').read_bytes());assert rates==forecast['current_creation_minute_rates'];combined=[max(a,b) for a,b in zip(rates,forecast['historical_seal_rates'])];assert combined==forecast['combined_rates']
common=ast.parse((I/'runtime/common.py').read_bytes());forecastnode=next(x for x in common.body if isinstance(x,ast.FunctionDef) and x.name=='forecast')
policy=json.loads((I/'runtime/capacity-policy.json').read_bytes());budgets=policy['spool_decoded_budgets'];g={'math':math,'configured_cohort':lambda:{'products':forecast['forecast']['configured_products'],'expected_spool_keys':set(budgets),'spools':42},'predecessor':lambda:policy,'Q':types.SimpleNamespace() if False else None}
class FakePath:
 def __truediv__(self,name):return self
 def exists(self):return False
g['Q']=FakePath();exec(compile(ast.Module(body=[forecastnode],type_ignores=[]),'<isolated-pure-forecast>','exec'),g)
recomputed=g['forecast'](v['counts'],v['metadata_bytes'],v['catalog_bytes'],v['free']['writer'],v['free']['archive'],88560);assert recomputed==forecast['forecast']
required=1200+86460+900+900+recomputed['audit_budget_125percent_seconds']+3600+180;assert required==forecast['required_seconds'];assert math.ceil((required+7200)/3600)*3600==forecast['proposed_finite_window_seconds']==230400
result={'decision':'ACCEPT','scope':'ACTUAL_FRESH_STOPPED_METADATA_HANDOFF_AND_PLANNING_ONLY','accepted_preflight_sha256':H,'input_bytes':len(b),'raw_pack_occurrences_verified':packcount,'command_occurrences_verified':len(commands),'coherent_attempt_count':1,'coherent_attempt_elapsed_wall_seconds':elapsed,'natural_archive_invocation':worker['InvocationID'],'natural_archive_journal_rows':len(jr),'future_monotonic_remaining_seconds_at_collection':f['remaining_us']/1e6,'partial_count':0,'files_scanned':0,'directories_scanned':1,'metadata_rows_reconstructed':len(rows),'creation_minute_bins':len(minutes),'current_creation_minute_rates':rates,'combined_planning_rates':combined,'forecast_exact_reconstruction':True,'required_seconds':required,'window_seconds':230400,'projected_writer_free_bytes':recomputed['projected_writer_free_bytes'],'projected_archive_free_bytes':recomputed['projected_archive_free_bytes'],'actual_admission':False,'real_linux_qualification':False,'current_capture_or_saved_frame_credit':False,'formal_credit_seconds':0,'review_only_local':True}
(R/'ACTUAL_RECONSTRUCTION.json').write_text(json.dumps(result,indent=2,sort_keys=True)+'\n');print(json.dumps(result,sort_keys=True))
