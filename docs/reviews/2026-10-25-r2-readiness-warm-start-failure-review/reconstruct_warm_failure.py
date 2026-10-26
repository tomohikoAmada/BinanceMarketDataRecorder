from pathlib import Path
import base64,collections,datetime,hashlib,json,shutil
R=Path(__file__).resolve().parent;I=R/'inputs';S=R/'source';S.mkdir(exist_ok=True);PUB=R/'publications';PUB.mkdir(exist_ok=True)
H='f8845961effd46b6ccd95865cecccbc53f90382342aa066a20f23ce04363bea9';Q='/srv/recorder-data/recorder-archive/evidence/R2-capacity-n2-readiness-retry-20261025-1s0kswlt';BOOT='59bb1735-ac48-408d-b3ec-bda79fc49b41';CAP='binance-recorder-r2-capacity-n2-readiness-retry-20261025-1s0kswlt-capture.service';REC='binance-market-data-recorder.service';AS='binance-market-data-archive.service';AT='binance-market-data-archive.timer';CINV='9d8bdd0b284e405b92639373d9664396';RINV='f5da35711bba4d268974a73858cf1266'
def sha(b):return hashlib.sha256(b).hexdigest()
def digest(p):return {'sha256':sha(p.read_bytes()),'bytes':p.stat().st_size}
file=I/'actual-n2-stage-read-1792938767090785000.json';b=file.read_bytes();assert sha(b)==H and len(b)==5604616;v=json.loads(b);e=json.loads(file.with_suffix('.execution.json').read_bytes());assert e['returncode']==0 and e['error_type'] is None and e['remote_retry'] is False and e['operator_sha256']=='f3b7b6bfc159b934408be326e6e157f031c3714b14fe8e09719c50dc06e603a9' and file.with_suffix('.stderr').read_bytes()==b''
assert v['scope']==Q and v['boot_id']==BOOT and v['parse_errors']=={} and len(v['files'])==22
control={}
for name,item in v['files'].items():
 raw=base64.b64decode(item['base64'],validate=True);assert len(raw)==item['bytes'] and sha(raw)==item['sha256'] and raw.decode()==item['content'];assert raw==(R/'original-controls'/name).read_bytes();assert item['uid']==0 and not(item['mode'] & 0o022);assert (item['gid'],item['mode'])==((0,0o644) if name=='warmup.jsonl' else ((0,0o640) if name=='expiry-cancellation.jsonl' else (988,0o640)))
 if name!='warmup.jsonl':control[name]=json.loads(raw)
for name in ['owner-start.json','warmup-result.json','warmup-end-observation.json','prestart-admission.json','start-observation.json','monitor-rate.json','monitoring.jsonl','terminal-start.json','terminal-audit/audit-root.json']:
 assert name not in v['files'] and name in v['missing_fixed_controls']
controller=Path('/private/var/tmp/bmdr-r2-n2-readiness-retry-20261025-1s0kswlt/runtime/controller.py');assert digest(controller)['sha256']=='3b3f8d50338c82c594cb346eb49686adb235f2d7c5d3ce13e966bf3cc4cd98f0';shutil.copyfile(controller,S/'controller.py')
C=Path('/Users/amada/.codex/worktrees/r2-close-diagnostics/BinanceMarketDataRecorder/src/binance_market_data_recorder')
refs={str(controller):digest(controller)}
for relative in ['cli.py','service/readiness.py']:
 p=C/relative;refs[str(p)]=digest(p);out=S/relative;out.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(p,out)
cat=C/'storage/catalog.py';refs[str(cat)]=digest(cat);(S/'catalog-integrity-excerpt.txt').write_text('\n'.join(cat.read_text().splitlines()[427:434])+'\n')
(R/'SOURCE_REFERENCES.json').write_text(json.dumps({'source_git_sha':'20e57f30682e88b4090cc2f0da34aa26fca4de2e','references':refs,'source_static_only':True},indent=2,sort_keys=True)+'\n')
rows=[json.loads(x) for x in (R/'original-controls/warmup.jsonl').read_text().splitlines()];assert len(rows)==38
expected={'spot:'+s+':exchange_info' for s in ['BTCUSDT','ETHUSDT']}|{'um_perpetual:'+s+':'+kind for s in ['BTCUSDT','ETHUSDT'] for kind in ['basis_5m','funding_history','global_long_short_ratio_5m','liquidation','mark_price','open_interest','open_interest_statistics_5m','premium_index_snapshot','taker_buy_sell_volume_5m','top_long_short_account_ratio_5m','top_long_short_position_ratio_5m']}|{'um_perpetual:GLOBAL:exchange_info','um_perpetual:GLOBAL:funding_info'};assert len(expected)==26
summary=[];paired=0;pubstatus=collections.Counter();gstatus=collections.Counter();serviceids=set();starttimes=set();maps=[]
for index,row in enumerate(rows):
 raw=base64.b64decode(row['state_publication_base64'],validate=True);assert len(raw)==row['state_publication_bytes'] and sha(raw)==row['state_sha256'];(PUB/(str(index).zfill(3)+'.state')).write_bytes(raw);s=json.loads(raw)
 assert row['publication_complete'] and row['observation_phase']=='complete' and row['boot_id']==BOOT and row['config_sha256']=='5b73db1b6ba6ac8cc7ab84cbcf0c7b688187b316644bef3df44bbb42f9283419'
 assert row['runtime']['InvocationID']==RINV and row['runtime']['MainPID']=='877744' and row['runtime']['NRestarts']=='0' and row['runtime']['ActiveState']=='active'
 assert s['pid']==877744 and s['service_instance_id']==row['recorder_service_instance_id'] and s['heartbeat_at_utc_ns']==row['heartbeat_at_utc_ns'] and s['core_ready']==row['core_ready'] and s['failure'] is None and s['shutdown_reason'] is None
 identity=s['deployment_identity'];assert identity['source_git_sha']=='20e57f30682e88b4090cc2f0da34aa26fca4de2e' and identity['identity_sha256']=='9b8ce8219105fd7962cbf4831a99c9764c3c2889088bda7e73c834ff6bffc715' and identity['config_sha256']==row['config_sha256']
 assert identity['wheel_sha256']=='dd2cc06fd182001588b00e03f3407b94bbd648e44b14436d8933dd957e0bcb2c';serviceids.add(s['service_instance_id']);starttimes.add(s['started_at_utc_ns'])
 observed={}
 for context in expected:
  market,symbol,kind=context.split(':');observed[context]=s['global_usdm_side_data'].get(kind,{}) if symbol=='GLOBAL' else s['products'].get(market,{}).get(symbol,{}).get('side_data',{}).get(kind,{})
 for gate_name in ['auxiliary_gate','strict_auxiliary_gate']:
  gate=row[gate_name];assert set(gate['contexts'])==expected and gate['contexts']==observed and gate['expected_auxiliary_contexts']==26 and gate['observed_at_utc_ns']==row['utc_ns']
  assert gate['allow_empty_recovery'] is False # all warm samples strict=True
  assert (gate['result']=='PASS')==(gate['reasons']==[]);paired+=26
 r=row['resources'];assert r['cgroup_swap_bytes']==r['cgroup_swap_peak_bytes']==0 and r['rss_bytes']<1024**3 and r['host_available_bytes']>2*1024**3 and min(r['writer_free_bytes'],r['archive_free_bytes'])>10*1024**3
 assert r['pid_cgroup'].strip()=='0::/system.slice/'+REC and r['recorder_cgroup_path']=='/system.slice/'+REC
 assert s['status']==('STARTING' if index<27 else 'RUNNING') and row['write_safety_verified']==(index>=27) and row['core_ready']==(index>=27)
 assert row['auxiliary_gate']['result']==row['strict_auxiliary_gate']['result']==('BLOCK' if index<27 else 'PASS')
 if index<27:assert s['capacity'] is None and s['startup_recovery_complete'] is False
 else:
  assert s['capacity']['actual_hard_reserve_reached'] is False and s['startup_recovery_complete'] is True and s['catalog_open'] is True
  assert 0<=row['utc_ns']-s['heartbeat_at_utc_ns']<=15*10**9
  for market,products in s['products'].items():
   for symbol,product in products.items():assert product['ready'] is True and product['failure'] is None and product['status']=='READY' and product['snapshot_persisted'] and product['orderbook_synchronized'] and set(product['connected_streams'])>= {'agg_trade','book_ticker','diff_depth'}
 if index:
  assert row['utc_ns']>rows[index-1]['utc_ns'] and row['boottime_ns']>rows[index-1]['boottime_ns']
  assert abs((row['utc_ns']-rows[0]['utc_ns'])-(row['boottime_ns']-rows[0]['boottime_ns']))<2*10**9
 pubstatus[s['status']]+=1;gstatus[row['strict_auxiliary_gate']['result']]+=1
 summary.append({'index':index,'utc_ns':row['utc_ns'],'boottime_ns':row['boottime_ns'],'state_sha256':row['state_sha256'],'status':s['status'],'write_safety_verified':row['write_safety_verified'],'core_ready':row['core_ready'],'official':row['auxiliary_gate']['result'],'strict':row['strict_auxiliary_gate']['result'],'strict_reasons':row['strict_auxiliary_gate']['reasons'],'heartbeat_age_ns':row['utc_ns']-s['heartbeat_at_utc_ns'],'resources':{k:r[k] for k in ['rss_bytes','rss_peak_bytes','host_available_bytes','cgroup_swap_bytes','cgroup_swap_peak_bytes','writer_free_bytes','archive_free_bytes','host_swap_used_bytes']}})
assert len(serviceids)==len(starttimes)==1 and paired==1976
first=rows[27];last=rows[-1];span_bt=(last['boottime_ns']-first['boottime_ns'])/1e9;assert span_bt>=300 and len(rows)>=12
f=control['capture-failure.json'];assert f['result']=='FAILED' and f['action']=='PAUSE_AFFECTED_CAPTURE' and f['error_type']=='AssertionError' and f['error']=='warmup exceeded 1200s' and 'line 201' in f['traceback'] and f['utc_ns']==1792938059608992364
# The failure occurs after the readiness state assertion, so READY passed; the
# original result bytes and private begin/request timestamps were never saved.
code=(S/'controller.py').read_text();assert "ready=json.loads(run(CLI+['deployment','readiness'],330));assert ready['state']=='READY'\n    assert time.monotonic()<warm_deadline,'warmup exceeded 1200s'" in code
assert "begin=time.monotonic();warm_deadline=begin+1200" in code
native_all=[json.loads(x) for x in v['unit_journals'][CAP].splitlines()];native=[x for x in native_all if (x.get('_SYSTEMD_INVOCATION_ID') or x.get('INVOCATION_ID'))==CINV];assert native and all(x['_BOOT_ID']==BOOT.replace('-','') for x in native)
opened=next(x for x in native if x.get('_COMM')=='runuser' and 'session opened' in x.get('MESSAGE',''));closed=next(x for x in native if x.get('_COMM')=='runuser' and 'session closed' in x.get('MESSAGE',''));assert opened['_PID']==closed['_PID']=='878240'
session=(int(closed['__REALTIME_TIMESTAMP'])-int(opened['__REALTIME_TIMESTAMP']))/1e6;assert session==115.54344
assert any('Main process exited, code=exited, status=1/FAILURE' in x.get('MESSAGE','') for x in native) and any("Failed with result 'exit-code'" in x.get('MESSAGE','') for x in native)
owned=[];events=[]
for x in map(json.loads,v['unit_journals'][REC].splitlines()):
 if (x.get('_SYSTEMD_INVOCATION_ID') or x.get('INVOCATION_ID'))!=RINV:continue
 assert x['_BOOT_ID']==BOOT.replace('-','');owned.append(x)
 try:event=json.loads(x.get('MESSAGE',''))
 except (ValueError,TypeError):continue
 events.append({'journal_utc_us':int(x['__REALTIME_TIMESTAMP']),'event':event})
disconnect=[x for x in events if x['event'].get('event')=='usdm_websocket_disconnected'];assert len(disconnect)==2 and [x['journal_utc_us'] for x in disconnect]==[1792937853044212,1792938008976699]
for x in disconnect:
 fields=x['event']['fields'];assert fields['market']=='um_perpetual' and fields['symbol']=='BTCUSDT' and fields['stream']=='book_ticker' and fields['error_type']=='ConnectionClosedError' and fields['cause_type'] is None and fields['os_errno'] is None and fields['close_received_code'] is None
# Connections after both losses must be observed; do not turn their sparse logs
# into exact READY downtime or a unique transport/CPU explanation.
for x in disconnect:
 following=[y for y in events if y['journal_utc_us']>x['journal_utc_us'] and y['event'].get('event')=='usdm_websocket_connected' and y['event'].get('fields',{}).get('stream')=='book_ticker' and y['event']['fields'].get('symbol')=='BTCUSDT'];assert following
 x['next_connected_journal_utc_us']=following[0]['journal_utc_us']
stop=next(x for x in events if x['event'].get('event')=='service_stop_requested');assert stop['event']['fields']['reason']=='SIGTERM' and stop['journal_utc_us']>f['utc_ns']//1000
rest=control['restoration.jsonl'];cancel=control['expiry-cancellation.jsonl'];assert rest['result']=='RESTORED' and rest['errors']==[] and rest['boot_id']==BOOT and rest['ended_at_utc_ns']==1792938079405053625==cancel['restoration_ended_at_utc_ns']<cancel['cancelled_at_utc_ns']==1792938079433376547
assert all(x['rc']==0 for x in rest['operations'])
for u in ['apt-daily.timer','apt-daily-upgrade.timer','unattended-upgrades.service']:assert rest['os_states'][u]['ActiveState']=='active' and rest['os_states'][u]['UnitFileState']=='enabled'
assert all(x['UnitFileState'] not in {'masked','masked-runtime'} for x in rest['os_states'].values())
assert rest['archive_timer']['ActiveState']=='active' and rest['archive_timer']['UnitFileState']=='enabled' and cancel['guard']['ActiveState']=='inactive'
states={name:dict(row.split('=',1) for row in body.splitlines()) for name,body in v['states'].items()};assert states[CAP]['ActiveState']=='failed' and states[CAP]['Result']=='exit-code' and states[CAP]['MainPID']=='0' and states[CAP]['InvocationID']==CINV
assert states[REC]['ActiveState']=='inactive' and states[REC]['UnitFileState']=='disabled' and states[REC]['MainPID']==states[REC]['NRestarts']=='0'
assert states[AT]['ActiveState']=='active' and states[AT]['UnitFileState']=='enabled' and states[AS]['ActiveState']=='inactive' and states[AS]['MainPID']=='0' and states[AS]['Result']=='success'
latest_inv=states[AS]['InvocationID'];latest=[x for x in map(json.loads,v['unit_journals'][AS].splitlines()) if (x.get('_SYSTEMD_INVOCATION_ID') or x.get('INVOCATION_ID'))==latest_inv];assert latest and any(x.get('JOB_RESULT')=='done' for x in latest)
(R/'OBSERVATION_SUMMARY.json').write_text(json.dumps(summary,indent=2,sort_keys=True)+'\n');(R/'owned-recorder-journal.json').write_text(json.dumps(owned,indent=2,sort_keys=True)+'\n');(R/'owned-native-journal.json').write_text(json.dumps(native,indent=2,sort_keys=True)+'\n');(R/'latest-owned-archive-journal.json').write_text(json.dumps(latest,indent=2,sort_keys=True)+'\n');(R/'DISCONNECT_OBSERVATIONS.json').write_text(json.dumps(disconnect,indent=2,sort_keys=True)+'\n')
resources={k:{'min':min(row['resources'][k] for row in rows),'max':max(row['resources'][k] for row in rows)} for k in ['rss_bytes','rss_peak_bytes','host_available_bytes','cgroup_swap_bytes','cgroup_swap_peak_bytes','writer_free_bytes','archive_free_bytes','host_swap_used_bytes']}
output={'decision':'ACCEPT','scope':'ACTUAL_PRE_T0_WARM_FAILURE_SOURCE_TIMING_AND_AUTOMATIC_RESTORATION_PROOF_ONLY','accepted_failed_read_sha256':H,'original_controls_count':22,'warm_publications_verified':38,'paired_context_entries_verified':paired,'warm_status_counts':dict(pubstatus),'warm_strict_counts':dict(gstatus),'initial_STARTING_capacity_pending_rows':27,'RUNNING_trusted_write_safe_rows':11,'first_strict_PASS_utc_ns':first['utc_ns'],'last_strict_PASS_utc_ns':last['utc_ns'],'strict_endpoint_BT_span_seconds':span_bt,'original_warm_loop_exit_criterion_met':True,'failure_utc_ns':f['utc_ns'],'failure_boottime_ns':f['boottime_ns'],'failure_error':f['error'],'canonical_READY_condition_passed_by_original_controlflow':True,'actual_canonical_readiness_result_bytes_not_persisted':True,'last_warm_to_failure_UTC_seconds':(f['utc_ns']-last['utc_ns'])/1e9,'last_warm_to_failure_BT_seconds':(f['boottime_ns']-last['boottime_ns'])/1e9,'owned_runuser_session_PID':878240,'owned_runuser_session_seconds':session,'warm_begin_request_absolute_time_not_exported':True,'exact_total_warm_elapsed_not_reconstructable':True,'source_catalog_integrity_check_possible_cost_not_unique_measured_attribution':True,'source_readiness_reads_service_snapshot_before_catalog_integrity':True,'source_wait_deadline_not_an_intra_evaluation_interrupt':True,'resource_bounds_observed':resources,'owned_recorder_invocation':RINV,'owned_recorder_journal_rows':len(owned),'owned_native_journal_rows':len(native),'two_recoverable_WS_disconnects_with_null_close_cause':True,'CPU_network_exact_READY_downtime_cause_unproven':True,'restoration_ended_utc_ns':rest['ended_at_utc_ns'],'guard_cancelled_utc_ns':cancel['cancelled_at_utc_ns'],'RESTORED_before_cancel_errors_empty':True,'guard194e_CLOSED_never_reuse_extend_or_reARM':True,'current_Recorder_inactive_AND_disabled_PID0_NRestarts0':True,'current_natural_archive_success_invocation':latest_inv,'healthy_capture_or_audit_at_read':False,'owner_start_T0_formalmonitor_or_full_saved_data_audit_exists':False,'formal_credit_seconds':0,'saved_frames_or_capacity_credit':False,'classifier621_fix_not_failure_trigger':True,'source_or_private_helper_execution_or_tests':False,'SSH_Raw_service_actions_by_review':False}
(R/'RECONSTRUCTION.json').write_text(json.dumps(output,indent=2,sort_keys=True)+'\n');print(json.dumps({k:v for k,v in output.items() if k!='resource_bounds_observed'},sort_keys=True))
