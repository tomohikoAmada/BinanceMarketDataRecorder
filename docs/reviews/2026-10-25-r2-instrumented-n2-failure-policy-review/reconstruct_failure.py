"""Independent failure/control/source reconstruction and proposed-rule model only.

Never imports or executes a candidate helper or production package, contacts SSH,
reads Raw/manifest payload, changes a production source or issues service actions.
"""
from pathlib import Path
import ast
import base64
from collections import Counter
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import tomllib

OUT=Path(__file__).resolve().parent
P=Path('/private/var/tmp/bmdr-r2-n2-instrumented-20261024-4r2aauoj')
OLD=Path('/private/var/tmp/bmdr-r2-instrumented-n2-startup-prefix-review-20261024-4JUarTPz')
C=Path('/Users/amada/.codex/worktrees/r2-close-diagnostics/BinanceMarketDataRecorder')
E=P/'actual-n2-stage-read-1792908556330206000.json'
ESHA='015902c0a9aa0a3b9e1ba129ddb4a0e2f34ce341f4d6a411f5d1388fd70f0768'
BOOT='59bb1735-ac48-408d-b3ec-bda79fc49b41'
INV='e038b1b4d7624081babaef33fd274a45'
CAPINV='9537822cd4b64757b1d26744b5114a52'
CSHA='5b73db1b6ba6ac8cc7ab84cbcf0c7b688187b316644bef3df44bbb42f9283419'
count=0
def ck(value,label):
 global count
 count+=1
 if not value:raise AssertionError(label)
def sha(b):return hashlib.sha256(b).hexdigest()
def dump(n,v):(OUT/n).write_text(json.dumps(v,indent=2,sort_keys=True)+'\n')
def utc(ns):return datetime.fromtimestamp(ns//10**9,timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')+'.'+str(ns%10**9).zfill(9)+'Z'

body=E.read_bytes();ck(len(body)==319802269 and sha(body)==ESHA,'complete current failure original')
d=json.loads(body);ck(d['boot_id']==BOOT and d['parse_errors']=={} and d['scope_exists'],'read sameboot parse scope')
raw={}
for n,x in d['files'].items():
 b=base64.b64decode(x['base64'],validate=True);ck(len(b)==x['bytes'] and sha(b)==x['sha256'],'original '+n);ck(b.decode(errors='replace')==x['content'],'content '+n);raw[n]=b
ck(len(raw)==45,'45 original members')
execution=json.loads(E.with_suffix('.execution.json').read_bytes())
ck(execution['returncode']==0 and execution['error_type'] is None and execution['remote_retry'] is False,'original one read no retry')
ck(execution['operator_sha256']=='4792d322feb8e7f2018cf6299e2054ea3ac1226ac5db219ad6307d4fd1c8cf00' and execution['remote_sha256']=='9c5cb8d217e98d8fa0f8eeb93d6bd24f78d63809eb9937df826d18d4cf9d4715','unchanged exact reader')
ck(E.with_suffix('.stderr').read_bytes()==b'','original empty stderr')
def get(n):return json.loads(raw[n])
start=get('owner-start.json');fail=get('capture-failure.json')
ck(raw['owner-start.json']==(OLD/'original-owner-start.json').read_bytes(),'original accepted owner start unchanged no rerun startup')
ck(sha(raw['owner-start.json'])=='17f11ac6d4e3723bf9bcb151d54cdd75ff780836d48be24553d8dd404f111213','original ownerstart sha')
ck(fail['result']=='FAILED' and fail['action']=='PAUSE_AFFECTED_CAPTURE' and fail['error']=='critical_product_terminal_failure' and fail['error_type']=='RuntimeError' and fail['requires_owner_notification'] is True,'real failure control')
elapsed=fail['boottime_ns']-start['t0_boottime_ns'];short=86400*10**9-elapsed
ck(elapsed==85410086844564 and short==989913155436,'own elapsed and shortfall actual failure BOOTTIME')
ck(abs((fail['utc_ns']-start['t0_utc_ns'])-elapsed)<2*10**9,'failure UTC BOOTTIME consistency')
ck(fail['utc_ns']==1792907322923359029 and fail['boottime_ns']==1972217103892898,'exact failure timing')
ck(not any(n in raw for n in ['owner-target.json','monitor-rate.json','native-result.json','terminal-start.json','terminal-audit/audit-root.json','terminal-audit/owner-selected-result.json']),'no target/final/full data audit')
diag=get('diagnostic-d572a3b2a1c04a1e8a92f592dce10d27.json');statebytes=raw[diag['publication_file']];pub=json.loads(statebytes);ob=diag['observation']
ck(sha(statebytes)=='5dcdd17d75bfbd9715ca44ead2ad9f19923db221909f8a68efa58904335957c3' and sha(statebytes)==diag['publication_sha256'] and len(statebytes)==diag['publication_bytes']==13508,'exact preserved failing publication')
ck(ob['observation_phase']=='write_safety' and diag['reason']=='observation_failed:RuntimeError:critical_product_terminal_failure','actual predicate phase')
ck(ob['publication_complete'] is True and ob['state_sha256']==sha(statebytes) and 'resources' not in ob and 'write_safety_verified' not in ob,'failure before resources/gate no fake completed observation')
ck(ob['runtime']['InvocationID']==INV and ob['runtime']['MainPID']=='825220' and ob['runtime']['NRestarts']=='0' and ob['runtime']['ActiveState']=='active','actual failure runtime incarnation')
ck(ob['boot_id']==BOOT and ob['config_sha256']==CSHA and abs((ob['utc_ns']-start['t0_utc_ns'])-(ob['boottime_ns']-start['t0_boottime_ns']))<2*10**9 and ob['utc_ns']<=fail['utc_ns'],'retained incomplete fatal observation clock/identity')
ck('qualification_owner_995.py", line 31, in check_write_safety' in fail['traceback'] and 'raise RuntimeError("critical_product_terminal_failure")' in fail['traceback'],'original exact predicate traceback')
ck(pub['status']=='RUNNING' and pub['failure'] is None and pub['shutdown_reason'] is None and pub['core_ready'] is False,'actual service not terminal')
ck(pub['pid']==825220 and pub['service_instance_id']=='4a9dbabd-8e46-4c6b-a3ee-8defc79151b0','failing raw publication sameincarnation')
ck(pub['deployment_identity']['identity_sha256']==start['identity_sha256'] and pub['deployment_identity']['config_sha256']==CSHA and pub['deployment_identity']['source_git_sha']==start['source_git_sha'],'failure identity source config')
ck(pub['capacity']['actual_hard_reserve_reached'] is False and pub['capacity']['free_bytes']>10*1024**3,'actual failing publication disk reserve safe')
ck(0<=ob['utc_ns']-pub['heartbeat_at_utc_ns']<15*10**9 and pub['heartbeat_at_utc_ns']==1792907320965383167,'fresh raw failing heartbeat')
products={f'{m}:{s}':v for m,ps in pub['products'].items() for s,v in ps.items()}
ck(len(products)==4 and all(v['failure']=='planned_rotation' and v['status']=='CONNECTING' and v['ready'] is False for v in products.values()),'all four exact recoverable planned rotation readiness tags')

# Static source binding/proof only: no source/candidate function is executed.
source_paths=['supervisor/readiness.py','service/runtime.py','collector/spot.py','collector/usdm.py','collector/resync.py','binance/spot/websocket.py','binance/usdm/websocket.py']
sources={n:(C/'src/binance_market_data_recorder'/n).read_bytes() for n in source_paths}
private=(P/'runtime/qualification_owner_995.py').read_bytes();tree=ast.parse(private)
fn=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='check_write_safety')
loop=next(n for n in fn.body if isinstance(n,ast.For))
ck("if product['failure'] is not None:" in ast.unparse(loop) and 'critical_product_terminal_failure' in ast.unparse(loop),'static unconditional overloaded productfailure predicate')
ck('else readiness.failure' in sources['service/runtime.py'].decode() and '"FAILED"' in sources['service/runtime.py'].decode() and 'if name in failures' in sources['service/runtime.py'].decode(),'production status distinguishes supervisor terminal from readiness field')
ck('self._failure = None' in sources['supervisor/readiness.py'].decode() and 'def restart_bootstrap' in sources['supervisor/readiness.py'].decode(),'readiness reset clears recoverable tag')
ck(all('"planned_rotation"' in sources[n].decode() and 'self.readiness.record_failure(event)' in sources[n].decode() and 'self.resync.request(event)' in sources[n].decode() and 'self.readiness.restart_bootstrap()' in sources[n].decode() for n in ['collector/spot.py','collector/usdm.py']),'both collectors resync planned rotation not terminal')
ck(all('self.readiness.record_failure("sequence_gap")' in sources[n].decode() for n in ['collector/spot.py','collector/usdm.py']),'fifth source-defined sequence_gap readiness tag')
ck('return "planned_rotation"' in sources['binance/spot/websocket.py'].decode() and 'return "planned_rotation"' in sources['binance/usdm/websocket.py'].decode(),'planned rotation produced by ordinary connection deadline')

# Reuse our frozen independent pure gate procedure, not any private candidate.
config_bytes=(OLD/'current-recorder.toml').read_bytes();ck(sha(config_bytes)==CSHA,'unchanged hash-bound config reference')
cfg=tomllib.loads(config_bytes.decode())['recorder'];flags=[k for k in cfg if k.endswith('_enabled') and (k.startswith('side_') or k=='spot_exchange_info_enabled')]
productkeys=[(m,s) for m in ['spot','um_perpetual'] for s in ['BTCUSDT','ETHUSDT']]
rest={'premium_index_snapshot','funding_history','open_interest','open_interest_statistics_5m','taker_buy_sell_volume_5m','global_long_short_ratio_5m','top_long_short_account_ratio_5m','top_long_short_position_ratio_5m','basis_5m'};five={x for x in rest if x.endswith('_5m')}
old_tree=ast.parse((OLD/'reconstruct.py').read_bytes());gate=next(n for n in old_tree.body if isinstance(n,ast.FunctionDef) and n.name=='independently_gate')
env={'cfg':cfg,'flags':flags,'products':productkeys,'rest':rest,'five':five}
exec(compile(ast.Module(body=[gate],type_ignores=[]),'<own-independent-pure-gate-procedure>','exec'),env)
gate=env['independently_gate']
rows=[json.loads(x) for x in raw['monitoring.jsonl'].splitlines()];probes=[json.loads(x) for x in raw['recovery-observations.jsonl'].splitlines()]
ck(len(rows)==2847 and len(probes)==8 and [r['monitor_slot'] for r in rows]==list(range(2847)),'full partial nominal prefix/probe count')
obs_summaries=[];resources=[];rawctx=pairedctx=0
for role,rr in [('nominal',rows),('probe',probes)]:
 for i,r in enumerate(rr):
  b=base64.b64decode(r['state_publication_base64'],validate=True);s=json.loads(b)
  ck(len(b)==r['state_publication_bytes'] and len(b)<=1024**2 and sha(b)==r['state_sha256'],'raw partial publication '+role+str(i))
  ck(r['publication_complete'] and r['write_safety_verified'] and s['failure'] is None and s['shutdown_reason'] is None and all(p['failure'] is None for ps in s['products'].values() for p in ps.values()),'complete prior safe row '+role+str(i))
  ck(r['runtime']['InvocationID']==INV and r['runtime']['MainPID']=='825220' and r['runtime']['NRestarts']=='0' and r['boot_id']==BOOT and r['config_sha256']==CSHA,'prior row identity '+role+str(i))
  ck(s['pid']==825220 and s['service_instance_id']=='4a9dbabd-8e46-4c6b-a3ee-8defc79151b0' and s['deployment_identity']['identity_sha256']==start['identity_sha256'] and s['deployment_identity']['config_sha256']==CSHA and s['deployment_identity']['source_git_sha']==start['source_git_sha'] and r['core_ready']==s['core_ready'] and r['heartbeat_at_utc_ns']==s['heartbeat_at_utc_ns'],'raw current identity/core/heartbeat '+role+str(i))
  ck(s['status']=='RUNNING' and s['capacity']['actual_hard_reserve_reached'] is False and r['runtime']['ActiveState']=='active' and r['observation_phase']=='complete','actual previous running write safety '+role+str(i))
  ck(abs((r['utc_ns']-start['t0_utc_ns'])-(r['boottime_ns']-start['t0_boottime_ns']))<2*10**9,'prior row trusted clock '+role+str(i))
  ck(r['auxiliary_gate']==gate(s,r['utc_ns'],True) and r['strict_auxiliary_gate']==gate(s,r['utc_ns'],False),'full reconstructed paired gates '+role+str(i))
  if role=='nominal':
   due=start['t0_boottime_ns']+r['monitor_slot']*30*10**9
   ck(r['observation_role']=='nominal_monitor' and r['scheduled_monitor_boottime_ns']==due and due<=r['boottime_ns']<due+30*10**9 and r['skipped_monitor_slots']==0,'no prefix missing/late/skipped '+str(i))
  else:ck(r['observation_role']=='recovery_probe','probe outside denominator')
  rs=r['resources'];ck(rs['cgroup_swap_bytes']==rs['cgroup_swap_peak_bytes']==0 and min(rs['writer_free_bytes'],rs['archive_free_bytes'])>10*1024**3,'observed prior resource bounds '+role+str(i));resources.append(rs)
  rawctx+=26;pairedctx+=52
  obs_summaries.append({'role':role,'ordinal':i,'utc_ns':r['utc_ns'],'boottime_ns':r['boottime_ns'],'publication_sha256':sha(b),'official':r['auxiliary_gate']['result'],'strict':r['strict_auxiliary_gate']['result'],'reasons':r['auxiliary_gate']['reasons']})
ck(sum(r['auxiliary_gate']['result']=='PASS' for r in rows)==2838 and sum(r['strict_auxiliary_gate']['result']=='PASS' for r in rows)==2804,'partial honest totals')
ck(rows[-1]['monitor_slot']==2846 and fail['boottime_ns']>=start['t0_boottime_ns']+2847*30*10**9,'fatal observation before slot2847 could be written')

restores=[json.loads(l) for l in raw['restoration.jsonl'].splitlines()];cancels=[json.loads(l) for l in raw['expiry-cancellation.jsonl'].splitlines()];ck(len(restores)==len(cancels)==1,'single actual automatic restoration/cancel')
rst=restores[0];cancel=cancels[0]
ck(rst['result']=='RESTORED' and rst['errors']==[] and rst['reason']=='capture-failure' and rst['boot_id']==BOOT,'actual restoration success not request')
ck(rst['ended_at_utc_ns']==1792907346512229095 and rst['ended_at_utc_ns']<cancel['cancelled_at_utc_ns']==1792907346667877194 and cancel['restoration_ended_at_utc_ns']==rst['ended_at_utc_ns'],'actual restoration before guard cancellation')
ck(all(x['rc']==0 for x in rst['operations']) and len(rst['operations'])==4,'all actual restore commands normal')
ck(rst['recorder']['ActiveState']=='inactive' and rst['recorder']['UnitFileState']=='disabled' and rst['recorder']['MainPID']=='0' and rst['recorder']['NRestarts']=='0','actual restored Recorder stopped disabled')
active={'apt-daily.timer','apt-daily-upgrade.timer','unattended-upgrades.service'}
units=['apt-daily.timer','apt-daily-upgrade.timer','apt-daily.service','apt-daily-upgrade.service','unattended-upgrades.service']
expected_commands=[['systemctl','disable','--now','binance-market-data-recorder.service'],['systemctl','enable','--now','binance-market-data-archive.timer'],['systemctl','unmask','--runtime',*units],['systemctl','start','apt-daily.timer','apt-daily-upgrade.timer','unattended-upgrades.service']]
ck([x['argv'] for x in rst['operations']]==expected_commands and set(rst['os_states'])==set(units),'only approved original OS/archive/Recorder controls')
ck(len(rst['os_states'])==5 and all(x['UnitFileState'] not in ['masked','masked-runtime'] for x in rst['os_states'].values()),'all5 approved OS unmasked')
ck(all(rst['os_states'][u]['ActiveState']=='active' and rst['os_states'][u]['UnitFileState']=='enabled' for u in active),'original3 OS owners active enabled')
ck(rst['archive_timer']['ActiveState']=='active' and rst['archive_timer']['UnitFileState']=='enabled','actual archive owner restored')
ck(rst['expiry_guard_before_cancellation']['ActiveState']=='active' and rst['expiry_guard_before_cancellation']['NextElapseUSecRealtime']=='Mon 2026-10-26 21:41:05 UTC','fixed guard protected until restore')
ck(cancel['guard']['ActiveState']=='inactive' and cancel['guard']['LoadState']=='not-found','guard CLOSED never reuse')
states={u:dict(x.split('=',1) for x in val.splitlines() if '=' in x) for u,val in d['states'].items()}
rec=states['binance-market-data-recorder.service'];cap=states['binance-recorder-r2-capacity-n2-instrumented-20261024-4r2aauoj-capture.service'];ar=states['binance-market-data-archive.service'];at=states['binance-market-data-archive.timer']
ck(rec['ActiveState']=='inactive' and rec['UnitFileState']=='disabled' and rec['MainPID']=='0' and rec['NRestarts']=='0','read current stopped disabled')
ck(cap['ActiveState']=='failed' and cap['MainPID']=='0' and cap['Result']=='exit-code' and cap['InvocationID']==CAPINV,'read real native failure no healthy capture')
guard=states['binance-recorder-r2-capacity-n2-instrumented-20261024-4r2aauoj-quiet-expiry.timer']
ck(guard['ActiveState']=='inactive' and guard['LoadState']=='not-found','current guard remains closed')
ck(ar['ActiveState']=='inactive' and ar['MainPID']=='0' and ar['Result']=='success' and ar['InvocationID']=='258f4d3bc3f146f1b091947db4b233e5' and at['ActiveState']=='active' and at['UnitFileState']=='enabled','current natural archive and timer actual success')

events=[];jrows=[json.loads(l) for l in d['recorder_owned_journal'].splitlines()]
for j in jrows:
 ck(j.get('_SYSTEMD_INVOCATION_ID')==INV,'currentfailure owned journal invocation')
 try:e=json.loads(j.get('MESSAGE',''))
 except ValueError:continue
 if isinstance(e,dict):events.append(dict(e,journal_utc_ns=int(j['__REALTIME_TIMESTAMP'])*1000))
resync=[e for e in events if e.get('event') in ['spot_depth_resync_restart','usdm_depth_resync_restart']]
ck(len(resync)==4 and {e['fields']['market']+':'+e['fields']['symbol'] for e in resync}==set(products) and all(e['fields']['reason']=='planned_rotation' and e['level']=='CRITICAL' for e in resync),'all4 actual expected resync records')
stop=[e for e in events if e.get('event')=='service_stop_requested'];ck(len(stop)==1 and stop[0]['fields']['reason']=='SIGTERM' and stop[0]['timestamp_utc']=='2026-10-25T05:48:49.273905+00:00' and stop[0]['journal_utc_ns']>fail['utc_ns'],'requested stop follows private critical assertion')
ck(not any(e.get('event') in ['core_market_terminal_failure','service_terminal_failure','global_side_data_terminal_failure'] for e in events),'no published native terminal product/service/global failure event')
connections_after=[e for e in events if e.get('event') in ['spot_websocket_connected','usdm_websocket_connected'] and fail['utc_ns']<e['journal_utc_ns']<stop[0]['journal_utc_ns']]
ck(len(connections_after)>=8,'actual reconnection progress after assertion before requested stop')
closes=[e for e in events if e.get('event','').endswith('_websocket_disconnected')]
close_modes=Counter((e['fields'].get('cause_type'),e['fields'].get('os_errno'),e['fields'].get('close_received_code'),e['fields'].get('close_received_reason')) for e in closes)
ck(len(jrows)==630 and len(closes)==123 and close_modes=={(None,None,None,None):79,(None,None,1008,'Pong timeout'):37,('ConnectionResetError',104,None,None):7},'all retained full-run bounded close modes')
rest_errors=Counter(e['fields']['error_type'] for e in events if e.get('event')=='usdm_side_rest_failed')
ck(rest_errors=={'EmptySideDataResponse':176,'NetworkError':7},'all retained full-run REST warning classes')
archive_journal=[json.loads(l) for l in d['unit_journals']['binance-market-data-archive.service'].splitlines()]
archive_success=[]
for j in archive_journal:
 if j.get('_SYSTEMD_INVOCATION_ID')!=ar['InvocationID']:continue
 try:e=json.loads(j.get('MESSAGE',''))
 except ValueError:continue
 if isinstance(e,dict) and e.get('command')=='archive.drain':archive_success.append(e)
ck(len(archive_success)==1 and archive_success[0]['backlog_files_after']==0 and archive_success[0]['failed_transactions']==0 and archive_success[0]['exit_reason']=='BACKLOG_EMPTY','current natural worker original drain result')

RECOVERABLE={'planned_rotation','unexpected_disconnect','server_shutdown','ingress_backpressure','sequence_gap'}
def old_model(s):
 if s['failure'] is not None:return 'PAUSE_SERVICE'
 if s['shutdown_reason'] is not None:return 'PAUSE_SHUTDOWN'
 if any(p['failure'] is not None for ps in s['products'].values() for p in ps.values()):return 'PAUSE_PRODUCT'
 c=s['capacity']
 if s['status']=='STARTING' and c is None:return 'PENDING_STARTING'
 if not isinstance(c,dict) or type(c.get('actual_hard_reserve_reached')) is not bool:return 'PAUSE_UNTRUSTED_CAPACITY'
 return 'PAUSE_ACTUAL_RESERVE' if c['actual_hard_reserve_reached'] else 'NO_TERMINAL_EVIDENCE'
def proposed_model(s):
 if s['failure'] is not None:return 'PAUSE_SERVICE'
 if s['shutdown_reason'] is not None:return 'PAUSE_SHUTDOWN'
 for ps in s['products'].values():
  for p in ps.values():
   if p.get('status')=='FAILED':return 'PAUSE_PRODUCT'
   if p.get('status') not in ['READY','CONNECTING'] or type(p.get('ready')) is not bool:return 'PAUSE_UNTRUSTED_PRODUCT'
   if p['failure'] is not None:
    if not (s['status']=='RUNNING' and type(p['failure']) is str and p['failure'] in RECOVERABLE and p['status']=='CONNECTING' and p['ready'] is False):return 'PAUSE_PRODUCT'
   elif (p['status']=='READY')!=p['ready']:return 'PAUSE_UNTRUSTED_PRODUCT'
 c=s['capacity']
 if s['status']=='STARTING' and c is None:return 'PENDING_STARTING'
 if not isinstance(c,dict) or type(c.get('actual_hard_reserve_reached')) is not bool:return 'PAUSE_UNTRUSTED_CAPACITY'
 return 'PAUSE_ACTUAL_RESERVE' if c['actual_hard_reserve_reached'] else 'NO_TERMINAL_EVIDENCE'
fixtures=[]
def case(name,s,expected):
 actual=proposed_model(s);ck(actual==expected,'design-model-only '+name);fixtures.append({'case':name,'expected':expected,'actual':actual,'PASS':actual==expected,'implementation_or_Linux_credit':False})
ck(old_model(pub)=='PAUSE_PRODUCT' and proposed_model(pub)=='NO_TERMINAL_EVIDENCE','original actual cause reproduced by independent old/new design models')
case('actual_all4_planned_rotation_connecting',pub,'NO_TERMINAL_EVIDENCE')
for tag in sorted(RECOVERABLE):
 s=deepcopy(pub)
 for ps in s['products'].values():
  for p in ps.values():p['failure']=tag
 case('source_defined_'+tag,s,'NO_TERMINAL_EVIDENCE')
 for ps in s['products'].values():
  for p in ps.values():p['status']='FAILED'
 case('FAILED_even_known_'+tag,s,'PAUSE_PRODUCT')
for name,mut,expected in [
 ('FAILED_even_failure_none',lambda s:s['products']['spot']['BTCUSDT'].update(status='FAILED',failure=None),'PAUSE_PRODUCT'),
 ('unknown_failure',lambda s:s['products']['spot']['BTCUSDT'].__setitem__('failure','UnclassifiedWriterError'),'PAUSE_PRODUCT'),
 ('READY_with_recovery_failure',lambda s:s['products']['spot']['BTCUSDT'].update(status='READY',ready=True),'PAUSE_PRODUCT'),
 ('connecting_ready_true',lambda s:s['products']['spot']['BTCUSDT'].__setitem__('ready',True),'PAUSE_PRODUCT'),
 ('unknown_status',lambda s:s['products']['spot']['BTCUSDT'].__setitem__('status','UNKNOWN'),'PAUSE_UNTRUSTED_PRODUCT'),
 ('missing_status',lambda s:s['products']['spot']['BTCUSDT'].pop('status'),'PAUSE_UNTRUSTED_PRODUCT'),
 ('malformed_ready',lambda s:s['products']['spot']['BTCUSDT'].__setitem__('ready','false'),'PAUSE_UNTRUSTED_PRODUCT'),
 ('malformed_failure',lambda s:s['products']['spot']['BTCUSDT'].__setitem__('failure',{'reason':'planned_rotation'}),'PAUSE_PRODUCT'),
 ('service_terminal',lambda s:s.__setitem__('failure',{'task':'writer','error_type':'OSError'}),'PAUSE_SERVICE'),
 ('service_shutdown',lambda s:s.__setitem__('shutdown_reason','HARD_RESERVE_SAFETY_STOP'),'PAUSE_SHUTDOWN'),
 ('actual_hard_reserve',lambda s:s['capacity'].__setitem__('actual_hard_reserve_reached',True),'PAUSE_ACTUAL_RESERVE'),
 ('missing_capacity_flag',lambda s:s['capacity'].pop('actual_hard_reserve_reached'),'PAUSE_UNTRUSTED_CAPACITY'),
 ('malformed_capacity_flag',lambda s:s['capacity'].__setitem__('actual_hard_reserve_reached','false'),'PAUSE_UNTRUSTED_CAPACITY'),
 ('RUNNING_capacity_none',lambda s:s.__setitem__('capacity',None),'PAUSE_UNTRUSTED_CAPACITY'),
 ('STARTING_recovery_tag_not_accepted',lambda s:s.__setitem__('status','STARTING'),'PAUSE_PRODUCT'),
]:
 s=deepcopy(pub);mut(s);case(name,s,expected)
s=deepcopy(pub);s['status']='STARTING';s['capacity']=None
for ps in s['products'].values():
 for p in ps.values():p['failure']=None
case('bounded_genuine_STARTING_capacity_pending',s,'PENDING_STARTING')
ck(gate(pub,ob['utc_ns'],True)['result']=='BLOCK','counterfactual tolerance would still retain official quality BLOCK')

result={'decision':'ACCEPT','scope':'FAILURE_DIAGNOSIS_AND_ORIGINAL_AUTOMATIC_RESTORATION_BINDING_ONLY','independent_assertions':count,'read':{'sha256':ESHA,'bytes':len(body),'utc_ns':d['utc_ns'],'original_members':45},'failure':{'sha256':sha(raw['capture-failure.json']),'utc':utc(fail['utc_ns']),'boottime_ns':fail['boottime_ns'],'own_elapsed_seconds':elapsed/1e9,'short_of_86400_seconds':short/1e9,'error':fail['error'],'phase':ob['observation_phase'],'publication_sha256':sha(statebytes),'actual_service_failure':pub['failure'],'actual_shutdown_reason':pub['shutdown_reason'],'product_failure_tags':{n:p['failure'] for n,p in products.items()},'source_semantic_misclassification_confirmed':True,'true_write_failure_established':False,'failing_row_has_fresh_resources':False},'partial_monitor':{'nominal':len(rows),'official_pass':2838,'official_block':9,'strict_diagnostic_pass':2804,'strict_diagnostic_block':43,'slots':[0,2846],'skipped_late_missing_prefix':0,'probes_excluded':8,'partial_percent':2838*100/2847,'fixed_final_denominator':2880,'final_monitor_artifact_present':False,'own24h_accepted':False,'fatal_observation_not_completed_nominal':True,'raw_publications_rehashed':len(rows)+len(probes),'raw_context_entries':rawctx,'paired_context_entries':pairedctx},'observed_previous_resources':{'rss_max':max(x['rss_bytes'] for x in resources),'rss_peak_max':max(x['rss_peak_bytes'] for x in resources),'host_available_min':min(x['host_available_bytes'] for x in resources),'current_owned_swap_max':max(x['cgroup_swap_bytes'] for x in resources),'lifetime_peak_owned_swap_max':max(x['cgroup_swap_peak_bytes'] for x in resources),'writer_free_min':min(x['writer_free_bytes'] for x in resources),'archive_free_min':min(x['archive_free_bytes'] for x in resources),'failure_publication_free_bytes':pub['capacity']['free_bytes']},'restoration':{'sha256':sha(raw['restoration.jsonl']),'ended_utc':utc(rst['ended_at_utc_ns']),'ended_at_utc_ns':rst['ended_at_utc_ns'],'errors':[],'cancel_sha256':sha(raw['expiry-cancellation.jsonl']),'cancelled_at_utc_ns':cancel['cancelled_at_utc_ns'],'before_cancel_margin_ns':cancel['cancelled_at_utc_ns']-rst['ended_at_utc_ns'],'original_OS_owners_restored':True,'guard_closed_never_reusable':True,'current_recorder_stopped_disabled_PID0_NRestarts0':True,'current_natural_archive_worker':ar['InvocationID'],'current_natural_archive_worker_success_backlog0':True,'fresh_canonical_VERIFY_handoff_or_future_clock_qualification':False},'corrective_design':{'kind':'PROPOSED_BOUNDED_SEMANTIC_RULE_ONLY_NOT_IMPLEMENTATION_ACCEPTANCE','recoverable_readiness_tags':sorted(RECOVERABLE),'requires_service_status':'RUNNING','requires_product_status':'CONNECTING','requires_product_ready':False,'reject_FAILED_first_even_known_tag':True,'unknown_or_malformed_still_pause':True,'official_monitor_BLOCK_and_fixed995_denominator_unchanged':True,'model_fault_cases':len(fixtures),'private_helper_candidate_implemented_or_qualified':False},'blocking_findings_for_review':[],'credits':{'own24h':False,'saved_frame_correctness':False,'N2_capacity':False,'stability_fix':False,'N4':False,'new_T0':False,'old_duration_transfer':False},'limitations':['The immediate stop cause is established as a private readiness-tag misclassification; this does not explain earlier transport closes or prove no other potential writing problem.','The fatal sample stopped before resources/gate/write_sample; no fresh failing-row resource snapshot or nominal sample was invented.','All Raw/archive/partial/failed original data remain outside this review; no complete saved-frame denominator/full LIVE audit exists.','Fresh handoff/VERIFY/future CLOCK_MONOTONIC, partial-byte custody before any startup and future helper implementation/LINUX/physical qualification are still pending.','The old guard is CLOSED; this halted run cannot resume16min or inherit its23h43m into a fresh own86400.','Design-model fixtures do not execute either private predicate and provide no candidate implementation, Linux or duration credit.','No SSH/network/Raw/manifestpayload/production mutation or sourceQA/build/CI action occurred.']}
dump('RESULT.json',result);dump('OBSERVATIONS.json',obs_summaries);dump('MODEL_ONLY_FAULT_CASES.json',fixtures)
dump('JOURNAL_SUMMARY.json',{'owned_rows':len(jrows),'event_counts':dict(Counter(e.get('event') for e in events)),'unexpected_closes':len(closes),'close_received_1008_Pong_timeout':37,'immediate_reset104':7,'no_recorded_closeframe_or_immediatecause':79,'REST_errors':dict(rest_errors),'planned_depth_resync_products':[e['fields'] for e in resync],'connections_after_assertion_before_SIGTERM':len(connections_after),'service_stop_request':stop[0],'no_unique_transport_rootcause_or_fix_claim':True})
dump('FAILURE_TIMELINE.json',[e for e in events if fail['utc_ns']-30*10**9<=e['journal_utc_ns']<=stop[0]['journal_utc_ns']+5*10**9])
dump('INPUTS.json',{'original_envelope':{'path':str(E),'bytes':len(body),'sha256':ESHA},'original_members':{n:{'sha256':sha(b),'bytes':len(b)} for n,b in raw.items()},'earlier_startup_result_reference':{'path':str(OLD/'RESULT.json'),'sha256':sha((OLD/'RESULT.json').read_bytes()),'no_repeated_startup_requalification':True},'production_source_reference':{'root':str(C),'source_git_sha':start['source_git_sha'],'members':{n:{'sha256':sha(b),'bytes':len(b)} for n,b in sources.items()}},'private_failed_predicate':{'path':str(P/'runtime/qualification_owner_995.py'),'sha256':sha(private)},'pure_independent_gate_reused':{'path':str(OLD/'reconstruct.py'),'sha256':sha((OLD/'reconstruct.py').read_bytes()),'only_own_gate_AST_extracted':True}})
for n in ['capture-failure.json','restoration.jsonl','expiry-cancellation.jsonl','diagnostic-d572a3b2a1c04a1e8a92f592dce10d27.json','diagnostic-d572a3b2a1c04a1e8a92f592dce10d27.state']:(OUT/('original-'+n)).write_bytes(raw[n])
(OUT/'original-execution.json').write_bytes(E.with_suffix('.execution.json').read_bytes())
(OUT/'private-failed-predicate.py').write_bytes(private)
for n,b in sources.items():f=OUT/'source'/n;f.parent.mkdir(parents=True,exist_ok=True);f.write_bytes(b)
print(json.dumps({'decision':result['decision'],'assertions':count,'failure':result['failure'],'partial_monitor':result['partial_monitor'],'model_cases':len(fixtures),'out':str(OUT)},sort_keys=True))
