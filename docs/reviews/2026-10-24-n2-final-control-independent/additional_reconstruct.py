from pathlib import Path
import json,hashlib,collections,tarfile,base64,sqlite3,datetime
O=Path(__file__).parent;P=Path('/private/var/tmp/bmdr-r2-report-isolation-20261020-yf61hyp0/final-capacity-n2-complete-controls-20261024/controls');C=lambda d:(json.dumps(d,sort_keys=True,separators=(',',':'),ensure_ascii=False)+'\n').encode();H=lambda b:hashlib.sha256(b).hexdigest();N=0

def ck(v,m):
 global N;N+=1
 if not v:raise AssertionError(m)
def doc(n):return json.loads((P/n).read_bytes())
def emit(n,d):(O/n).write_text(json.dumps(d,sort_keys=True,indent=2)+'\n')
manifest=doc('EXPORT_MANIFEST.json');want={x['path']:x for x in manifest['members']};want['EXPORT_MANIFEST.json']={'path':'EXPORT_MANIFEST.json','size':(P/'EXPORT_MANIFEST.json').stat().st_size,'sha256':H((P/'EXPORT_MANIFEST.json').read_bytes())};seen=set()
with tarfile.open(P.parent/'controls.tar.gz','r|gz') as tf:
 for m in tf:
  ck(m.name not in seen and m.name in want and m.isfile() and not Path(m.name).is_absolute() and '..' not in Path(m.name).parts,'safe exacttar member');seen.add(m.name);h=hashlib.sha256();f=tf.extractfile(m)
  while b:=f.read(1048576):h.update(b)
  ck(m.size==want[m.name]['size'] and h.hexdigest()==want[m.name]['sha256'],'fulltar contentreadback')
ck(seen==set(want),'exactalltar inventory')
owner=doc('audit/owner-selected-result.json');native=doc('capture/native-result.json');target=doc('capture/owner-target.json');root=doc('audit/audit-root.json');cor=doc('audit/quiescence-corpus.json');receipt=doc('audit/owner-scope-receipt.json');pre=doc('audit/baseline-preflight.json');start=doc('capture/owner-start.json');freeze=doc('capture/terminal-freeze.json')
ck(owner['live_passes']==2 and owner['completed_verification_mode']=='IMMUTABLE_CONTROL_RECONSTRUCTION_AFTER_DUAL_LIVE' and owner['completion_state']=='COMPLETE' and owner['audit_sha256']==owner['completed_control_verification_sha256']==H((P/'audit/audit-root.json').read_bytes()),'complete dualLIVE completedverify binding');ck(owner==doc('capture/terminal-audit-result.json')==native['selected_scope_result'],'all3 result agreement');ck(owner['scope_receipt_sha256']==H((P/'audit/owner-scope-receipt.json').read_bytes()) and owner['owner_target_sha256']==H((P/'capture/owner-target.json').read_bytes()),'receipt target binding');ck(native['monitor_sha256']==H((P/'capture/monitor-rate.json').read_bytes()) and native['data_scope_start_sha256']==H((P/'capture/data-scope-start.json').read_bytes()),'native monitorboundary binding');ck(pre['run_id']==owner['run_id']==target['run_id']==start['run_id'] and pre['configured_products']==root['configured_products']==start['configured_products'],'exactrun all4products');ck(root['quiescence_corpus_sha256']==H((P/'audit/quiescence-corpus.json').read_bytes()),'corpusSHA');ck(root['formal_duration_credit_ns']==0 and root['stage'] is None and root['predecessor'] is None and not owner['canonical_v5_final_eligible'],'no canonicalstage duration predecessor');ck(freeze['result']=='STOPPED_DISABLED_DRAINED_FROZEN' and freeze['census']['backlog']==freeze['census']['transaction_backlog']==0 and freeze['census']['high_water']==receipt['source_high_water'],'terminal freeze no backlog + highwater');ck(freeze['recorder']['InvocationID']==start['recorder_invocation'] and freeze['recorder']['ActiveState']=='inactive' and freeze['recorder']['MainPID']=='0' and freeze['recorder']['NRestarts']=='0' and freeze['recorder']['UnitFileState']=='disabled','same native normal stopfreeze')
progress=[]
for p in sorted((P/'audit').glob('audit-progress-*.json')):
 d=json.loads(p.read_bytes());ck(d['anchor_sha256']==root['anchor_sha256'] and d['quiescence_corpus_sha256']==root['quiescence_corpus_sha256'],'all progress anchor');progress.append(d)
ck(progress[-1]['phase']=='audit-complete' and progress[-1]['published_prefix']==root['shards'],'actualcomplete progressprefix')
rows=lambda n:[json.loads(b) for b in (P/'capture'/n).read_bytes().splitlines()];mon=rows('monitoring.jsonl');probes=rows('recovery-observations.jsonl');corebad=[r for r in mon if not r['core_ready']];episodes=[]
for r in corebad:
 later=sorted([x for x in mon+probes if x['boottime_ns']>r['boottime_ns'] and x['core_ready']],key=lambda x:x['boottime_ns']);n=later[0];state=json.loads(base64.b64decode(r['state_publication_base64']));missing=[{'market':m,'symbol':s,'missing':sorted({'agg_trade','book_ticker','diff_depth'}-set(v['connected_streams'])),'status':v['status']} for m,z in state['products'].items() for s,v in z.items() if not v['ready']];ck(all(x['missing']==['book_ticker'] for x in missing),'all17 badtransport bookticker');episodes.append({'slot':r['monitor_slot'],'missing':missing,'observed_bad_utc_ns':r['utc_ns'],'next_core_good_utc_ns':n['utc_ns'],'observed_recovery_ns':n['boottime_ns']-r['boottime_ns'],'next_role':n['observation_role']})
ck(len(episodes)==17 and sum(x['observed_recovery_ns'] for x in episodes)==round(target['core_recovery']['observed_recovery_seconds']*1e9),'all17 episode sampledaccounting')
aux=[]
for r in mon:
 if r['core_ready'] and r['auxiliary_gate']['result']=='BLOCK':
  following=sorted([x for x in mon+probes if x['boottime_ns']>r['boottime_ns'] and x['auxiliary_gate']['result']=='PASS'],key=lambda x:x['boottime_ns'])[0];aux.append({'slot':r['monitor_slot'],'reasons':r['auxiliary_gate']['reasons'],'next_good_role':following['observation_role'],'next_good_seconds':(following['boottime_ns']-r['boottime_ns'])/1e9})
ck([x['slot'] for x in aux]==[1612,2720],'only2 auxOI failures')
j=doc('capture/final-owned-journal-20261024.json');events=[]
for b in j['journals']['recorder'].splitlines():
 d=json.loads(b)
 try:v=json.loads(d.get('MESSAGE',''))
 except:continue
 if isinstance(v,dict):events.append(v)
levels=collections.Counter(x.get('level') for x in events);critical=[x for x in events if x.get('level') in ['ERROR','CRITICAL']];ck(len(critical)==4 and all(x['event'].endswith('depth_resync_restart') and x['fields']['reason']=='planned_rotation' for x in critical),'all4 criticalplanned depthresync, no otherERROR')
db=sqlite3.connect('file:'+str(P/'audit/catalog.sqlite')+'?mode=ro&immutable=1',uri=True);db.row_factory=sqlite3.Row;ops=[dict(r) for r in db.execute('select e.*,s.event_seq from operational_events e join operational_event_sequence s using(event_id) where event_seq>4777')];oc=collections.Counter((r['event_type'],json.loads(r['evidence_json']).get('reason')) for r in ops)
rot=[x for x in events if x.get('event') in ['spot_planned_rotation','usdm_planned_rotation']];ck(len(rot)==10 and {(x['fields']['market'],x['fields']['symbol']) for x in rot}=={tuple(x) for x in start['configured_products']},'actual plannedrotationall4products')
for e in rot:
 f=e['fields'];matched=[r for r in ops if r['event_type']=='STREAM_DISCONTINUITY_STARTED' and r['market']==f['market'] and r['symbol']==f['symbol'] and r['stream']==f['stream'] and json.loads(r['evidence_json']).get('original_connection_id')==f['connection_id'] and json.loads(r['evidence_json']).get('reason')=='planned_rotation'];ck(len(matched)==1,'eachrotation exactcurrent START');rr=matched[0];ck(any(r['event_type']=='STREAM_DISCONTINUITY_COMPLETED' and r['gap_id']==rr['gap_id'] for r in ops),'eachrotation exactcompletion')
for m,s in start['configured_products']:ck(any(x.get('event') in ['spot_websocket_connected','usdm_websocket_connected'] and x.get('fields',{}).get('market')==m and x.get('fields',{}).get('symbol')==s for x in events),'configuredproductactualconnections')
recovery=[x for x in events if x.get('event') in ['spot_ingress_stream_recovery','usdm_ingress_stream_recovery']];rc=collections.Counter(x['fields'].get('outcome') for x in recovery);ck(rc=={'STREAM_RECONNECT':360,'DEPTH_RESYNC_REQUIRED':4},'current recoverycount noqueuefullnamedoutcome');wm=[x['fields'] for x in recovery if x.get('event')=='usdm_ingress_stream_recovery'];writerkeys=['queue_wait_count','receipt_queue_high_watermark','writer_append_max_ns','writer_drain_max_ns','writer_fsync_max_ns','writer_seal_max_ns','writer_write_max_ns'];maxima={k:max((x.get(k,0) for x in wm),default=0) for k in writerkeys};ck(maxima['queue_wait_count']==0 and maxima['receipt_queue_high_watermark']<8192,'currentUSD-Mqueue metrics notsaturated');ck(oc[('STREAM_DISCONTINUITY_COMPLETED','ingress_backpressure')]==2 and oc[('STREAM_DISCONTINUITY_STARTED','ingress_backpressure')]==0,'two priorSTART contexts notnew N2 backpressure')
h=doc('capture/final-restored-handoff-20261024.json');application=[]
for b in h['archive_worker_journal'].splitlines():
 d=json.loads(b)
 try:v=json.loads(d.get('MESSAGE',''))
 except:continue
 if isinstance(v,dict) and v.get('command')=='archive.drain':application.append(v)
ck(len(application)==1,'exact naturalworker appresult');a=application[0];ck(a['lock_acquired'] and a['exit_reason']=='BACKLOG_EMPTY' and a['target_state']=='READY' and a['failed_transactions']==a['backlog_files_after']==a['backlog_bytes_after']==0 and a['started_at_utc_ns']>h['restoration']['ended_at_utc_ns'] and a['ended_at_utc_ns']<h['future_deadline_check']['checked_at_utc_ns'],'natural archiveworker appsuccess afterrestore beforedeadline')
out={'assertions':N,'tar_verified_members_including_manifest':len(seen),'declared_payload_members':len(manifest['members']),'dual_live_completed_control_binding':True,'progress_files':len(progress),'sampled_core_recovery':episodes,'auxiliary_only_bad_slots':aux,'journal':{'levels':dict(levels),'events':dict(collections.Counter(x.get('event') for x in events)),'critical_planned_depth_resync':critical,'operational_counts':[{'event':k[0],'reason':k[1],'count':v} for k,v in sorted(oc.items(),key=lambda x:str(x[0]))],'current_recovery_outcomes':dict(rc),'usdm_writer_metric_maxima':maxima,'cause_limitation':'Exception classes and cumulative latency metrics do not establish unique underlying network/CPU/I/O origin; source close-code diagnostic remains separate future change.'},'archive_worker_result':a};emit('ADDITIONAL_RECONSTRUCTION.json',out);print('ADDITIONAL_PASS',json.dumps({'assertions':N,'tar_members':len(seen),'episodes':len(episodes),'writer':maxima}),flush=True)
