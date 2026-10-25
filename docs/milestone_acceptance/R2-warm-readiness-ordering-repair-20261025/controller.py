"""One new-release owner-policy86400s capture, followed by existing full stopped data auditor.

This is explicitly not a canonical V5 stage-final or predecessor. The finite
capture service and independent OS expiry survive LLM/SSH interruptions.
"""
from common import *
import sys, uuid, pwd, traceback

def write_sample(out, row):
    try:
        out.write(json.dumps(row,sort_keys=True,separators=(',',':'))+'\n');out.flush();os.fsync(out.fileno())
    except BaseException as e:
        retain_observation(row,'monitor_write_failed:'+repr(e))
        raise

def resources(runtime):
    pid=int(runtime['MainPID']);status=Path(f'/proc/{pid}/status').read_text()
    values={line.split(':')[0]:line.split(':',1)[1].strip() for line in status.splitlines() if ':' in line}
    mem={line.split(':')[0]:int(line.split()[1])*1024 for line in Path('/proc/meminfo').read_text().splitlines()}
    vm=dict(line.split() for line in Path('/proc/vmstat').read_text().splitlines())
    cg_name=runtime['ControlGroup']
    if cg_name!='/system.slice/'+REC:raise RuntimeError('recorder_cgroup_path_changed')
    proc_cgroup=Path(f'/proc/{pid}/cgroup').read_text()
    if proc_cgroup.strip()!='0::'+cg_name:raise RuntimeError('recorder_pid_cgroup_mismatch')
    cg=Path('/sys/fs/cgroup')/cg_name.lstrip('/')
    peak_text=(cg/'memory.swap.peak').read_text().strip()
    if not peak_text.isdecimal():raise RuntimeError('recorder_swap_peak_invalid')
    swap_peak=int(peak_text)
    return {'rss_bytes':int(values['VmRSS'].split()[0])*1024,'rss_peak_bytes':int(values['VmHWM'].split()[0])*1024,'host_available_bytes':mem['MemAvailable'],'host_swap_used_bytes':mem['SwapTotal']-mem['SwapFree'],'pswpin':int(vm['pswpin']),'pswpout':int(vm['pswpout']),'cgroup_swap_bytes':int((cg/'memory.swap.current').read_text()),'cgroup_swap_peak_bytes':swap_peak,'recorder_cgroup_path':cg_name,'pid_cgroup':proc_cgroup,'host_stat':Path('/proc/stat').read_text(),'host_loadavg':Path('/proc/loadavg').read_text(),'cpu_stat':(cg/'cpu.stat').read_text(),'cgroup_memory_bytes':int((cg/'memory.current').read_text()),'cpu_psi':Path('/proc/pressure/cpu').read_text(),'io_psi':Path('/proc/pressure/io').read_text(),'memory_psi':Path('/proc/pressure/memory').read_text(),'writer_free_bytes':os.statvfs(D).f_bavail*os.statvfs(D).f_frsize,'archive_free_bytes':os.statvfs(Q).f_bavail*os.statvfs(Q).f_frsize}

class Observation(dict):
    # Exact already-read bytes travel with the row, never into monitoring JSON.
    publication = None


def retain_observation(row, reason):
    """Exclusive diagnostics; a write failure never replaces the initiating fault."""
    try:
        prefix = 'diagnostic-' + uuid.uuid4().hex
        data = getattr(row, 'publication', None)
        if data is not None:
            with (Q / (prefix + '.state')).open('xb') as out:
                out.write(data); out.flush(); os.fsync(out.fileno())
                os.fchown(out.fileno(), 0, grp.getgrnam('bmdr').gr_gid)
                os.fchmod(out.fileno(), 0o640)
        publish(prefix + '.json', {'reason':reason,'observation':dict(row),
            'publication_file':prefix+'.state' if data is not None else None,
            'publication_sha256':hashlib.sha256(data).hexdigest() if data is not None else None,
            'publication_bytes':len(data) if data is not None else None})
    except Exception as diagnostic_error:
        print('DIAGNOSTIC_WRITE_FAILED '+repr(diagnostic_error)+'; original='+reason, file=sys.stderr, flush=True)


def sample(config, invocation, pid, strict=False):
    from qualification_auxiliary_gate import check
    row=Observation(utc_ns=time.time_ns(),boottime_ns=bt(),boot_id=boot(),
                    config_sha256=CONFIG_SHA,observation_phase='runtime')
    try:
        runtime=state(REC);row['runtime']=runtime
        row['observation_phase']='publication_read'
        with (D/'state/service_state.json').open('rb') as inp:
            row.publication=inp.read(1024*1024+1)
        row['state_sha256']=hashlib.sha256(row.publication).hexdigest()
        row['publication_complete']=len(row.publication)<=1024*1024
        if not row['publication_complete']:raise RuntimeError('publication_exceeds_1MiB; retained bounded prefix')
        row['observation_phase']='publication_parse'
        s=json.loads(row.publication)
        row.update(utc_ns=time.time_ns(),boottime_ns=bt(),core_ready=s.get('core_ready'),
            heartbeat_at_utc_ns=s.get('heartbeat_at_utc_ns'),recorder_service_instance_id=s.get('service_instance_id'))
        row['observation_phase']='runtime_identity'
        if not (runtime['InvocationID']==invocation and runtime['MainPID']==pid and runtime['ActiveState']=='active' and runtime['NRestarts']=='0'):
            raise RuntimeError('recorder_runtime_identity_changed')
        if s.get('pid')!=int(pid) or s.get('status') not in {'STARTING','RUNNING'}:
            raise RuntimeError('publication_pid_or_status_invalid')
        if sha(CONFIG)!=CONFIG_SHA or sha(DEPLOYMENT)!=IDENTITY:
            raise RuntimeError('config_or_deployment_identity_changed')
        row['observation_phase']='write_safety'
        row['write_safety_verified']=check_write_safety(s,allow_starting_capacity=not (Q/'owner-start.json').exists() and s.get('status')=='STARTING')
        row['observation_phase']='resources'
        row['resources']=resources(runtime)
        row.update(utc_ns=time.time_ns(),boottime_ns=bt())
        row['observation_phase']='auxiliary_gate'
        row['auxiliary_gate']=check(s,config,now_ns=row['utc_ns'],allow_empty_recovery=not strict)
        row['strict_auxiliary_gate']=check(s,config,now_ns=row['utc_ns'],allow_empty_recovery=False)
        row['state_publication_base64']=__import__('base64').b64encode(row.publication).decode('ascii')
        row['state_publication_bytes']=len(row.publication)
        row['observation_phase']='complete'
        return row
    except BaseException as e:
        retain_observation(row,'observation_failed:'+type(e).__name__+':'+str(e))
        raise


from qualification_owner_995 import (RecoveryObserver as CoreRecovery, POLICY, FINALIZATION_POLICY,
    FINALIZATION_RESERVE_NS as RECOVERY_LIMIT_NS, RECOVERY_POLL_NS, check_write_safety)
from qualification_scoped_corpus import capture_boundary


def validate_capture(row,start,initial_swap,recovery,*,endpoint=False,finishing=False):
    try:
        if row['boot_id']!=start['boot_id']:raise RuntimeError('boot_identity_changed')
        resource_gate(row,initial_swap,runtime=True)
        if abs((row['utc_ns']-start['t0_utc_ns'])-(row['boottime_ns']-start['t0_boottime_ns']))>=2*10**9:
            raise RuntimeError('clock_discontinuity')
        if finishing:
            if row['core_ready'] is not True or any(x in row['auxiliary_gate']['reasons'] for x in ['core_not_ready','heartbeat_not_fresh']):
                retain_observation(row,'bounded_minimum_target_core_recovery')
            return recovery.finish_endpoint(row,checked_at_ns=bt())
        if endpoint:recovery.endpoint(row,checked_at_ns=bt())
        else:
            if row['core_ready'] is not True or any(x in row['auxiliary_gate']['reasons'] for x in ['core_not_ready','heartbeat_not_fresh']):
                retain_observation(row,'bounded_core_heartbeat_recovery; reasons='+repr(row['auxiliary_gate']['reasons']))
            recovery.check(row,checked_at_ns=bt())
    except BaseException as e:
        retain_observation(row,'capture_validation_failed:'+type(e).__name__+':'+str(e))
        raise

def strict_finish(config, invocation, pid, start, initial_swap, recovery, target_bt):
    """Observe up to minimum+60s, preserving a non-READY endpoint honestly."""
    deadline = target_bt + RECOVERY_LIMIT_NS
    row = sample(config, invocation, pid)
    publish('minimum-duration-observation.json', dict(row))
    with (Q/'target-finalization-observations.jsonl').open('x') as out:
        while True:
            check_window()
            row['observation_role']='finish_probe'
            write_sample(out,row)
            complete=validate_capture(row,start,initial_swap,recovery,finishing=True)
            if bt()>deadline or row['boottime_ns']>deadline:
                publish('finalization-timing-failure.json',{'result':'TIMING_CONTROL_FAILED','actual_observation':dict(row),'deadline_boottime_ns':deadline,'qualified_minimum_duration_ns':TARGET_NS,'note':'Preserve actual duration/data; repair affected closeout, no automatic recapture.'})
                raise RuntimeError('target_finalization_observation_exceeded_bound')
            if complete or bt()>=deadline:return row
            due=min(bt()+RECOVERY_POLL_NS,deadline)
            time.sleep(max(0,due-bt())/10**9)
            if bt()>=deadline:return row
            row=sample(config,invocation,pid)

def resource_gate(row, initial_swap, *, runtime=False):
    r=row['resources']
    if not runtime and not (r['rss_bytes']<1024**3 and r['host_available_bytes']>2*1024**3 and r['cgroup_swap_bytes']==0 and r['cgroup_swap_peak_bytes']==0):
        raise RuntimeError('recorder_memory_resource_bound:'+repr(r))
    # Host counters remain recorded diagnostics. Admission uses this Recorder
    # cgroup's current and lifetime peak, never a reset or missing-as-zero value.
    if not (r['writer_free_bytes']>10*1024**3 and r['archive_free_bytes']>10*1024**3):
        raise RuntimeError('filesystem_free_space_bound:'+repr(r))

def validate_warm(row,initial_swap,request,*,strict=False):
    try:
        resource_gate(row,initial_swap)
        if row['heartbeat_at_utc_ns']<request:raise RuntimeError('warm_heartbeat_predates_start_request')
        if strict and row['strict_auxiliary_gate']['result']!='PASS':raise RuntimeError('strict_warm_endpoint_failed')
    except BaseException as e:
        retain_observation(row,'warm_validation_failed:'+type(e).__name__+':'+str(e))
        raise


def drain():
    run(['systemctl','disable','--now',REC],150); stopped()
    deadline=time.monotonic()+900
    while True:
        c=census()
        if c['backlog']==0 and c['transaction_backlog']==0:break
        assert time.monotonic()<deadline,'natural archive drain timeout';time.sleep(5)
    run(['systemctl','stop',AT])
    while True:
        s=state(AS)
        if s['ActiveState']=='inactive' and s['MainPID']=='0':assert s['Result']=='success';break
        assert time.monotonic()<deadline,'archive settle timeout';time.sleep(2)
    assert not list(D.rglob('*.partial'))
    publish('terminal-freeze.json',{'result':'STOPPED_DISABLED_DRAINED_FROZEN','utc_ns':time.time_ns(),'census':c,'recorder':stopped(),'archive_worker':s,'archive_timer':state(AT)})

def canonical_readiness_during_warm(invocation, pid, warm_deadline, expected_boot):
    """One retained canonical check inside the existing1200s warm budget.

    This earlier result supplies deployment verification, never final freshness.
    Fresh strict end/start observations remain required after warm completion.
    """
    import base64
    def raw(value):
        if value is None:return None
        value=value.encode('utf-8') if isinstance(value,str) else value
        return {'base64':base64.b64encode(value).decode('ascii'),'bytes':len(value),'sha256':hashlib.sha256(value).hexdigest()}
    record={'kind':'CANONICAL_READINESS_DURING_EXISTING_WARM_BUDGET','began_utc_ns':time.time_ns(),'began_boottime_ns':bt(),'began_monotonic':time.monotonic(),'expected_boot_id':expected_boot,'recorder_invocation':invocation,'recorder_pid':pid,'formal_duration_credit_ns':0,'supplies_final_start_freshness':False}
    ready=None
    try:
        before=state(REC);record['runtime_before']=before
        assert (before['InvocationID'],before['MainPID'],before['ActiveState'],before['NRestarts'])==(invocation,pid,'active','0')
        record.update(observed_boot_before=boot(),config_sha_before=sha(CONFIG),identity_sha_before=sha(DEPLOYMENT))
        assert record['observed_boot_before']==expected_boot and record['config_sha_before']==CONFIG_SHA and record['identity_sha_before']==IDENTITY
        remaining=warm_deadline-time.monotonic();assert remaining>0,'warm_deadline_before_canonical_check'
        record['timeout_seconds']=min(330,remaining)
        try:
            completed=subprocess.run(CLI+['deployment','readiness'],capture_output=True,timeout=record['timeout_seconds'])
            record.update(returncode=completed.returncode,stdout_raw=raw(completed.stdout),stderr_raw=raw(completed.stderr),error_type=None)
            try:ready=json.loads(completed.stdout);record['parse_error']=None
            except (ValueError,UnicodeError,TypeError) as error:record['parse_error']=type(error).__name__
        except (subprocess.TimeoutExpired,OSError) as error:
            record.update(returncode=None,error_type=type(error).__name__,stdout_raw=raw(getattr(error,'stdout',None)),stderr_raw=raw(getattr(error,'stderr',None)))
        record.update(ended_utc_ns=time.time_ns(),ended_boottime_ns=bt(),ended_monotonic=time.monotonic())
        after=state(REC);record['runtime_after']=after
        assert (after['InvocationID'],after['MainPID'],after['ActiveState'],after['NRestarts'])==(invocation,pid,'active','0')
        record.update(observed_boot_after=boot(),config_sha_after=sha(CONFIG),identity_sha_after=sha(DEPLOYMENT))
        assert record['observed_boot_after']==expected_boot and record['config_sha_after']==CONFIG_SHA and record['identity_sha_after']==IDENTITY
        assert record['returncode']==0 and record['error_type'] is None and isinstance(ready,dict) and ready.get('state')=='READY','canonical_readiness_failed'
        identity=ready['evidence']['identity'];runtime=ready['evidence']['systemd_runtime']
        assert identity['identity_sha256']==IDENTITY and identity['source_git_sha']==SOURCE and identity['config_sha256']==CONFIG_SHA
        assert runtime['active_state']=='active' and type(runtime['main_pid']) is int and runtime['main_pid']==int(pid)
        assert time.monotonic()<warm_deadline,'canonical_readiness_exceeded_existing_warm_deadline'
        record['result']='CANONICAL_CHECK_PASSED_WITHIN_WARM_BUDGET'
    except BaseException as error:
        record.update(result='CANONICAL_CHECK_FAILED',failure_type=type(error).__name__)
        raise
    finally:
        record['recorded_utc_ns']=time.time_ns()
        record['recorded_boottime_ns']=bt()
        record['recorded_monotonic']=time.monotonic()
        try:publish('warm-readiness-check.json',record)
        except BaseException as publication_error:
            print('WARM_READINESS_EVIDENCE_WRITE_FAILED '+type(publication_error).__name__,file=sys.stderr,flush=True)
            if record.get('result')!='CANONICAL_CHECK_FAILED':raise
    return ready


def capture():
    from binance_market_data_recorder.config import load_config
    from qualification_owner_995 import evaluate_fixed_slots as evaluate
    exact();stopped();assert not (Q/'owner-start.json').exists() and not (Q/'terminal-audit').exists()
    publish('capture-admission.json',admission(TARGET_SECONDS+1860,TARGET_SECONDS+60,1200))
    config=load_config(config_file=CONFIG,environ={}).config
    cohort=configured_cohort(config)
    begin=time.monotonic();warm_deadline=begin+1200
    boundary=capture_boundary(D,inventory_root=Q,utc_ns=time.time_ns(),boot_id=boot())
    inventory=Path(boundary['start_catalog_path']);os.chown(inventory,0,grp.getgrnam('bmdr').gr_gid);os.chmod(inventory,0o640)
    publish('data-scope-start.json',boundary)
    request=time.time_ns();run(['systemctl','enable','--now',REC],120);r=state(REC)
    assert r['MainPID']!='0' and r['NRestarts']=='0'
    inv,pid=r['InvocationID'],r['MainPID']; first=None; ready=None; initial_swap=None; samples=0; warm_boot=boot()
    with (Q/'warmup.jsonl').open('x') as out:
        while True:
            # Publication from the new incarnation is required before any warm sample.
            path=D/'state/service_state.json';s=read(path)
            if s.get('pid')!=int(pid):
                assert time.monotonic()<warm_deadline;time.sleep(2);continue
            row=sample(config,inv,pid,True);write_sample(out,row);samples+=1
            swap=(row['resources']['pswpin'],row['resources']['pswpout'])
            if initial_swap is None:initial_swap=swap
            validate_warm(row,initial_swap,request)
            assert row['boot_id']==warm_boot
            if first is not None:assert row['recorder_service_instance_id']==first['recorder_service_instance_id']
            assert time.monotonic()<warm_deadline,'strict all-enabled warmup timeout'
            if row['auxiliary_gate']['result']=='PASS':
                if first is None:
                    first=row
                    assert isinstance(first['recorder_service_instance_id'],str) and first['recorder_service_instance_id']
                    ready=canonical_readiness_during_warm(inv,pid,warm_deadline,warm_boot)
                elif row['boottime_ns']-first['boottime_ns']>=300*10**9 and samples>=12:break
            assert time.monotonic()<warm_deadline,'strict all-enabled warmup timeout';time.sleep(30)
    assert ready is not None and ready['state']=='READY'
    assert time.monotonic()<warm_deadline,'warmup exceeded 1200s'
    end=sample(config,inv,pid,True);publish('warmup-end-observation.json',dict(end));validate_warm(end,initial_swap,request,strict=True)
    assert end['boot_id']==warm_boot and end['recorder_service_instance_id']==first['recorder_service_instance_id']
    assert time.monotonic()<warm_deadline,'strict warm completion exceeded1200s'
    publish('warmup-result.json',{'result':'ACTUAL_ALL_ENABLED_WARMUP_PASS','first_strict':first,'end_strict':end,'samples':samples,'core_readiness':ready,'elapsed_seconds':time.monotonic()-begin})
    adm=admission(TARGET_SECONDS+660,TARGET_SECONDS+60,0);publish('prestart-admission.json',adm)
    start_row=sample(config,inv,pid,True);publish('start-observation.json',dict(start_row));validate_warm(start_row,initial_swap,request,strict=True)
    assert start_row['boot_id']==warm_boot and start_row['recorder_service_instance_id']==first['recorder_service_instance_id']
    t0_utc=time.time_ns();t0_bt=bt();run_id=uuid.uuid4().hex
    start={'kind':'R2_CAPACITY_N2_OWNER_POLICY_24H_START','run_id':run_id,'t0_utc_ns':t0_utc,'t0_boottime_ns':t0_bt,'target_duration_ns':TARGET_NS,'target_utc_ns_estimate':t0_utc+TARGET_NS,'boot_id':boot(),'source_git_sha':SOURCE,'identity_sha256':IDENTITY,'config_sha256':CONFIG_SHA,'accepted_deployment_sha256':DEPLOY_SHA,'recorder_invocation':inv,'recorder_pid':pid,'native_capture_invocation':os.environ.get('INVOCATION_ID'),'strict_start':start_row,'admission_sha256':sha(Q/'prestart-admission.json'),'canonical_v5_predecessor':False,'owner_policy_predecessor':predecessor(),'core_recovery_policy':POLICY,'target_duration_semantics':'minimum','target_finalization_policy':FINALIZATION_POLICY,'target_finalization_reserve_seconds':60,'data_scope_start_sha256':sha(Q/'data-scope-start.json'),'data_scope':boundary['scope'],'configured_products':cohort['products'],'configured_auxiliary_contexts':sorted(cohort['expected_contexts']),'configured_spools':cohort['spools']}
    publish('owner-start.json',start);print(json.dumps({'phase':'CAPTURE','run_id':run_id,'t0_utc_ns':t0_utc,'target_utc_ns':t0_utc+TARGET_NS}),flush=True)
    next_bt=t0_bt; recovery=CoreRecovery();target_bt=t0_bt+TARGET_NS
    with (Q/'monitoring.jsonl').open('x') as out, (Q/'recovery-observations.jsonl').open('x') as probes:
        while bt()<target_bt:
            regular=bt()>=next_bt
            row=sample(config,inv,pid)
            # Separate extra probes: never change the fixed30s monitor denominator.
            if regular:
                row['scheduled_monitor_boottime_ns']=next_bt
                row['monitor_slot']=(next_bt-t0_bt)//(30*10**9)
                row['skipped_monitor_slots']=max(0,(row['boottime_ns']-next_bt)//(30*10**9))
            row['observation_role']='nominal_monitor' if regular else 'recovery_probe'
            write_sample(out if regular else probes,row)
            validate_capture(row,start,initial_swap,recovery)
            if regular:next_bt=t0_bt+((bt()-t0_bt)//(30*10**9)+1)*30*10**9
            due=recovery.next_check_ns(bt(),next_bt,target_bt)
            time.sleep(max(0,due-bt())/10**9)
    check_window();endpoint=strict_finish(config,inv,pid,start,initial_swap,recovery,target_bt)
    publish('target-observation.json',dict(endpoint))
    validate_capture(endpoint,start,initial_swap,recovery,endpoint=True)
    target={'kind':'R2_OWNER_POLICY_24H_TARGET','run_id':run_id,'t0_utc_ns':t0_utc,'t0_boottime_ns':t0_bt,'terminal_utc_ns':endpoint['utc_ns'],'terminal_boottime_ns':endpoint['boottime_ns'],'elapsed_boottime_ns':endpoint['boottime_ns']-t0_bt,'strict_endpoint':endpoint,'start_sha256':sha(Q/'owner-start.json'),'canonical_v5_final_eligible':False,'core_recovery':recovery.summary(),'target_duration_semantics':'minimum','target_finalization_policy':FINALIZATION_POLICY,'qualified_minimum_duration_ns':TARGET_NS,'monitor_interval_start_utc_ns':t0_utc,'monitor_interval_end_utc_ns':t0_utc+TARGET_NS,'finalization_elapsed_ns':max(0,endpoint['boottime_ns']-target_bt),'result':'CAPTURE_TARGET_REACHED','actual_strict_endpoint_result':endpoint['strict_auxiliary_gate']['result'],'actual_official_endpoint_result':endpoint['auxiliary_gate']['result'],'owner_quality_policy':POLICY}
    publish('owner-target.json',target)
    run(['systemctl','disable','--now',REC],150);stopped()
    assert target['elapsed_boottime_ns']>=TARGET_NS
    try:
        with (Q/'monitoring.jsonl').open('rb') as inp:rate=evaluate(inp,start_ns=t0_utc,end_ns=t0_utc+TARGET_NS,config_sha256=CONFIG_SHA,expected_contexts=cohort['expected_contexts'],start_boottime_ns=t0_bt)
        rate['strict_owner_threshold_result']='PASS' if rate['passed']*1000>=rate['total']*995 else 'BLOCK'
    except (ValueError,KeyError,TypeError,OSError) as error:
        # Missing/invalid availability evidence cannot establish a percentage.
        # Retain its failure and finish the independent saved-data audit.
        rate={'result':'BLOCK','strict_owner_threshold_result':'BLOCK',
              'evidence_state':'INVALID','error_type':type(error).__name__,'error':str(error),
              'note':'No availability percentage established; saved-data correctness is separate.'}
    rate.update(monitor_interval_semantics='fixed_nominal_minimum_duration',monitor_interval_end_utc_ns=t0_utc+TARGET_NS,actual_strict_endpoint_utc_ns=endpoint['utc_ns'],finish_observations_in_monitor_denominator=False)
    publish('monitor-rate.json',rate)  # Separate availability metric; always finish the saved-data audit.
    drain();publish('terminal-admission.json',admission(0,0,0))
    args=['systemd-run','--no-block','--unit',TERMINAL,'--property=Type=oneshot','--property=TimeoutStartSec=infinity','--property=TimeoutStopSec=600','--property=CPUAccounting=yes','--property=MemoryAccounting=yes','--property=ExecStopPost=/usr/bin/python3.12 '+str(Q/'restore.py')+' terminal-exit','--setenv=TMPDIR='+str(Q/'tmp'),'--setenv=SQLITE_TMPDIR='+str(Q/'tmp'),'/usr/bin/python3.12',str(Q/'pin-unit-exec.py'),PYTHON,str(Q/'controller.py'),'terminal']
    launch=run(args);publish('terminal-launch.json',{'argv':args,'stdout':launch,'utc_ns':time.time_ns(),'target_sha256':sha(Q/'owner-target.json')})
    limit=time.monotonic()+60
    while not (Q/'terminal-start.json').exists():assert time.monotonic()<limit;time.sleep(1)
    print('CAPTURE_COMPLETE_TERMINAL_NATIVE_STARTED',flush=True)

def terminal():
    exact();stopped();target=read(Q/'owner-target.json');assert target['result']=='CAPTURE_TARGET_REACHED' and target['elapsed_boottime_ns']>=TARGET_NS
    assert read(Q/'terminal-freeze.json')['result']=='STOPPED_DISABLED_DRAINED_FROZEN'
    publish('terminal-start.json',{'phase':'FULL_SAVED_DATA_AUDIT','utc_ns':time.time_ns(),'boot_id':boot(),'invocation':os.environ.get('INVOCATION_ID'),'target_sha256':sha(Q/'owner-target.json')})
    assert not (Q/'terminal-audit').exists()
    p=Q/'terminal-audit';p.mkdir(mode=0o750);os.chown(p,pwd.getpwnam('bmdr').pw_uid,grp.getgrnam('bmdr').gr_gid)
    with (Q/'terminal-audit-result.json').open('x') as out:
        subprocess.run(['/usr/sbin/runuser','-u','bmdr','--',PYTHON,str(Q/'scoped-terminal.py')],stdout=out,check=True)
        out.flush();os.fsync(out.fileno());os.fchown(out.fileno(),0,grp.getgrnam('bmdr').gr_gid);os.fchmod(out.fileno(),0o640)
    a=read(p/'audit-root.json'); scoped=read(p/'owner-selected-result.json')
    assert a['completion_state']=='COMPLETE' and a['result']=='PASS_CANDIDATE' and not a['blocking_findings']
    assert a['deployment_identity']==read(DEPLOYMENT) and a['formal_duration_credit_ns']==0
    assert scoped['run_id']==target['run_id'] and scoped['owner_target_sha256']==sha(Q/'owner-target.json')
    assert scoped['selected_counts']['frames']>0
    exact();stopped();assert check_window()>3780,'insufficient restored-review handoff budget'
    publish('native-result.json',{'kind':'R2_OWNER_POLICY_SAVED_DATA_RESULT','result':'PENDING_INDEPENDENT_REVIEW_AND_RESTORED_HANDOFF','utc_ns':time.time_ns(),'target_sha256':sha(Q/'owner-target.json'),'audit_sha256':sha(p/'audit-root.json'),'verification_sha256':sha(p/'owner-selected-result.json'),'selected_scope_result':scoped,'data_scope_start_sha256':sha(Q/'data-scope-start.json'),'canonical_v5_final_eligible':False,'saved_data_policy':'at_least_995_per_1000_observed_saved_frames_not_every_exchange_event','monitor_sha256':sha(Q/'monitor-rate.json')})
    print('FULL_DUAL_LIVE_AND_COMPLETED_VERIFY_NATIVE_COMPLETE',flush=True)

if __name__=='__main__':
    mode=sys.argv[1];assert mode in {'capture','terminal'}
    try: globals()[mode]()
    except BaseException as e:
        try:
            publish(mode+'-failure.json',{'result':'FAILED','utc_ns':time.time_ns(),'boottime_ns':bt(),'error_type':type(e).__name__,'error':str(e),'traceback':traceback.format_exc(),'action':'PAUSE_AFFECTED_CAPTURE','requires_owner_notification':True})
        except Exception as diagnostic_error:
            print('FAILURE_RECEIPT_WRITE_FAILED '+repr(diagnostic_error)+'; original='+repr(e),file=sys.stderr,flush=True)
        raise
