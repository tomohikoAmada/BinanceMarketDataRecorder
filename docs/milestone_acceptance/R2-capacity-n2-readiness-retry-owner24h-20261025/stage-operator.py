"""Exact N2 runtime custody, separate finite arm/launch, or metadata-only read.

All mutating modes require the actual Linux result and independent acceptance.
This operator never supplies capture duration credit or reconstructs Raw data.
"""
from pathlib import Path
import base64
import hashlib
import json
import subprocess
import sys
import time

P = Path(__file__).resolve().parent
RUNTIME = P / 'runtime'
MANIFEST = P / 'stage-controls-manifest.json'
BINDING = P / 'stage-qualification.json'
CANONICAL = '/opt/binance-market-data-recorder/venv/bin/python'
SYSTEM = '/usr/bin/python3.12'
REMOTE_PARENT = '/srv/recorder-data/recorder-archive/evidence'
QA_SCOPE = REMOTE_PARENT + '/R2-capacity-n2-readiness-retry-20261025-1s0kswlt'
UNIT_PREFIX = 'binance-recorder-r2-capacity-n2-readiness-retry-20261025-1s0kswlt-'
REC = 'binance-market-data-recorder.service'
AS = 'binance-market-data-archive.service'
AT = 'binance-market-data-archive.timer'


def sha(data):
    return hashlib.sha256(data).hexdigest()


def json_bytes(body):
    return (json.dumps(body, sort_keys=True) + '\n').encode()


def load_design():
    m = json.loads(MANIFEST.read_bytes())
    assert m['scope'] == REMOTE_PARENT + '/R2-capacity-n2-readiness-retry-20261025-1s0kswlt'
    assert m['source'] == '20e57f30682e88b4090cc2f0da34aa26fca4de2e'
    assert m['actual_deployment_sha256'] == 'e148f51a26aea4d0da81d96d9700900f3f4f171e8b1c0e1f8d9aa6c03f859a7f'
    assert m['actual_identity_sha256'] == '9b8ce8219105fd7962cbf4831a99c9764c3c2889088bda7e73c834ff6bffc715'
    assert m['actual_config_sha256'] == '5b73db1b6ba6ac8cc7ab84cbcf0c7b688187b316644bef3df44bbb42f9283419'
    assert m['state'] == 'FRESH_PREFLIGHT_BOUND_DESIGN'
    assert m['target_duration_seconds'] == 86400 and m['finite_window_seconds'] == 230400
    payload = {n: (RUNTIME / n).read_bytes() for n in m['members']}
    assert len(payload) == 14 and len([n for n in payload if n.endswith('.py')]) == 13
    assert all(Path(n).name == n for n in payload)
    assert {n: sha(b) for n, b in payload.items()} == m['members']
    return m, payload


def approved_payload(m, runtime, mode):
    """Keep historical algorithm QA separate from the current exact-byte delta."""
    binding = json.loads(BINDING.read_bytes())
    assert binding['manifest_sha256'] == sha(MANIFEST.read_bytes())
    assert binding['operator_sha256'] == sha(Path(__file__).read_bytes())
    assert binding['state'] in {'DESIGN_ACCEPTED_COPY_ONLY', 'CURRENT_LINUX_PHYSICAL_INDEPENDENTLY_ACCEPTED'}
    def artifact(item):
        path = Path(item['path'])
        assert path.is_absolute() and path.is_file() and not path.is_symlink()
        data = path.read_bytes(); assert sha(data) == item['sha256']
        return data
    design = {name: artifact(item) for name, item in binding['design'].items()}
    assert set(design) == {'report', 'result', 'manifest'}
    design_result = json.loads(design['result'])
    assert design_result['decision'] == 'ACCEPT'
    checked = design_result['bindings']
    assert checked['operator_sha256'] == binding['operator_sha256']
    assert checked['controls_manifest_sha256'] == binding['manifest_sha256']
    assert checked['runtime_members'] == m['members'] and checked['scope'] == m['scope']
    assert checked['source'] == m['source'] and checked['identity_sha256'] == m['actual_identity_sha256']
    old = binding['historical_algorithm_qa']
    old_result = json.loads(artifact(old['result']))
    old_members = json.loads(artifact(old['runtime_manifest']))['members']
    assert old_result['result'] == 'LINUX_N2_OWNER995_FULL_CLOCK_AND_PUBLICATION_QA_PASS'
    assert old_result['runtime_manifest'] == old_members and len(old_result['cases']) == 18
    assert old_result['production_or_raw_actions'] is False and old_result['real_capture_duration_credit_ns'] == 0
    unchanged = {name for name in runtime if sha(runtime[name]) == old_members[name]}
    assert unchanged == set(runtime) - {'common.py', 'capacity-policy.json', 'qualification_owner_995.py'}
    preflight_bytes = artifact(binding['current_preflight'])
    preflight = json.loads(preflight_bytes)
    assert preflight['result'] == 'CURRENT_N2_METADATA_PREFLIGHT_NOT_ADMISSION'
    assert preflight['source'] == m['source'] and preflight['identity_sha256'] == m['actual_identity_sha256']
    assert preflight['config_sha256'] == m['actual_config_sha256']
    assert preflight['counts'] == m['expected_stopped_counts'] and preflight['backlog'] == preflight['transaction_backlog'] == 0
    handoff = preflight['fresh_failed_attempt_handoff']
    assert handoff['result'] == 'VERIFIED_PENDING_INDEPENDENT_REVIEW'
    assert handoff['startup_custody_gate'] is True and handoff['partial_count'] == 0 and handoff['partial_inventory'] == []
    accepted_preflight = json.loads(artifact(binding['preflight_acceptance']['result']))
    assert accepted_preflight['decision'] == 'ACCEPT' and accepted_preflight['accepted_preflight_sha256'] == sha(preflight_bytes)
    for n in ['report','manifest']:artifact(binding['preflight_acceptance'][n])
    code = json.loads(artifact(binding['classifier_code']['result']))
    assert code['decision'] == 'ACCEPT' and code['bindings']['candidate_sha256'] == sha(runtime['qualification_owner_995.py'])
    assert code['provided_fixtures_independently_executed_cases'] == 39
    for n in ['report','manifest']:artifact(binding['classifier_code'][n])
    fixtures = {n: (P/'classifier-fixtures'/n).read_bytes() for n in m['classifier_fixture_members']}
    assert len(fixtures) == 3 and all(Path(n).name == n for n in fixtures)
    assert {n:sha(b) for n,b in fixtures.items()} == m['classifier_fixture_members']
    # This stable design payload is identical before and after the copy-time
    # Linux import/binding smoke. Actual acceptance is a separate ARM predicate.
    payload = dict(runtime);payload.update(fixtures)
    for name in ['report','result','manifest']:
        payload['classifier-code-' + name + ('.md' if name=='report' else '.json')] = artifact(binding['classifier_code'][name])
    payload['stage-controls-manifest.json'] = MANIFEST.read_bytes()
    for name, data in design.items():
        payload['stage-design-' + name + ('.md' if name == 'report' else '.json')] = data
    payload['historical-linux-qa-result.json'] = artifact(old['result'])
    payload['historical-runtime-manifest.json'] = artifact(old['runtime_manifest'])
    payload['stage-design-evidence.json'] = json_bytes({
        'kind': 'CURRENT_DELTA_DESIGN_WITH_DISTINCT_HISTORICAL_QA',
        'operator_sha256': binding['operator_sha256'], 'design': binding['design'],
        'historical_algorithm_qa': old, 'current_preflight_sha256': sha(preflight_bytes),
        'historical_duration_or_current_source_pass_credit': False,
        'actual_current_linux_smoke_required_before_arm': True})
    if mode in {'arm', 'launch'}:
        assert binding['state'] == 'CURRENT_LINUX_PHYSICAL_INDEPENDENTLY_ACCEPTED'
        actual = binding['actual']
        copy = json.loads(artifact(actual['copy']))
        assert copy['result'] == 'EXCLUSIVE_N2_CONTROLS_COPIED_NOT_ARMED' and copy['scope'] == m['scope']
        assert copy['census']['counts'] == m['expected_stopped_counts']
        assert {name: item['sha256'] for name, item in copy['physical'].items()} == {name: sha(data) for name, data in payload.items()}
        smoke = copy['linux_binding_smoke']
        assert smoke['result'] == 'CURRENT_LINUX_CLEAN_IMPORT_AND_BINDING_SMOKE_PASS'
        assert smoke['members'] == m['members'] and len(smoke['imports']) == 12
        linux = copy['linux_classifier_qualification']
        assert linux['result'] == 'GENUINE_LINUX_CLASSIFIER39_PASS' and linux['platform'] == 'linux'
        assert linux['candidate_sha256'] == m['members']['qualification_owner_995.py']
        assert linux['fixture_result_sha256'] == '4b5ad6d4c8ae395f7520e54d2d48dbb59192dcc9de7228b15a5f7b8dfe3bcd81'
        assert linux['case_count'] == 39 and linux['returncode'] == 0
        assert smoke['source'] == m['source'] and smoke['identity_sha256'] == m['actual_identity_sha256']
        assert smoke['historical_n1_source'] == 'c3686dd76406c8d483b23eb10417349e5a1ffecd'
        assert smoke['inherited_duration_ns'] == 0 and smoke['formal_credit_seconds'] == 0
        acceptance = json.loads(artifact(actual['independent_result']))
        assert acceptance['decision'] == 'ACCEPT'
        assert acceptance['accepted_copy_sha256'] == sha(artifact(actual['copy']))
        assert acceptance['controls_manifest_sha256'] == sha(MANIFEST.read_bytes())
        assert actual['accepted_copy_sha256'] == sha(artifact(actual['copy']))
        for name in ['independent_report', 'independent_manifest']:
            artifact(actual[name])
    return payload


SMOKE = r"""
import sys,json,time,hashlib,importlib.util
from pathlib import Path
q=Path(sys.argv[1]);sys.dont_write_bytecode=True;sys.path.insert(0,str(q))
manifest=json.loads((q/'stage-controls-manifest.json').read_bytes());imports={}
for name in sorted(manifest['members']):
 if not name.endswith('.py') or name=='pin-unit-exec.py':continue
 key=name[:-3].replace('-','_');spec=importlib.util.spec_from_file_location(key,q/name)
 module=importlib.util.module_from_spec(spec);sys.modules[key]=module;spec.loader.exec_module(module)
 imports[name]=str(Path(module.__file__).resolve());assert imports[name]==str(q/name)
import common as c
import binance_market_data_recorder as package
assert Path(package.__file__).is_relative_to('/opt/binance-market-data-recorder/venv')
c.exact();cohort=c.configured_cohort();policy=c.predecessor()
prior=json.loads(Path(policy['prior_stage_receipt']['path']).read_bytes())
assert prior['source_git_sha']=='c3686dd76406c8d483b23eb10417349e5a1ffecd'
assert policy['inherited_duration_ns']==0 and c.TARGET_SECONDS==86400
import controller
assert controller.CoreRecovery.__module__=='qualification_owner_995'
print(json.dumps({'result':'CURRENT_LINUX_CLEAN_IMPORT_AND_BINDING_SMOKE_PASS','imports':imports,'members':manifest['members'],'package_origin':package.__file__,'source':c.SOURCE,'identity_sha256':c.IDENTITY,'historical_n1_source':prior['source_git_sha'],'inherited_duration_ns':0,'formal_credit_seconds':0,'boottime_ns':time.clock_gettime_ns(time.CLOCK_BOOTTIME),'configured_products':cohort['products'],'contexts':len(cohort['expected_contexts']),'spools':cohort['spools']}))
"""


def build_remote(mode):
    assert mode in {'copy', 'arm', 'launch', 'read'}
    m, runtime = load_design()
    q = m['scope']
    payload = approved_payload(m, runtime, mode) if mode != 'read' else None
    head = '''from pathlib import Path
import os,sys,json,hashlib,pwd,stat,time,subprocess,base64
q=Path(SCOPE);members=MEMBERS
assert os.geteuid()==0
os.umask(0o027)
def fresh_active_inventory():
 root=Path('/var/lib/binance-market-data-recorder/data/active');ended=time.monotonic()+30
 evidence={'root':str(root),'began_utc_ns':time.time_ns(),'entries':[],'directories':[],'partial_count':0,'raw_or_partial_bytes_opened':False}
 def check():assert time.monotonic()<ended,'bounded_fresh_inventory_timeout'
 def onerror(error):raise error
 try:
  for parent in [root.parent.parent,root.parent,root]:
   st=parent.lstat();assert stat.S_ISDIR(st.st_mode) and not parent.is_symlink() and parent.resolve(strict=True)==parent;check()
  for base,dirs,names in os.walk(root,followlinks=False,onerror=onerror):
   evidence['directories'].append(base);check()
   for name in dirs:
    path=Path(base)/name;st=path.lstat();evidence['entries'].append({'path':str(path),'mode':st.st_mode,'inode':st.st_ino,'device':st.st_dev,'bytes':st.st_size,'kind':'directory'});assert stat.S_ISDIR(st.st_mode) and not path.is_symlink();check()
   for name in names:
    path=Path(base)/name;st=path.lstat();evidence['entries'].append({'path':str(path),'mode':st.st_mode,'inode':st.st_ino,'device':st.st_dev,'bytes':st.st_size,'mtime_ns':st.st_mtime_ns,'kind':'file'});assert stat.S_ISREG(st.st_mode) and not path.is_symlink();evidence['partial_count']+=name.endswith('.partial');check()
  evidence.update(ended_utc_ns=time.time_ns(),file_count=sum(item['kind']=='file' for item in evidence['entries']),walk_complete=True)
  assert evidence['file_count']==evidence['partial_count']==0,evidence
  return evidence
 except BaseException as error:
  evidence.update(result='FAILED_FRESH_ZERO_INVENTORY',error_type=type(error).__name__,startup_allowed=False)
  print(json.dumps(evidence,sort_keys=True),flush=True);raise
'''.replace('SCOPE', repr(q)).replace('MEMBERS', repr(m['members']))
    if mode == 'copy':
        encoded = {n: base64.b64encode(data).decode('ascii') for n, data in payload.items()}
        body = '''
assert q.parent==Path('/srv/recorder-data/recorder-archive/evidence') and q.parent.resolve(strict=True)==q.parent
assert not q.exists() and not q.is_symlink()
a=pwd.getpwnam('bmdr');tmp=q/'tmp'
q.mkdir(mode=0o750);os.chown(q,0,a.pw_gid);os.chmod(q,0o750)
tmp.mkdir(mode=0o770);os.chown(tmp,a.pw_uid,a.pw_gid);os.chmod(tmp,0o770)
payload=PAYLOAD
for name,encoded in payload.items():
 assert Path(name).name==name
 data=base64.b64decode(encoded,validate=True)
 fd=os.open(q/name,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o640)
 os.fchown(fd,0,a.pw_gid);os.fchmod(fd,0o640)
 with os.fdopen(fd,'wb') as out:out.write(data);out.flush();os.fsync(out.fileno())
for d in [tmp,q,q.parent]:
 fd=os.open(d,os.O_DIRECTORY);os.fsync(fd);os.close(fd)
physical={}
for name,encoded in payload.items():
 f=q/name;s=f.lstat();data=f.read_bytes()
 assert stat.S_ISREG(s.st_mode) and not f.is_symlink() and data==base64.b64decode(encoded,validate=True),name
 assert (s.st_uid,s.st_gid,stat.S_IMODE(s.st_mode))==(0,a.pw_gid,0o640),name
 physical[name]={'sha256':hashlib.sha256(data).hexdigest(),'bytes':len(data),'uid':s.st_uid,'gid':s.st_gid,'mode':stat.S_IMODE(s.st_mode)}
sys.dont_write_bytecode=True;sys.path.insert(0,str(q));import common as c
assert c.stopped()['NRestarts']=='0';c.exact();fresh_inventory=fresh_active_inventory();cs=c.census()
assert tuple(cs['counts'])==c.EXPECTED_STOPPED_COUNTS and cs['backlog']==cs['transaction_backlog']==0,cs
assert os.stat(q).st_dev!=os.stat(c.D).st_dev and os.stat(tmp).st_dev==os.stat(q).st_dev
admission=c.admission(c.TARGET_SECONDS+2160,c.TARGET_SECONDS+60,1200)
c.publish('copy-admission.json',admission)
qa_error=None
try:
 qa_run=subprocess.run([CANONICAL,'-B',str(q/'test_classifier.py')],cwd=str(q),capture_output=True,timeout=120)
 qa_stdout,qa_stderr,qa_rc=qa_run.stdout,qa_run.stderr,qa_run.returncode
except (subprocess.TimeoutExpired,OSError) as error:
 qa_error=type(error).__name__;qa_stdout=getattr(error,'stdout',None);qa_stderr=getattr(error,'stderr',None);qa_rc=None
qa_bytes=b''
if (q/'parent-tests.json').exists():
 fd=os.open(q/'parent-tests.json',os.O_RDONLY|os.O_NOFOLLOW)
 os.fchown(fd,0,a.pw_gid);os.fchmod(fd,0o640);os.fsync(fd)
 with os.fdopen(fd,'rb') as src:qa_bytes=src.read()
qa_parsed=None;qa_parse_error=None
try:qa_parsed=json.loads(qa_bytes) if qa_bytes else None
except (json.JSONDecodeError,UnicodeDecodeError) as error:qa_parse_error=type(error).__name__
def raw(value):
 if value is None:return None
 return {'base64':base64.b64encode(value).decode('ascii'),'bytes':len(value),'sha256':hashlib.sha256(value).hexdigest()}
qa={'result':'GENUINE_LINUX_CLASSIFIER39_PASS' if qa_rc==0 and hashlib.sha256(qa_bytes).hexdigest()=='4b5ad6d4c8ae395f7520e54d2d48dbb59192dcc9de7228b15a5f7b8dfe3bcd81' else 'FAILED_CLASSIFIER_LINUX_QUALIFICATION','utc_ns':time.time_ns(),'platform':sys.platform,'python_executable':sys.executable,'boottime_ns':time.clock_gettime_ns(time.CLOCK_BOOTTIME),'candidate_sha256':hashlib.sha256((q/'qualification_owner_995.py').read_bytes()).hexdigest(),'fixture_result_sha256':hashlib.sha256(qa_bytes).hexdigest(),'fixture_result_raw':raw(qa_bytes),'case_count':qa_parsed.get('case_count') if isinstance(qa_parsed,dict) else None,'fixture_parse_error':qa_parse_error,'returncode':qa_rc,'error_type':qa_error,'stdout_raw':raw(qa_stdout),'stderr_raw':raw(qa_stderr),'real_capture_duration_credit_ns':0,'production_or_raw_actions':False}
c.publish('linux-classifier-qa.json',qa)
assert sys.platform=='linux' and qa['result']=='GENUINE_LINUX_CLASSIFIER39_PASS' and qa['case_count']==39,qa
body={'result':'EXCLUSIVE_N2_CONTROLS_COPIED_NOT_ARMED','utc_ns':time.time_ns(),'scope':str(q),'fresh_active_inventory':fresh_inventory,'physical':physical,'scope_stat':[q.stat().st_uid,q.stat().st_gid,stat.S_IMODE(q.stat().st_mode)],'tmp_stat':[tmp.stat().st_uid,tmp.stat().st_gid,stat.S_IMODE(tmp.stat().st_mode)],'admission':admission,'census':cs,'recorder':c.stopped(),'owner_predecessor':c.predecessor(),'linux_classifier_qualification':qa,'linux_binding_smoke':json.loads(c.run([CANONICAL,'-c',SMOKE,str(q)],120)),'formal_credit_seconds':0}
c.publish('software-copy.json',body);print(json.dumps(body))
'''.replace('PAYLOAD', repr(encoded)).replace('CANONICAL',repr(CANONICAL)).replace('SMOKE',repr(SMOKE))
    elif mode in {'arm', 'launch'}:
        ph = {n: sha(data) for n, data in payload.items()}
        body = '''
assert q.resolve(strict=True)==q
account=pwd.getpwnam('bmdr')
assert (q.stat().st_uid,q.stat().st_gid,stat.S_IMODE(q.stat().st_mode))==(0,account.pw_gid,0o750)
assert (q/'tmp').resolve(strict=True)==q/'tmp'
assert ((q/'tmp').stat().st_uid,(q/'tmp').stat().st_gid,stat.S_IMODE((q/'tmp').stat().st_mode))==(account.pw_uid,account.pw_gid,0o770)
for n,h in PAYLOAD_HASHES.items():
 f=q/n;s=f.lstat()
 assert stat.S_ISREG(s.st_mode) and not f.is_symlink() and hashlib.sha256(f.read_bytes()).hexdigest()==h,n
 assert (s.st_uid,s.st_gid,stat.S_IMODE(s.st_mode))==(0,account.pw_gid,0o640),n
sys.dont_write_bytecode=True;sys.path.insert(0,str(q));import common as c
c.exact();assert c.stopped()['NRestarts']=='0';fresh_inventory=fresh_active_inventory()
assert c.read(q/'copy-admission.json')['result']=='ADMITTED'
'''.replace('PAYLOAD_HASHES', repr(ph))
        if mode == 'arm':
            unit = UNIT_PREFIX + 'arm.service'
            args = ['systemd-run', '--no-block', '--unit', unit,
                    '--property=Type=oneshot', '--property=TimeoutStartSec=2400',
                    '--property=TimeoutStopSec=1500', '--property=UMask=0027',
                    '--property=CPUAccounting=yes', '--property=MemoryAccounting=yes',
                    '--property=ExecStopPost=' + SYSTEM + ' ' + q + '/restore.py capture-failure',
                    '--setenv=TMPDIR=' + q + '/tmp', '--setenv=SQLITE_TMPDIR=' + q + '/tmp',
                    SYSTEM, q + '/pin-unit-exec.py', CANONICAL, q + '/arm-natural-wait.py']
            body += '''
assert not (q/'quiet-window-actual.json').exists() and not (q/'owner-start.json').exists()
assert c.state(ARM_UNIT)['LoadState']=='not-found'
assert not (q/'arm-native-attempt.json').exists()
cs=c.census();assert tuple(cs['counts'])==c.EXPECTED_STOPPED_COUNTS and cs['backlog']==cs['transaction_backlog']==0,cs
c.admission(c.TARGET_SECONDS+2160,c.TARGET_SECONDS+60,1200)
'''.replace('ARM_UNIT', repr(unit))
        else:
            args = [CANONICAL, q + '/launch.py']
            body += '''
assert c.read(q/'quiet-window-actual.json')['result']=='ACTIVE_VERIFIED'
assert c.read(q/'postarm-admission.json')['result']=='ADMITTED'
assert c.read(q/'canonical-verification.json')['status']=='VERIFIED'
assert not any((q/n).exists() for n in ['capture-launch.json','owner-start.json','stage-launch-attempt.json'])
assert c.state(c.CAPTURE)['LoadState']=='not-found' and c.state(c.TERMINAL)['LoadState']=='not-found'
c.check_window();c.admission(c.TARGET_SECONDS+2160,c.TARGET_SECONDS+60,1200)
'''
        body += '''
a=ARGV
attempt='arm-native-attempt.json' if a[0]=='systemd-run' else 'stage-launch-attempt.json'
c.publish(attempt,{'utc_ns':time.time_ns(),'argv':a,'status':'ATTEMPT_NOT_PASS'})
r=subprocess.run(a,capture_output=True,text=True,timeout=120)
result={'argv':a,'rc':r.returncode,'stdout':r.stdout,'stderr':r.stderr,'utc_ns':time.time_ns(),'fresh_active_inventory':fresh_inventory,'recorder':c.state(c.REC),'capture':c.state(c.CAPTURE)}
if (q/'capture-launch.json').exists():result['capture_launch']=c.read(q/'capture-launch.json')
unit=ARM_UNIT if a[0]=='systemd-run' else c.CAPTURE
result['effective_unit']=subprocess.check_output(['systemctl','show',unit,'--all','-p',PROPERTIES],text=True,timeout=15)
print(json.dumps(result));r.check_returncode()
'''.replace('ARGV', repr(args)).replace('ARM_UNIT', repr(UNIT_PREFIX + 'arm.service')).replace('PROPERTIES', repr(unit_properties()))
    else:
        fixed = ['software-copy.json','linux-classifier-qa.json','parent-tests.json','copy-admission.json','stage-controls-manifest.json','stage-design-evidence.json',
                 'canonical-verification.json','prearm-admission.json','quiet-window-actual.json','postarm-admission.json',
                 'kernel-update-pending.json','launch-admission.json','capture-launch.json','capture-admission.json',
                 'warmup-result.json','warmup-end-observation.json','prestart-admission.json','owner-start.json',
                 'start-observation.json','owner-target.json','target-observation.json','minimum-duration-observation.json',
                 'finalization-timing-failure.json','native-result.json','capture-failure.json','terminal-failure.json',
                 'restoration.jsonl','expiry-cancellation.jsonl','terminal-start.json','monitor-rate.json',
                 'terminal-freeze.json','terminal-admission.json','terminal-launch.json','terminal-audit-result.json',
                 'capacity-policy.json','arm-native-attempt.json','stage-launch-attempt.json','data-scope-start.json',
                 'terminal-audit/owner-scope-receipt.json','terminal-audit/quiescence-corpus.json',
                 'terminal-audit/baseline-preflight.json','terminal-audit/audit-root.json','terminal-audit/owner-selected-result.json',
                 'warmup.jsonl','monitoring.jsonl','recovery-observations.jsonl','target-finalization-observations.jsonl']
        units = [UNIT_PREFIX + n for n in ['arm.service','capture.service','terminal.service','quiet-expiry.timer','quiet-expiry.service']]+[REC, AT, AS]
        body = '''
result={'utc_ns':time.time_ns(),'scope':str(q),'boot_id':Path('/proc/sys/kernel/random/boot_id').read_text().strip(),'scope_exists':q.exists(),'files':{},'states':{},'unit_journals':{}}
def record(f):
 assert f.resolve(strict=True).is_relative_to(q) and f.is_file() and not f.is_symlink()
 data=f.read_bytes();s=f.lstat();name=str(f.relative_to(q))
 result['files'][name]={'sha256':hashlib.sha256(data).hexdigest(),'bytes':len(data),'content':data.decode(errors='replace'),'base64':base64.b64encode(data).decode('ascii'),'uid':s.st_uid,'gid':s.st_gid,'mode':stat.S_IMODE(s.st_mode),'mtime_ns':s.st_mtime_ns}
 if f.suffix=='.state':result['files'][name]['base64']=base64.b64encode(data).decode('ascii')
if q.exists():
 assert q.resolve(strict=True)==q and q.stat().st_uid==0
 for n in FIXED:
  f=q/n
  if f.exists():record(f)
 for pattern in ['diagnostic-*.json','diagnostic-*.state','critical-*.json','notification-*.json']:
  for f in sorted(q.glob(pattern)):record(f)
 audit=q/'terminal-audit'
 if audit.exists():
  assert audit.resolve(strict=True)==audit
  entries=list(audit.iterdir())
  result['audit_top_level_inventory']=[{'name':f.name,'bytes':f.lstat().st_size,'mtime_ns':f.lstat().st_mtime_ns,'is_file':f.is_file()} for f in sorted(entries)]
  for prefix in ['freeze-progress-','audit-progress-','audit-interruption-']:
   files=sorted(f for f in entries if f.name.startswith(prefix) and f.suffix=='.json' and f.is_file())
   result[prefix+'count']=len(files)
   for f in files[-6:]:record(f)
origin_ns=None;result['parse_errors']={}
result['missing_fixed_controls']=[n for n in FIXED if not (q/n).exists()]
for name in ['linux-classifier-qa.json','parent-tests.json']:
 f=q/name
 if f.exists():
  try:json.loads(f.read_bytes())
  except (ValueError,UnicodeError) as error:result['parse_errors'][name]=type(error).__name__
for name in ['copy-admission.json','arm-native-attempt.json','stage-launch-attempt.json','capture-launch.json','owner-start.json']:
 f=q/name
 if f.exists():
  try:
   value=json.loads(f.read_bytes());stamp=value.get('utc_ns',value.get('t0_utc_ns'))
   if type(stamp) is int and stamp>0:origin_ns=stamp if origin_ns is None else min(origin_ns,stamp)
  except (ValueError,UnicodeError,AttributeError) as error:result['parse_errors'][name]=type(error).__name__
result['journal_origin_utc_ns']=origin_ns
for u in UNITS:
 result['states'][u]=subprocess.check_output(['systemctl','show',u,'--all','-p',PROPERTIES],text=True,timeout=15)
 if origin_ns is not None:result['unit_journals'][u]=subprocess.check_output(['journalctl','--all','--no-pager','-u',u,'--since','@'+str(origin_ns//10**9-5),'-o','json'],text=True,timeout=45)
start=q/'owner-start.json'
if start.exists():
 try:
  s=json.loads(start.read_bytes());inv=s.get('recorder_invocation')
  if isinstance(inv,str) and len(inv)==32 and all(c in '0123456789abcdef' for c in inv):
   result['recorder_owned_journal']=subprocess.check_output(['journalctl','--all','--no-pager','_SYSTEMD_INVOCATION_ID='+inv,'-o','json'],text=True,timeout=45)
 except (ValueError,UnicodeError,AttributeError) as error:result['parse_errors']['owner-start.json']=type(error).__name__
for label,u in [('capture',CAPTURE),('terminal',TERMINAL),('recorder',RECORDER)]:
 cg=Path('/sys/fs/cgroup/system.slice')/u
 if cg.is_dir():result[label+'_cgroup']={n:(cg/n).read_text() for n in ['memory.current','memory.peak','memory.swap.current','memory.swap.peak','cpu.stat','cgroup.procs'] if (cg/n).is_file()}
result['host_meminfo']=Path('/proc/meminfo').read_text()
print(json.dumps(result))
'''.replace('FIXED', repr(fixed)).replace('UNITS', repr(units)).replace('PROPERTIES', repr(unit_properties())).replace('CAPTURE', repr(UNIT_PREFIX+'capture.service')).replace('TERMINAL', repr(UNIT_PREFIX+'terminal.service')).replace('RECORDER', repr(REC))
    code = head + body
    compile(code, '<n2-stage-' + mode + '>', 'exec')
    return code


def unit_properties():
    return 'LoadState,ActiveState,SubState,UnitFileState,MainPID,NRestarts,InvocationID,Result,Type,RuntimeMaxUSec,TimeoutStartUSec,TimeoutStopUSec,ExecStart,ExecStopPost,KillMode,UMask,CPUAccounting,MemoryAccounting,CPUUsageNSec,MemoryCurrent,MemoryPeak,MemorySwapCurrent,MemorySwapPeak,NextElapseUSecRealtime,NextElapseUSecMonotonic,ControlGroup'


def main():
    assert len(sys.argv) == 2
    mode = sys.argv[1]
    code = build_remote(mode)
    # Package-dependent copy/admission/arm/launch uses the installed canonical
    # interpreter. The remote restore ExecStopPost deliberately stays system Python.
    out = P / ('actual-n2-stage-' + mode + '-' + str(time.time_ns()) + '.json')
    error = None
    try:
        r = subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=10','greencloud-tokyo-01',CANONICAL,'-'], input=code.encode(), capture_output=True, timeout=600 if mode=='read' else 180)
        stdout,stderr,returncode = r.stdout,r.stderr,r.returncode
    except (subprocess.TimeoutExpired,OSError) as caught:
        error=caught;stdout,stderr=getattr(caught,'stdout',None),getattr(caught,'stderr',None);returncode=None
    for f,data in [(out,stdout),(out.with_suffix('.stderr'),stderr)]:
        raw=data if isinstance(data,bytes) else (data or '').encode()
        with f.open('xb') as dest:dest.write(raw);dest.flush();__import__('os').fsync(dest.fileno())
    with out.with_suffix('.execution.json').open('x') as dest:
        json.dump({'returncode':returncode,'error_type':type(error).__name__ if error else None,'operator_sha256':sha(Path(__file__).read_bytes()),'remote_sha256':sha(code.encode()),'remote_retry':False},dest,sort_keys=True);dest.write('\n');dest.flush();__import__('os').fsync(dest.fileno())
    print(str(out),sha(out.read_bytes()),flush=True)
    if error is not None:raise error
    assert returncode==0,'Original stdout/stderr preserved; inspect actual outcome, do not retry a mutation blindly'
    d = json.loads(stdout)
    print(json.dumps({k:d[k] for k in ['result','scope','rc','stdout','stderr'] if k in d}))


if __name__ == '__main__':
    main()
