from pathlib import Path
import os,sys,json,hashlib,pwd,stat,time,subprocess,base64
q=Path('/srv/recorder-data/recorder-archive/evidence/R2-capacity-n2-readiness-retry-20261025-1s0kswlt');members={'qualification_scoped_corpus.py': '2ce6d5815e9e6fe507382d7f0d143e2811062de9e0fb5af7ce6d731f8fc1501d', 'controller.py': '3b3f8d50338c82c594cb346eb49686adb235f2d7c5d3ce13e966bf3cc4cd98f0', 'qualification_owner_995.py': '621063b04ba21103206282519fa4b14731e599118a05f5a132c11b72abf3cdd2', 'scoped-terminal.py': '2350e186b34676e45af0125831d737172ab3d2058fcdff52e1a43efc730857f6', 'qualification_core_recovery.py': '656b2ddba6505fa77555c5edb1d2599c7f1a7f20f084cc00ca225c392b4fcec8', 'arm.py': '5c19aab6f043559b31dcd31154ddfbfbbb805658cfd6f0d14d4aeb6d94b72079', 'launch.py': 'a5e4689181be9277f4a459665e6d28d7552747a717689fae044f1c7d80cb9c21', 'common.py': '54afe94976211634b2a0e1d74c0f3651c03d4e1676cb0a1798f88fe732a5c526', 'capacity-policy.json': 'db8566b8d9bd960e61e5309ccf96d7ad14635f2db940bc3b530f4a7c17b7ccf7', 'pin-unit-exec.py': 'ebe6da40c6b7cd12da9419200d835ea5069f956e33ff1bb8c0c9d1e5a5b0be3d', 'arm-natural-wait.py': '9b43657f66bcdbb2d20ca0d92b74f1b0c729457fc48ad60b25f492fdb0f198f2', 'qualification_monitor_rate.py': '07a0a4f31740fe09bf65c5410b95206c48de27207c687fe7a24ccb69329f3eb4', 'qualification_auxiliary_gate.py': '79435a68408df7a1bf7fb9fd2541251bb62fd8cb767242c89aaea07051f3a594', 'restore.py': 'cdef4bcc8dc936dccb851abec368538528e1736453c5beb7b8495a3037233b3d'}
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

assert q.resolve(strict=True)==q
account=pwd.getpwnam('bmdr')
assert (q.stat().st_uid,q.stat().st_gid,stat.S_IMODE(q.stat().st_mode))==(0,account.pw_gid,0o750)
assert (q/'tmp').resolve(strict=True)==q/'tmp'
assert ((q/'tmp').stat().st_uid,(q/'tmp').stat().st_gid,stat.S_IMODE((q/'tmp').stat().st_mode))==(account.pw_uid,account.pw_gid,0o770)
for n,h in {'qualification_scoped_corpus.py': '2ce6d5815e9e6fe507382d7f0d143e2811062de9e0fb5af7ce6d731f8fc1501d', 'controller.py': '3b3f8d50338c82c594cb346eb49686adb235f2d7c5d3ce13e966bf3cc4cd98f0', 'qualification_owner_995.py': '621063b04ba21103206282519fa4b14731e599118a05f5a132c11b72abf3cdd2', 'scoped-terminal.py': '2350e186b34676e45af0125831d737172ab3d2058fcdff52e1a43efc730857f6', 'qualification_core_recovery.py': '656b2ddba6505fa77555c5edb1d2599c7f1a7f20f084cc00ca225c392b4fcec8', 'arm.py': '5c19aab6f043559b31dcd31154ddfbfbbb805658cfd6f0d14d4aeb6d94b72079', 'launch.py': 'a5e4689181be9277f4a459665e6d28d7552747a717689fae044f1c7d80cb9c21', 'common.py': '54afe94976211634b2a0e1d74c0f3651c03d4e1676cb0a1798f88fe732a5c526', 'capacity-policy.json': 'db8566b8d9bd960e61e5309ccf96d7ad14635f2db940bc3b530f4a7c17b7ccf7', 'pin-unit-exec.py': 'ebe6da40c6b7cd12da9419200d835ea5069f956e33ff1bb8c0c9d1e5a5b0be3d', 'arm-natural-wait.py': '9b43657f66bcdbb2d20ca0d92b74f1b0c729457fc48ad60b25f492fdb0f198f2', 'qualification_monitor_rate.py': '07a0a4f31740fe09bf65c5410b95206c48de27207c687fe7a24ccb69329f3eb4', 'qualification_auxiliary_gate.py': '79435a68408df7a1bf7fb9fd2541251bb62fd8cb767242c89aaea07051f3a594', 'restore.py': 'cdef4bcc8dc936dccb851abec368538528e1736453c5beb7b8495a3037233b3d'}.items():
 f=q/n;s=f.lstat()
 assert stat.S_ISREG(s.st_mode) and not f.is_symlink() and hashlib.sha256(f.read_bytes()).hexdigest()==h,n
 assert (s.st_uid,s.st_gid,stat.S_IMODE(s.st_mode))==(0,account.pw_gid,0o640),n
sys.dont_write_bytecode=True;sys.path.insert(0,str(q));import common as c
c.exact();assert c.stopped()['NRestarts']=='0';fresh_inventory=fresh_active_inventory()
assert c.read(q/'copy-admission.json')['result']=='ADMITTED'

assert not (q/'quiet-window-actual.json').exists() and not (q/'owner-start.json').exists()
assert c.state('binance-recorder-r2-capacity-n2-readiness-retry-20261025-1s0kswlt-arm.service')['LoadState']=='not-found'
assert not (q/'arm-native-attempt.json').exists()
cs=c.census();assert tuple(cs['counts'])==c.EXPECTED_STOPPED_COUNTS and cs['backlog']==cs['transaction_backlog']==0,cs
c.admission(c.TARGET_SECONDS+2160,c.TARGET_SECONDS+60,1200)

a=['systemd-run', '--no-block', '--unit', 'binance-recorder-r2-capacity-n2-readiness-retry-20261025-1s0kswlt-arm.service', '--property=Type=oneshot', '--property=TimeoutStartSec=2400', '--property=TimeoutStopSec=1500', '--property=UMask=0027', '--property=CPUAccounting=yes', '--property=MemoryAccounting=yes', '--property=ExecStopPost=/usr/bin/python3.12 /srv/recorder-data/recorder-archive/evidence/R2-capacity-n2-readiness-retry-20261025-1s0kswlt/restore.py capture-failure', '--setenv=TMPDIR=/srv/recorder-data/recorder-archive/evidence/R2-capacity-n2-readiness-retry-20261025-1s0kswlt/tmp', '--setenv=SQLITE_TMPDIR=/srv/recorder-data/recorder-archive/evidence/R2-capacity-n2-readiness-retry-20261025-1s0kswlt/tmp', '/usr/bin/python3.12', '/srv/recorder-data/recorder-archive/evidence/R2-capacity-n2-readiness-retry-20261025-1s0kswlt/pin-unit-exec.py', '/opt/binance-market-data-recorder/venv/bin/python', '/srv/recorder-data/recorder-archive/evidence/R2-capacity-n2-readiness-retry-20261025-1s0kswlt/arm-natural-wait.py']
attempt='arm-native-attempt.json' if a[0]=='systemd-run' else 'stage-launch-attempt.json'
c.publish(attempt,{'utc_ns':time.time_ns(),'argv':a,'status':'ATTEMPT_NOT_PASS'})
r=subprocess.run(a,capture_output=True,text=True,timeout=120)
result={'argv':a,'rc':r.returncode,'stdout':r.stdout,'stderr':r.stderr,'utc_ns':time.time_ns(),'fresh_active_inventory':fresh_inventory,'recorder':c.state(c.REC),'capture':c.state(c.CAPTURE)}
if (q/'capture-launch.json').exists():result['capture_launch']=c.read(q/'capture-launch.json')
unit='binance-recorder-r2-capacity-n2-readiness-retry-20261025-1s0kswlt-arm.service' if a[0]=='systemd-run' else c.CAPTURE
result['effective_unit']=subprocess.check_output(['systemctl','show',unit,'--all','-p','LoadState,ActiveState,SubState,UnitFileState,MainPID,NRestarts,InvocationID,Result,Type,RuntimeMaxUSec,TimeoutStartUSec,TimeoutStopUSec,ExecStart,ExecStopPost,KillMode,UMask,CPUAccounting,MemoryAccounting,CPUUsageNSec,MemoryCurrent,MemoryPeak,MemorySwapCurrent,MemorySwapPeak,NextElapseUSecRealtime,NextElapseUSecMonotonic,ControlGroup'],text=True,timeout=15)
print(json.dumps(result));r.check_returncode()
