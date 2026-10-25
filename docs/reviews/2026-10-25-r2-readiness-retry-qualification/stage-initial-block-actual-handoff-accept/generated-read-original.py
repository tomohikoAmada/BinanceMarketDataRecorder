from pathlib import Path
import os,sys,json,hashlib,pwd,stat,time,subprocess,base64
q=Path('/srv/recorder-data/recorder-archive/evidence/R2-capacity-n2-readiness-retry-20261025-1s0kswlt');members={'qualification_scoped_corpus.py': '2ce6d5815e9e6fe507382d7f0d143e2811062de9e0fb5af7ce6d731f8fc1501d', 'controller.py': '3b3f8d50338c82c594cb346eb49686adb235f2d7c5d3ce13e966bf3cc4cd98f0', 'qualification_owner_995.py': '621063b04ba21103206282519fa4b14731e599118a05f5a132c11b72abf3cdd2', 'scoped-terminal.py': '2350e186b34676e45af0125831d737172ab3d2058fcdff52e1a43efc730857f6', 'qualification_core_recovery.py': '656b2ddba6505fa77555c5edb1d2599c7f1a7f20f084cc00ca225c392b4fcec8', 'arm.py': '5c19aab6f043559b31dcd31154ddfbfbbb805658cfd6f0d14d4aeb6d94b72079', 'launch.py': 'a5e4689181be9277f4a459665e6d28d7552747a717689fae044f1c7d80cb9c21', 'common.py': '54afe94976211634b2a0e1d74c0f3651c03d4e1676cb0a1798f88fe732a5c526', 'capacity-policy.json': 'db8566b8d9bd960e61e5309ccf96d7ad14635f2db940bc3b530f4a7c17b7ccf7', 'pin-unit-exec.py': 'ebe6da40c6b7cd12da9419200d835ea5069f956e33ff1bb8c0c9d1e5a5b0be3d', 'arm-natural-wait.py': '9b43657f66bcdbb2d20ca0d92b74f1b0c729457fc48ad60b25f492fdb0f198f2', 'qualification_monitor_rate.py': '07a0a4f31740fe09bf65c5410b95206c48de27207c687fe7a24ccb69329f3eb4', 'qualification_auxiliary_gate.py': '79435a68408df7a1bf7fb9fd2541251bb62fd8cb767242c89aaea07051f3a594', 'restore.py': 'cdef4bcc8dc936dccb851abec368538528e1736453c5beb7b8495a3037233b3d'}
assert os.geteuid()==0
os.umask(0o027)

result={'utc_ns':time.time_ns(),'scope':str(q),'boot_id':Path('/proc/sys/kernel/random/boot_id').read_text().strip(),'scope_exists':q.exists(),'files':{},'states':{},'unit_journals':{}}
def record(f):
 assert f.resolve(strict=True).is_relative_to(q) and f.is_file() and not f.is_symlink()
 data=f.read_bytes();s=f.lstat();name=str(f.relative_to(q))
 result['files'][name]={'sha256':hashlib.sha256(data).hexdigest(),'bytes':len(data),'content':data.decode(errors='replace'),'base64':base64.b64encode(data).decode('ascii'),'uid':s.st_uid,'gid':s.st_gid,'mode':stat.S_IMODE(s.st_mode),'mtime_ns':s.st_mtime_ns}
 if f.suffix=='.state':result['files'][name]['base64']=base64.b64encode(data).decode('ascii')
if q.exists():
 assert q.resolve(strict=True)==q and q.stat().st_uid==0
 for n in ['software-copy.json', 'copy-admission.json', 'stage-controls-manifest.json', 'stage-design-evidence.json', 'canonical-verification.json', 'prearm-admission.json', 'quiet-window-actual.json', 'postarm-admission.json', 'kernel-update-pending.json', 'launch-admission.json', 'capture-launch.json', 'capture-admission.json', 'warmup-result.json', 'warmup-end-observation.json', 'prestart-admission.json', 'owner-start.json', 'start-observation.json', 'owner-target.json', 'target-observation.json', 'minimum-duration-observation.json', 'finalization-timing-failure.json', 'native-result.json', 'capture-failure.json', 'terminal-failure.json', 'restoration.jsonl', 'expiry-cancellation.jsonl', 'terminal-start.json', 'monitor-rate.json', 'terminal-freeze.json', 'terminal-admission.json', 'terminal-launch.json', 'terminal-audit-result.json', 'capacity-policy.json', 'arm-native-attempt.json', 'stage-launch-attempt.json', 'data-scope-start.json', 'terminal-audit/owner-scope-receipt.json', 'terminal-audit/quiescence-corpus.json', 'terminal-audit/baseline-preflight.json', 'terminal-audit/audit-root.json', 'terminal-audit/owner-selected-result.json', 'warmup.jsonl', 'monitoring.jsonl', 'recovery-observations.jsonl', 'target-finalization-observations.jsonl']:
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
for name in ['copy-admission.json','arm-native-attempt.json','stage-launch-attempt.json','capture-launch.json','owner-start.json']:
 f=q/name
 if f.exists():
  try:
   value=json.loads(f.read_bytes());stamp=value.get('utc_ns',value.get('t0_utc_ns'))
   if type(stamp) is int and stamp>0:origin_ns=stamp if origin_ns is None else min(origin_ns,stamp)
  except (ValueError,UnicodeError,AttributeError) as error:result['parse_errors'][name]=type(error).__name__
result['journal_origin_utc_ns']=origin_ns
for u in ['binance-recorder-r2-capacity-n2-readiness-retry-20261025-1s0kswlt-arm.service', 'binance-recorder-r2-capacity-n2-readiness-retry-20261025-1s0kswlt-capture.service', 'binance-recorder-r2-capacity-n2-readiness-retry-20261025-1s0kswlt-terminal.service', 'binance-recorder-r2-capacity-n2-readiness-retry-20261025-1s0kswlt-quiet-expiry.timer', 'binance-recorder-r2-capacity-n2-readiness-retry-20261025-1s0kswlt-quiet-expiry.service', 'binance-market-data-recorder.service', 'binance-market-data-archive.timer', 'binance-market-data-archive.service']:
 result['states'][u]=subprocess.check_output(['systemctl','show',u,'--all','-p','LoadState,ActiveState,SubState,UnitFileState,MainPID,NRestarts,InvocationID,Result,Type,RuntimeMaxUSec,TimeoutStartUSec,TimeoutStopUSec,ExecStart,ExecStopPost,KillMode,UMask,CPUAccounting,MemoryAccounting,CPUUsageNSec,MemoryCurrent,MemoryPeak,MemorySwapCurrent,MemorySwapPeak,NextElapseUSecRealtime,NextElapseUSecMonotonic,ControlGroup'],text=True,timeout=15)
 if origin_ns is not None:result['unit_journals'][u]=subprocess.check_output(['journalctl','--all','--no-pager','-u',u,'--since','@'+str(origin_ns//10**9-5),'-o','json'],text=True,timeout=45)
start=q/'owner-start.json'
if start.exists():
 try:
  s=json.loads(start.read_bytes());inv=s.get('recorder_invocation')
  if isinstance(inv,str) and len(inv)==32 and all(c in '0123456789abcdef' for c in inv):
   result['recorder_owned_journal']=subprocess.check_output(['journalctl','--all','--no-pager','_SYSTEMD_INVOCATION_ID='+inv,'-o','json'],text=True,timeout=45)
 except (ValueError,UnicodeError,AttributeError) as error:result['parse_errors']['owner-start.json']=type(error).__name__
for label,u in [('capture','binance-recorder-r2-capacity-n2-readiness-retry-20261025-1s0kswlt-capture.service'),('terminal','binance-recorder-r2-capacity-n2-readiness-retry-20261025-1s0kswlt-terminal.service'),('recorder','binance-market-data-recorder.service')]:
 cg=Path('/sys/fs/cgroup/system.slice')/u
 if cg.is_dir():result[label+'_cgroup']={n:(cg/n).read_text() for n in ['memory.current','memory.peak','memory.swap.current','memory.swap.peak','cpu.stat','cgroup.procs'] if (cg/n).is_file()}
result['host_meminfo']=Path('/proc/meminfo').read_text()
print(json.dumps(result))
