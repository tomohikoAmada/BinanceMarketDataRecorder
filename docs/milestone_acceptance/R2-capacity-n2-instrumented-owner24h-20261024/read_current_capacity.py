"""One metadata-only stopped preflight for the fresh instrumented N2 design.

No Raw/manifest payload is opened, no deployment verify/capture/OS action occurs.
This reads current facts; it cannot admit or launch the proposed formal stage.
"""
from pathlib import Path
import hashlib
import json
import subprocess
import time

P = Path(__file__).resolve().parent
REMOTE = r'''
from pathlib import Path
import fcntl,hashlib,json,os,sqlite3,subprocess,time
D=Path('/var/lib/binance-market-data-recorder')
A=Path('/srv/recorder-data/recorder-archive')
Q=Path('/srv/recorder-data/recorder-archive/evidence/R2-capacity-n2-instrumented-20261024-4r2aauoj')
SOURCE='20e57f30682e88b4090cc2f0da34aa26fca4de2e'
IDENTITY='9b8ce8219105fd7962cbf4831a99c9764c3c2889088bda7e73c834ff6bffc715'
CONFIG='5b73db1b6ba6ac8cc7ab84cbcf0c7b688187b316644bef3df44bbb42f9283419'
DEPLOY=Path('/srv/recorder-data/recorder-archive/evidence/R2-stream-close-release-20261024-l2hr7wd9/deployment-result.json')
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def run(argv):
 r=subprocess.run(argv,capture_output=True,text=True,timeout=30)
 return {'argv':argv,'returncode':r.returncode,'stdout':r.stdout,'stderr':r.stderr}
def state(unit):
 r=run(['systemctl','show',unit,'-p','LoadState,ActiveState,SubState,UnitFileState,MainPID,NRestarts,InvocationID,Result,TimeoutStopUSec'])
 assert r['returncode']==0,r
 return dict(s.split('=',1) for s in r['stdout'].splitlines())
assert os.geteuid()==0 and not Q.exists() and not Q.is_symlink()
lock=os.open(D/'state/service.lock',os.O_RDONLY|os.O_NOFOLLOW)
fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
s=os.fstat(lock);assert (s.st_dev,s.st_ino,s.st_uid,s.st_gid,s.st_mode&0o777)==(64769,870332,999,988,0o600)
boot=Path('/proc/sys/kernel/random/boot_id').read_text().strip()
assert boot=='59bb1735-ac48-408d-b3ec-bda79fc49b41'
identity_path=Path('/etc/binance-market-data-recorder/deployment-identity.json')
assert sha(identity_path)==IDENTITY
identity=json.loads(identity_path.read_bytes());assert identity['source_git_sha']==SOURCE
assert sha(Path('/etc/binance-market-data-recorder/recorder.toml'))==CONFIG
assert sha(DEPLOY)=='e148f51a26aea4d0da81d96d9700900f3f4f171e8b1c0e1f8d9aa6c03f859a7f'
rec=state('binance-market-data-recorder.service')
assert rec['ActiveState']=='inactive' and rec['UnitFileState']=='disabled' and rec['MainPID']=='0' and rec['NRestarts']=='0',rec
# Only the completed new2h metadata window supplies detailed load rows. These
# are forecast observations, never a selected Raw correctness denominator.
start=1792804582614558469;end=1792811808863586663
db=sqlite3.connect('file:'+str(D/'state/catalog.sqlite')+'?mode=ro',uri=True,timeout=5)
db.execute('PRAGMA query_only=ON');db.execute('BEGIN')
counts=db.execute('SELECT count(*),sum(record_count),sum(stored_bytes),sum(uncompressed_bytes),max(uncompressed_bytes) FROM chunks').fetchone()
backlog=db.execute("SELECT count(*) FROM chunks WHERE state!='LOCAL_DELETED'").fetchone()[0]
tx=db.execute("SELECT count(*) FROM archive_transactions WHERE state!='LOCAL_DELETED'").fetchone()[0]
high={'chunk':db.execute('SELECT COALESCE(MAX(transition_id),0) FROM chunk_transitions').fetchone()[0],
 'archive':db.execute('SELECT COALESCE(MAX(event_id),0) FROM archive_transaction_events').fetchone()[0],
 'operational':db.execute('SELECT COALESCE(MAX(event_seq),0) FROM operational_event_sequence').fetchone()[0]}
columns=['chunk_id','state','sealed_path','record_count','uncompressed_bytes','stored_bytes','created_at_utc_ns']
rows=[dict(zip(columns,r)) for r in db.execute('SELECT '+','.join(columns)+' FROM chunks WHERE created_at_utc_ns>=? AND created_at_utc_ns<=? ORDER BY created_at_utc_ns,chunk_id',(start,end))]
db.close()
files=[p for p in D.rglob('*') if p.is_file()]
metadata_bytes=sum(p.stat().st_size for p in files)
free={name:os.statvfs(path).f_bavail*os.statvfs(path).f_frsize for name,path in [('writer',D),('archive',A)]}
memory={s.split(':')[0]:int(s.split()[1])*1024 for s in Path('/proc/meminfo').read_text().splitlines()}
markers={}
for name in ['/run/reboot-required','/run/reboot-required.pkgs','/run/systemd/shutdown/scheduled']:
 p=Path(name)
 markers[name]={'exists':p.exists()}
 if p.exists():
  s=p.lstat();markers[name].update(sha256=sha(p),text=p.read_text(),uid=s.st_uid,gid=s.st_gid,mode=s.st_mode&0o777,mtime_ns=s.st_mtime_ns)
original={u:state(u) for u in ['apt-daily.timer','apt-daily-upgrade.timer','apt-daily.service','apt-daily-upgrade.service','unattended-upgrades.service','binance-market-data-archive.timer','binance-market-data-archive.service']}
result={'result':'CURRENT_N2_METADATA_PREFLIGHT_NOT_ADMISSION','utc_ns':time.time_ns(),'boottime_ns':time.clock_gettime_ns(time.CLOCK_BOOTTIME),'boot':boot,
 'source':SOURCE,'identity_sha256':IDENTITY,'config_sha256':CONFIG,'deployment_result_sha256':sha(DEPLOY),'proposed_scope_absent':True,
 'counts':counts,'backlog':backlog,'transaction_backlog':tx,'high_water':high,'metadata_bytes':metadata_bytes,'catalog_bytes':(D/'state/catalog.sqlite').stat().st_size,
 'devices':{'writer':D.stat().st_dev,'archive':A.stat().st_dev},'free':free,'host_available':memory['MemAvailable'],'host_swap_bytes':memory['SwapTotal']-memory['SwapFree'],
 'recorder':rec,'current_owners':original,'diagnostic_metadata_window':{'start_utc_ns':start,'end_utc_ns':end,'columns':columns,'rows':rows,'qualification_credit':False},
 'kernel':run(['uname','-r']),'reboot_markers':markers,'auto_reboot_config':run(['apt-config','shell','R2_AUTO_REBOOT','Unattended-Upgrade::Automatic-Reboot']),
 'package_audit':run(['dpkg','--audit']),'pending_package_versions':run(['dpkg-query','-W','-f=${Package} ${Version} ${Status}\n','linux-base','linux-image-6.8.0-147-generic']),
 'ntp':run(['timedatectl','show','-p','NTPSynchronized','--value']),
 'formal_credit_seconds':0,'read_only_metadata':True,'raw_or_manifest_payload_opened':False,'new_scope_created':False}
print(json.dumps(result,sort_keys=True))
'''

def main():
    compile(REMOTE, '<metadata-only-N2-preflight>', 'exec')
    stamp = str(time.time_ns())
    error = None
    try:
        result = subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=15',
            'greencloud-tokyo-01','/usr/bin/python3.12 -'],input=REMOTE.encode(),
            capture_output=True,timeout=180)
        stdout,stderr,returncode=result.stdout,result.stderr,result.returncode
    except (subprocess.TimeoutExpired,OSError) as caught:
        error=caught
        stdout,stderr=getattr(caught,'stdout',None),getattr(caught,'stderr',None)
        returncode=None
    for name,data in [('stdout',stdout),('stderr',stderr)]:
        data=data if isinstance(data,bytes) else (data or '').encode()
        with (P/('preflight-'+stamp+'-'+name+'.txt')).open('xb') as output:
            output.write(data)
    with (P/('preflight-'+stamp+'-execution.json')).open('x') as output:
        json.dump({'returncode':returncode,'error_type':type(error).__name__ if error else None,
            'helper_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            'remote_sha256':hashlib.sha256(REMOTE.encode()).hexdigest(),
            'formal_credit_seconds':0,'remote_retry':False},output,sort_keys=True)
        output.write('\n')
    if error is not None:
        raise error
    assert returncode==0,'Original preflight stdout/stderr retained; no remote scope or capture created'
    value=json.loads(stdout)
    target=P/('actual-current-preflight-'+stamp+'.json')
    with target.open('x') as output:
        json.dump(value,output,sort_keys=True);output.write('\n')
    print(json.dumps({'path':str(target),'sha256':hashlib.sha256(target.read_bytes()).hexdigest(),
        'result':value['result'],'utc_ns':value['utc_ns'],'formal_credit_seconds':0}))

if __name__=='__main__':
    main()
