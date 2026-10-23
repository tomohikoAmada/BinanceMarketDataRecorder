import sys
sys.dont_write_bytecode=True
from pathlib import Path
import json,hashlib,shutil,time
O=Path(__file__).parent;P=Path('/private/var/tmp/bmdr-r2-report-isolation-20261020-yf61hyp0/final-capacity-n2-complete-controls-20261024/controls');R=O/'strict-control-copy'
if not R.exists():shutil.copytree(P/'audit',R)
source=Path('/Users/amada/.codex/worktrees/r2-report-isolation/BinanceMarketDataRecorder/src');sys.path.insert(0,str(source))
metadata_helpers=[]
def audit(event,args):
 if event=='subprocess.Popen' and Path(args[0]).name=='file' and list(args[1])[:2]==['file','-b'] and len(args[1])==3 and Path(args[1][2]).resolve()==Path(sys.executable).resolve():metadata_helpers.append(list(args[1]));return
 if event in ['socket.connect','socket.bind','subprocess.Popen','os.system']:raise RuntimeError('offline verifier forbidden '+event)
 if event=='open' and isinstance(args[0],(str,bytes)):
  name=str(args[0]);
  if name.startswith(('/srv/','/var/lib/binance-market-data-recorder','/etc/binance-market-data-recorder')) or name.endswith(('.zst','.raw','.partial')):raise RuntimeError('forbidden Raw/production open '+name)
sys.addaudithook(audit)
from binance_market_data_recorder.service.deployment_identity import DeploymentIdentity
from binance_market_data_recorder.service.acceptance_v5_finalize import verify_audit
calls=[]
def no_raw(*a,**k):calls.append(True);raise RuntimeError('Raw execution forbidden')
identity=DeploymentIdentity.from_document(json.loads((R/'quiescence-corpus.json').read_bytes())['deployment_identity']);beg=time.monotonic();result,digest=verify_audit(root=R,identity=identity,archive_roots={},historical_control_only=True,raw_unit=no_raw)
assert not calls and digest=='7011d408f3d72274e9bfae0fba2f6dd913b720434a5189874e418830bccd0265' and result['result']=='PASS_CANDIDATE' and result['blocking_findings']==[]
files={str(p.relative_to(source)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted((source/'binance_market_data_recorder').rglob('*.py'))}
(O/'STRICT_CONTROL_SOURCE_HASHES.json').write_text(json.dumps(files,sort_keys=True,indent=2)+'\n')
out={'result':'ACCEPT_COMPLETE_STRICT_IMMUTABLE_CONTROL_REPLAY','audit_sha256':digest,'elapsed_seconds':time.monotonic()-beg,'raw_unit_calls':len(calls),'historical_control_only':True,'copied_evidence_root':str(R),'no_original_evidence_writes':True,'process_network_production_path_guard':True,'counts_by_family':result['counts_by_family'],'blocking_findings':result['blocking_findings'],'source_module_count':len(files),'permitted_local_metadata_helpers':metadata_helpers};(O/'STRICT_CONTROL_VERIFY.json').write_text(json.dumps(out,sort_keys=True,indent=2)+'\n');print(json.dumps(out),flush=True)
