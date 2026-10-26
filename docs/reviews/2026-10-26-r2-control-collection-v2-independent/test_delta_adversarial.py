"""Narrow V2 fd-chain/parsed-view probes; LOCAL temporary metadata only."""
from pathlib import Path
import base64,hashlib,json,os,sys,tempfile
from unittest.mock import patch
R=Path(__file__).resolve().parent;sys.path.insert(0,str(R/'run'));import control_files as c
CASES=[];(R/'delta-results').mkdir(exist_ok=True)
def record(name,result,raw=None):
 if raw is not None:
  assert base64.b64decode(result['raw']['base64'])==raw
  assert result['raw']['bytes']==len(raw) and result['raw']['sha256']==hashlib.sha256(raw).hexdigest()
 blob=json.dumps(result,allow_nan=False,sort_keys=True).encode();f=R/'delta-results'/(str(len(CASES)+1)+'.json');f.write_bytes(blob)
 CASES.append({'case':name,'PASS':True,'result_path':str(f.relative_to(R)),'result_bytes':len(blob),'result_sha256':hashlib.sha256(blob).hexdigest(),'strict_JSON_serializable':True})
for mode in ['absolute_ancestor_after_open','root_after_open','relative_parent_after_open']:
 with tempfile.TemporaryDirectory(prefix='collection-v2-fd-race-',dir='/private/var/tmp') as td:
  b=Path(td);ancestor=b/'ancestor';root=ancestor/'root';(root/'nested').mkdir(parents=True)
  foreign=b/'foreign';(foreign/'root/nested').mkdir(parents=True)
  original=b'{"origin":"TRUSTED"}';(root/'nested/control.json').write_bytes(original);(foreign/'root/nested/control.json').write_bytes(b'{"origin":"FOREIGN"}')
  once=[False];original_read=c.os.read
  def swap_after_read(fd,n):
   out=original_read(fd,n)
   if not once[0]:
    once[0]=True
    if mode=='absolute_ancestor_after_open':ancestor.rename(b/'kept-ancestor');ancestor.symlink_to(foreign,target_is_directory=True)
    elif mode=='root_after_open':root.rename(ancestor/'kept-root');root.symlink_to(foreign/'root',target_is_directory=True)
    else:(root/'nested').rename(root/'kept-nested');(root/'nested').symlink_to(foreign/'root/nested',target_is_directory=True)
   return out
  with patch.object(c.os,'read',swap_after_read):x=c.collect_file(root,'nested/control.json',allowed_names={'nested/control.json'},max_bytes=1024,timeout_seconds=1)
  assert x['custody_race'] and not x['read_complete'] and x['parsed']=={'origin':'TRUSTED'},x
  record(mode,x,original)
# A root ancestor substituted with an ordinary directory between stat and open
# must also fail identity validation before any file data is read.
with tempfile.TemporaryDirectory(prefix='collection-v2-root-open-race-',dir='/private/var/tmp') as td:
 b=Path(td);ancestor=b/'ancestor';root=ancestor/'root';root.mkdir(parents=True);(root/'control.json').write_bytes(b'{"origin":"TRUSTED"}')
 foreign=b/'foreign';(foreign/'root').mkdir(parents=True);(foreign/'root/control.json').write_bytes(b'{"origin":"FOREIGN"}')
 original_open=c.os.open;once=[False]
 def replace_before_open(path,flags,*a,**kw):
  if path=='ancestor' and kw.get('dir_fd') is not None and not once[0]:
   once[0]=True;ancestor.rename(b/'kept');foreign.rename(ancestor)
  return original_open(path,flags,*a,**kw)
 with patch.object(c.os,'open',replace_before_open):x=c.collect_file(root,'control.json',allowed_names={'control.json'},max_bytes=1024,timeout_seconds=1)
 assert once[0] and x['raw'] is None and x['primary_error'] and not x['read_complete'];record('ordinary ancestor replacement between stat/open rejects before data',x)
# Final entry replacement before open: the new fd never contributes FOREIGN data.
with tempfile.TemporaryDirectory(prefix='collection-v2-entry-open-race-',dir='/private/var/tmp') as td:
 root=Path(td);f=root/'control.json';f.write_bytes(b'{"origin":"TRUSTED"}');original_open=c.os.open;once=[False]
 def replace_entry(path,flags,*a,**kw):
  if path=='control.json' and kw.get('dir_fd') is not None and flags & os.O_NONBLOCK and not once[0]:
   once[0]=True;f.rename(root/'kept');f.write_bytes(b'{"origin":"FOREIGN"}')
  return original_open(path,flags,*a,**kw)
 with patch.object(c.os,'open',replace_entry):x=c.collect_file(root,'control.json',allowed_names={'control.json'},max_bytes=1024,timeout_seconds=1)
 assert once[0] and x['primary_error'] and not x['read_complete'] and base64.b64decode(x['raw']['base64'])==b'';record('entry replacement between stat/open rejects before bytes',x,b'')
with tempfile.TemporaryDirectory(prefix='collection-v2-depth-bound-',dir='/private/var/tmp') as td:
 root=Path(td)
 for depth,limit,accepted in [(1,1,True),(2,1,False),(64,64,True),(65,64,False),(256,256,True),(257,256,False)]:
  raw=b'['*depth+b'0'+b']'*depth;(root/'control.json').write_bytes(raw)
  x=c.collect_file(root,'control.json',allowed_names={'control.json'},max_bytes=100000,timeout_seconds=1,max_parse_depth=limit)
  assert x['read_complete'] and bool(not x['parse_errors'])==accepted
  record('parsed depth %s limit %s accepted %s'%(depth,limit,accepted),x,raw)
 raw=b'{"before":1}\n'+b'['*20000+b'0'+b']'*20000+b'\n{"after":3}\n';(root/'rows.jsonl').write_bytes(raw)
 x=c.collect_file(root,'rows.jsonl',allowed_names={'rows.jsonl'},max_bytes=100000,timeout_seconds=1)
 assert x['read_complete'] and x['parse_errors'][0]['line']==2 and x['parse_errors'][0]['byte_offset']==len(b'{"before":1}\n')
 assert x['parsed']==[{'line':1,'value':{'before':1}},{'line':3,'value':{'after':3}}];record('JSONL before and after deep resource error retained',x,raw)
 for limit in [True,0,257,1.5,None]:
  try:c.collect_file(root,'control.json',allowed_names={'control.json'},max_bytes=1024,timeout_seconds=1,max_parse_depth=limit)
  except ValueError:CASES.append({'case':'invalid depth parameter '+repr(limit),'PASS':True,'invalid_parameter_before_collection':True})
  else:raise AssertionError('invalid parsed-depth parameter accepted')
(R/'INDEPENDENT_DELTA_CASES.json').write_text(json.dumps({'case_count':len(CASES),'cases':CASES,'all_PASS':True,'scope':'LOCAL metadata/io-provider fixtures and parsed-view serialization only; no Linux/whole reader/T0 credit'},indent=2,sort_keys=True)+'\n')
print(json.dumps({'independent_delta_cases':len(CASES),'all_PASS':True,'strict_JSON_returns_verified':True}))
