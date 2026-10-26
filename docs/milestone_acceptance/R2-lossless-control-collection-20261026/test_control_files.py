"""LOCAL regular-file/control-byte tests only; no remote, source or T0 credit."""
from pathlib import Path
import base64,hashlib,json,os,tempfile,time
from unittest.mock import patch
import control_files as c
R=Path(__file__).resolve().parent;CASES=[]
def record(name,result,raw=None):
 if raw is not None:
  assert result['raw'] and base64.b64decode(result['raw']['base64'])==raw
  assert result['raw']['bytes']==len(raw) and result['raw']['sha256']==hashlib.sha256(raw).hexdigest()
 CASES.append({'case':name,'PASS':True,'result':result})
with tempfile.TemporaryDirectory(prefix='control-reader-files-',dir='/private/var/tmp') as td:
 root=Path(td);allowed={'good.json','rows.jsonl','missing.json','empty.json','bad.json','partial.jsonl','link.json','nested/good.json','../good.json','fifo.json','large.json','atomic.json','grow.jsonl'}
 def get(n,**kw):return c.collect_file(root,n,allowed_names=allowed,max_bytes=kw.get('max_bytes',1024),timeout_seconds=kw.get('timeout_seconds',1))
 raw=b'{ "original" : [1,2] }\n';(root/'good.json').write_bytes(raw);r=get('good.json');assert r['read_complete'] and r['parsed']=={'original':[1,2]};record('valid original bytes and same-fd JSON',r,raw)
 raw=b'{"i":1}\n{"i":2}\n';(root/'rows.jsonl').write_bytes(raw);r=get('rows.jsonl');assert r['read_complete'] and len(r['parsed'])==2 and not r['parse_errors'];record('JSONL originals and line provenance',r,raw)
 r=get('missing.json');assert r['missing'] and r['raw'] is None and not r['read_complete'];record('missing remains missing, not empty',r)
 (root/'empty.json').write_bytes(b'');r=get('empty.json');assert not r['missing'] and r['parse_errors'] and r['read_complete'];record('empty file bytes distinct from missing and invalid JSON',r,b'')
 raw=b'\xff{"not": "UTF8"}';(root/'bad.json').write_bytes(raw);r=get('bad.json');assert r['parse_errors'];record('invalid UTF8 kept without replacement decoding',r,raw)
 raw=b'{"ok":1}\n{"partial":';(root/'partial.jsonl').write_bytes(raw);r=get('partial.jsonl');assert r['parse_errors'][0]['line']==2 and r['parse_errors'][0]['byte_offset']==9;record('partial JSONL original and exact failing line',r,raw)
 (root/'link.json').symlink_to(root/'good.json');r=get('link.json');assert r['raw'] is None and r['primary_error'];record('final symlink never opened as data',r)
 (root/'nested').symlink_to(root);r=get('nested/good.json');assert r['raw'] is None and r['primary_error'];record('symlink parent blocked by directory fd',r)
 r=get('../good.json');assert r['raw'] is None and r['primary_error']['phase']=='request';record('allowlist cannot authorize traversal',r)
 os.mkfifo(root/'fifo.json');before=time.monotonic();r=get('fifo.json');assert time.monotonic()-before<.5 and r['raw'] is None;record('FIFO rejected before any blocking open',r)
 raw=b'123456789';(root/'large.json').write_bytes(raw);r=get('large.json',max_bytes=4);assert r['truncated'] and not r['read_complete'];record('explicit bounded prefix never complete even if parseable',r,raw[:5])
 original_read=c.os.read;clock=[0.0]
 def read_and_delay(fd,n):
  value=original_read(fd,n);clock[0]=2.0;return value
 with patch.object(c.time,'monotonic',lambda:clock[0]),patch.object(c.os,'read',read_and_delay):r=get('good.json',timeout_seconds=1)
 assert r['primary_error']['type']=='TimeoutError' and not r['read_complete'];record('cooperative deadline preserves already-read bytes',r,(root/'good.json').read_bytes())
 (root/'atomic.json').write_bytes(b'{"old":1}');done=[False]
 def atomic_replace(fd,n):
  value=original_read(fd,n)
  if not done[0]:
   done[0]=True;(root/'replacement').write_bytes(b'{"new":2}');os.replace(root/'replacement',root/'atomic.json')
  return value
 with patch.object(c.os,'read',atomic_replace):r=get('atomic.json')
 assert r['custody_race'] and not r['read_complete'];record('atomic path replacement retains pinned old-fd bytes',r,b'{"old":1}')
 (root/'grow.jsonl').write_bytes(b'{"first":1}\n');done=[False]
 def append_during(fd,n):
  value=original_read(fd,n)
  if not done[0]:
   done[0]=True
   with (root/'grow.jsonl').open('ab') as out:out.write(b'{"later":2}\n')
  return value
 with patch.object(c.os,'read',append_during):r=get('grow.jsonl')
 assert r['custody_race'] and not r['read_complete'];record('growth is observed without pretending immutable completeness',r,b'{"first":1}\n{"later":2}\n')
 for bad in [{'max_bytes':True},{'timeout_seconds':float('inf')}]:
  try:get('good.json',**bad)
  except ValueError:pass
  else:raise AssertionError('invalid explicit limit accepted')
 CASES.append({'case':'nonboolean finite explicit limits required','PASS':True})
 original_close=c.os.close;once=[False]
 def close_then_error(fd):
  original_close(fd)
  if not once[0]:once[0]=True;raise OSError('explicit close-result fixture')
 with patch.object(c.os,'close',close_then_error):r=get('good.json')
 assert r['secondary_errors'] and not r['read_complete'];record('secondary close error cannot erase original bytes or qualify completeness',r,(root/'good.json').read_bytes())
(R/'LOCAL_FILE_READER_CASES.json').write_text(json.dumps({'case_count':len(CASES),'cases':CASES,'all_PASS':True,'scope':'LOCAL_TEMP_METADATA_FILES_AND_EXPLICIT_IO_FAULTS_ONLY','Linux_or_T0_or_whole_reader_qualification':False},indent=2)+'\n')
print(json.dumps({'LOCAL_file_reader_cases':len(CASES),'all_PASS':True,'whole_reader_or_Linux_credit':False}))
