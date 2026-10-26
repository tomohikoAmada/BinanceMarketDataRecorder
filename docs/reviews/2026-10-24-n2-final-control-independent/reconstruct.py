from pathlib import Path
import hashlib,json,sqlite3,collections,base64,tomllib,tarfile,time
O=Path(__file__).parent
P=Path('/private/var/tmp/bmdr-r2-report-isolation-20261020-yf61hyp0/final-capacity-n2-complete-controls-20261024/controls')
H=lambda b:hashlib.sha256(b).hexdigest()
C=lambda o:(json.dumps(o,sort_keys=True,separators=(',',':'),ensure_ascii=False)+'\n').encode()
checks=collections.Counter();failures=[]
def ck(v,area,msg):
 checks[area]+=1
 if not v:failures.append({'area':area,'finding':msg});raise AssertionError((area,msg))
def emit(n,x):(O/n).write_text(json.dumps(x,sort_keys=True,indent=2)+'\n')
def hashfile(p):
 h=hashlib.sha256()
 with p.open('rb') as f:
  while b:=f.read(1048576):h.update(b)
 return h.hexdigest()
def doc(n):return json.loads((P/n).read_bytes())
def db(n):
 d=sqlite3.connect('file:'+str(P/n)+'?mode=ro&immutable=1',uri=True);d.row_factory=sqlite3.Row;return d
manifest=doc('EXPORT_MANIFEST.json');ck(hashfile(P/'EXPORT_MANIFEST.json')=='7a9d5885c2e537bf7e130423f4b7acc4570f5f92bc4251bae769ba51ac8c73c0','export','manifest SHA')
ck(manifest['member_count']==len(manifest['members'])==28484,'export','all28484 members');paths=set()
for x in manifest['members']:
 n=x['path'];q=P/n;ck(n not in paths and not Path(n).is_absolute() and '..' not in Path(n).parts,'export','canonical unique '+n);paths.add(n);ck(q.is_file() and not q.is_symlink() and q.stat().st_size==x['size'] and hashfile(q)==x['sha256'],'export','sizeSHA '+n)
ck(paths=={str(p.relative_to(P)) for p in P.rglob('*') if p.is_file()}-{'EXPORT_MANIFEST.json'},'export','no undeclared members')
pack=P.parent/'controls.tar.gz';ck(pack.stat().st_size==684551202 and hashfile(pack)=='d1b4cabaca36ef87d97f6066fa21d55c969879bdf2f071ba14db53a0202152e2','export','exact pack')
ck(manifest['raw_content_members']==manifest['historical_manifest_content_members']==0,'export','no Raw/history payload')
print('ALL_EXPORT_SHA_PASS',flush=True)
root=doc('audit/audit-root.json');cor=doc('audit/quiescence-corpus.json');scope=doc('audit/owner-scope-receipt.json');target=doc('capture/owner-target.json');owner=doc('audit/owner-selected-result.json');start=doc('capture/owner-start.json');boundary=doc('capture/data-scope-start.json')
for n,sha in [('audit/audit-root.json','7011d408f3d72274e9bfae0fba2f6dd913b720434a5189874e418830bccd0265'),('audit/owner-selected-result.json','68b79a1a38a7a30d6b83a86a98aadd37b372bf8d29cf786da99d8f99bddbb37c'),('capture/native-result.json','854ea7e42c36e0a88fd1020dd87779aacdbfcd39fd606f32ce3e12f22c23f656')]:ck(hashfile(P/n)==sha,'binding',n+'original result')
summary={};maps={};manifests={}
def chain(folder,anchor,domain,kind,wanted):
 digest=H(domain+bytes.fromhex(anchor));prev=None;count=0
 files=sorted((P/folder).iterdir());ck(len(files)==wanted['shard_count'],'chain',folder+'count')
 for i,p in enumerate(files):
  b=p.read_bytes();d=json.loads(b);sha=H(b)
  ck(p.name==f'shard-{i:08d}.json' and b==C(d),'chain',folder+'ordinal canonical')
  ck(d['anchor']==anchor and d['evidence_kind']==kind and d['ordinal']==i and d['previous_shard_sha256']==prev,'chain',folder+'lineage')
  ck(1<=len(d['records'])<=512 and len(b)<=1048576,'chain',folder+'caps')
  for row in d['records']:count+=1;yield row
  digest=H(bytes.fromhex(digest)+i.to_bytes(8,'big')+bytes.fromhex(sha));prev=sha
 ck(count==wanted['record_count'] and digest==wanted['root_digest'],'chain',folder+'rootdigest')
for m in chain('audit/manifest-freeze',cor['catalog_sha256'],b'BMDR-V5-MANIFEST-FREEZE\0','manifest-freeze-shard',cor['manifest_freeze']):
 b=(P/'audit'/m['object']).read_bytes();v=json.loads(b);ck(len(b)==m['byte_length'] and H(b)==m['sha256'] and v['chunk_id']==m['chunk_id'],'manifest','frozen object bytes identity');ck(m['chunk_id'] not in maps,'manifest','unique mapping');maps[m['chunk_id']]=m;manifests[m['chunk_id']]=v
ck(len(maps)==len(list((P/'audit/manifest-objects').iterdir()))==26324,'manifest','complete26324')
ms=list(manifests.values());counts={'chunks':len(ms),'frames':sum(m['record_count'] for m in ms),'stored_bytes':sum(m['stored_bytes'] for m in ms),'decoded_bytes':sum(m['uncompressed_bytes'] for m in ms)};ck(counts==scope['selected_counts']==owner['selected_counts'],'counts','full denominator')
ck(counts=={'chunks':26324,'frames':62687921,'stored_bytes':3318145280,'decoded_bytes':47682580261},'counts','exact new dataset')
flags={k:sum(fn(m) for m in ms) for k,fn in {'gap':lambda m:m['gap'],'incomplete':lambda m:not m['complete'],'gap_or_incomplete':lambda m:m['gap'] or not m['complete'],'zero_frame':lambda m:m['record_count']==0,'zero_with_gap':lambda m:m['record_count']==0 and m['gap'],'overlap':lambda m:m['overlap'],'recovered':lambda m:m['recovered']}.items()}
tail=[m for m in ms if m['receive_time_utc_range_ns']['max'] is not None and m['receive_time_utc_range_ns']['max']>target['terminal_utc_ns']]
summary['counts']=counts;summary['flags']=flags;summary['tail']={'manifests_with_receive_after_actual_target':len(tail),'max_after_actual_target_ns':max(m['receive_time_utc_range_ns']['max'] for m in ms if m['record_count'])-target['terminal_utc_ns'],'max_after_nominal_target_ns':max(m['receive_time_utc_range_ns']['max'] for m in ms if m['record_count'])-target['monitor_interval_end_utc_ns'],'created_after_actual_target':sum(m['created_at_utc_ns']>target['terminal_utc_ns'] for m in ms),'duration_credit_ns':0};emit('COUNTS_FLAGS_TAIL.json',summary)
selected=db('audit/catalog.sqlite');source=db('selection/source-catalog.sqlite');startdb=db('selection/start-catalog.sqlite')
ck(hashfile(P/'selection/start-catalog.sqlite')==boundary['start_catalog_sha256']==scope['boundary']['start_catalog_sha256'],'selection','startSHA');ck(hashfile(P/'selection/source-catalog.sqlite')==scope['source_catalog_sha256'],'selection','sourceSHA');ck(hashfile(P/'audit/catalog.sqlite')==scope['selected_catalog_sha256']==root['catalog_sha256'],'selection','selectedSHA')
ids=lambda d:{r[0] for r in d.execute('select chunk_id from chunks')}
selids,sourceids,startids=map(ids,[selected,source,startdb]);high=boundary['high_water']['chunk'];created={r[0] for r in source.execute("SELECT chunk_id FROM chunk_transitions WHERE transition_id>? AND from_state IS NULL AND to_state='ACTIVE'",(high,))}
ck(selids==sourceids-startids==created==set(maps),'selection','selected==source-start==newACTIVE==manifest IDs');ck(not(selids&startids),'selection','no inherited IDs')
# All prior metadata chunk rows remain exactly preserved. No prior manifest/Raw payload is opened.
oldrows={r['chunk_id']:dict(r) for r in startdb.execute('select * from chunks')};compared=0
for r in source.execute('select * from chunks'):
 if r['chunk_id'] in oldrows:ck(dict(r)==oldrows[r['chunk_id']],'selection','prior metadata row preserved');compared+=1
ck(compared==len(startids)==137062,'selection','all137062 prior chunk metadata rows preserved')
ops=lambda d:[dict(r) for r in d.execute('SELECT e.*,s.event_seq FROM operational_events e JOIN operational_event_sequence s USING(event_id) ORDER BY s.event_seq')]
newops,srcops=ops(selected),ops(source);byseq={r['event_seq']:r for r in srcops};newbyseq={r['event_seq']:r for r in newops};ophigh=boundary['high_water']['operational'];companions=[r for r in newops if r['event_seq']<=ophigh]
ck(len(newops)==1538 and len(companions)==2 and {r['event_seq'] for r in companions}=={4257,4259},'ops','1536 postboundary plus2 START companions')
ck([r for r in newops if r['event_seq']>ophigh]==[r for r in srcops if r['event_seq']>ophigh],'ops','all1536 exact postboundary')
for r in companions:ck(r==byseq[r['event_seq']] and r['event_type']=='STREAM_DISCONTINUITY_STARTED' and any(x['gap_id']==r['gap_id'] and x['event_seq']>ophigh and x['event_type']=='STREAM_DISCONTINUITY_COMPLETED' for x in newops),'ops','exact source companion paired currentcompletion')
ck(selected.execute("select count(*) from chunks where state!='LOCAL_DELETED'").fetchone()[0]==selected.execute("select count(*) from archive_transactions where state!='LOCAL_DELETED'").fetchone()[0]==0,'database','allselected retired')
summary['selection']={'start_chunks':len(startids),'terminal_chunks':len(sourceids),'selected_chunks':len(selids),'creation_ids':len(created),'operational':len(newops),'current_postboundary_operational':1536,'prior_context_companions':[4257,4259],'prior_metadata_rows_preserved':compared};emit('SELECTION_AUTHORITY.json',summary['selection'])
familytable={'catalog_chunk':('chunks','chunk_id'),'chunk_transition':('chunk_transitions','transition_id'),'archive_transaction':('archive_transactions','transaction_id'),'archive_event':('archive_transaction_events','event_id')};authorities={f:{r[k]:dict(r) for r in selected.execute('SELECT * FROM '+t)} for f,(t,k) in familytable.items()};authorities['operational_event']=newbyseq
agg=hashlib.sha256();families=collections.Counter();seen=collections.defaultdict(set);rawproofs={};manproofs={};frameproofsum=0
for r in chain('audit/shards',root['anchor_sha256'],b'BMDR-V5-AUDIT-SHARDS\0','terminal-audit-shard',root['shards']):
 fam=r['family'];key=r['key'];a=r['authority'];ck(H(C(a))==r['authority_sha256'] and r['findings']==[] and r['comparison_authority_sha256'] is None and r['timing']=='BASELINE_CORPUS','authority','digest findings timing');ck(key not in seen[fam],'authority','unique keys');seen[fam].add(key)
 if fam in authorities:ck(a==authorities[fam][key],'authority','exact SQL authority')
 elif fam=='manifest':
  m=a['manifest'];cid=m['chunk_id'];ck(m==manifests[cid] and a['mapping']==maps[cid] and key==maps[cid]['path'],'authority','exact frozen manifest mapping');proof=a['proof'];manproofs[cid]=proof;ck(proof.get('local') is None and proof.get('archive') is not None and 'error' not in proof,'proof','archiveproof only noerror')
 elif fam=='raw_location':rawproofs[key]=a
 else:ck(False,'authority','unexpectedfamily')
 families[fam]+=1;agg.update(C(r))
ck(dict(families)==root['counts_by_family'] and agg.hexdigest()==root['aggregate_sha256'],'authority','full449046 aggregate')
ck(len(rawproofs)==len(manproofs)==26324,'proof','full26324 proofs')
path_to_chunk={v['path']:k for k,v in maps.items()}
for key,a in rawproofs.items():
 cid=key if key in manifests else path_to_chunk.get(key,a.get('chunk_id'));ck(cid in manifests,'proof','chunkkey');m=manifests[cid];ck(a['proof']==manproofs[cid] and a['manifest_sha256']==maps[cid]['sha256'],'proof','same complete proof + manifestSHA')
 pr=a['proof']['archive'];n=pr['last_frame']['frame_index']+1 if pr['last_frame'] is not None else 0;ck(n==m['record_count'] and ((pr['first_frame'] is None) if n==0 else pr['first_frame']['frame_index']==0),'proof','frame index denominator');ck(all(pr[k]==m[k] for k in ['stored_bytes','stored_sha256','uncompressed_bytes','uncompressed_sha256']),'proof','every bytehash');frameproofsum+=n
ck(frameproofsum==counts['frames']==62687921,'proof','complete62687921 proofdenominator')
summary['audit']={'record_count':sum(families.values()),'families':dict(families),'aggregate_sha256':agg.hexdigest(),'archive_bytes_verified':root['archive_bytes_verified'],'raw_bytes_verified':root['raw_bytes_verified'],'proof_frame_count':frameproofsum};emit('AUDIT_RECONSTRUCTION.json',summary['audit'])
emit('STAGE1.json',{'summary':summary,'checks':dict(checks),'assertions':sum(checks.values()),'findings':failures});print('STAGE1_PASS',json.dumps({'assertions':sum(checks.values()),'counts':counts,'flags':flags,'tail':summary['tail']}),flush=True)
