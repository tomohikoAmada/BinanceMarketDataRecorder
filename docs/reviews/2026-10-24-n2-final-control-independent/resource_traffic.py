from pathlib import Path
import json,base64,collections
P=Path('/private/var/tmp/bmdr-r2-report-isolation-20261020-yf61hyp0/final-capacity-n2-complete-controls-20261024/controls');O=Path(__file__).parent
first=json.loads((P/'capture/monitoring.jsonl').read_bytes().splitlines()[0]);last=json.loads((P/'capture/target-observation.json').read_bytes());seconds=(last['boottime_ns']-first['boottime_ns'])/1e9
cpu=lambda r:[int(x) for x in r['resources']['host_stat'].splitlines()[0].split()[1:]]
a,b=cpu(first),cpu(last);d=[y-x for x,y in zip(a,b)];den=sum(d[:8]);cs=lambda r:dict(line.split() for line in r['resources']['cpu_stat'].splitlines());ca,cb=cs(first),cs(last);rs=lambda r:json.loads(base64.b64decode(r['state_publication_base64']))['runtime_metrics'];ra,rb=rs(first),rs(last)
products=collections.defaultdict(lambda:collections.Counter());spools=collections.defaultdict(lambda:collections.Counter())
for p in (P/'audit/manifest-objects').iterdir():
 m=json.loads(p.read_bytes());k=m['market']+':'+m['symbol'];f=k+':'+m['stream']
 for q in [products[k],spools[f]]:q['chunks']+=1;q['saved_frames']+=m['record_count'];q['stored_bytes']+=m['stored_bytes'];q['decoded_bytes']+=m['uncompressed_bytes']
out={'period':'first nominal sample through strict actual endpoint','elapsed_seconds':seconds,'host_cpu_percent':{'user':d[0]/den*100,'system':d[2]/den*100,'idle':d[3]/den*100,'iowait':d[4]/den*100,'steal':d[7]/den*100},'recorder_cgroup_average_cores':(int(cb['usage_usec'])-int(ca['usage_usec']))/1e6/seconds,'recorder_process_average_cores':(rb['process_cpu_seconds']-ra['process_cpu_seconds'])/seconds,'note':'Host counters are shared VPS diagnostics; cumulative writer latencies and host steal do not identify unique disconnect origin or hardware capacity limit. Product counters include new warm-up and orderly tail.','by_product':{k:dict(v) for k,v in sorted(products.items())},'by_spool':{k:dict(v) for k,v in sorted(spools.items())}}
assert len(spools)==42 and sum(x['saved_frames'] for x in products.values())==62687921
(O/'RESOURCE_TRAFFIC.json').write_text(json.dumps(out,sort_keys=True,indent=2)+'\n');print(json.dumps({k:v for k,v in out.items() if k not in ['by_product','by_spool']}))
