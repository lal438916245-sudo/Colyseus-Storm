"""Audit sampled ACKs and analyze V8 CPU profiles from a completed staircase."""
import argparse
from collections import Counter, defaultdict
import hashlib
import json
from pathlib import Path


def load(path): return json.loads(path.read_text(encoding='utf-8'))


def quantile(values,p):
    import math
    return sorted(values)[max(0,math.ceil(len(values)*p)-1)] if values else None


def profile_analysis(path):
    data=load(path);nodes={n['id']:n for n in data['nodes']};parents={}
    for n in nodes.values():
        for child in n.get('children',[]): parents[child]=n['id']
    self_us=Counter();inclusive_us=Counter();category_us=Counter();function_us=Counter()
    total=0
    for node,delta in zip(data.get('samples',[]),data.get('timeDeltas',[])):
        self_us[node]+=delta;total+=delta;seen=set()
        cursor=node
        while cursor in nodes:
            inclusive_us[cursor]+=delta
            url=nodes[cursor]['callFrame']['url']
            category=('schema_runtime' if '@colyseus/schema' in url else 'websocket_transport' if '/ws/' in url.replace('\\','/') else
                      'game_room' if 'MMORoom.ts' in url else 'colyseus_framework' if '/colyseus/' in url.replace('\\','/') else
                      'experiment_probe' if 'step_server.cjs' in url else 'other')
            seen.add(category)
            if cursor not in parents: break
            cursor=parents[cursor]
        for category in seen: category_us[category]+=delta
    def rows(counter):
        result=[]
        for node,duration in counter.most_common(30):
            f=nodes[node]['callFrame']
            result.append({'function':f['functionName'],'url':f['url'],'line':f['lineNumber']+1,'sample_ms':duration/1000,'share_of_sample_time':duration/total if total else 0})
        return result
    for node,duration in self_us.items():
        f=nodes[node]['callFrame'];function_us[(f['functionName'],f['url'])]+=duration
    return {'profile':path.name,'sample_duration_ms':total/1000,'samples':len(data.get('samples',[])),
        'self':rows(self_us),'inclusive':rows(inclusive_us),
        'self_by_function':[{'function':key[0],'url':key[1],'sample_ms':v/1000,'share':v/total} for key,v in function_us.most_common(20)],
        'stack_category_ms':{k:v/1000 for k,v in category_us.items()},
        'interpretation':'Weighted V8 wall-clock sample stacks, including idle/native frames; not per-function CPU cycle accounting. Categories overlap.'}


def audit(folder):
    m=load(folder/'manifest.json');s=load(folder/'summary.json');cleanup=load(folder/'cleanup.json')
    failures=[];checks=0
    def check(ok,message):
        nonlocal checks
        checks+=1
        if not ok:failures.append(message)
    check(cleanup['ok'],'owned-resource cleanup and runtime source hashes')
    check(cleanup['remaining']==0 and cleanup['unrelated_untouched'],'dedicated accounts removed')
    for name,digest in m['source_sha256'].items():
        check(hashlib.sha256((folder/'source-snapshot'/name).read_bytes()).hexdigest()==digest,'snapshot '+name)
    server=[]
    with (folder/'server-events.jsonl').open(encoding='utf-8') as raw:
        server=[json.loads(line) for line in raw if line.strip()]
    auth={e['session_id']:e for e in server if e['event']=='auth'}
    ack_counts=Counter();ack_rtts=defaultdict(list);ready={};stopped=[];unique=set();worker_samples=defaultdict(list)
    worker_cohorts=defaultdict(list)
    for path in folder.glob('worker-*-events.jsonl'):
        with path.open(encoding='utf-8') as raw:
            for line in raw:
                row=json.loads(line)
                if row['event']=='sample':worker_samples[row['worker']].append(row)
                if row['event']=='ready':ready[row['session_id']]=row
                if row['event']=='stopped':stopped.append(row)
                if row['event']!='business_ack' or not row.get('phase'):continue
                key=(row['session_id'],row['seq'])
                check(key not in unique,'duplicate business ACK '+str(key));unique.add(key)
                check(row['session_id'] in auth,'ACK has actual authenticated server session')
                check(abs(row['x']-row['seq']/1000)<1e-10,'ACK sequence correlates applied coordinate')
                ack_counts[row['phase']]+=1;ack_rtts[row['phase']].append(row['rtt_ms'])
                worker_cohorts[(row['worker'],row['phase'])].append(row['rtt_ms'])
    for sid,row in ready.items():
        check(sid in auth and auth[sid]['username']==row['username'] and auth[sid]['room_id']==row['room_id'],'ready identity '+sid)
    for stage in s['stages']:
        label=stage['phase'];reports=load(folder/f"workers-{stage['target']}.json")
        observed=[]
        for worker,report in enumerate(reports):
            raw=worker_cohorts[(worker,label)]
            check(raw[:len(report['rtts'])]==report['rtts'],label+' raw ACK snapshot prefix')
            check(len(report['rtts'])==report['acked'],label+' ACK snapshot count')
            observed.extend(raw[:len(report['rtts'])])
        check(sum(r['sent'] for r in reports)==stage['sent'],label+' client sends')
        check(sum(r['expected_samples'] for r in reports)==stage['ack_expected'],label+' expected sampled cohort')
        check(len(observed)==stage['ack_matched'],label+' raw ACK count at report snapshot')
        check(len({c['session_id'] for r in reports for c in r['clients'] if not c['closed']})==stage['live'],label+' unique live clients')
        check(sum(r['clients'] for r in stage['rooms'])==stage['server_after']['open'],label+' room/server count consistency')
        check(all(r['clients']<=150 for r in stage['rooms']),label+' original per-room cap')
        for percentile in (.5,.95,.99):
            name='rtt_p'+str(int(percentile*100))+'_ms'
            actual=quantile(observed,percentile);expected=stage[name]
            check(actual==expected,label+' raw '+name)
        if stage['passed']:
            check(stage['generator_valid'] and stage['live']==stage['target'],label+' valid full target')
            raw_samples=[e for e in server if e['event']=='sample' and stage['start_wall_ms']<=e['wall_ms']<=stage['end_wall_ms']]
            check(bool(raw_samples) and all(e['open']==stage['target'] for e in raw_samples),label+' full live population throughout sampled window')
            check(stage['drops']==0 and stage['errors']==0 and stage['ack_ratio']>=.99 and stage['applied_ratio']>=.98,label+' business criteria')
    check(len(stopped)==m['parameters']['workers'],'all worker raw shutdown records')
    check(any(e['event']=='stopped' for e in server),'raw server shutdown')
    final=load(folder/'final-server.json') if (folder/'final-server.json').exists() else None
    server_stop=next((e for e in reversed(server) if e['event']=='stopped'),{})
    if final:
        check(final['totals']['updates']==server_stop.get('totals',{}).get('updates'),'final snapshot / raw shutdown application count')
        check(final['totals']['updates']<=sum(r['counters']['sent'] for r in stopped),'server cannot apply more updates than owned clients sent')
    summary={'run_id':m['run_id'],'passed':not failures,'checks':checks,'failures':failures,
        'raw_authenticated_sessions':len(auth),'raw_ready_sessions':len(ready),'raw_measurement_acks':sum(ack_counts.values()),
        'lifetime_client_sends':sum(r['counters']['sent'] for r in stopped),
        'lifetime_server_applied':load(folder/'final-server.json')['totals']['updates'] if (folder/'final-server.json').exists() else None,
        'source_snapshot_hashes_verified':not any(message.startswith('snapshot ') for message in failures)}
    profiles=[profile_analysis(path) for path in folder.glob('*.cpuprofile')]
    if profiles:(folder/'profile-analysis.json').write_text(json.dumps(profiles,ensure_ascii=False,indent=2),encoding='utf-8')
    resource=[]
    with (folder/'resource-samples.jsonl').open(encoding='utf-8') as raw:
        resource=[json.loads(line) for line in raw if line.strip()]
    secondary=[]
    server_samples=[e for e in server if e['event']=='sample']
    for stage in s['stages']:
        start,end=stage['start_wall_ms'],stage['end_wall_ms'];patches=0;elapsed=0
        for previous,row in zip(server_samples,server_samples[1:]):
            if start<=row['wall_ms']<=end:
                patches+=row['patch_count'];elapsed+=(row['wall_ms']-previous['wall_ms'])/1000
        received_rate=0
        for rows in worker_samples.values():
            window=[r for r in rows if start<=r['wall_ms']<=end]
            if len(window)<2:continue
            first,last=window[0],window[-1]
            delta=(last['counters']['received_frames']-first['counters']['received_frames'])-(last['counters']['acked']-first['counters']['acked'])
            received_rate+=delta/((last['wall_ms']-first['wall_ms'])/1000)
        resources=[r for r in resource if start<=r['wall_ms']<=end]
        mongo=[r['processes']['mongo']['cpu_one_core_pct'] for r in resources if 'mongo' in r['processes']]
        secondary.append({'target':stage['target'],'mean_room_patch_calls_per_second':patches/elapsed/len(stage['rooms']) if elapsed else None,
            'received_state_frames_per_player_per_second':received_rate/stage['target'],
            'server_outbound_MiB_per_second':stage['server_bytes_per_second']/2**20,
            'mongo_cpu_mean_one_core_pct':sum(mongo)/len(mongo) if mongo else None,
            'min_available_MiB':min(r['available_bytes'] for r in resources)/2**20 if resources else None,
            'host_cpu_mean_pct':sum(r['host_cpu_pct'] for r in resources)/len(resources) if resources else None,
            'interpretation':'Patch calls and received state frame rates are observed secondary metrics, not additional retrospective pass criteria.'})
    (folder/'secondary-metrics.json').write_text(json.dumps(secondary,ensure_ascii=False,indent=2),encoding='utf-8')
    summary['artifact_sha256']={str(p.relative_to(folder)).replace('\\','/'):hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(folder.rglob('*')) if p.is_file() and p.name!='audit.json'}
    (folder/'audit.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2),encoding='utf-8')
    return summary


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('run_dir',type=Path);a=p.parse_args()
    result=audit(a.run_dir)
    print(json.dumps({k:v for k,v in result.items() if k!='artifact_sha256'},ensure_ascii=False))
    raise SystemExit(0 if result['passed'] else 1)
