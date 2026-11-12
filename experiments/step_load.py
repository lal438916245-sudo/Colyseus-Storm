"""Local staircase capacity experiment with real authentication and game updates."""
from __future__ import annotations
import argparse
from concurrent.futures import ThreadPoolExecutor
import csv
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import queue
import socket
import subprocess
import sys
import threading
import time
import uuid
import os
import signal

import psutil
from pymongo import MongoClient
import requests

ROOT = Path(__file__).resolve().parents[1]
SERVER = Path(os.environ.get('STORM_SERVER_ROOT', 'F:/Colyseus_SDET_Project/colyseus-unity-demo/colyseus-game-client/Server'))
NODE = os.environ.get('STORM_NODE', 'F:/Colyseus_SDET_Project/Node/node.exe' if os.name == 'nt' else 'node')
ACTIVE_FOLDER = None


def save(path, obj):
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding='utf-8')


def quantile(values, p):
    import math
    return sorted(values)[max(0, math.ceil(len(values)*p)-1)] if values else None


def evaluate(row, p95_limit=200, p99_limit=500):
    """Capacity SLO and load-generator validity are deliberately separate."""
    generator = []
    if row['offered_ratio'] < .98 or row['skipped_ratio'] > .01 or row['schedule_p95_ms'] > 25:
        generator.append('load_generator_pacing')
    if row['worker_loop_p95_ms'] > 50:
        generator.append('load_generator_event_loop')
    failures = []
    for ok, reason in [(row['live'] == row['target'], 'live_connections'),
                       (row['admission_errors'] == 0, 'admission'),
                       (row['drops'] == 0 and row['errors'] == 0, 'connection_or_protocol_errors'),
                       (row['applied_ratio'] >= .98, 'server_applied_throughput'),
                       (row['ack_ratio'] >= .99, 'sampled_application_ack_coverage'),
                       (row['rtt_p95_ms'] is not None and row['rtt_p95_ms'] <= p95_limit, 'application_rtt_p95'),
                       (row['rtt_p99_ms'] is not None and row['rtt_p99_ms'] <= p99_limit, 'application_rtt_p99')]:
        if not ok: failures.append(reason)
    return {'passed': not generator and not failures, 'generator_valid': not generator,
            'generator_issues': generator, 'failed_criteria': failures}


class Worker:
    def __init__(self, number, node, env, folder):
        self.number, self.sequence = number, 0
        self.queue = queue.Queue()
        self.stderr = (folder/f'worker-{number}-stderr.log').open('w', encoding='utf-8')
        self.process = subprocess.Popen([node, str(ROOT/'experiments/step_worker.cjs')], env=env, cwd=folder,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.stderr, text=True, encoding='utf-8',
            creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == 'win32' else 0)
        def read():
            for line in self.process.stdout:
                try: self.queue.put(json.loads(line))
                except ValueError: self.queue.put({'ok': False, 'error': line})
        threading.Thread(target=read, daemon=True).start()

    def call(self, op, timeout=240, **kwargs):
        self.sequence += 1
        self.process.stdin.write(json.dumps({'id': self.sequence, 'op': op, **kwargs})+'\n')
        self.process.stdin.flush()
        result = self.queue.get(timeout=timeout)
        if not result.get('ok') or result.get('id') != self.sequence:
            raise RuntimeError(f'worker {self.number}: {result}')
        return result['result']


def _run(argv=None):
    global ACTIVE_FOLDER
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--steps', default='20,50,100,150,200,300,500,750,1000,1500,2000')
    p.add_argument('--hold', type=float, default=60)
    p.add_argument('--warmup', type=float, default=10)
    p.add_argument('--workers', type=int, default=4)
    p.add_argument('--spawn-rate', type=float, default=20)
    p.add_argument('--server-root', type=Path, default=SERVER)
    p.add_argument('--node', default=NODE)
    p.add_argument('--profile-seconds', type=int, default=12)
    p.add_argument('--output', type=Path)
    p.add_argument('--p95-limit-ms', type=float, default=200)
    p.add_argument('--p99-limit-ms', type=float, default=500)
    args = p.parse_args(argv)
    steps = [int(n) for n in args.steps.split(',')]
    if not steps or steps != sorted(set(steps)) or not 1 <= steps[0] <= steps[-1] <= 5000 or not 1 <= args.workers <= 8 or not 10 <= args.hold <= 600 or not 0 <= args.warmup <= 60 or not 1 <= args.spawn_rate <= 40:
        p.error('Use increasing unique steps <=5000, hold 10..600, warmup 0..60, workers 1..8, spawn-rate 1..40')
    run = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'_'+uuid.uuid4().hex[:8]
    if not (0 < args.p95_limit_ms <= args.p99_limit_ms): p.error('Require 0 < p95 limit <= p99 limit')
    folder = (args.output or ROOT/'reports/staircase'/run).resolve(); folder.mkdir(parents=True, exist_ok=False)
    ACTIVE_FOLDER = folder
    save(folder/'ownership.json', {'run_id':run,'database':'storm_step_'+run,'processes':[]})
    def own(process, kind):
        record=json.loads((folder/'ownership.json').read_text(encoding='utf-8'))
        record['processes'].append({'pid':process.pid,'created':psutil.Process(process.pid).create_time(),'kind':kind})
        save(folder/'ownership.json',record)
    snapshots = folder/'source-snapshot'; snapshots.mkdir()
    source = {n: ROOT/'experiments'/n for n in ['step_load.py','step_worker.cjs','step_server.cjs','step_statistics.cjs','observe_socket_close.cjs','product.py','analyze_step_results.py','plot_step_results.py']}
    source.update({n: args.server_root/n for n in ['src/rooms/MMORoom.ts','src/controllers/authController.ts','src/helpers/matchmakerHelper.ts',
        'src/config/mikro-orm.config.ts','node_modules/colyseus/lib/Room.js','node_modules/colyseus/lib/transport/WebSocket/WebSocketTransport.js']})
    hashes = {}
    for name, path in source.items():
        content = path.read_bytes(); hashes[name] = hashlib.sha256(content).hexdigest()
        target = snapshots/name; target.parent.mkdir(parents=True, exist_ok=True); target.write_bytes(content)
    mongo = MongoClient('mongodb://127.0.0.1:27017', serverSelectionTimeoutMS=4000); mongo.admin.command('ping')
    dbname = 'storm_step_'+run; db = mongo[dbname]
    if db.list_collection_names(): raise RuntimeError('Database exists')
    mongo_pid = mongo.admin.command('serverStatus')['pid']
    with socket.socket() as s: s.bind(('127.0.0.1',0)); port = s.getsockname()[1]
    base, token = f'http://127.0.0.1:{port}', uuid.uuid4().hex
    import os
    env = dict(os.environ, STEP_SERVER_ROOT=str(args.server_root), STEP_DATABASE=dbname,
        STEP_MONGO_URI=f'mongodb://127.0.0.1:27017/{dbname}', STEP_PORT=str(port), STEP_TOKEN=token, STEP_RUN_ID=run,
        STEP_EVENTS=str(folder/'server-events.jsonl'), NODE_ENV='production', NODE_ARENA='true')
    manifest = {'run_id':run,'started_at':datetime.now(timezone.utc).isoformat(),'parameters':{k:str(v) if isinstance(v,Path) else v for k,v in vars(args).items()},
        'steps':steps,'database':dbname,'base_url':base,'python':sys.version,'node':subprocess.check_output([args.node,'--version'],text=True).strip(),
        'host':{'logical_cpus':psutil.cpu_count(),'physical_cpus':psutil.cpu_count(logical=False),'memory_bytes':psutil.virtual_memory().total,'available_bytes_at_start':psutil.virtual_memory().available},
        'source_sha256':hashes,'design':{'updates_per_connection_per_second':10,'application_ack_every_updates':10,'room_limit_unchanged':150,
        'p95_rtt_limit_ms':args.p95_limit_ms,'p99_rtt_limit_ms':args.p99_limit_ms,'ack_coverage_min':.99,'applied_rate_min':.98,'offered_rate_min':.98,
        'max_schedule_p95_ms':25,'max_worker_loop_p95_ms':50,'max_skipped_ratio':.01,'memory_guard_available_bytes':900*2**20}}
    save(folder/'manifest.json',manifest)
    control = requests.Session(); control.trust_env = False
    control.headers['Authorization']='Bearer '+token
    workers, stages, samples = [], [], []
    stopped = threading.Event(); guard = threading.Event(); guard_reasons = []
    server = None; sampler = None; fatal = None; profile_file = None
    console = (folder/'server-console.log').open('w',encoding='utf-8')
    events = (folder/'runner-events.jsonl').open('w',encoding='utf-8',buffering=1)
    def event(kind, **fields): events.write(json.dumps({'event':kind,'wall_ms':time.time()*1000,**fields})+'\n')
    def snap(details=False):
        r=control.get(base+'/__step/snapshot',params={'details':'1' if details else '0'},timeout=15);r.raise_for_status();return r.json()
    def calls(op, **kwargs):
        with ThreadPoolExecutor(max_workers=len(workers)) as pool:
            return list(pool.map(lambda w:w.call(op,**kwargs),workers))
    def wait(seconds):
        deadline=time.monotonic()+seconds
        while time.monotonic()<deadline:
            if (folder/'cancel.requested').exists(): raise RuntimeError('cancelled by operator')
            if guard.is_set(): raise RuntimeError('Resource guard: '+','.join(guard_reasons))
            if server.poll() is not None: raise RuntimeError('Server exited unexpectedly')
            time.sleep(min(.25,max(0,deadline-time.monotonic())))
    try:
        server=subprocess.Popen([args.node,str(ROOT/'experiments/step_server.cjs')],env=env,cwd=folder,stdout=console,stderr=subprocess.STDOUT,
            creationflags=subprocess.CREATE_NO_WINDOW if sys.platform=='win32' else 0)
        own(server,'server')
        deadline=time.monotonic()+40
        while True:
            if server.poll() is not None: raise RuntimeError('Server startup failed; see console log')
            try:
                health=control.get(base+'/__step/health',timeout=1).json()
                if health['run_id']!=run or health['database']!=dbname: raise RuntimeError('Service isolation failed')
                break
            except (requests.RequestException,ValueError):
                if time.monotonic()>deadline: raise TimeoutError('Readiness timed out')
                time.sleep(.1)
        manifest['health']=health; save(folder/'manifest.json',manifest)
        for i in range(args.workers):
            wenv=dict(env, STEP_BASE=base, STEP_WORKER=str(i), STEP_EVENTS=str(folder/f'worker-{i}-events.jsonl'),STEP_ACCOUNTS=str(folder/f'worker-{i}-accounts.jsonl'))
            workers.append(Worker(i,args.node,wenv,folder))
            own(workers[-1].process,'worker')
        def sample():
            processes={'server':psutil.Process(server.pid),**{f'worker_{w.number}':psutil.Process(w.process.pid) for w in workers}}
            try:
                candidate=psutil.Process(mongo_pid)
                if 'mongod' in candidate.name().lower():processes['mongo']=candidate
            except psutil.Error:pass  # Docker/remote PID namespaces are not host Mongo PIDs.
            for process in processes.values(): process.cpu_percent(None)
            psutil.cpu_percent(None)
            with (folder/'resource-samples.jsonl').open('w',encoding='utf-8',buffering=1) as out:
                while not stopped.wait(1):
                    row={'wall_ms':time.time()*1000,'available_bytes':psutil.virtual_memory().available,'host_cpu_pct':psutil.cpu_percent(None),'processes':{}}
                    for name, process in processes.items():
                        try: row['processes'][name]={'pid':process.pid,'cpu_one_core_pct':process.cpu_percent(None),'rss':process.memory_info().rss}
                        except psutil.Error: pass
                    samples.append(row);out.write(json.dumps(row)+'\n')
                    child_rss=sum(v['rss'] for n,v in row['processes'].items() if n!='mongo')
                    if row['available_bytes']<900*2**20 or child_rss>1800*2**20:
                        guard_reasons.append('available_memory_or_owned_process_rss');guard.set()
                        (folder/'STOP_REQUESTED').write_text('resource guard',encoding='utf-8')
        sampler=threading.Thread(target=sample,daemon=True);sampler.start()
        print('RUN_DIR='+str(folder),flush=True);event('ready',pids=[server.pid]+[w.process.pid for w in workers])
        print('SLO: no drops/errors; rate>=98%; ACK>=99%; business RTT p95<=200ms / p99<=500ms; pacing/worker validity checked separately.',flush=True)
        last_profile_needed=False
        for n in steps:
            print(f'RAMP target={n}, spawn_rate={args.spawn_rate}/s',flush=True);event('ramp_start',target=n)
            with ThreadPoolExecutor(max_workers=len(workers)) as pool:
                admission=list(pool.map(lambda w:w.call('ramp',target=n//len(workers)+(w.number<n%len(workers)),rate=args.spawn_rate/len(workers)),workers))
            save(folder/f'admission-{n}.json',admission)
            admission_errors=sum(len(a['failures']) for a in admission)
            if admission_errors or sum(a['live'] for a in admission)!=n:
                raise RuntimeError(f'Admission failed at {n}; no capacity claim for this incomplete level')
            wait(args.warmup)
            phase=f'n{n}'; before=snap(); calls('phase_start',phase=phase)
            start=time.time()*1000;event('hold_start',target=n,phase=phase);print(f'HOLD {n}: {args.hold}s',flush=True)
            wait(args.hold)
            calls('phase_end');end=time.time()*1000;after=snap();wait(2)
            reports=calls('report',phase=phase);save(folder/f'workers-{n}.json',reports)
            resource=[s for s in samples if start<=s['wall_ms']<=end]
            server_events=[json.loads(line) for line in (folder/'server-events.jsonl').read_text(encoding='utf-8').splitlines() if line.strip()]
            server_samples=[s for s in server_events if s['event']=='sample' and start<=s['wall_ms']<=end]
            worker_samples=[]
            for i in range(args.workers):
                with (folder/f'worker-{i}-events.jsonl').open(encoding='utf-8') as raw:
                    for line in raw:
                        if not line.startswith('{"event":"sample",'): continue
                        s=json.loads(line)
                        if start<=s['wall_ms']<=end: worker_samples.append(s)
            rtts=[v for r in reports for v in r['rtts']];sent=sum(r['sent'] for r in reports);expected=sum(r['expected_samples'] for r in reports)
            seconds=(end-start)/1000;applied=after['totals']['updates']-before['totals']['updates']
            row={'target':n,'phase':phase,'start_wall_ms':start,'end_wall_ms':end,'seconds':seconds,'live':sum(r['live'] for r in reports),
                'admission_errors':admission_errors,'sent':sent,'applied':applied,'applied_per_second':applied/seconds,
                'offered_ratio':sum(r['sent']/(r['seconds']*10) for r in reports)/n,'applied_ratio':applied/(seconds*n*10),
                'ack_expected':expected,'ack_matched':len(rtts),'ack_ratio':len(rtts)/expected if expected else 0,
                'rtt_p50_ms':quantile(rtts,.5),'rtt_p95_ms':quantile(rtts,.95),'rtt_p99_ms':quantile(rtts,.99),
                'drops':sum(r['drops'] for r in reports),'errors':sum(r['errors'] for r in reports),
                'skipped_ratio':sum(r['skipped'] for r in reports)/(n*seconds*10),
                'schedule_p95_ms':max(r['lateness_p95_ms'] or 0 for r in reports),'worker_loop_p95_ms':quantile([s['loop_p99_ms'] for s in worker_samples],.95) or 0,
                'server_cpu_mean':sum(s['processes']['server']['cpu_one_core_pct'] for s in resource)/len(resource) if resource else None,
                'server_cpu_p95':quantile([s['processes']['server']['cpu_one_core_pct'] for s in resource],.95),
                'server_rss_max':max(s['processes']['server']['rss'] for s in resource) if resource else None,
                'server_loop_p95_ms':quantile([s['loop_p99_ms'] for s in server_samples],.95),
                'server_loop_utilization_mean':sum(s['loop_utilization'] for s in server_samples)/len(server_samples) if server_samples else None,
                'server_bytes_per_second':(after['totals']['bytes']-before['totals']['bytes'])/seconds,
                'rooms':after['rooms'],'server_before':before,'server_after':after,
                'worker_cpu_p95':quantile([s['cpu_one_core_pct'] for s in worker_samples],.95)}
            row.update(evaluate(row,args.p95_limit_ms,args.p99_limit_ms));stages.append(row);save(folder/f'stage-{n}.json',row);save(folder/'stages.json',stages)
            print(f"RESULT {n}: pass={row['passed']} generator_valid={row['generator_valid']} RTT_p95={row['rtt_p95_ms']:.2f}ms applied/s={row['applied_per_second']:.1f} server_cpu={row['server_cpu_mean']:.1f}% rooms={len(row['rooms'])} failures={row['failed_criteria']+row['generator_issues']}",flush=True)
            event('hold_result',target=n,passed=row['passed']);last_profile_needed=(row['server_cpu_mean'] or 0)>=70 or not row['passed']
            if not row['passed']: break
        if args.profile_seconds and stages and (last_profile_needed or stages[-1]['target']>=300):
            print('DIAGNOSTIC CPU profile (outside measured hold)',flush=True)
            r=control.post(base+'/__step/profile',json={'seconds':args.profile_seconds},timeout=args.profile_seconds+30);r.raise_for_status();profile_file=r.json()['file']
    except Exception as e:
        import traceback
        fatal={'type':type(e).__name__,'message':str(e),'traceback':traceback.format_exc()};save(folder/'fatal-error.json',fatal);print('EXPERIMENT ERROR: '+str(e),flush=True)
    finally:
        cleanup={'database':dbname,'worker_exit_codes':[]}
        for w in workers:
            try: w.call('stop',timeout=15);w.process.wait(timeout=10)
            except Exception as e:
                cleanup.setdefault('worker_stop_errors',[]).append(str(e))
                if w.process.poll() is None: w.process.terminate();w.process.wait(timeout=10)
            cleanup['worker_exit_codes'].append(w.process.returncode);w.stderr.close()
        if server:
            try:
                if server.poll() is None:
                    save(folder/'final-server.json',snap(True))
                    control.post(base+'/__step/shutdown',timeout=5).raise_for_status();server.wait(timeout=15)
            except Exception as e:
                cleanup['server_stop_error']=str(e)
                if server.poll() is None:server.terminate();server.wait(timeout=10)
            cleanup['server_exit_code']=server.returncode
        stopped.set()
        if sampler:sampler.join(timeout=3)
        accounts=[]
        for f in folder.glob('worker-*-accounts.jsonl'):
            accounts.extend(json.loads(line) for line in f.read_text(encoding='utf-8').splitlines() if line.strip())
        try:
            query={'$or':accounts} if accounts else None
            before=db.user.count_documents({});owned=db.user.count_documents(query) if query else 0
            deleted=db.user.delete_many(query).deleted_count if query else 0
            remaining=db.user.count_documents({})
            cleanup.update(account_attempts=len(accounts),accounts_before=before,owned_before=owned,deleted=deleted,remaining=remaining,
                unrelated_untouched=remaining==before-owned,database_cleanup_ok=deleted==owned and remaining==before-owned)
        except Exception as e:cleanup.update(database_cleanup_ok=False,database_error=str(e))
        cleanup['source_unchanged']=all(hashlib.sha256(f.read_bytes()).hexdigest()==hashes[n] for n,f in source.items())
        cleanup['ok']=bool(cleanup.get('database_cleanup_ok') and cleanup['source_unchanged'] and cleanup.get('server_exit_code')==0 and all(c==0 for c in cleanup['worker_exit_codes']))
        save(folder/'cleanup.json',cleanup);mongo.close();control.close();console.close();event('cleanup',**cleanup);events.close()
        summary={'run_id':run,'mode':'verify','complete_planned_steps':len(stages)==len(steps),'ended_at':datetime.now(timezone.utc).isoformat(),
            'highest_passed_target':max((s['target'] for s in stages if s['passed']),default=0),'first_nonpassing_target':next((s['target'] for s in stages if not s['passed']),None),
            'fatal_error':fatal,'guard_reasons':guard_reasons,'profile_file':profile_file,'cleanup':cleanup,'stages':stages}
        save(folder/'summary.json',summary)
        fields=['target','passed','generator_valid','live','seconds','sent','applied_per_second','offered_ratio','applied_ratio','ack_ratio','rtt_p50_ms','rtt_p95_ms','rtt_p99_ms','drops','errors','server_cpu_mean','server_cpu_p95','server_rss_max','server_loop_p95_ms','server_bytes_per_second','worker_cpu_p95','schedule_p95_ms','worker_loop_p95_ms','failed_criteria','generator_issues']
        with (folder/'results.csv').open('w',encoding='utf-8-sig',newline='') as out:
            writer=csv.DictWriter(out,fieldnames=fields,extrasaction='ignore');writer.writeheader();writer.writerows(stages)
    print(json.dumps({'run_dir':str(folder),'highest_passed_target':summary['highest_passed_target'],'first_nonpassing_target':summary['first_nonpassing_target'],'fatal_error':fatal,'cleanup':cleanup},ensure_ascii=False),flush=True)
    return folder


def main(argv=None):
    global ACTIVE_FOLDER
    ACTIVE_FOLDER=None
    from product import finalize, failure_report
    def interrupt(signum, frame): raise RuntimeError('cancelled by signal')
    signal.signal(signal.SIGINT,interrupt)
    if hasattr(signal,'SIGTERM'): signal.signal(signal.SIGTERM,interrupt)
    try:
        folder=_run(argv)
    except SystemExit as exc:
        if exc.code == 0:return 0
        folder=ACTIVE_FOLDER or ROOT/'reports/staircase'/('error_'+uuid.uuid4().hex[:12])
        return failure_report(folder,'invalid command arguments')
    except Exception as exc:
        folder=ACTIVE_FOLDER or ROOT/'reports/staircase'/('error_'+uuid.uuid4().hex[:12])
        if (folder/'ownership.json').exists():
            try:
                from managed import recover
                save(folder/'emergency-recovery.json',recover(folder))
            except Exception as recovery_error:
                save(folder/'emergency-recovery.json',{'ok':False,'error':str(recovery_error)})
        return failure_report(folder,str(exc))
    return finalize(folder)


if __name__=='__main__':raise SystemExit(main())
