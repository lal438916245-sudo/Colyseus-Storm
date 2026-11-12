"""Managed legacy lifecycle, strict join audit and fail-safe cleanup."""
import csv
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from managed import LocalServer,recover
from product import finish,save

ROOT=Path(__file__).resolve().parents[1]


def run(args,folder):
    folder.mkdir(parents=True,exist_ok=False);service=LocalServer(folder,args.node,args.server_root)
    process=None;fatal=None;exit_code=3;cleanup={'ok':False}
    try:
        sources=[ROOT/'locustfile.py',ROOT/'storm_locust.py',ROOT/'storm.py',ROOT/'analyze_locust.py',ROOT/'requirements.txt',*sorted((ROOT/'experiments').glob('*.py')),*sorted((ROOT/'experiments').glob('*.cjs'))]
        hashes={str(p.relative_to(ROOT)).replace('\\','/'):hashlib.sha256(p.read_bytes()).hexdigest() for p in sources}
        for name in hashes:
            target=folder/'source-snapshot'/name;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes((ROOT/name).read_bytes())
        save(folder/'manifest.json',{'run_id':service.run,'mode':'locust','users':args.users,'run_time':args.run_time,'spawn_rate':args.spawn_rate,'source_sha256':hashes,'python':sys.version})
        service.start()
        env=dict(os.environ,STORM_OUTPUT=str(folder),STORM_RUN_ID=service.run,STORM_MONGO_URI='mongodb://127.0.0.1:27017',
                 STORM_DATABASE=service.database,STORM_NODE=args.node,STORM_SERVER_ROOT=args.server_root,PYTHONIOENCODING='utf-8')
        command=[sys.executable,'-m','locust','-f',str(ROOT/'locustfile.py'),'--headless','-u',str(args.users),'-r',str(args.spawn_rate),
                 '--host',service.base,'--run-time',args.run_time,'--stop-timeout','5','--csv',str(folder/'locust'),'--html',str(folder/'locust.html')]
        process=subprocess.Popen(command,env=env,cwd=ROOT,creationflags=subprocess.CREATE_NO_WINDOW if os.name=='nt' else 0)
        service.own(process,'locust')
        while process.poll() is None:
            if (folder/'cancel.requested').exists():
                process.terminate();process.wait(timeout=10);raise RuntimeError('cancelled by operator')
            time.sleep(.2)
        exit_code=process.returncode
    except Exception as exc:fatal={'message':str(exc)}
    finally:
        if process and process.poll() is None:process.terminate();process.wait(timeout=10)
        try:cleanup=service.close()
        except Exception as exc:cleanup={'ok':False,'error':str(exc),'recovery':recover(folder)}
    checks=[];events=[]
    if 'hashes' not in locals() or any(hashlib.sha256((ROOT/name).read_bytes()).hexdigest()!=digest for name,digest in hashes.items()):checks.append('source changed during run')
    for path in folder.glob('locust-events-*.jsonl'):
        events.extend(json.loads(line) for line in path.read_text(encoding='utf-8').splitlines())
    verified=sum(r['event']=='join_verified' for r in events);sent=sum(r['event']=='entity_sent' for r in events)
    teardown=[json.loads(p.read_text(encoding='utf-8')) for p in folder.glob('locust-cleanup-*.json')]
    final=json.loads((folder/'final-server.json').read_text(encoding='utf-8')) if (folder/'final-server.json').exists() else {}
    applied=final.get('totals',{}).get('updates',0)
    for ok,message in [(verified>0,'no verified joins'),(sent>0,'no gameplay sends'),(sent==applied,'send/application count mismatch'),
                       (bool(teardown) and all(r['ok'] for r in teardown),'test_stop cleanup missing or failed'),(cleanup.get('ok'),'owned-resource cleanup failed')]:
        if not ok:checks.append(message)
    requests=failures=0
    stats=folder/'locust_stats.csv'
    if stats.exists():
        with stats.open(encoding='utf-8-sig',newline='') as raw:
            for row in csv.DictReader(raw):
                if row['Name']=='Aggregated':requests=int(row['Request Count']);failures=int(row['Failure Count'])
    if not requests:checks.append('empty Locust request statistics')
    code=3 if fatal or checks or exit_code not in (0,1) else (2 if failures or exit_code else 0)
    summary={'run_id':service.run,'mode':'locust','complete_planned_steps':not fatal,'fatal_error':fatal,'cleanup':cleanup,'stages':[],
             'requests':requests,'failures':failures,'verified_joins':verified,'sent_updates':sent,'server_applied_updates':applied,
             'metric_scope':'Locust local send and verified room-admission timings, not sampled capacity RTT.'}
    # Standard-library audit plot; no pandas or GUI backend.
    try:
        sys.path.insert(0,str(ROOT))
        from analyze_locust import analyze_locust_data
        analyze_locust_data(folder/'locust_stats_history.csv',folder/'locust_stats.csv',folder/'locust_failures.csv',folder)
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        fig,axis=plt.subplots(figsize=(8,3));axis.bar(['Identity verified joins','Sent updates','Server applied updates'],[verified,sent,applied])
        axis.set_title('Verified lifecycle counts');fig.tight_layout();fig.savefig(folder/'staircase.png',dpi=140);plt.close(fig)
    except Exception as exc:checks.append('plot: '+str(exc));code=3
    save(folder/'cleanup.json',cleanup)
    return finish(folder,summary,{'passed':not checks,'failures':checks,'verified_joins':verified,'sent':sent,'applied':applied},exit_code=code)
