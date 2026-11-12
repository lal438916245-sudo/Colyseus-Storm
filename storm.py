"""One-command verification, bounded capacity search and managed Locust runs."""
from pathlib import Path
import argparse
from datetime import datetime,timezone
import json
import os
import signal
import subprocess
import sys
import time
import uuid

ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT/'experiments'))
from product import save,finish,failure_report


def child(command,folder,cancel_root=None,env=None):
    process=subprocess.Popen(command,env=env,creationflags=subprocess.CREATE_NO_WINDOW if os.name=='nt' else 0)
    try:
        while process.poll() is None:
            if cancel_root and (cancel_root/'cancel.requested').exists():
                folder.mkdir(parents=True,exist_ok=True);(folder/'cancel.requested').touch()
            time.sleep(.2)
        return process.returncode
    except BaseException:
        folder.mkdir(parents=True,exist_ok=True);(folder/'cancel.requested').touch()
        try:process.wait(timeout=25)
        except subprocess.TimeoutExpired:
            process.terminate();process.wait(timeout=10)
            from managed import recover
            recover(folder)
        raise


def search(args,folder):
    folder.mkdir(parents=True,exist_ok=False);attempts=[];low=0;upper=None;number=args.start
    save(folder/'storm-run.json',{'mode':'search','schema':'storm/1.0'})
    def probe(n,hold,kind):
        path=folder/f'{len(attempts)+1:03}_{kind}_{n}'
        code=child([sys.executable,str(ROOT/'experiments/step_load.py'),'--steps',str(n),'--hold',str(hold),'--warmup',str(args.warmup),
            '--workers',str(args.workers),'--spawn-rate',str(args.spawn_rate),'--node',args.node,'--server-root',args.server_root,
            '--profile-seconds','0','--output',str(path),'--p95-limit-ms',str(args.p95_limit_ms),'--p99-limit-ms',str(args.p99_limit_ms)],path,folder)
        result=json.loads((path/'summary.json').read_text(encoding='utf-8'))
        attempts.append({'target':n,'kind':kind,'exit_code':code,'folder':path.name,'result':result})
        save(folder/'attempts.json',attempts)
        return code
    stop='';confirmed=0
    while True:
        code=probe(number,args.hold,'coarse')
        if code==3:stop='inconclusive';break
        if code==2:upper=number;break
        low=number
        if number==args.max_users:stop='lower_bound_only';break
        number=min(args.max_users,max(number+1,number*2))
    while not stop and upper-low>args.precision:
        number=(low+upper)//2;code=probe(number,args.hold,'refine')
        if code==3:stop='inconclusive';break
        if code==0:low=number
        else:upper=number
    if not stop:stop='bounded'
    if stop!='inconclusive' and low:
        code=probe(low,args.confirm_hold,'confirm')
        if code==0:confirmed=low
        elif code==3:stop='inconclusive'
        else:
            stop='bounded'
            upper=low
            candidates=sorted({a['target'] for a in attempts if a['exit_code']==0 and a['target']<upper},reverse=True)
            low=0
            for candidate in candidates:
                code=probe(candidate,args.confirm_hold,'confirm_fallback')
                if code==3:stop='inconclusive';break
                if code==0:confirmed=low=candidate;break
                upper=candidate
            if stop!='inconclusive' and not confirmed:stop='confirmation_failed'
            while stop!='inconclusive' and confirmed and upper-low>args.precision:
                number=(low+upper)//2;code=probe(number,args.confirm_hold,'long_refine')
                if code==3:stop='inconclusive';break
                if code==0:confirmed=low=number
                else:upper=number
    elif not low and stop=='bounded':stop='no_passing_level'
    clean=all(a['result'].get('cleanup',{}).get('ok') for a in attempts)
    valid=all(a['result'].get('audit',{}).get('passed') for a in attempts)
    code=3 if stop=='inconclusive' or not clean or not valid else (0 if confirmed>=args.min_capacity else 2)
    result={'run_id':folder.name,'mode':'search','search_status':stop,'confirmed_capacity':confirmed,'passed_lower':low,'failed_upper':upper,
        'precision':args.precision,'minimum_required_capacity':args.min_capacity,'stages':[r for a in attempts for r in a['result'].get('stages',[])],
        'attempts':[{k:v for k,v in a.items() if k!='result'} for a in attempts],'cleanup':{'ok':clean},'fatal_error':None}
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        fig,axis=plt.subplots(figsize=(9,4))
        for i,a in enumerate(attempts):
            rows=a['result'].get('stages',[])
            if rows:axis.scatter(a['target'],rows[-1]['rtt_p95_ms'],color='green' if a['exit_code']==0 else 'red',marker='*' if a['kind'].startswith(('confirm','long')) else 'o')
        axis.axhline(args.p95_limit_ms,ls='--',color='gray');axis.set(xlabel='Concurrent players',ylabel='Sampled business RTT p95 / ms',title='Independent search probes; stars = long confirmation')
        fig.tight_layout();fig.savefig(folder/'staircase.png',dpi=140);plt.close(fig)
    except Exception as exc:return finish(folder,result,{'passed':valid,'failures':[]},['plot: '+str(exc)],3)
    return finish(folder,result,{'passed':valid,'failures':[]},exit_code=code)


def main(argv=None):
    NODE=os.environ.get('STORM_NODE','F:/Colyseus_SDET_Project/Node/node.exe' if os.name=='nt' else 'node')
    SERVER=os.environ.get('STORM_SERVER_ROOT','F:/Colyseus_SDET_Project/colyseus-unity-demo/colyseus-game-client/Server')
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode',choices=['verify','search','locust','recover'])
    parser.add_argument('--output',type=Path)
    parser.add_argument('--steps',default='20,50,100')
    parser.add_argument('--hold',type=float,default=60)
    parser.add_argument('--warmup',type=float,default=10)
    parser.add_argument('--workers',type=int,default=4)
    parser.add_argument('--spawn-rate',type=float,default=20)
    parser.add_argument('--node',default=NODE)
    parser.add_argument('--server-root',default=str(SERVER))
    parser.add_argument('--p95-limit-ms',type=float,default=200)
    parser.add_argument('--p99-limit-ms',type=float,default=500)
    parser.add_argument('--profile-seconds',type=int,default=12)
    parser.add_argument('--start',type=int,default=100)
    parser.add_argument('--max-users',type=int,default=2000)
    parser.add_argument('--precision',type=int,default=100)
    parser.add_argument('--confirm-hold',type=float,default=300)
    parser.add_argument('--min-capacity',type=int,default=1)
    parser.add_argument('--users',type=int,default=10)
    parser.add_argument('--run-time',default='60s')
    args=parser.parse_args(argv)
    folder=(args.output or ROOT/'reports/runs'/(datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'_'+uuid.uuid4().hex[:8])).resolve()
    if args.mode!='recover' and folder.exists():
        print('Output already exists; historical results are never overwritten.',file=sys.stderr);return 3
    def cancel(signum,frame):
        folder.mkdir(parents=True,exist_ok=True);(folder/'cancel.requested').touch()
    signal.signal(signal.SIGINT,cancel)
    if hasattr(signal,'SIGTERM'):signal.signal(signal.SIGTERM,cancel)
    try:
        if args.mode=='recover':
            from managed import recover
            result=recover(folder)
            if not (folder/'summary.json').exists():
                finish(folder,{'run_id':folder.name,'mode':'recovery','stages':[],'cleanup':result,'fatal_error':{'message':'interrupted run; cleanup recovered'}},
                       {'passed':False,'failures':['measurement interrupted']},exit_code=3)
            print(json.dumps(result));return 0 if result['ok'] else 3
        if args.mode=='verify':
            code=child([sys.executable,str(ROOT/'experiments/step_load.py'),'--steps',args.steps,'--hold',str(args.hold),'--warmup',str(args.warmup),
                '--workers',str(args.workers),'--spawn-rate',str(args.spawn_rate),'--node',args.node,'--server-root',args.server_root,
                '--profile-seconds',str(args.profile_seconds),'--output',str(folder),'--p95-limit-ms',str(args.p95_limit_ms),'--p99-limit-ms',str(args.p99_limit_ms)],folder,folder)
            return code if code in (0,2,3) and (folder/'summary.json').exists() else failure_report(folder,'worker did not produce a finalized result')
        if args.mode=='search':
            if not (1<=args.start<=args.max_users<=5000 and 1<=args.precision<=5000 and 1<=args.min_capacity<=args.max_users
                    and 10<=args.hold<=600 and 10<=args.confirm_hold<=600):raise ValueError('invalid bounded search parameters')
            return search(args,folder)
        import re
        duration=re.fullmatch(r'(\d+)([smh]?)',args.run_time)
        if not (1<=args.users<=1000 and 1<=args.spawn_rate<=40 and duration and 1<=int(duration[1])*{'':1,'s':1,'m':60,'h':3600}[duration[2]]<=600):
            raise ValueError('locust requires users 1..1000, spawn-rate 1..40 and duration 1..600 seconds')
        from locust_product import run
        return run(args,folder)
    except Exception as exc:
        if folder.exists() and ((folder/'ownership.json').exists() or (folder/'storm-run.json').exists()):
            from managed import recover
            recover(folder)
        return failure_report(folder,str(exc))


if __name__=='__main__':
    try:code=main()
    except SystemExit as exc:
        code=0 if exc.code==0 else failure_report(ROOT/'reports/runs'/('error_'+uuid.uuid4().hex[:12]),'invalid command arguments')
    raise SystemExit(code)
