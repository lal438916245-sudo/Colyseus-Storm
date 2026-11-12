"""Owned local processes and exact-account recovery, including forced exits."""
from pathlib import Path
import json
import os
import socket
import subprocess
import time
import uuid
import psutil
from pymongo import MongoClient
import requests
from product import save

ROOT=Path(__file__).resolve().parents[1]


def recover(folder):
    folder=Path(folder);results=[]
    if not (folder/'ownership.json').exists() and not (folder/'storm-run.json').exists():
        return {'ok':False,'runs':[],'error':'not a managed Storm run root'}
    for file in sorted(folder.rglob('ownership.json')):
        owner=json.loads(file.read_text(encoding='utf-8'));here=file.parent;errors=[]
        if not str(owner.get('database','')).startswith('storm_step_'):
            results.append({'ok':False,'error':'invalid owned database'});continue
        processes=owner.get('processes',[])
        for extra in here.glob('locust-processes-*.json'):
            processes+=json.loads(extra.read_text(encoding='utf-8'))
        for item in reversed(processes):
            try:
                proc=psutil.Process(item['pid'])
                if abs(proc.create_time()-item['created'])>.01:continue
                descendants=proc.children(recursive=True)
                for child in descendants:child.terminate()
                proc.terminate()
                _,alive=psutil.wait_procs(descendants+[proc],timeout=4)
                for process in alive:process.kill()
                _,alive=psutil.wait_procs(alive,timeout=4)
                if alive:errors.append('owned process did not exit')
            except psutil.NoSuchProcess:pass
            except psutil.Error as exc:errors.append(str(exc))
        accounts={}
        for pattern in ('worker-*-accounts.jsonl','locust_accounts_*.jsonl'):
            for registry in here.glob(pattern):
                for line in registry.read_text(encoding='utf-8').splitlines():
                    row=json.loads(line);account={k:row[k] for k in ('username','email')}
                    if not account['email'].endswith(('@step.invalid','@storm.invalid')):
                        errors.append('account ownership mismatch');continue
                    accounts[account['username']]=account
        deleted=remaining=0
        try:
            with MongoClient('mongodb://127.0.0.1:27017',serverSelectionTimeoutMS=3000) as client:
                users=client[owner['database']].user
                if accounts:
                    query={'$or':list(accounts.values())};deleted=users.delete_many(query).deleted_count;remaining=users.count_documents(query)
                total=users.count_documents({})
                if remaining or total:errors.append('owned database still has records')
        except Exception as exc:errors.append(str(exc))
        row={'run_id':owner['run_id'],'database':owner['database'],'deleted':deleted,'remaining':remaining,'errors':errors,'ok':not errors}
        save(here/'recovery.json',row);results.append(row)
    return {'ok':bool(results) and all(r['ok'] for r in results),'runs':results}


class LocalServer:
    def __init__(self,folder,node,server_root):
        self.folder=Path(folder);self.node=node;self.server_root=Path(server_root);self.process=None
        self.run=self.folder.name+'_'+uuid.uuid4().hex[:8]
        self.run=''.join(c if c.isalnum() or c=='_' else '_' for c in self.run)
        self.database='storm_step_'+self.run;self.token=uuid.uuid4().hex
        with socket.socket() as sock:sock.bind(('127.0.0.1',0));self.port=sock.getsockname()[1]
        self.base=f'http://127.0.0.1:{self.port}'
        self.http=requests.Session();self.http.trust_env=False;self.http.headers['Authorization']='Bearer '+self.token
    def own(self,process,kind):
        path=self.folder/'ownership.json';value=json.loads(path.read_text(encoding='utf-8'))
        value['processes'].append({'pid':process.pid,'created':psutil.Process(process.pid).create_time(),'kind':kind});save(path,value)
    def start(self):
        self.folder.mkdir(parents=True,exist_ok=True)
        save(self.folder/'ownership.json',{'run_id':self.run,'database':self.database,'processes':[]})
        self.console=(self.folder/'server-console.log').open('w',encoding='utf-8')
        env=dict(os.environ,STEP_SERVER_ROOT=str(self.server_root),STEP_DATABASE=self.database,STEP_MONGO_URI='mongodb://127.0.0.1:27017/'+self.database,
            STEP_PORT=str(self.port),STEP_TOKEN=self.token,STEP_RUN_ID=self.run,STEP_EVENTS=str(self.folder/'server-events.jsonl'),NODE_ENV='production')
        self.process=subprocess.Popen([self.node,str(ROOT/'experiments/step_server.cjs')],cwd=self.folder,env=env,stdout=self.console,stderr=subprocess.STDOUT,
            creationflags=subprocess.CREATE_NO_WINDOW if os.name=='nt' else 0)
        self.own(self.process,'server');deadline=time.monotonic()+40
        while time.monotonic()<deadline:
            if self.process.poll() is not None:raise RuntimeError('test server startup failed')
            try:
                response=self.http.get(self.base+'/__step/health',timeout=1);response.raise_for_status()
                if response.json()['database']!=self.database:raise RuntimeError('database mismatch')
                return self
            except requests.RequestException:time.sleep(.15)
        raise TimeoutError('test server readiness timed out')
    def close(self):
        try:
            if self.process and self.process.poll() is None:
                response=self.http.get(self.base+'/__step/snapshot',params={'details':'1'},timeout=5)
                save(self.folder/'final-server.json',response.json())
                self.http.post(self.base+'/__step/shutdown',timeout=5).raise_for_status();self.process.wait(timeout=15)
        finally:
            result=recover(self.folder)
            if hasattr(self,'console'):self.console.close()
            self.http.close()
        return result
