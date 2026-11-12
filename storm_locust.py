"""Strict room admission, per-worker ownership and exact MongoDB teardown."""
from pathlib import Path
import json
import os
import subprocess
import threading
import time
import uuid
from urllib.parse import urlparse

import psutil
from pymongo import MongoClient
import websocket

ROOT=Path(__file__).resolve().parent


def receive_join(ws,session_id,username,decoder,timeout=10):
    deadline=time.monotonic()+timeout
    def receive():
        remaining=deadline-time.monotonic()
        if remaining<=0:raise TimeoutError('room admission timed out')
        ws.settimeout(remaining);frame=ws.recv()
        if not isinstance(frame,bytes) or not frame:raise AssertionError('room closed before admission')
        if frame[0]==11:raise AssertionError('server rejected room admission: '+frame.hex())
        return frame
    join=receive()
    if join[0]!=10:raise AssertionError('expected JOIN_ROOM(10)')
    size=join[1]
    if join[2:2+size]!=b'schema':raise AssertionError('unsupported serializer')
    handshake=join[2+size:]
    if not handshake:raise AssertionError('missing reflected schema')
    ws.send(b'\x0a',opcode=websocket.ABNF.OPCODE_BINARY)
    state=receive()
    if state[0]!=14:raise AssertionError('expected full ROOM_STATE(14), not a patch')
    identity=decoder.check(handshake,state[1:],session_id,username)
    if identity.get('session_id')!=session_id or identity.get('username')!=username:raise AssertionError('decoded identity mismatch')
    return identity


class Registry:
    def __init__(self,environment):
        self.environment=environment;self.run=os.environ.get('STORM_RUN_ID') or uuid.uuid4().hex
        self.worker=uuid.uuid4().hex[:10];self.accounts={};self.registered=set();self.lock=threading.Lock();self.decoder_lock=threading.Lock()
        self.folder=Path(os.environ.get('STORM_OUTPUT',str(ROOT/'reports/locust'/self.run)));self.folder.mkdir(parents=True,exist_ok=True)
        self.database=os.environ.get('STORM_DATABASE','');self.uri=os.environ.get('STORM_MONGO_URI','mongodb://127.0.0.1:27017')
        parsed=urlparse(self.uri)
        if parsed.hostname not in ('127.0.0.1','localhost') or not self.database or self.database in ('admin','local','config'):
            raise ValueError('Explicit loopback STORM_MONGO_URI and STORM_DATABASE are required; use python storm.py locust for an isolated managed run')
        self.mongo=MongoClient(self.uri,serverSelectionTimeoutMS=3000);self.mongo.admin.command('ping')
        self.server_root=os.environ.get('STORM_SERVER_ROOT','F:/Colyseus_SDET_Project/colyseus-unity-demo/colyseus-game-client/Server')
        node=os.environ.get('STORM_NODE','F:/Colyseus_SDET_Project/Node/node.exe' if os.name=='nt' else 'node')
        self.stderr=(self.folder/f'locust-decoder-{self.worker}.log').open('w',encoding='utf-8')
        self.decoder=subprocess.Popen([node,str(ROOT/'experiments/schema_decoder.cjs')],env=dict(os.environ,STORM_SERVER_ROOT=self.server_root),
            stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=self.stderr,text=True,encoding='utf-8',
            creationflags=subprocess.CREATE_NO_WINDOW if os.name=='nt' else 0)
        (self.folder/f'locust-processes-{self.worker}.json').write_text(json.dumps([{'pid':self.decoder.pid,'created':psutil.Process(self.decoder.pid).create_time(),'kind':'decoder'}]),encoding='utf-8')
        self.path=self.folder/f'locust_accounts_{self.run}_{self.worker}.jsonl'
        self.log=self.folder/f'locust-events-{self.worker}.jsonl'
        self.emit('start',database=self.database)
    def emit(self,event,**data):
        with self.lock:
            with self.log.open('a',encoding='utf-8') as out:out.write(json.dumps({'event':event,'wall_ms':time.time()*1000,**data},ensure_ascii=False)+'\n')
    def account(self):
        name='locust_'+self.run+'_'+uuid.uuid4().hex
        row={'username':name,'email':name+'@storm.invalid'};self.accounts[name]=row
        with self.lock:
            with self.path.open('a',encoding='utf-8') as out:out.write(json.dumps(row)+'\n')
        return row
    def check(self,handshake,state,sid,username):
        with self.decoder_lock:
            if self.decoder.poll() is not None:raise RuntimeError('schema decoder exited')
            self.decoder.stdin.write(json.dumps({'handshake':handshake.hex(),'state':state.hex(),'session_id':sid,'username':username})+'\n');self.decoder.stdin.flush()
            # gevent patches subprocess pipe I/O under Locust; one serialized RPC per join.
            import gevent
            with gevent.Timeout(10,TimeoutError('schema decoder timeout')):line=self.decoder.stdout.readline()
            row=json.loads(line)
            if not row.get('ok'):raise AssertionError(row.get('error','decode failed'))
        self.emit('join_verified',session_id=sid,username=username);return row
    def close(self):
        result={'ok':False,'registered':len(self.registered),'attempted':len(self.accounts)}
        try:
            if self.decoder.poll() is None:
                self.decoder.stdin.close()
                self.decoder.wait(timeout=5)
            # Stop sends first; let actual onLeave DB updates/reconnection timeout finish.
            import gevent
            gevent.sleep(3.5)
            users=self.mongo[self.database].user;query={'$or':list(self.accounts.values())} if self.accounts else None
            before=users.count_documents(query) if query else 0
            deleted=users.delete_many(query).deleted_count if query else 0
            remaining=users.count_documents(query) if query else 0
            if before<len(self.registered):raise RuntimeError('registered accounts absent from configured database; mapping/ownership cannot be verified')
            result.update(ok=remaining==0,owned_before=before,deleted=deleted,remaining=remaining)
        except Exception as exc:
            result['error']=str(exc);self.environment.process_exit_code=3
        finally:
            if self.decoder.poll() is None:self.decoder.terminate();self.decoder.wait(timeout=5)
            self.stderr.close();self.mongo.close()
            (self.folder/f'locust-cleanup-{self.worker}.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
        return result
