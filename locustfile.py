"""Colyseus player lifecycle with verified admission and owned-account teardown."""
import random
import socket
import time
from urllib.parse import urlparse
import websocket
import msgpack
from locust import HttpUser, task, between, events
from locust.exception import StopUser
from storm_locust import Registry, receive_join

ROOM_DATA = 13
DRAIN_MAX_FRAMES = 256


def encode_colyseus_message(message_type, payload):
    return bytes([ROOM_DATA])+msgpack.packb(message_type)+msgpack.packb(payload)


def drain_inbound(ws):
    """Consume control frames (automatic pong); never swallow business rejection."""
    frames=nbytes=0;previous=ws.gettimeout();ws.settimeout(0)
    try:
        while frames<DRAIN_MAX_FRAMES:
            try:frame=ws.recv()
            except (BlockingIOError,websocket.WebSocketTimeoutException):break
            if not frame:raise RuntimeError('WebSocket closed during gameplay')
            if isinstance(frame,bytes) and frame[0]==11:raise RuntimeError('Colyseus business error: '+frame.hex())
            frames+=1
            if isinstance(frame,(bytes,bytearray)):nbytes+=len(frame)
    finally:
        try:ws.settimeout(previous)
        except Exception:pass
    return frames,nbytes


@events.test_start.add_listener
def start_registry(environment,**kwargs):
    from locust.runners import MasterRunner
    if isinstance(environment.runner,MasterRunner):return
    try:environment.storm_registry=Registry(environment)
    except Exception as exc:
        environment.storm_start_error=str(exc);environment.process_exit_code=3
        import gevent
        gevent.spawn(environment.runner.quit)


@events.test_stop.add_listener
def stop_registry(environment,**kwargs):
    registry=getattr(environment,'storm_registry',None)
    if registry:
        result=registry.close()
        if not result['ok']:environment.process_exit_code=3


class GamePlayerBot(HttpUser):
    wait_time=between(1,3)
    host='http://localhost:2567'

    def _ws_base(self):
        base=self.host.rstrip('/')
        return ('wss://'+base[8:]) if base.startswith('https://') else 'ws://'+base[7:]

    def on_start(self):
        self.ws=None;self.seat=None;self.round=0;self.cooldown=0
        self.registry=getattr(self.environment,'storm_registry',None)
        if not self.registry:raise StopUser(getattr(self.environment,'storm_start_error','registry unavailable'))
        self.client.trust_env=False
        self.account=self.registry.account();self.password='Storm_Test_Only_2026!'
        self.seat=self._request_seat('/users/signup',register=True)

    def _request_seat(self,path,register=False):
        valid=None
        with self.client.post(path,json={**self.account,'password':self.password},catch_response=True) as response:
            try:
                if response.status_code!=200:raise AssertionError(f'HTTP {response.status_code}')
                output=response.json()['output'];seat=output['seatReservation']
                if output['user']['username']!=self.account['username'] or not all([seat['sessionId'],seat['room']['roomId'],seat['room']['processId']]):
                    raise AssertionError('account or seat mismatch')
                valid=seat;response.success()
                if register:self.registry.registered.add(self.account['username'])
            except Exception as exc:response.failure(str(exc))
        if valid is None:raise StopUser('account/seat request failed')
        return valid

    def _event(self,name,started,error=None,length=0):
        self.environment.events.request.fire(request_type='WebSocket',name=name,response_time=(time.monotonic()-started)*1000,
            response_length=length,exception=error)

    @task
    def play_game(self):
        self.round+=1
        if self.round>1:
            time.sleep(max(0,self.cooldown-time.monotonic()))
            self.seat=self._request_seat('/users/login')
        seat=self.seat;sid=seat['sessionId'];room=seat['room'];raw=None;failure=None
        started=time.monotonic();joined=False
        try:
            base=urlparse(self.host)
            raw=socket.create_connection((base.hostname,base.port or (443 if base.scheme=='https' else 80)),timeout=10)
            self.ws=websocket.create_connection(self._ws_base()+f"/{room['processId']}/{room['roomId']}?sessionId={sid}",socket=raw,timeout=10)
            self._event('[1] Transport connect',started)
            started=time.monotonic()
            receive_join(self.ws,sid,self.account['username'],self.registry)
            self._event('[2] Join room (identity verified)',started);joined=True
            self.registry.emit('round_start',session_id=sid,room_id=room['roomId'],username=self.account['username'])
            until=time.monotonic()+random.randint(10,20)
            while time.monotonic()<until:
                sent=time.monotonic()
                body=encode_colyseus_message('entityUpdate',[sid,'xPos',round(random.uniform(-100,100),2),'yPos',0,'zPos',round(random.uniform(-100,100),2)])
                self.ws.send(body,opcode=websocket.ABNF.OPCODE_BINARY)
                self._event('[3] Send entityUpdate (local send)',sent,length=len(body))
                self.registry.emit('entity_sent',session_id=sid,bytes=len(body))
                drain_inbound(self.ws);time.sleep(.1)
        except Exception as exc:
            failure=exc
            self._event('[4] Gameplay' if joined else '[2] Join room (identity verified)',started,exc)
            self.registry.emit('round_error',session_id=sid,message=str(exc))
        finally:
            try:
                if self.ws:
                    if self.ws.connected:self.ws.send(b'\x0c',opcode=websocket.ABNF.OPCODE_BINARY)
                    self.ws.close(timeout=2)
            except Exception as exc:
                self._event('[5] Connection cleanup',time.monotonic(),exc)
                self.registry.emit('connection_cleanup_error',message=str(exc),primary_error=str(failure) if failure else None)
            finally:
                if self.ws:self.ws.shutdown();self.ws=None
                if raw:raw.close()
                self.cooldown=time.monotonic()+30
        if failure and not joined:raise StopUser('room admission failed')

    def on_stop(self):
        if self.ws:
            try:self.ws.close(timeout=1)
            finally:self.ws.shutdown();self.ws=None
