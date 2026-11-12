import importlib
import json
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT),str(ROOT/'experiments')]
from product import decide,finish,finalize
from storm_locust import receive_join
import storm


class GateTests(unittest.TestCase):
    def summary(self,passed=True):
        return dict(stages=[{'passed':passed,'generator_valid':True}],cleanup={'ok':True},complete_planned_steps=True,fatal_error=None)
    def test_exit_zero_requires_complete_pass(self):self.assertEqual(decide(self.summary(),True),0)
    def test_valid_business_failure_is_two(self):self.assertEqual(decide(self.summary(False),True),2)
    def test_audit_and_cleanup_override_business_failure(self):
        row=self.summary(False);self.assertEqual(decide(row,False),3)
        row['cleanup']['ok']=False;self.assertEqual(decide(row,True),3)
    def test_generator_limit_is_not_server_boundary(self):
        row=self.summary();row['stages'][0]['generator_valid']=False;self.assertEqual(decide(row,True),3)
    def test_empty_and_partial_runs_cannot_pass(self):
        row=self.summary();row['stages']=[];self.assertEqual(decide(row,True),3)
        row=self.summary();row['complete_planned_steps']=False;self.assertEqual(decide(row,True),3)
    def test_report_embeds_chart_and_seals_final_bytes(self):
        with tempfile.TemporaryDirectory() as name:
            folder=Path(name);(folder/'staircase.png').write_bytes(b'png-test')
            self.assertEqual(finish(folder,self.summary(),{'passed':True}),0)
            text=(folder/'report.html').read_text(encoding='utf-8');self.assertIn('data:image/png;base64,',text)
            import hashlib
            manifest=json.loads((folder/'ARTIFACTS.sha256.json').read_text())
            self.assertTrue(all(hashlib.sha256((folder/p).read_bytes()).hexdigest()==h for p,h in manifest.items()))
    def test_business_failure_explanation_does_not_pollute_data_audit(self):
        with tempfile.TemporaryDirectory() as name:
            row=self.summary(False);row['stages'][0]['failed_criteria']=['application_rtt_p95']
            audit={'passed':True,'failures':[]}
            self.assertEqual(finish(Path(name),row,audit),2)
            self.assertEqual(audit['failures'],[])


class JoinTests(unittest.TestCase):
    class Socket:
        def __init__(self,frames):self.frames=iter(frames);self.sent=[]
        def recv(self):return next(self.frames)
        def send(self,data,**kw):self.sent.append(data)
        def settimeout(self,seconds):pass
    class Decoder:
        def check(self,*args):return {'session_id':'seat','username':'owner'}
    def test_raw_ack_and_verified_full_state(self):
        ws=self.Socket([b'\x0a\x06schemaREFLECTION',b'\x0eSTATE'])
        receive_join(ws,'seat','owner',self.Decoder());self.assertEqual(ws.sent,[b'\x0a'])
    def test_patch_is_not_an_initial_state(self):
        ws=self.Socket([b'\x0a\x06schemaREFLECTION',b'\x0fPATCH'])
        with self.assertRaises(AssertionError):receive_join(ws,'seat','owner',self.Decoder())
    def test_wrong_identity_and_error_frames_fail(self):
        ws=self.Socket([b'\x0a\x06schemaREFLECTION',b'\x0eSTATE'])
        with self.assertRaises(AssertionError):receive_join(ws,'different','owner',self.Decoder())
        with self.assertRaises(AssertionError):receive_join(self.Socket([b'\x0bERROR']),'seat','owner',self.Decoder())


class CsvTests(unittest.TestCase):
    def test_locust_empty_percentile_window_is_a_gap(self):
        from analyze_locust import analyze_locust_data
        with tempfile.TemporaryDirectory() as name:
            folder=Path(name)
            (folder/'stats.csv').write_text('Name,Request Count,95%\nAggregated,10,3\n')
            (folder/'failures.csv').write_text('Name,Occurrences\n')
            (folder/'history.csv').write_text('Timestamp,Name,Requests/s,User Count,95%,Failures/s\n1,Aggregated,0,0,N/A,0\n2,Aggregated,10,2,3,0\n2,entityUpdate,99,2,100,0\n')
            result=analyze_locust_data(folder/'history.csv',folder/'stats.csv',folder/'failures.csv',folder)
            self.assertEqual(result['history_points'],2)
            self.assertTrue((folder/'colyseus_performance_report.png').is_file())


class RecoveryTests(unittest.TestCase):
    def test_unexpected_controller_exception_attempts_owned_recovery(self):
        import step_load
        with tempfile.TemporaryDirectory() as name:
            folder=Path(name);(folder/'ownership.json').write_text('{}')
            def broken(argv):
                step_load.ACTIVE_FOLDER=folder
                raise RuntimeError('injected unexpected teardown exception')
            with patch.object(step_load,'_run',broken),patch('managed.recover',return_value={'ok':True}) as recovery:
                self.assertEqual(step_load.main([]),3)
                recovery.assert_called_once_with(folder)
            self.assertTrue(json.loads((folder/'emergency-recovery.json').read_text())['ok'])


class SearchTests(unittest.TestCase):
    def execute(self,policy,max_users=16):
        parent=tempfile.TemporaryDirectory();self.addCleanup(parent.cleanup)
        folder=Path(parent.name)/'search';called=[]
        args=types.SimpleNamespace(start=2,max_users=max_users,precision=1,hold=10,confirm_hold=20,min_capacity=1,
            warmup=0,workers=1,spawn_rate=2,node='node',server_root='unused',p95_limit_ms=200,p99_limit_ms=500)
        def fake(command,path,cancel_root):
            n=int(command[command.index('--steps')+1]);hold=float(command[command.index('--hold')+1]);code=policy(n,hold)
            called.append((n,hold,code));path.mkdir()
            row={'target':n,'live':n,'seconds':hold,'passed':code==0,'generator_valid':code!=3,'rtt_p95_ms':1 if code==0 else 250,'rtt_p99_ms':2,'applied_per_second':n*10,'ack_ratio':1}
            (path/'summary.json').write_text(json.dumps({'stages':[row],'cleanup':{'ok':True},'audit':{'passed':True}}))
            return code
        with patch.object(storm,'child',fake):code=storm.search(args,folder)
        return code,json.loads((folder/'summary.json').read_text(encoding='utf-8')),called
    def test_finds_boundary_and_confirms(self):
        code,data,calls=self.execute(lambda n,h:0 if n<=7 else 2)
        self.assertEqual(code,0);self.assertEqual(data['confirmed_capacity'],7);self.assertEqual(data['failed_upper'],8)
        self.assertIn((7,20.0,0),calls)
    def test_cap_is_only_a_lower_bound(self):
        code,data,_=self.execute(lambda n,h:0,max_users=8)
        self.assertEqual(code,0);self.assertEqual(data['search_status'],'lower_bound_only');self.assertIsNone(data['failed_upper'])
    def test_invalid_load_stops_search_without_false_upper(self):
        code,data,_=self.execute(lambda n,h:0 if n<4 else 3)
        self.assertEqual(code,3);self.assertIsNone(data['failed_upper'])
    def test_long_confirmation_can_lower_the_short_result(self):
        code,data,_=self.execute(lambda n,h:0 if n<=(7 if h==10 else 5) else 2)
        self.assertEqual(code,0);self.assertEqual(data['confirmed_capacity'],5);self.assertEqual(data['failed_upper'],6)


if __name__=='__main__':unittest.main()
