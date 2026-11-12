import unittest
from step_load import evaluate


class CapacityJudgementTests(unittest.TestCase):
    def good(self):
        return dict(target=200,live=200,admission_errors=0,drops=0,errors=0,offered_ratio=1,
            skipped_ratio=0,schedule_p95_ms=4,worker_loop_p95_ms=12,applied_ratio=1,
            ack_ratio=1,rtt_p95_ms=20,rtt_p99_ms=40)

    def test_healthy_multiple_rooms_can_pass(self):
        self.assertTrue(evaluate(self.good())['passed'])

    def test_insufficient_live_clients_cannot_pass(self):
        row=self.good();row['live']=199
        self.assertIn('live_connections',evaluate(row)['failed_criteria'])

    def test_passing_send_rate_without_business_progress_fails(self):
        row=self.good();row['applied_ratio']=.5
        self.assertFalse(evaluate(row)['passed'])

    def test_missing_sampled_acks_cannot_look_fast(self):
        row=self.good();row.update(ack_ratio=.8,rtt_p95_ms=1)
        self.assertIn('sampled_application_ack_coverage',evaluate(row)['failed_criteria'])

    def test_load_generator_failure_is_separate(self):
        row=self.good();row['offered_ratio']=.8
        result=evaluate(row)
        self.assertFalse(result['generator_valid']);self.assertFalse(result['passed'])
        self.assertEqual(result['failed_criteria'],[])

    def test_slow_server_rtt_is_not_a_generator_failure(self):
        row=self.good();row['rtt_p95_ms']=250
        result=evaluate(row)
        self.assertTrue(result['generator_valid']);self.assertFalse(result['passed'])


if __name__=='__main__':unittest.main()
