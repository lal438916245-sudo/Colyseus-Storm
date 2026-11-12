"""Guard against mistaking setup failures or censored observations for proof."""
import unittest

try:
    from heartbeat_ab import classify_trial, summarize
except ModuleNotFoundError as exc:
    if exc.name != 'heartbeat_ab':
        raise
    raise unittest.SkipTest('Optional historical heartbeat experiment is not included in this release') from exc


class HeartbeatEvidenceTests(unittest.TestCase):
    def row(self, arm="read_off", classification="heartbeat_timeout"):
        return {"arm": arm, "classification": classification, "precondition_ok": True,
                "phase_started_wall_ms": 1000, "phase_ended_wall_ms": 10100,
                "reached_target": False, "business_verified": True, "cleanup_ok": True,
                "server_final": {"open": False, "termination_count": 1, "terminate_wall_ms": 10000, "terminate_ping_count": 2},
                "server_termination_seconds": 9, "phase_server_pings": 2, "phase_server_pongs": 0,
                "server_phase_updates": 90}

    def test_setup_failure_cannot_be_a_heartbeat_result(self):
        row = self.row()
        row["precondition_ok"] = False
        self.assertEqual(classify_trial(row, 2), "invalid_precondition")

    def test_unrelated_termination_is_not_attributed_to_current_window(self):
        row = self.row()
        row["server_final"]["terminate_wall_ms"] = 500
        self.assertEqual(classify_trial(row, 2), "other_failure")

    def test_both_transport_threshold_and_server_termination_are_required(self):
        row = self.row()
        self.assertEqual(classify_trial(row, 2), "heartbeat_timeout")
        row["server_final"]["terminate_ping_count"] = 1
        self.assertEqual(classify_trial(row, 2), "other_failure")

    def test_target_requires_business_progress_and_live_server_socket(self):
        row = self.row("read_on", "target_reached")
        row.update(reached_target=True, server_final={"open": True})
        self.assertEqual(classify_trial(row, 2), "target_reached")
        row["business_verified"] = False
        self.assertEqual(classify_trial(row, 2), "other_failure")

    def test_censored_successes_are_not_reported_as_failure_lifetimes(self):
        off = self.row()
        on = self.row("read_on", "target_reached")
        on.update(phase_server_pings=20, phase_server_pongs=20)
        summary = summarize([off, on], 1, 60)
        self.assertTrue(summary["controlled_effect_confirmed"])
        self.assertIsNone(summary["groups"]["read_on"]["termination_seconds"])
        self.assertFalse(summarize([off, on], 30, 60)["controlled_effect_confirmed"])


if __name__ == "__main__":
    unittest.main()
