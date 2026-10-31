import io
import json
import unittest
from unittest.mock import patch

import monitor


class MonitorTests(unittest.TestCase):
    def test_single_check_reports_only_an_explicit_pid_and_marks_health_unknown(self):
        events = []
        checked = []
        monitor.monitor_pids(
            [1234], status_check=lambda pid: checked.append(pid) or "alive",
            emit=events.append, timestamp=lambda: "2026-10-31T00:00:00Z",
        )

        self.assertEqual([1234], checked)
        self.assertEqual(1, len(events))
        self.assertEqual(
            {
                "event": "process_status",
                "pid": 1234,
                "process_status": "alive",
                "previous_process_status": None,
                "agent_health": "unknown",
                "health_reason": monitor.HEALTH_UNKNOWN_REASON,
                "observed_at": "2026-10-31T00:00:00Z",
            },
            events[0],
        )

    def test_repeated_checks_emit_initial_and_changed_status_events_only(self):
        events = []
        statuses = iter(["alive", "alive", "not_running", "not_running"])
        sleeps = []
        monitor.monitor_pids(
            [42], interval=0.25, checks=4,
            status_check=lambda _pid: next(statuses), emit=events.append,
            sleep=sleeps.append, timestamp=lambda: "t",
        )

        self.assertEqual([0.25, 0.25, 0.25], sleeps)
        self.assertEqual(["process_status", "process_status_changed"],
                         [event["event"] for event in events])
        self.assertEqual(["alive", "not_running"],
                         [event["process_status"] for event in events])
        self.assertIsNone(events[0]["previous_process_status"])
        self.assertEqual("alive", events[1]["previous_process_status"])
        self.assertTrue(all(event["agent_health"] == "unknown" for event in events))

    def test_unknown_status_is_preserved_and_not_reported_as_exited_or_unhealthy(self):
        events = []
        monitor.monitor_pids([9], status_check=lambda _pid: "unknown", emit=events.append)

        self.assertEqual("unknown", events[0]["process_status"])
        self.assertEqual("unknown", events[0]["agent_health"])

    def test_duplicate_pids_are_checked_once_per_round(self):
        events = []
        checked = []
        monitor.monitor_pids(
            [8, 8, 9], status_check=lambda pid: checked.append(pid) or "alive",
            emit=events.append,
        )

        self.assertEqual([8, 9], checked)
        self.assertEqual([8, 9], [event["pid"] for event in events])

    def test_pid_and_polling_options_are_validated(self):
        for bad_pid in (0, -1, True, "12"):
            with self.subTest(pid=bad_pid), self.assertRaises(ValueError):
                monitor.monitor_pids([bad_pid], emit=lambda _event: None)
        for kwargs in (
            {"pids": []},
            {"pids": [1], "checks": 2},
            {"pids": [1], "interval": 0},
            {"pids": [1], "interval": float("nan")},
            {"pids": [1], "interval": 1, "checks": 0},
        ):
            pids = kwargs.pop("pids")
            with self.subTest(options=kwargs), self.assertRaises(ValueError):
                monitor.monitor_pids(pids, emit=lambda _event: None, **kwargs)

    def test_os_process_check_is_tri_state(self):
        with patch("monitor.os.name", "posix"), patch("monitor.os.kill") as kill:
            self.assertEqual("alive", monitor.process_status(1))
            kill.side_effect = PermissionError()
            self.assertEqual("alive", monitor.process_status(1))
            kill.side_effect = ProcessLookupError()
            self.assertEqual("not_running", monitor.process_status(1))
            kill.side_effect = OSError("indeterminate")
            self.assertEqual("unknown", monitor.process_status(1))

    def test_windows_uses_windows_process_check(self):
        with patch("monitor.os.name", "nt"), \
                patch("monitor._windows_process_status", return_value="unknown") as windows_check, \
                patch("monitor.os.kill") as kill:
            self.assertEqual("unknown", monitor.process_status(1234))

        windows_check.assert_called_once_with(1234)
        kill.assert_not_called()

    def test_cli_requires_explicit_pid_and_emits_json_events_only(self):
        output = io.StringIO()
        with patch("monitor._emit_event", side_effect=lambda event: print(
                json.dumps(event, sort_keys=True, separators=(",", ":")), file=output)), \
                patch("monitor.process_status", return_value="alive"):
            code = monitor.main(["--pid", "77"])

        self.assertEqual(0, code)
        record = json.loads(output.getvalue())
        self.assertEqual(77, record["pid"])
        self.assertEqual("alive", record["process_status"])
        self.assertEqual("unknown", record["agent_health"])
        self.assertEqual(1, len(output.getvalue().splitlines()))

    def test_cli_invalid_arguments_do_not_start_monitoring(self):
        with patch("monitor.monitor_pids") as run_monitor:
            self.assertEqual(2, monitor.main([]))
            run_monitor.assert_not_called()


if __name__ == "__main__":
    unittest.main()
