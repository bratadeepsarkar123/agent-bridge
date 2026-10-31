import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import codex_client
import status


class FakeBrokerClient:
    def __init__(self, discovery_file, queues=None):
        self.discovery_file = Path(discovery_file)
        self.queues = queues or {}
        self.polled = []
        self.discovered = 0

    def poll(self, session_id):
        self.polled.append(session_id)
        return self.queues[session_id]

    def discover(self):
        self.discovered += 1
        return {"sessions": []}


class StatusTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.discovery_file = Path(self.tempdir.name) / "broker-session.json"
        self.discovery_file.write_text(json.dumps({"pid": 1234}), encoding="utf-8")
        self.client = FakeBrokerClient(self.discovery_file)

    def test_reports_only_counts_for_explicit_ids(self):
        self.client.queues = {
            "recipient-a": {
                "messages": [
                    {"status": "queued", "text": "do not expose this body"},
                    {"status": "acknowledged", "text": "or this body"},
                    {"status": "queued", "text": "nor this one"},
                ]
            }
        }
        with patch("status._process_is_alive", return_value=True):
            result = status.inspect_status(["recipient-a"], self.client)

        self.assertTrue(result["broker"]["process_alive"])
        self.assertTrue(result["broker"]["reachable"])
        self.assertEqual(
            result["queues"],
            {"recipient-a": {"count": 3, "statuses": {"queued": 2, "acknowledged": 1}}},
        )
        self.assertEqual(self.client.polled, ["recipient-a"])
        self.assertEqual(self.client.discovered, 0)
        self.assertNotIn("do not expose", json.dumps(result))

    def test_no_ids_checks_reachability_without_reporting_registered_sessions(self):
        with patch("status._process_is_alive", return_value=False):
            result = status.inspect_status([], self.client)

        self.assertFalse(result["broker"]["process_alive"])
        self.assertTrue(result["broker"]["reachable"])
        self.assertEqual(result["queues"], {})
        self.assertEqual(self.client.discovered, 1)
        self.assertEqual(self.client.polled, [])

    def test_cli_emits_stable_summary_and_deduplicates_ids(self):
        self.client.queues = {"recipient-a": {"messages": [{"status": "queued"}]}}
        output = io.StringIO()
        with patch("status.codex_client.BrokerClient", return_value=self.client), \
                patch("status._process_is_alive", return_value=True), \
                patch("sys.stdout", output):
            code = status.main(["recipient-a", "--id", "recipient-a"])

        self.assertEqual(code, 0)
        self.assertEqual(
            output.getvalue(),
            '{"broker":{"process_alive":true,"reachable":true},"ok":true,'
            '"queues":{"recipient-a":{"count":1,"statuses":{"acknowledged":0,"queued":1}}}}\n',
        )
        self.assertEqual(self.client.polled, ["recipient-a"])

    def test_cli_errors_are_stable_and_do_not_echo_client_secrets(self):
        class FailingClient(FakeBrokerClient):
            def poll(self, _session_id):
                raise codex_client.ClientError("transport_error", "failed with secret-value")

        client = FailingClient(self.discovery_file)
        output = io.StringIO()
        with patch("status.codex_client.BrokerClient", return_value=client), \
                patch("status._process_is_alive", return_value=True), \
                patch("sys.stdout", output):
            code = status.main(["recipient-a"])

        self.assertEqual(code, 2)
        self.assertEqual(
            output.getvalue(),
            '{"error":{"code":"broker_unreachable","message":"Could not reach the local broker"},"ok":false}\n',
        )
        self.assertNotIn("secret-value", output.getvalue())

    def test_malformed_queue_response_has_a_stable_error(self):
        self.client.queues = {"recipient-a": {"messages": [{"status": "unexpected"}]}}
        with self.assertRaises(codex_client.ClientError) as raised:
            status.inspect_status(["recipient-a"], self.client)

        self.assertEqual(raised.exception.code, "invalid_response")

    def test_windows_liveness_check_never_uses_os_kill(self):
        with patch("status.os.name", "nt"), \
                patch("status._windows_process_is_alive", return_value=True) as windows_check, \
                patch("status.os.kill") as kill:
            self.assertTrue(status._process_is_alive(1234))

        windows_check.assert_called_once_with(1234)
        kill.assert_not_called()

    def test_missing_or_invalid_pid_fails_without_reading_queue(self):
        self.discovery_file.write_text(json.dumps({"pid": True}), encoding="utf-8")
        with self.assertRaises(codex_client.ClientError) as raised:
            status.inspect_status(["recipient-a"], self.client)

        self.assertEqual(raised.exception.code, "discovery_file_invalid")
        self.assertEqual(self.client.polled, [])


if __name__ == "__main__":
    unittest.main()
