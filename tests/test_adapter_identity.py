"""Adapter identity guard: an adapter registers only under the ID its Claude process carries.

Pure logic plus a main() refusal check with the parent lookup stubbed out.
No broker, database, credential or live process is touched.
"""
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import claude_adapter

OWN = "11111111-2222-4333-8444-555555555555"
OTHER = "22222222-3333-4444-8555-666666666666"


class ClaudeSessionMatchesTests(unittest.TestCase):
    def test_session_id_flag_matches(self):
        line = 'C:\\Users\\example\\.local\\bin\\claude.exe --session-id ' + OWN + ' --mcp-config x.json'
        self.assertTrue(claude_adapter.claude_session_matches(line, OWN))

    def test_resume_flag_matches(self):
        line = 'claude.exe --resume ' + OWN
        self.assertTrue(claude_adapter.claude_session_matches(line, OWN))

    def test_quoted_id_matches(self):
        line = 'claude.exe --session-id "' + OWN + '"'
        self.assertTrue(claude_adapter.claude_session_matches(line, OWN))

    def test_other_session_is_refused(self):
        line = 'claude.exe --session-id ' + OTHER
        self.assertFalse(claude_adapter.claude_session_matches(line, OWN))

    def test_id_in_unrelated_argument_is_not_a_match(self):
        line = 'claude.exe --name ' + OWN
        self.assertFalse(claude_adapter.claude_session_matches(line, OWN))

    def test_forked_chat_without_flag_is_refused(self):
        line = 'claude.exe --fork-session --resume 33333333-4444-4555-8666-777777777777'
        self.assertFalse(claude_adapter.claude_session_matches(line, OWN))

    def test_empty_or_unreadable_parent_is_refused(self):
        self.assertFalse(claude_adapter.claude_session_matches('', OWN))
        self.assertFalse(claude_adapter.claude_session_matches(None, OWN))


class LiveSessionIdTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.sessions = Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def write_record(self, pid, payload):
        (self.sessions / "{}.json".format(pid)).write_text(json.dumps(payload), encoding="utf-8")

    def test_reads_session_id_from_record(self):
        self.write_record(4321, {"pid": 4321, "sessionId": OWN})
        self.assertEqual(OWN, claude_adapter.live_session_id(4321, self.sessions))

    def test_follows_resumed_window_to_its_new_session_id(self):
        self.write_record(4321, {"pid": 4321, "sessionId": OTHER})
        self.assertEqual(OTHER, claude_adapter.live_session_id(4321, self.sessions))

    def test_missing_record_returns_empty(self):
        self.assertEqual("", claude_adapter.live_session_id(9999, self.sessions))

    def test_malformed_record_returns_empty(self):
        (self.sessions / "4321.json").write_text("{not json", encoding="utf-8")
        self.assertEqual("", claude_adapter.live_session_id(4321, self.sessions))
        self.write_record(4321, {"sessionId": "bad id with spaces"})
        self.assertEqual("", claude_adapter.live_session_id(4321, self.sessions))


@unittest.skipUnless(os.name == "nt", "the parent-process check runs only on Windows")
class MainUsesLiveRecordTests(unittest.TestCase):
    def test_record_mismatch_refuses(self):
        with mock.patch.object(claude_adapter, "live_session_id", return_value=OTHER), \
                mock.patch.object(claude_adapter, "BrokerClient") as broker:
            result = claude_adapter.main(["--session-id", OWN])
        self.assertEqual(3, result)
        broker.assert_not_called()

    def test_record_match_proceeds(self):
        with mock.patch.object(claude_adapter, "live_session_id", return_value=OWN), \
                mock.patch.object(claude_adapter, "BrokerClient",
                                  side_effect=claude_adapter.BrokerError("stub")) as broker:
            result = claude_adapter.main(["--session-id", OWN])
        broker.assert_called_once()
        self.assertEqual(1, result)


@unittest.skipUnless(os.name == "nt", "the parent-process check runs only on Windows")
class MainRefusesMismatchTests(unittest.TestCase):
    def test_mismatched_parent_refuses_before_registering(self):
        with mock.patch.object(claude_adapter, "parent_command_line",
                               return_value="claude.exe --session-id " + OTHER), \
                mock.patch.object(claude_adapter, "BrokerClient") as broker:
            result = claude_adapter.main(["--session-id", OWN])
        self.assertEqual(3, result)
        broker.assert_not_called()

    def test_matching_parent_proceeds_to_broker_startup(self):
        with mock.patch.object(claude_adapter, "parent_command_line",
                               return_value="claude.exe --session-id " + OWN), \
                mock.patch.object(claude_adapter, "BrokerClient",
                                  side_effect=claude_adapter.BrokerError("stub")) as broker:
            result = claude_adapter.main(["--session-id", OWN])
        broker.assert_called_once()
        self.assertEqual(1, result)


if __name__ == "__main__":
    unittest.main()
