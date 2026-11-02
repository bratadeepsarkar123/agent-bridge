"""Live identity switch: the adapter follows its window when /resume changes the chat ID.

Uses a fake broker and a temporary records folder. No broker, database or live process is touched.
"""
import io
import json
import tempfile
import unittest
from pathlib import Path

import claude_adapter

OLD = "11111111-2222-4333-8444-555555555555"
NEW = "66666666-7777-4888-8999-aaaaaaaaaaaa"
PARENT = 4321


class FakeBroker:
    def __init__(self, fail_register=False):
        self.registered = []
        self.sent = []
        self.fail_register = fail_register

    def register(self, session_id):
        if self.fail_register:
            raise claude_adapter.BrokerError("refused")
        self.registered.append(session_id)
        return {"ok": True}

    def send(self, sender, recipient, text, **options):
        self.sent.append((sender, recipient, text))
        return {"id": options.get("message_id"), "status": "queued"}

    def messages(self, recipient, status=None):
        return {"messages": []}

    def wait_for_messages(self, recipient, cursor, timeout=25):
        return {"messages": [], "cursor": cursor}


class SwitchTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.sessions = Path(self.tmp.name)
        self.err = io.StringIO()

    def tearDown(self):
        self.tmp.cleanup()

    def write_record(self, session_id):
        (self.sessions / "{}.json".format(PARENT)).write_text(
            json.dumps({"pid": PARENT, "sessionId": session_id}), encoding="utf-8")

    def make(self, broker=None):
        return claude_adapter.ClaudeAdapter(
            OLD, "codex", broker or FakeBroker(), stdout=io.StringIO(), stderr=self.err,
            parent_pid=PARENT, sessions_dir=self.sessions)

    def test_no_record_keeps_identity_and_does_not_register(self):
        broker = FakeBroker()
        adapter = self.make(broker)
        self.assertEqual(OLD, adapter.current_id())
        self.assertEqual([], broker.registered)

    def test_same_record_does_not_re_register(self):
        self.write_record(OLD)
        broker = FakeBroker()
        adapter = self.make(broker)
        self.assertEqual(OLD, adapter.current_id())
        self.assertEqual([], broker.registered)

    def test_changed_record_switches_and_registers_new_id(self):
        self.write_record(NEW)
        broker = FakeBroker()
        adapter = self.make(broker)
        self.assertEqual(NEW, adapter.current_id())
        self.assertEqual(NEW, adapter.session_id)
        self.assertEqual([NEW], broker.registered)
        self.assertIn("now session " + NEW, self.err.getvalue())

    def test_register_failure_keeps_old_id_and_retries_later(self):
        self.write_record(NEW)
        broker = FakeBroker(fail_register=True)
        adapter = self.make(broker)
        self.assertEqual(OLD, adapter.current_id())
        self.assertEqual(OLD, adapter.session_id)
        broker.fail_register = False
        self.assertEqual(NEW, adapter.current_id())

    def test_send_after_switch_uses_new_sender(self):
        self.write_record(NEW)
        broker = FakeBroker()
        adapter = self.make(broker)
        target = "bbbbbbbb-cccc-4ddd-8eee-ffffffffffff"
        result = adapter.call_tool({"name": "bridge_send",
                                    "arguments": {"recipient": target, "text": "hello"}})
        self.assertNotIn("isError", result)
        self.assertEqual(1, len(broker.sent))
        self.assertEqual(NEW, broker.sent[0][0])

    def test_inbox_delivery_follows_switch(self):
        self.write_record(NEW)
        adapter = self.make()
        adapter.current_id()
        out = io.StringIO()
        adapter.stdout = out
        adapter.deliver_messages([
            {"id": "11111111-1111-4111-8111-111111111111", "sender": "x", "recipient": OLD,
             "status": "queued", "text": "for old"},
            {"id": "22222222-2222-4222-8222-222222222222", "sender": "x", "recipient": NEW,
             "status": "queued", "text": "for new"},
        ])
        written = out.getvalue()
        self.assertIn("for new", written)
        self.assertNotIn("for old", written)


if __name__ == "__main__":
    unittest.main()
