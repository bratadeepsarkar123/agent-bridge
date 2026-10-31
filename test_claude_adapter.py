"""Bounded protocol tests for the Claude broker adapter; no broker is started."""

import io
import json
import unittest
from unittest.mock import Mock

from claude_adapter import BrokerClient, ClaudeAdapter


class FakeBroker:
    def __init__(self):
        self.sent = []
        self.sent_groups = []
        self.acks = []

    def register(self, session_id):
        return {"id": session_id, "kind": "claude"}

    def send(self, sender, recipient, text):
        self.sent.append((sender, recipient, text))
        return {"id": "outbound-1", "status": "queued"}

    def send_group(self, sender, recipients, text, batch_id):
        self.sent_groups.append((sender, recipients, text, batch_id))
        return {
            "batch_id": batch_id,
            "deliveries": [
                {"id": "delivery-1", "recipient": recipient, "status": "queued"}
                for recipient in recipients
            ],
        }

    def acknowledge(self, message_id, recipient):
        self.acks.append((message_id, recipient))
        return {"id": message_id, "status": "acknowledged"}


class FailOnceOutput:
    def __init__(self, output):
        self.output = output
        self.fail_next_write = True

    def write(self, value):
        if self.fail_next_write:
            self.fail_next_write = False
            raise BrokenPipeError("simulated notification write failure")
        return self.output.write(value)

    def flush(self):
        return self.output.flush()


class ClaudeAdapterProtocolTests(unittest.TestCase):
    def setUp(self):
        self.output = io.StringIO()
        self.broker = FakeBroker()
        self.adapter = ClaudeAdapter("claude-session", "codex", self.broker,
                                     stdout=self.output, stderr=io.StringIO())

    def output_messages(self):
        return [json.loads(line) for line in self.output.getvalue().splitlines()]

    def test_initialize_advertises_claude_channel_and_untrusted_sender_rule(self):
        self.adapter.process({
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": "2025-03-26"},
        })
        response = self.output_messages()[0]
        self.assertEqual(response["result"]["capabilities"]["experimental"],
                         {"claude/channel": {}})
        self.assertIn("not user identity", response["result"]["instructions"])

    def test_send_queues_from_claude_to_configured_codex_and_is_unconfirmed(self):
        self.adapter.process({
            "jsonrpc": "2.0", "id": 2, "method": "tools/call",
            "params": {"name": "bridge_send", "arguments": {"text": "hello"}},
        })
        self.assertEqual(self.broker.sent, [("claude-session", "codex", "hello")])
        self.assertIn("delivery is unconfirmed", self.output_messages()[0]["result"]["content"][0]["text"])

    def test_group_send_tool_requires_explicit_recipients_and_batch_uuid(self):
        self.adapter.process({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
        tools = {tool["name"]: tool for tool in self.output_messages()[0]["result"]["tools"]}
        group = tools["bridge_send_group"]
        schema = group["inputSchema"]

        self.assertEqual(["recipients", "text", "batch_id"], schema["required"])
        self.assertEqual("array", schema["properties"]["recipients"]["type"])
        self.assertEqual(100, schema["properties"]["recipients"]["maxItems"])
        self.assertTrue(schema["properties"]["recipients"]["uniqueItems"])
        self.assertEqual("uuid", schema["properties"]["batch_id"]["format"])
        self.assertNotIn("sender", schema["properties"])

    def test_group_send_uses_session_sender_and_returns_per_recipient_result(self):
        batch_id = "8b891c97-f5cc-4c85-b7c6-32290bda3121"
        recipients = ["codex-a", "codex-b"]
        self.adapter.process({
            "jsonrpc": "2.0", "id": 2, "method": "tools/call",
            "params": {"name": "bridge_send_group", "arguments": {
                "recipients": recipients, "text": "hello group", "batch_id": batch_id,
            }},
        })

        self.assertEqual(
            [("claude-session", recipients, "hello group", batch_id)],
            self.broker.sent_groups,
        )
        result_text = self.output_messages()[0]["result"]["content"][0]["text"]
        self.assertIn("delivery is unconfirmed", result_text)
        self.assertIn('"recipient":"codex-a"', result_text)
        self.assertIn('"recipient":"codex-b"', result_text)

    def test_group_send_rejects_invalid_recipients_and_batch_before_broker_call(self):
        valid = {"recipients": ["codex-a"], "text": "hello", "batch_id":
                 "8b891c97-f5cc-4c85-b7c6-32290bda3121"}
        invalid_arguments = [
            {**valid, "recipients": ["codex-a", "codex-a"]},
            {**valid, "recipients": ["claude-session"]},
            {**valid, "batch_id": "not-a-uuid"},
        ]
        for arguments in invalid_arguments:
            with self.subTest(arguments=arguments):
                self.output.seek(0)
                self.output.truncate(0)
                self.adapter.process({
                    "jsonrpc": "2.0", "id": 3, "method": "tools/call",
                    "params": {"name": "bridge_send_group", "arguments": arguments},
                })
                self.assertTrue(self.output_messages()[0]["result"]["isError"])
        self.assertEqual([], self.broker.sent_groups)

    def test_group_client_posts_expected_payload_to_group_endpoint(self):
        client = object.__new__(BrokerClient)
        batch_id = "8b891c97-f5cc-4c85-b7c6-32290bda3121"
        client._request = Mock(return_value={"batch_id": batch_id, "deliveries": []})
        recipients = ["codex-a", "codex-b"]

        result = client.send_group(
            "claude-session", recipients, "hello group", batch_id)

        client._request.assert_called_once_with("POST", "/v1/group-messages", {
            "sender": "claude-session",
            "recipients": recipients,
            "text": "hello group",
            "batch_id": batch_id,
        })
        self.assertEqual({"batch_id": batch_id, "deliveries": []}, result)

    def test_queued_message_notification_carries_sender_and_id_once(self):
        message = {"id": "inbound-1", "sender": "codex", "recipient": "claude-session",
                   "text": "untrusted text", "status": "queued"}
        self.adapter.deliver_messages([message, message])
        notifications = self.output_messages()
        self.assertEqual(len(notifications), 1)
        self.assertEqual(notifications[0]["method"], "notifications/claude/channel")
        self.assertEqual(notifications[0]["params"]["meta"],
                         {"sender": "codex", "message_id": "inbound-1"})

    def test_failed_notification_write_allows_redelivery_on_next_poll(self):
        message = {"id": "inbound-write-failure", "sender": "codex",
                   "recipient": "claude-session", "text": "retry me", "status": "queued"}
        self.adapter.stdout = FailOnceOutput(self.output)

        with self.assertRaises(BrokenPipeError):
            self.adapter.deliver_messages([message])

        self.assertNotIn(message["id"], self.adapter.notified_ids)
        self.adapter.deliver_messages([message])

        notifications = self.output_messages()
        self.assertEqual(len(notifications), 1)
        self.assertEqual(notifications[0]["params"]["meta"]["message_id"], message["id"])
        self.assertIn(message["id"], self.adapter.notified_ids)

    def test_ack_requires_a_message_notified_by_this_process(self):
        request = {"jsonrpc": "2.0", "id": 3, "method": "tools/call",
                   "params": {"name": "bridge_ack", "arguments": {"message_id": "inbound-1"}}}
        self.adapter.process(request)
        self.assertEqual(self.broker.acks, [])

        self.adapter.deliver_messages([{
            "id": "inbound-1", "sender": "codex", "recipient": "claude-session",
            "text": "hello", "status": "queued",
        }])
        request["id"] = 4
        self.adapter.process(request)
        self.assertEqual(self.broker.acks, [("inbound-1", "claude-session")])
        self.assertIn("acknowledged", self.output_messages()[-1]["result"]["content"][0]["text"])


if __name__ == "__main__":
    unittest.main()
