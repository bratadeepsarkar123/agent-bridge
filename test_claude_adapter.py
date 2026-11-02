"""Bounded protocol tests for the Claude broker adapter; no broker is started."""

import io
import json
import os
import sys
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from urllib.error import HTTPError
from unittest.mock import Mock, patch

from claude_adapter import (
    BrokerClient,
    BrokerError,
    ClaudeAdapter,
    SCOPED_CREDENTIAL_FILE_ENV,
    SCOPED_CREDENTIAL_FORMAT_ENV,
)


class FakeBroker:
    def __init__(self):
        self.sent = []
        self.reply_links = []
        self.message_ids = []
        self.sent_groups = []
        self.acks = []

    def register(self, session_id):
        return {"id": session_id, "kind": "claude"}

    def send(self, sender, recipient, text, reply_to=None, message_id=None):
        self.sent.append((sender, recipient, text))
        self.reply_links.append(reply_to)
        self.message_ids.append(message_id)
        return {"id": message_id or "outbound-1", "status": "queued"}

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


class ScopedCredentialTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.root = Path(self.temp_dir.name)
        self.session_file = self.root / "broker-session.json"
        self.session_file.write_text(json.dumps({
            "port": 43210,
            "token": "legacy-shared-discovery-token",
        }), encoding="utf-8")

    def write_credential(self, value):
        path = self.root / "scoped-token.txt"
        path.write_text(value, encoding="ascii")
        return path

    @staticmethod
    def ok_response():
        class Response:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def read(self, _limit):
                return b"{}"

        return Response()

    def test_configured_scoped_file_supplies_bearer_instead_of_discovery_token(self):
        scoped_token = "Scoped_token-0123456789ABCDEFGHIJKLMN"
        credential_file = self.write_credential(scoped_token + "\n")
        with patch_environment({SCOPED_CREDENTIAL_FILE_ENV: str(credential_file)}):
            client = BrokerClient(self.session_file)
            opener = Mock()
            opener.open.return_value = self.ok_response()
            client.opener = opener

            self.assertEqual({}, client._request("GET", "/v1/sessions"))

        request = opener.open.call_args.args[0]
        self.assertEqual("Bearer " + scoped_token, request.get_header("Authorization"))
        self.assertNotEqual("Bearer legacy-shared-discovery-token",
                            request.get_header("Authorization"))

    def test_unset_environment_preserves_legacy_discovery_bearer(self):
        with patch_environment({}):
            client = BrokerClient(self.session_file)
            opener = Mock()
            opener.open.return_value = self.ok_response()
            client.opener = opener

            self.assertEqual({}, client._request("GET", "/v1/sessions"))

        request = opener.open.call_args.args[0]
        self.assertEqual("Bearer legacy-shared-discovery-token",
                         request.get_header("Authorization"))

    def test_missing_or_invalid_configured_file_fails_closed_without_fallback(self):
        invalid_contents = ["short", "A" * 32 + "!", "B" * 32 + "\n\n"]
        missing_path = self.root / "missing-token.txt"
        cases = [(missing_path, None)]
        cases.extend((self.write_credential(value), value) for value in invalid_contents)

        for path, contents in cases:
            with self.subTest(case="missing" if path == missing_path else "invalid"):
                with patch_environment({SCOPED_CREDENTIAL_FILE_ENV: str(path)}):
                    with self.assertRaises(BrokerError) as caught:
                        BrokerClient(self.session_file)
                self.assertNotIn("legacy-shared-discovery-token", str(caught.exception))
                if contents is not None:
                    self.assertFalse(contents in str(caught.exception))
                self.assertIsNone(caught.exception.__cause__)

    def test_scoped_http_error_does_not_echo_credential_body(self):
        scoped_token = "Scoped_token-0123456789ABCDEFGHIJKLMN"
        credential_file = self.write_credential(scoped_token)
        with patch_environment({SCOPED_CREDENTIAL_FILE_ENV: str(credential_file)}):
            client = BrokerClient(self.session_file)
            client.opener = Mock()
            client.opener.open.side_effect = HTTPError(
                "http://127.0.0.1:43210/v1/sessions", 403, "forbidden", {},
                io.BytesIO(scoped_token.encode("ascii")),
            )

            with self.assertRaises(BrokerError) as caught:
                client._request("GET", "/v1/sessions")

        self.assertFalse(scoped_token in str(caught.exception))
        self.assertEqual("broker returned HTTP 403", str(caught.exception))
        self.assertIsNone(caught.exception.__cause__)

    def test_dpapi_format_uses_credential_store_and_decodes_without_exposing_token(self):
        import credential_store

        token = "Dpapi_token-0123456789ABCDEFGHIJKLMNOP"
        credential_file = self.root / "synthetic-generation.dpapi"
        credential_file.write_bytes(b"synthetic encrypted-generation fixture")
        with patch_environment({
            SCOPED_CREDENTIAL_FILE_ENV: str(credential_file),
            SCOPED_CREDENTIAL_FORMAT_ENV: "dpapi",
        }), patch.object(credential_store, "read_generation", return_value=token.encode("ascii")) as read:
            client = BrokerClient(self.session_file)
            opener = Mock()
            opener.open.return_value = self.ok_response()
            client.opener = opener
            self.assertEqual({}, client._request("GET", "/v1/sessions"))

        self.assertEqual(2, read.call_count)
        read.assert_any_call(str(credential_file))
        self.assertEqual("Bearer " + token,
                         opener.open.call_args.args[0].get_header("Authorization"))

    def test_dpapi_failure_is_sanitized_and_does_not_fall_back(self):
        import credential_store

        token = "Dpapi_token-0123456789ABCDEFGHIJKLMNOP"
        credential_file = self.root / "synthetic-invalid-generation.dpapi"
        credential_file.write_bytes(b"synthetic encrypted-generation fixture")
        with patch_environment({
            SCOPED_CREDENTIAL_FILE_ENV: str(credential_file),
            SCOPED_CREDENTIAL_FORMAT_ENV: "dpapi",
        }), patch.object(credential_store, "read_generation", side_effect=RuntimeError(token)):
            with self.assertRaises(BrokerError) as caught:
                BrokerClient(self.session_file)

        self.assertFalse(token in str(caught.exception))
        self.assertEqual("cannot read scoped broker credential file", str(caught.exception))
        self.assertIsNone(caught.exception.__cause__)

    def test_unknown_format_and_dpapi_without_file_fail_closed(self):
        cases = [
            ({SCOPED_CREDENTIAL_FORMAT_ENV: "other"}, "invalid scoped credential format"),
            ({SCOPED_CREDENTIAL_FORMAT_ENV: "dpapi"}, "requires a scoped credential file"),
        ]
        for environment, message in cases:
            with self.subTest(format=environment[SCOPED_CREDENTIAL_FORMAT_ENV]):
                with patch_environment(environment):
                    with self.assertRaisesRegex(BrokerError, message) as caught:
                        BrokerClient(self.session_file)
                self.assertNotIn("legacy-shared-discovery-token", str(caught.exception))

    @unittest.skipUnless(sys.platform == "win32", "real user-bound DPAPI requires Windows")
    def test_windows_dpapi_generation_is_used_as_scoped_bearer(self):
        from credential_store import create_generation

        token = "Windows_dpapi_token-0123456789ABCDEFG"
        credential_file = self.root / "windows-generation.dpapi"
        create_generation(credential_file, token.encode("ascii"))
        with patch_environment({
            SCOPED_CREDENTIAL_FILE_ENV: str(credential_file),
            SCOPED_CREDENTIAL_FORMAT_ENV: "dpapi",
        }):
            client = BrokerClient(self.session_file)
            opener = Mock()
            opener.open.return_value = self.ok_response()
            client.opener = opener
            self.assertEqual({}, client._request("GET", "/v1/sessions"))

        self.assertEqual("Bearer " + token,
                         opener.open.call_args.args[0].get_header("Authorization"))


@contextmanager
def patch_environment(values):
    """Temporarily set only the adapter credential variable."""
    with patch.dict(os.environ, {}):
        os.environ.pop(SCOPED_CREDENTIAL_FILE_ENV, None)
        os.environ.pop(SCOPED_CREDENTIAL_FORMAT_ENV, None)
        os.environ.update(values)
        yield


class ClaudeAdapterProtocolTests(unittest.TestCase):
    def setUp(self):
        self.output = io.StringIO()
        self.broker = FakeBroker()
        self.adapter = ClaudeAdapter("claude-session", "codex", self.broker,
                                     stdout=self.output, stderr=io.StringIO())

    def output_messages(self):
        return [json.loads(line) for line in self.output.getvalue().splitlines()]

    def test_oem_stdout_preserves_unicode_channel_content(self):
        raw = io.BytesIO()
        output = io.TextIOWrapper(raw, encoding='cp437')
        adapter = ClaudeAdapter('claude-session', 'codex', self.broker, stdout=output)
        adapter.deliver_messages([{'id': 'm1', 'sender': 'codex', 'recipient': 'claude-session',
                                   'text': 'hello Ω', 'status': 'queued'}])
        output.flush()
        event = json.loads(raw.getvalue().decode('ascii'))
        self.assertEqual('hello Ω', event['params']['content'])

    def test_oem_stdin_decodes_utf8_request(self):
        raw = io.BytesIO((json.dumps({'jsonrpc': '2.0', 'id': 1, 'method': 'initialize',
                                      'params': {'protocolVersion': 'unicode-Ω'}}, ensure_ascii=False) + '\n').encode('utf8'))
        source = io.TextIOWrapper(raw, encoding='cp437')
        self.adapter.run(stdin=source)
        self.assertEqual('unicode-Ω', self.output_messages()[0]['result']['protocolVersion'])

    def test_room_event_and_reply_preserve_context(self):
        self.broker.room_request = Mock(return_value={"deliveries": []})
        self.adapter.deliver_messages([{"id": "message-1", "sender": "codex",
            "recipient": "claude-session", "status": "queued", "text": "hello",
            "room_id": "team", "reply_to": "prior"}])
        self.assertEqual("team", self.output_messages()[0]["params"]["meta"]["room_id"])
        result = self.adapter.call_tool({"name": "bridge_room_reply", "arguments": {
            "room_id": "team", "reply_to": "message-1", "text": "answer", "batch_id": "batch-1"}})
        self.assertNotIn("isError", result)
        self.broker.room_request.assert_called_once_with("team", "reply-all", {
            "sender": "claude-session", "text": "answer", "batch_id": "batch-1", "reply_to": "message-1"})

    def test_direct_reply_is_linked_and_received_event_keeps_link(self):
        self.adapter.deliver_messages([{"id": "direct-reply", "sender": "codex",
            "recipient": "claude-session", "status": "queued", "text": "answer",
            "reply_to": "original-message"}])
        self.assertEqual("original-message",
                         self.output_messages()[0]["params"]["meta"]["reply_to"])
        result = self.adapter.call_tool({"name": "bridge_send", "arguments": {
            "recipient": "codex", "text": "follow-up", "reply_to": "original-message"}})
        self.assertNotIn("isError", result)
        self.assertEqual(["original-message"], self.broker.reply_links)

    def test_direct_reply_tool_rejects_invalid_link_before_broker_call(self):
        result = self.adapter.call_tool({"name": "bridge_send", "arguments": {
            "recipient": "codex", "text": "follow-up", "reply_to": "bad id"}})
        self.assertTrue(result["isError"])
        self.assertEqual([], self.broker.sent)

    def test_direct_message_id_is_forwarded_and_can_be_reused_for_retry(self):
        message_id = "a7dd8be8-f2bd-4f6a-a687-cd9f5cbef08c"
        arguments = {"recipient": "codex", "text": "retryable", "message_id": message_id}

        for _ in range(2):
            result = self.adapter.call_tool({"name": "bridge_send", "arguments": arguments})
            self.assertNotIn("isError", result)

        self.assertEqual([message_id, message_id], self.broker.message_ids)
        self.assertIn(message_id, result["content"][0]["text"])

    def test_direct_message_id_validation_rejects_noncanonical_values(self):
        for message_id in ("", "not-a-uuid", "A7DD8BE8-F2BD-4F6A-A687-CD9F5CBEE08C", 42):
            with self.subTest(message_id=message_id):
                result = self.adapter.call_tool({"name": "bridge_send", "arguments": {
                    "recipient": "codex", "text": "invalid", "message_id": message_id}})
                self.assertTrue(result["isError"])
        self.assertEqual([], self.broker.sent)

    def test_initialize_advertises_claude_channel_and_untrusted_sender_rule(self):
        self.adapter.process({
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": "2025-03-26"},
        })
        response = self.output_messages()[0]
        self.assertEqual(response["result"]["capabilities"]["experimental"],
                         {"claude/channel": {}})
        self.assertIn("not user identity", response["result"]["instructions"])

    def test_send_queues_from_claude_to_explicit_recipient_and_is_unconfirmed(self):
        self.adapter.process({
            "jsonrpc": "2.0", "id": 2, "method": "tools/call",
            "params": {"name": "bridge_send", "arguments": {"recipient": "codex", "text": "hello"}},
        })
        self.assertEqual(self.broker.sent, [("claude-session", "codex", "hello")])
        self.assertIn("delivery is unconfirmed", self.output_messages()[0]["result"]["content"][0]["text"])

    def test_send_without_id_generates_one_and_failure_reports_it_for_retry(self):
        failing = Mock(side_effect=BrokerError("broker not reachable"))
        self.broker.send = failing

        result = self.adapter.call_tool({"name": "bridge_send", "arguments": {
            "recipient": "codex", "text": "hello"}})

        self.assertTrue(result["isError"])
        sent_id = failing.call_args.kwargs["message_id"]
        self.assertRegex(sent_id, r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
        self.assertIn(sent_id, result["content"][0]["text"])
        self.assertIn("Retry with message_id", result["content"][0]["text"])

        # A retry that reuses the reported ID is forwarded unchanged.
        self.broker.send = Mock(return_value={"id": sent_id, "status": "queued"})
        retry = self.adapter.call_tool({"name": "bridge_send", "arguments": {
            "recipient": "codex", "text": "hello", "message_id": sent_id}})
        self.assertNotIn("isError", retry)
        self.broker.send.assert_called_once_with("claude-session", "codex", "hello", message_id=sent_id)

    def test_send_requires_explicit_recipient(self):
        result = self.adapter.call_tool({"name": "bridge_send", "arguments": {"text": "hello"}})
        self.assertTrue(result["isError"])
        self.assertEqual([], self.broker.sent)

    def test_register_explicitly_opts_in_claude_session(self):
        client = object.__new__(BrokerClient)
        client._request = Mock(return_value={"id": "claude-session", "kind": "claude"})

        client.register("claude-session")

        client._request.assert_called_once_with("POST", "/v1/register", {
            "id": "claude-session", "kind": "claude", "opt_in": True,
        })

    def test_broker_client_send_includes_optional_reply_link(self):
        client = object.__new__(BrokerClient)
        client._request = Mock(return_value={"id": "reply-id", "status": "queued"})

        result = client.send("claude-session", "codex", "answer", reply_to="parent-id")

        client._request.assert_called_once_with("POST", "/v1/messages", {
            "sender": "claude-session", "recipient": "codex", "text": "answer",
            "reply_to": "parent-id",
        })
        self.assertEqual({"id": "reply-id", "status": "queued"}, result)

    def test_broker_client_send_includes_caller_supplied_message_id(self):
        client = object.__new__(BrokerClient)
        client._request = Mock(return_value={"id": "stable-id", "status": "queued"})
        message_id = "a7dd8be8-f2bd-4f6a-a687-cd9f5cbef08c"

        result = client.send("claude-session", "codex", "retryable", message_id=message_id)

        client._request.assert_called_once_with("POST", "/v1/messages", {
            "sender": "claude-session", "recipient": "codex", "text": "retryable",
            "id": message_id,
        })
        self.assertEqual({"id": "stable-id", "status": "queued"}, result)

    def test_broker_client_send_rejects_invalid_message_id_before_request(self):
        client = object.__new__(BrokerClient)
        client._request = Mock()

        with self.assertRaises(BrokerError):
            client.send("claude-session", "codex", "retryable", message_id="not-a-uuid")

        client._request.assert_not_called()

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

    def test_send_tool_requires_explicit_recipient(self):
        self.adapter.process({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
        tools = {tool["name"]: tool for tool in self.output_messages()[0]["result"]["tools"]}
        self.assertEqual(["text", "recipient"], tools["bridge_send"]["inputSchema"]["required"])
        self.assertEqual("string", tools["bridge_send"]["inputSchema"]["properties"]["reply_to"]["type"])
        message_id = tools["bridge_send"]["inputSchema"]["properties"]["message_id"]
        self.assertEqual("uuid", message_id["format"])
        self.assertNotIn("message_id", tools["bridge_send"]["inputSchema"]["required"])

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
