import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError

import codex_client


class FakeResponse:
    def __init__(self, body, status=200):
        self.body = json.dumps(body).encode("utf-8")
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def getcode(self):
        return self.status

    def read(self):
        return self.body


class BrokerClientTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.discovery_file = Path(self.tempdir.name) / "broker-session.json"
        self.discovery_file.write_text(
            json.dumps({"port": 43123, "token": "test-secret-token"}),
            encoding="utf-8",
        )
        self.client = codex_client.BrokerClient(self.discovery_file)

    def test_register_uses_contract_payload_and_bearer_auth(self):
        response = FakeResponse({"registered": True})
        with patch("codex_client.urllib.request.urlopen", return_value=response) as open_url:
            result = self.client.register("codex-session", "codex")

        request = open_url.call_args.args[0]
        self.assertEqual(request.full_url, "http://127.0.0.1:43123/v1/register")
        self.assertEqual(request.get_method(), "POST")
        self.assertEqual(request.get_header("Authorization"), "Bearer test-secret-token")
        self.assertEqual(json.loads(request.data), {"id": "codex-session", "kind": "codex"})
        self.assertEqual(result, {"registered": True})

    def test_discover_uses_sessions_endpoint(self):
        with patch(
            "codex_client.urllib.request.urlopen",
            return_value=FakeResponse({"sessions": []}),
        ) as open_url:
            result = self.client.discover()

        self.assertEqual(open_url.call_args.args[0].full_url, "http://127.0.0.1:43123/v1/sessions")
        self.assertEqual(result, {"sessions": []})

    def test_send_uses_explicit_sender_and_recipient(self):
        with patch(
            "codex_client.urllib.request.urlopen",
            return_value=FakeResponse({"id": "m-1", "status": "sent"}),
        ) as open_url:
            result = self.client.send("codex-1", "claude-2", "hello")

        request = open_url.call_args.args[0]
        self.assertEqual(request.full_url, "http://127.0.0.1:43123/v1/messages")
        self.assertEqual(
            json.loads(request.data),
            {"sender": "codex-1", "recipient": "claude-2", "text": "hello"},
        )
        self.assertEqual(result, {"id": "m-1", "status": "sent"})

    def test_send_group_uses_stable_batch_id_and_explicit_recipients(self):
        response = {
            "batch_id": "8b891c97-f5cc-4c85-b7c6-32290bda3121",
            "deliveries": [
                {"id": "delivery-a", "recipient": "claude-a", "status": "queued"},
                {"id": "delivery-b", "recipient": "claude-b", "status": "queued"},
            ],
        }
        with patch(
            "codex_client.urllib.request.urlopen",
            return_value=FakeResponse(response),
        ) as open_url:
            result = self.client.send_group(
                "codex-1",
                ["claude-a", "claude-b"],
                "hello group",
                "8b891c97-f5cc-4c85-b7c6-32290bda3121",
            )

        request = open_url.call_args.args[0]
        self.assertEqual(request.full_url, "http://127.0.0.1:43123/v1/group-messages")
        self.assertEqual(
            json.loads(request.data),
            {
                "sender": "codex-1",
                "recipients": ["claude-a", "claude-b"],
                "text": "hello group",
                "batch_id": "8b891c97-f5cc-4c85-b7c6-32290bda3121",
            },
        )
        self.assertEqual(result, response)

    def test_send_group_rejects_duplicate_recipients_before_network_access(self):
        with patch("codex_client.urllib.request.urlopen") as open_url:
            with self.assertRaises(codex_client.ClientError) as raised:
                self.client.send_group(
                    "codex-1", ["claude-a", "claude-a"], "hello",
                    "8b891c97-f5cc-4c85-b7c6-32290bda3121",
                )
        open_url.assert_not_called()
        self.assertEqual(raised.exception.code, "invalid_argument")

    def test_poll_url_encodes_recipient_and_does_not_ack(self):
        with patch(
            "codex_client.urllib.request.urlopen",
            return_value=FakeResponse({"messages": []}),
        ) as open_url:
            result = self.client.poll("codex:session-1")

        request = open_url.call_args.args[0]
        self.assertEqual(
            request.full_url,
            "http://127.0.0.1:43123/v1/messages?recipient=codex%3Asession-1",
        )
        self.assertEqual(request.get_method(), "GET")
        self.assertEqual(result, {"messages": []})

    def test_ack_uses_contract_payload(self):
        with patch(
            "codex_client.urllib.request.urlopen",
            return_value=FakeResponse({"status": "acknowledged"}),
        ) as open_url:
            result = self.client.ack("m-1", "codex-1")

        request = open_url.call_args.args[0]
        self.assertEqual(request.full_url, "http://127.0.0.1:43123/v1/ack")
        self.assertEqual(json.loads(request.data), {"id": "m-1", "recipient": "codex-1"})
        self.assertEqual(result, {"status": "acknowledged"})

    def test_broker_error_is_structured_and_token_is_redacted(self):
        response_body = json.dumps({"error": "rejected test-secret-token"}).encode("utf-8")
        error = HTTPError(
            "http://127.0.0.1:43123/v1/sessions",
            403,
            "Forbidden",
            {},
            io.BytesIO(response_body),
        )
        with patch("codex_client.urllib.request.urlopen", side_effect=error):
            with self.assertRaises(codex_client.ClientError) as raised:
                self.client.discover()

        self.assertEqual(
            raised.exception.as_json(),
            {
                "ok": False,
                "error": {
                    "code": "broker_error",
                    "message": "rejected [redacted]",
                    "status": 403,
                },
            },
        )

    def test_missing_discovery_file_is_a_structured_error(self):
        missing = codex_client.BrokerClient(Path(self.tempdir.name) / "absent.json")
        with self.assertRaises(codex_client.ClientError) as raised:
            missing.discover()
        self.assertEqual(raised.exception.code, "discovery_file_missing")

    def test_invalid_sender_is_rejected_before_network_access(self):
        with patch("codex_client.urllib.request.urlopen") as open_url:
            with self.assertRaises(codex_client.ClientError) as raised:
                self.client.send("  ", "claude-2", "hello")
        open_url.assert_not_called()
        self.assertEqual(raised.exception.code, "invalid_argument")


class CliTests(unittest.TestCase):
    def test_cli_success_is_one_stable_json_envelope(self):
        output = io.StringIO()
        with patch("codex_client.BrokerClient") as client_type, patch("sys.stdout", output):
            client_type.return_value.discover.return_value = {"sessions": [{"id": "claude-1"}]}
            code = codex_client.main(["discover"])

        self.assertEqual(code, 0)
        self.assertEqual(
            output.getvalue(),
            '{"ok":true,"result":{"sessions":[{"id":"claude-1"}]}}\n',
        )

    def test_cli_argument_errors_are_json(self):
        output = io.StringIO()
        with patch("sys.stdout", output):
            code = codex_client.main(["send", "--sender", "codex-1"])

        self.assertEqual(code, 2)
        self.assertEqual(
            json.loads(output.getvalue()),
            {
                "ok": False,
                "error": {
                    "code": "invalid_arguments",
                    "message": "the following arguments are required: --recipient, --text",
                },
            },
        )

    def test_cli_send_group_passes_batch_id_and_each_recipient(self):
        output = io.StringIO()
        batch_id = "8b891c97-f5cc-4c85-b7c6-32290bda3121"
        response = {"batch_id": batch_id, "deliveries": []}
        with patch("codex_client.BrokerClient") as client_type, patch("sys.stdout", output):
            client_type.return_value.send_group.return_value = response
            code = codex_client.main([
                "send-group", "--sender", "codex-1", "--recipient", "claude-a",
                "--recipient", "claude-b", "--batch-id", batch_id, "--text", "hello",
            ])

        self.assertEqual(code, 0)
        client_type.return_value.send_group.assert_called_once_with(
            "codex-1", ["claude-a", "claude-b"], "hello", batch_id
        )
        self.assertEqual(json.loads(output.getvalue()), {"ok": True, "result": response})


if __name__ == "__main__":
    unittest.main()
