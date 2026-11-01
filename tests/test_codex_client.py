import io
import json
import os
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
        self.env_patcher = patch.dict(os.environ)
        self.env_patcher.start()
        self.addCleanup(self.env_patcher.stop)
        os.environ.pop(codex_client.SESSION_CREDENTIAL_FILE_ENV, None)
        os.environ.pop(codex_client.SESSION_CREDENTIAL_FORMAT_ENV, None)

        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.discovery_file = Path(self.tempdir.name) / "broker-session.json"
        self.discovery_file.write_text(
            json.dumps({"port": 43123, "token": "test-secret-token"}),
            encoding="utf-8",
        )
        self.client = codex_client.BrokerClient(self.discovery_file)

    def test_scoped_credential_file_overrides_shared_discovery_token(self):
        token = "scoped_test-token_0123456789ABCDEF"
        credential_file = Path(self.tempdir.name) / "scoped-session.token"
        credential_file.write_text(token + "\n", encoding="utf-8")

        with patch.dict(os.environ, {
            codex_client.SESSION_CREDENTIAL_FILE_ENV: str(credential_file),
        }), patch(
            "codex_client._open_broker",
            return_value=FakeResponse({"sessions": []}),
        ) as open_url:
            self.client.discover()

        request = open_url.call_args.args[0]
        self.assertEqual(request.get_header("Authorization"), f"Bearer {token}")
        self.assertNotEqual(request.get_header("Authorization"), "Bearer test-secret-token")

    def test_default_credential_file_setting_tracks_environment_changes(self):
        first_token = "scoped_first-token_0123456789ABCDEF"
        second_token = "scoped_second-token_0123456789ABCDEF"
        first_file = Path(self.tempdir.name) / "first-session.token"
        second_file = Path(self.tempdir.name) / "second-session.token"
        first_file.write_text(first_token, encoding="utf-8")
        second_file.write_text(second_token, encoding="utf-8")

        with patch(
            "codex_client._open_broker",
            side_effect=[FakeResponse({"sessions": []}), FakeResponse({"sessions": []})],
        ) as open_url:
            os.environ[codex_client.SESSION_CREDENTIAL_FILE_ENV] = str(first_file)
            self.client.discover()
            os.environ[codex_client.SESSION_CREDENTIAL_FILE_ENV] = str(second_file)
            self.client.discover()

        authorizations = [
            call.args[0].get_header("Authorization") for call in open_url.call_args_list
        ]
        self.assertEqual(authorizations, [f"Bearer {first_token}", f"Bearer {second_token}"])

    def test_explicit_none_uses_shared_token_even_when_environment_is_set(self):
        env_token = "scoped_env-token_0123456789ABCDEF"
        credential_file = Path(self.tempdir.name) / "environment-session.token"
        credential_file.write_text(env_token, encoding="utf-8")
        client = codex_client.BrokerClient(self.discovery_file, credential_file=None)

        with patch.dict(os.environ, {
            codex_client.SESSION_CREDENTIAL_FILE_ENV: str(credential_file),
            codex_client.SESSION_CREDENTIAL_FORMAT_ENV: "dpapi",
        }), patch(
            "codex_client._open_broker",
            return_value=FakeResponse({"sessions": []}),
        ) as open_url:
            client.discover()

        self.assertEqual(
            open_url.call_args.args[0].get_header("Authorization"),
            "Bearer test-secret-token",
        )

    def test_dpapi_credential_file_uses_credential_store(self):
        token = "dpapi_test-token_0123456789ABCDEF"
        credential_file = Path(self.tempdir.name) / "dpapi-session.credential"
        client = codex_client.BrokerClient(self.discovery_file)

        with patch.dict(os.environ, {
            codex_client.SESSION_CREDENTIAL_FILE_ENV: str(credential_file),
            codex_client.SESSION_CREDENTIAL_FORMAT_ENV: "dpapi",
        }), patch(
            "codex_client.credential_store.read_generation",
            return_value=token.encode("ascii"),
        ) as read_generation, patch(
            "codex_client._open_broker",
            return_value=FakeResponse({"sessions": []}),
        ) as open_url:
            client.discover()

        read_generation.assert_called_once_with(str(credential_file))
        self.assertEqual(
            open_url.call_args.args[0].get_header("Authorization"),
            f"Bearer {token}",
        )

    def test_dpapi_credential_file_errors_are_sanitized(self):
        credential_file = Path(self.tempdir.name) / "dpapi-session.credential"
        client = codex_client.BrokerClient(self.discovery_file)

        with patch.dict(os.environ, {
            codex_client.SESSION_CREDENTIAL_FILE_ENV: str(credential_file),
            codex_client.SESSION_CREDENTIAL_FORMAT_ENV: "dpapi",
        }), patch(
            "codex_client.credential_store.read_generation",
            side_effect=codex_client.credential_store.CredentialStoreError(
                "synthetic-secret-must-not-escape"
            ),
        ), patch("codex_client._open_broker") as open_url:
            with self.assertRaises(codex_client.ClientError) as raised:
                client.discover()

        open_url.assert_not_called()
        self.assertEqual(raised.exception.code, "session_credential_file_invalid")
        self.assertNotIn("synthetic-secret-must-not-escape", str(raised.exception))
        self.assertNotIn(str(credential_file), str(raised.exception))

    def test_dpapi_format_without_scoped_file_fails_closed(self):
        client = codex_client.BrokerClient(self.discovery_file)

        with patch.dict(os.environ, {
            codex_client.SESSION_CREDENTIAL_FORMAT_ENV: "dpapi",
        }), patch("codex_client._open_broker") as open_url:
            with self.assertRaises(codex_client.ClientError) as raised:
                client.discover()

        open_url.assert_not_called()
        self.assertEqual(raised.exception.code, "session_credential_file_missing")

    def test_unknown_credential_format_fails_closed(self):
        credential_file = Path(self.tempdir.name) / "scoped-session.credential"
        credential_file.write_text("scoped_test-token_0123456789ABCDEF", encoding="utf-8")
        client = codex_client.BrokerClient(self.discovery_file)

        with patch.dict(os.environ, {
            codex_client.SESSION_CREDENTIAL_FILE_ENV: str(credential_file),
            codex_client.SESSION_CREDENTIAL_FORMAT_ENV: "future-format",
        }), patch("codex_client._open_broker") as open_url:
            with self.assertRaises(codex_client.ClientError) as raised:
                client.discover()

        open_url.assert_not_called()
        self.assertEqual(raised.exception.code, "session_credential_format_invalid")
        self.assertNotIn("future-format", str(raised.exception))

    @unittest.skipUnless(os.name == "nt", "Windows DPAPI is required")
    def test_dpapi_generation_reads_with_current_windows_user(self):
        token = "dpapi_windows-token_0123456789ABCDEF"
        credential_file = Path(self.tempdir.name) / "dpapi-session.credential"
        codex_client.credential_store.create_generation(
            str(credential_file), token.encode("ascii")
        )
        client = codex_client.BrokerClient(self.discovery_file)

        with patch.dict(os.environ, {
            codex_client.SESSION_CREDENTIAL_FILE_ENV: str(credential_file),
            codex_client.SESSION_CREDENTIAL_FORMAT_ENV: "dpapi",
        }), patch(
            "codex_client._open_broker",
            return_value=FakeResponse({"sessions": []}),
        ) as open_url:
            client.discover()

        self.assertEqual(
            open_url.call_args.args[0].get_header("Authorization"),
            f"Bearer {token}",
        )

    def test_explicit_credential_file_overrides_environment_and_is_reread(self):
        first_token = "scoped_explicit-token_0123456789ABCDEF"
        rotated_token = "scoped_rotated-token_0123456789ABCDEF"
        env_token = "scoped_env-token_0123456789ABCDEF"
        explicit_file = Path(self.tempdir.name) / "explicit-session.token"
        env_file = Path(self.tempdir.name) / "environment-session.token"
        explicit_file.write_text(first_token, encoding="utf-8")
        env_file.write_text(env_token, encoding="utf-8")
        client = codex_client.BrokerClient(
            self.discovery_file,
            credential_file=explicit_file,
        )

        with patch.dict(os.environ, {
            codex_client.SESSION_CREDENTIAL_FILE_ENV: str(env_file),
        }), patch(
            "codex_client._open_broker",
            side_effect=[FakeResponse({"sessions": []}), FakeResponse({"sessions": []})],
        ) as open_url:
            client.discover()
            explicit_file.write_text(rotated_token, encoding="utf-8")
            client.discover()

        authorizations = [
            call.args[0].get_header("Authorization") for call in open_url.call_args_list
        ]
        self.assertEqual(
            authorizations,
            [f"Bearer {first_token}", f"Bearer {rotated_token}"],
        )

    def test_explicit_missing_credential_file_fails_closed_despite_environment(self):
        env_token = "scoped_env-token_0123456789ABCDEF"
        env_file = Path(self.tempdir.name) / "environment-session.token"
        missing_file = Path(self.tempdir.name) / "missing-explicit-session.token"
        env_file.write_text(env_token, encoding="utf-8")
        client = codex_client.BrokerClient(
            self.discovery_file,
            credential_file=missing_file,
        )

        with patch.dict(os.environ, {
            codex_client.SESSION_CREDENTIAL_FILE_ENV: str(env_file),
        }), patch("codex_client._open_broker") as open_url:
            with self.assertRaises(codex_client.ClientError) as raised:
                client.discover()

        open_url.assert_not_called()
        self.assertEqual(raised.exception.code, "session_credential_file_missing")
        self.assertNotIn(str(missing_file), str(raised.exception))
        self.assertNotIn(env_token, str(raised.exception))

    def test_scoped_credential_file_does_not_require_shared_token(self):
        token = "scoped_test-token_0123456789ABCDEF"
        credential_file = Path(self.tempdir.name) / "scoped-session.token"
        credential_file.write_text(token, encoding="utf-8")
        self.discovery_file.write_text(json.dumps({"port": 43123}), encoding="utf-8")

        with patch.dict(os.environ, {
            codex_client.SESSION_CREDENTIAL_FILE_ENV: str(credential_file),
        }), patch(
            "codex_client._open_broker",
            return_value=FakeResponse({"sessions": []}),
        ) as open_url:
            self.client.discover()

        self.assertEqual(
            open_url.call_args.args[0].get_header("Authorization"),
            f"Bearer {token}",
        )

    def test_missing_scoped_credential_fails_closed_without_exposing_path(self):
        missing_file = Path(self.tempdir.name) / "missing-secret-file"
        with patch.dict(os.environ, {
            codex_client.SESSION_CREDENTIAL_FILE_ENV: str(missing_file),
        }), patch("codex_client._open_broker") as open_url:
            with self.assertRaises(codex_client.ClientError) as raised:
                self.client.discover()

        open_url.assert_not_called()
        self.assertEqual(raised.exception.code, "session_credential_file_missing")
        self.assertNotIn(str(missing_file), str(raised.exception))
        self.assertNotIn("test-secret-token", str(raised.exception))

    def test_invalid_scoped_credential_is_not_used_or_exposed(self):
        invalid_token = "do-not-leak/this-token!"
        credential_file = Path(self.tempdir.name) / "invalid-session.token"
        credential_file.write_text(invalid_token, encoding="utf-8")

        with patch.dict(os.environ, {
            codex_client.SESSION_CREDENTIAL_FILE_ENV: str(credential_file),
        }), patch("codex_client._open_broker") as open_url:
            with self.assertRaises(codex_client.ClientError) as raised:
                self.client.discover()

        open_url.assert_not_called()
        self.assertEqual(raised.exception.code, "session_credential_file_invalid")
        self.assertNotIn(invalid_token, str(raised.exception))

    def test_scoped_token_is_redacted_from_broker_errors(self):
        token = "scoped_test-token_0123456789ABCDEF"
        credential_file = Path(self.tempdir.name) / "scoped-session.token"
        credential_file.write_text(token, encoding="utf-8")
        response_body = json.dumps({"error": f"rejected {token}"}).encode("utf-8")
        error = HTTPError(
            "http://127.0.0.1:43123/v1/sessions",
            403,
            "Forbidden",
            {},
            io.BytesIO(response_body),
        )

        with patch.dict(os.environ, {
            codex_client.SESSION_CREDENTIAL_FILE_ENV: str(credential_file),
        }), patch("codex_client._open_broker", side_effect=error):
            with self.assertRaises(codex_client.ClientError) as raised:
                self.client.discover()

        self.assertEqual(raised.exception.code, "broker_error")
        self.assertNotIn(token, str(raised.exception))
        self.assertIn("[redacted]", str(raised.exception))

    def test_scoped_token_is_redacted_from_transport_errors(self):
        token = "scoped_test-token_0123456789ABCDEF"
        credential_file = Path(self.tempdir.name) / "scoped-session.token"
        credential_file.write_text(token, encoding="utf-8")

        with patch.dict(os.environ, {
            codex_client.SESSION_CREDENTIAL_FILE_ENV: str(credential_file),
        }), patch(
            "codex_client._open_broker",
            side_effect=codex_client.urllib.error.URLError(f"failure {token}"),
        ):
            with self.assertRaises(codex_client.ClientError) as raised:
                self.client.discover()

        self.assertEqual(raised.exception.code, "transport_error")
        self.assertNotIn(token, str(raised.exception))
        self.assertIn("[redacted]", str(raised.exception))

    def test_register_uses_contract_payload_and_bearer_auth(self):
        response = FakeResponse({"registered": True})
        with patch("codex_client._open_broker", return_value=response) as open_url:
            result = self.client.register("codex-session", "codex")

        request = open_url.call_args.args[0]
        self.assertEqual(request.full_url, "http://127.0.0.1:43123/v1/register")
        self.assertEqual(request.get_method(), "POST")
        self.assertEqual(request.get_header("Authorization"), "Bearer test-secret-token")
        self.assertEqual(
            json.loads(request.data),
            {"id": "codex-session", "kind": "codex", "opt_in": True},
        )
        self.assertEqual(result, {"registered": True})

    def test_discover_uses_sessions_endpoint(self):
        with patch(
            "codex_client._open_broker",
            return_value=FakeResponse({"sessions": []}),
        ) as open_url:
            result = self.client.discover()

        self.assertEqual(open_url.call_args.args[0].full_url, "http://127.0.0.1:43123/v1/sessions")
        self.assertEqual(result, {"sessions": []})

    def test_send_uses_explicit_sender_and_recipient(self):
        with patch(
            "codex_client._open_broker",
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

    def test_send_uses_caller_supplied_message_id(self):
        message_id = "123e4567-e89b-12d3-a456-426614174000"
        with patch(
            "codex_client._open_broker",
            return_value=FakeResponse({"id": message_id, "status": "queued"}),
        ) as open_url:
            result = self.client.send(
                "codex-1", "claude-2", "retry-safe message", message_id=message_id
            )

        request = open_url.call_args.args[0]
        self.assertEqual(json.loads(request.data), {
            "sender": "codex-1", "recipient": "claude-2", "text": "retry-safe message",
            "id": message_id,
        })
        self.assertEqual(result, {"id": message_id, "status": "queued"})

    def test_send_rejects_noncanonical_message_id_before_network_access(self):
        with patch("codex_client._open_broker") as open_url:
            with self.assertRaises(codex_client.ClientError) as raised:
                self.client.send("codex-1", "claude-2", "hello", message_id="not-a-uuid")
        open_url.assert_not_called()
        self.assertEqual("invalid_argument", raised.exception.code)

    def test_send_can_link_a_reply_without_changing_default_payload(self):
        reply_to = "123e4567-e89b-12d3-a456-426614174001"
        with patch(
            "codex_client._open_broker",
            return_value=FakeResponse({"id": "m-2", "status": "queued"}),
        ) as open_url:
            self.client.send("codex-1", "claude-2", "answer", reply_to=reply_to)

        request = open_url.call_args.args[0]
        self.assertEqual(json.loads(request.data), {
            "sender": "codex-1", "recipient": "claude-2", "text": "answer",
            "reply_to": reply_to,
        })

    def test_send_rejects_invalid_reply_id_before_network_access(self):
        with patch("codex_client._open_broker") as open_url:
            with self.assertRaises(codex_client.ClientError) as raised:
                self.client.send("codex-1", "claude-2", "answer", reply_to="bad id")
        open_url.assert_not_called()
        self.assertEqual("invalid_argument", raised.exception.code)

    def test_send_group_uses_stable_batch_id_and_explicit_recipients(self):
        response = {
            "batch_id": "8b891c97-f5cc-4c85-b7c6-32290bda3121",
            "deliveries": [
                {"id": "delivery-a", "recipient": "claude-a", "status": "queued"},
                {"id": "delivery-b", "recipient": "claude-b", "status": "queued"},
            ],
        }
        with patch(
            "codex_client._open_broker",
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
        with patch("codex_client._open_broker") as open_url:
            with self.assertRaises(codex_client.ClientError) as raised:
                self.client.send_group(
                    "codex-1", ["claude-a", "claude-a"], "hello",
                    "8b891c97-f5cc-4c85-b7c6-32290bda3121",
                )
        open_url.assert_not_called()
        self.assertEqual(raised.exception.code, "invalid_argument")

    def test_poll_url_encodes_recipient_and_does_not_ack(self):
        with patch(
            "codex_client._open_broker",
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
            "codex_client._open_broker",
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
        with patch("codex_client._open_broker", side_effect=error):
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
        with patch("codex_client._open_broker") as open_url:
            with self.assertRaises(codex_client.ClientError) as raised:
                self.client.send("  ", "claude-2", "hello")
        open_url.assert_not_called()
        self.assertEqual(raised.exception.code, "invalid_argument")

    def test_create_room_uses_explicit_room_owner_and_members(self):
        payload = {
            "room_id": "room:design-team",
            "name": "Design team",
            "owner": "codex-owner",
            "members": ["codex-owner", "claude-reviewer"],
        }
        with patch(
            "codex_client._open_broker",
            return_value=FakeResponse(payload),
        ) as open_url:
            result = self.client.create_room(**payload)

        request = open_url.call_args.args[0]
        self.assertEqual(request.full_url, "http://127.0.0.1:43123/v1/rooms")
        self.assertEqual(request.get_method(), "POST")
        self.assertEqual(json.loads(request.data), payload)
        self.assertEqual(result, payload)

    def test_room_info_encodes_explicit_member_query(self):
        with patch(
            "codex_client._open_broker",
            return_value=FakeResponse({"room_id": "room:design-team"}),
        ) as open_url:
            result = self.client.room_info("room:design-team", "codex:thread-1")

        request = open_url.call_args.args[0]
        self.assertEqual(
            request.full_url,
            "http://127.0.0.1:43123/v1/rooms/room:design-team?member=codex%3Athread-1",
        )
        self.assertEqual(request.get_method(), "GET")
        self.assertEqual(result, {"room_id": "room:design-team"})

    def test_update_room_member_uses_explicit_actor_member_and_action(self):
        payload = {"actor": "codex-owner", "member": "claude-reviewer", "action": "add"}
        with patch(
            "codex_client._open_broker",
            return_value=FakeResponse({"room_id": "room:design-team", "members": []}),
        ) as open_url:
            result = self.client.update_room_member("room:design-team", **payload)

        request = open_url.call_args.args[0]
        self.assertEqual(
            request.full_url,
            "http://127.0.0.1:43123/v1/rooms/room:design-team/members",
        )
        self.assertEqual(json.loads(request.data), payload)
        self.assertEqual(result, {"room_id": "room:design-team", "members": []})

    def test_send_room_message_uses_canonical_batch_id(self):
        response = {"room_id": "room:design-team", "reply_to": None, "deliveries": []}
        with patch(
            "codex_client._open_broker",
            return_value=FakeResponse(response),
        ) as open_url:
            result = self.client.send_room_message(
                "room:design-team", "codex-owner", "hello room", "123e4567-e89b-12d3-a456-426614174003"
            )

        request = open_url.call_args.args[0]
        self.assertEqual(
            request.full_url,
            "http://127.0.0.1:43123/v1/rooms/room:design-team/messages",
        )
        self.assertEqual(json.loads(request.data), {
            "sender": "codex-owner", "text": "hello room",
            "batch_id": "123e4567-e89b-12d3-a456-426614174003",
        })
        self.assertEqual(result, response)

    def test_reply_room_uses_brokers_reply_all_endpoint_and_reply_to(self):
        batch_id = "123e4567-e89b-12d3-a456-426614174003"
        reply_to = "123e4567-e89b-12d3-a456-426614174004"
        response = {"room_id": "room:design-team", "reply_to": reply_to, "deliveries": []}
        with patch(
            "codex_client._open_broker",
            return_value=FakeResponse(response),
        ) as open_url:
            result = self.client.reply_room(
                "room:design-team", "codex-owner", "reply", batch_id, reply_to
            )

        request = open_url.call_args.args[0]
        self.assertEqual(
            request.full_url,
            "http://127.0.0.1:43123/v1/rooms/room:design-team/reply-all",
        )
        self.assertEqual(json.loads(request.data), {
            "sender": "codex-owner", "text": "reply", "batch_id": batch_id,
            "reply_to": reply_to,
        })
        self.assertEqual(result, response)

    def test_room_client_rejects_bad_membership_action_and_batch_before_network(self):
        with patch("codex_client._open_broker") as open_url:
            cases = [
                lambda: self.client.create_room(
                    "room:test", "Test", "codex-owner", ["codex-owner", "codex-owner"]
                ),
                lambda: self.client.create_room("room:test", "Test", "not-member", ["codex-owner"]),
                lambda: self.client.update_room_member(
                    "room:test", "codex-owner", "claude-1", "replace"
                ),
                lambda: self.client.update_room_member(
                    "room:test", "codex-owner", "claude-1", ["add"]
                ),
                lambda: self.client.send_room_message("room:test", "codex-owner", "hello", "not-a-uuid"),
            ]
            for call in cases:
                with self.subTest(call=call), self.assertRaises(codex_client.ClientError) as raised:
                    call()
                self.assertEqual(raised.exception.code, "invalid_argument")
        open_url.assert_not_called()


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

    def test_cli_send_passes_optional_reply_link(self):
        output = io.StringIO()
        with patch("codex_client.BrokerClient") as client_type, patch("sys.stdout", output):
            client_type.return_value.send.return_value = {"id": "reply-id", "status": "queued"}
            code = codex_client.main([
                "send", "--sender", "codex-1", "--recipient", "claude-2", "--text", "answer",
                "--reply-to", "parent-id",
            ])

        self.assertEqual(code, 0)
        client_type.return_value.send.assert_called_once_with(
            "codex-1", "claude-2", "answer", reply_to="parent-id")

    def test_cli_send_without_message_id_keeps_default_call(self):
        output = io.StringIO()
        with patch("codex_client.BrokerClient") as client_type, patch("sys.stdout", output):
            client_type.return_value.send.return_value = {"id": "generated-id", "status": "queued"}
            code = codex_client.main([
                "send", "--sender", "codex-1", "--recipient", "claude-2", "--text", "hello",
            ])

        self.assertEqual(code, 0)
        client_type.return_value.send.assert_called_once_with(
            "codex-1", "claude-2", "hello", reply_to=None)

    def test_cli_send_passes_explicit_stable_message_id(self):
        output = io.StringIO()
        message_id = "123e4567-e89b-12d3-a456-426614174000"
        with patch("codex_client.BrokerClient") as client_type, patch("sys.stdout", output):
            client_type.return_value.send.return_value = {"id": message_id, "status": "queued"}
            code = codex_client.main([
                "send", "--sender", "codex-1", "--recipient", "claude-2", "--text", "hello",
                "--message-id", message_id,
            ])

        self.assertEqual(code, 0)
        client_type.return_value.send.assert_called_once_with(
            "codex-1", "claude-2", "hello", reply_to=None, message_id=message_id)


if __name__ == "__main__":
    unittest.main()
