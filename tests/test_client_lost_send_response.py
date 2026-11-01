"""Exercise committed send response loss through both staged Python clients."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from http.server import ThreadingHTTPServer
import json
import os
from pathlib import Path
import secrets
import socket
import tempfile
import threading
import unittest
import uuid
from unittest.mock import patch

import claude_adapter
import codex_client
from broker import Broker, handler_for


class ClientLostSendResponseTests(unittest.TestCase):
    def setUp(self):
        self.environment = patch.dict(os.environ)
        self.environment.start()
        os.environ.pop(codex_client.SESSION_CREDENTIAL_FILE_ENV, None)
        os.environ.pop(codex_client.SESSION_CREDENTIAL_FORMAT_ENV, None)
        os.environ.pop(claude_adapter.SCOPED_CREDENTIAL_FILE_ENV, None)
        os.environ.pop(claude_adapter.SCOPED_CREDENTIAL_FORMAT_ENV, None)
        self.addCleanup(self.environment.stop)

        self.temp = tempfile.TemporaryDirectory(prefix="client-lost-send-response-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.database = self.root / "synthetic-broker.sqlite3"
        self.broker = Broker(self.database)
        self.token = secrets.token_urlsafe(32)
        self.sessions = {
            "codex": ("codex-sender", "codex-recipient"),
            "claude": ("claude-sender", "claude-recipient"),
        }
        for sender, recipient in self.sessions.values():
            self.broker.register({"id": sender, "kind": "codex", "opt_in": True})
            self.broker.register({"id": recipient, "kind": "claude", "opt_in": True})

        self.drop_lock = threading.Lock()
        self.drop_next_send = False
        self.response_entered = threading.Event()
        self.release_response = threading.Event()
        base_handler = handler_for(self.broker, self.token)
        test_case = self

        class DropAfterCommitHandler(base_handler):
            def response(self, status, value):
                should_drop = False
                if status == 200 and self.command == "POST" and self.path == "/v1/messages":
                    with test_case.drop_lock:
                        if test_case.drop_next_send:
                            test_case.drop_next_send = False
                            should_drop = True
                if not should_drop:
                    return super().response(status, value)

                # handler_for calls response only after Broker.enqueue commits.
                test_case.response_entered.set()
                test_case.release_response.wait(timeout=5)
                try:
                    self.connection.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                self.connection.close()

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), DropAfterCommitHandler)
        self.server_thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.server_thread.start()
        self.addCleanup(self.close_server)

        self.discovery = self.root / "broker-session.json"
        self.discovery.write_text(
            json.dumps({"port": self.server.server_port, "token": self.token}),
            encoding="utf-8",
        )
        self.clients = {
            "codex": codex_client.BrokerClient(self.discovery, credential_file=None),
            "claude": claude_adapter.BrokerClient(self.discovery),
        }

    def close_server(self):
        self.release_response.set()
        self.server.shutdown()
        self.server.server_close()
        self.server_thread.join(timeout=3)
        self.assertFalse(self.server_thread.is_alive(), "temporary HTTP server did not stop")

    def arm_lost_response(self):
        self.response_entered.clear()
        self.release_response.clear()
        with self.drop_lock:
            self.assertFalse(self.drop_next_send, "previous response-loss injection is still armed")
            self.drop_next_send = True

    def count_direct(self, sender, recipient, text, message_id=None):
        with closing(self.broker.connect()) as db:
            if message_id is not None:
                return db.execute(
                    "SELECT COUNT(*) FROM messages WHERE id=? AND sender=? AND recipient=? AND text=?",
                    (message_id, sender, recipient, text),
                ).fetchone()[0]
            return db.execute(
                "SELECT COUNT(*) FROM messages WHERE sender=? AND recipient=? AND text=?",
                (sender, recipient, text),
            ).fetchone()[0]

    def send_with_lost_response(self, client, sender, recipient, text, *, message_id=None):
        self.arm_lost_response()
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(
                client.send, sender, recipient, text, message_id=message_id
            )
            self.assertTrue(
                self.response_entered.wait(timeout=5),
                "HTTP handler did not reach its post-commit response gate",
            )
            self.assertEqual(1, self.count_direct(sender, recipient, text, message_id))
            self.release_response.set()
            return future

    def test_both_clients_retry_lost_response_with_stable_id_without_duplicate(self):
        for client_name, (sender, recipient) in self.sessions.items():
            with self.subTest(client=client_name):
                client = self.clients[client_name]
                message_id = str(uuid.uuid4())
                text = "stable retry for " + client_name
                future = self.send_with_lost_response(
                    client, sender, recipient, text, message_id=message_id
                )
                expected_error = (
                    codex_client.ClientError if client_name == "codex"
                    else claude_adapter.BrokerError
                )
                with self.assertRaises(expected_error):
                    future.result(timeout=5)

                result = client.send(sender, recipient, text, message_id=message_id)
                self.assertEqual({"id": message_id, "status": "queued"}, result)
                self.assertEqual(1, self.count_direct(sender, recipient, text, message_id))

    def test_repeated_no_id_requests_create_distinct_messages_for_both_clients(self):
        for client_name, (sender, recipient) in self.sessions.items():
            with self.subTest(client=client_name):
                client = self.clients[client_name]
                text = "intentional no-id retry for " + client_name
                future = self.send_with_lost_response(client, sender, recipient, text)
                expected_error = (
                    codex_client.ClientError if client_name == "codex"
                    else claude_adapter.BrokerError
                )
                with self.assertRaises(expected_error):
                    future.result(timeout=5)

                retry = client.send(sender, recipient, text)
                self.assertEqual("queued", retry["status"])
                self.assertEqual(2, self.count_direct(sender, recipient, text))
                with closing(self.broker.connect()) as db:
                    ids = [row[0] for row in db.execute(
                        "SELECT id FROM messages WHERE sender=? AND recipient=? AND text=? ORDER BY seq",
                        (sender, recipient, text),
                    )]
                self.assertEqual(2, len(set(ids)))


if __name__ == "__main__":
    unittest.main()
