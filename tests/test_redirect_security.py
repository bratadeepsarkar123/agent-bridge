"""Authenticated clients must never forward credentials to redirect targets."""

import json
import os
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

import claude_adapter
import codex_client


class RedirectSecurityTests(unittest.TestCase):
    def test_both_clients_reject_redirect_without_contacting_target(self):
        received = []

        class Target(BaseHTTPRequestHandler):
            def do_GET(self):
                received.append(self.headers.get("Authorization"))
                self.send_response(200)
                self.end_headers()

            def log_message(self, *args):
                pass

        target = ThreadingHTTPServer(("127.0.0.1", 0), Target)
        class Redirect(BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(302)
                self.send_header("Location", "http://127.0.0.1:%d/capture" % target.server_port)
                self.end_headers()

            def log_message(self, *args):
                pass

        broker = ThreadingHTTPServer(("127.0.0.1", 0), Redirect)
        for server in (target, broker):
            threading.Thread(target=server.serve_forever, daemon=True).start()
            self.addCleanup(server.server_close)
            self.addCleanup(server.shutdown)
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {
            "BRIDGE_SESSION_CREDENTIAL_FILE": "",
        }, clear=False):
            os.environ.pop("BRIDGE_SESSION_CREDENTIAL_FILE")
            os.environ.pop("BRIDGE_SESSION_CREDENTIAL_FORMAT", None)
            discovery = Path(directory) / "broker-session.json"
            discovery.write_text(json.dumps({"port": broker.server_port, "token": "synthetic-secret"}), encoding="utf-8")
            for client, error in ((codex_client.BrokerClient(discovery), codex_client.ClientError),
                                  (claude_adapter.BrokerClient(discovery), claude_adapter.BrokerError)):
                with self.subTest(client=type(client).__module__), self.assertRaises(error):
                    client._request("GET", "/redirect")
            self.assertEqual(received, [])
