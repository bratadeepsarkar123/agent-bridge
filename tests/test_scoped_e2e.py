"""Isolated profile credentials across both real bridge clients."""
from contextlib import closing
import json
import os
import secrets
import sys
import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

from broker import Broker, handler_for
from claude_adapter import BrokerClient as ClaudeClient, BrokerError
from codex_client import BrokerClient as CodexClient, ClientError
from message_status import message_status
from multi_dispatcher import MultiRecipientDispatcher, validate_holder_participants
from codex_queue import DeliveryLedger
import credential_store
from credential_store import provision_initial_profile

A = '123e4567-e89b-12d3-a456-426614174000'
B = '223e4567-e89b-12d3-a456-426614174000'


class ScopedClientIntegrationTests(unittest.TestCase):
    @unittest.skipUnless(sys.platform == 'win32', 'Windows DPAPI required')
    def test_user_protected_credentials_work_across_both_clients(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            # Provisioning refuses any directory whose ACL is not private.
            credential_store.apply_private_acl(root, directory=True)
            broker = Broker(root / 'db.sqlite3')
            a = provision_initial_profile(broker, A, root)
            b = provision_initial_profile(broker, B, root)
            server = ThreadingHTTPServer(('127.0.0.1', 0), handler_for(broker, 'shared-token'))
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                state = root / 'state.json'
                state.write_text(json.dumps({'port': server.server_port, 'token': 'shared-token'}), encoding='utf-8')
                a_env = {'BRIDGE_SESSION_CREDENTIAL_FILE': a['credential_file'], 'BRIDGE_SESSION_CREDENTIAL_FORMAT': 'dpapi'}
                b_env = {'BRIDGE_SESSION_CREDENTIAL_FILE': b['credential_file'], 'BRIDGE_SESSION_CREDENTIAL_FORMAT': 'dpapi'}
                with patch.dict(os.environ, a_env):
                    codex = CodexClient(state)
                    codex.register(A, 'codex')
                with patch.dict(os.environ, b_env):
                    claude = ClaudeClient(state)
                    claude.register(B)
                with patch.dict(os.environ, a_env):
                    message = codex.send(A, B, 'question')
                with patch.dict(os.environ, b_env):
                    claude.acknowledge(message['id'], B)
                    reply = claude.send(B, A, 'answer', reply_to=message['id'])
                    with self.assertRaises(BrokerError):
                        claude.send(A, B, 'forged')
                with patch.dict(os.environ, a_env):
                    status = message_status(codex, B, message['id'], root / 'missing.sqlite3', actor=A)
                self.assertEqual('acknowledged', status['broker_status'])
                self.assertEqual(reply['id'], status['reply_message_id'])
            finally:
                server.shutdown()
                server.server_close()
                thread.join()

    def test_one_dispatcher_uses_distinct_credentials_for_two_participants(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            broker = Broker(root / 'db.sqlite3')
            a_secret, b_secret = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
            a_file, b_file = root / 'a.token', root / 'b.token'
            a_file.write_text(a_secret, encoding='ascii')
            b_file.write_text(b_secret, encoding='ascii')
            for ident, secret in ((A, a_secret), (B, b_secret)):
                broker.provision_session_credential(ident, secret)
                broker.register({'id': ident, 'kind': 'codex', 'opt_in': True}, credential=secret)
            broker.register({'id': 'legacy-sender', 'kind': 'claude', 'opt_in': True})
            server = ThreadingHTTPServer(('127.0.0.1', 0), handler_for(broker, 'shared-token'))
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            dispatcher = None
            try:
                state = root / 'state.json'
                state.write_text(json.dumps({'port': server.server_port, 'token': 'shared-token'}), encoding='utf-8')
                shared = CodexClient(state, credential_file=None)
                clients = {A: CodexClient(state, credential_file=a_file),
                           B: CodexClient(state, credential_file=b_file)}
                with self.assertRaises(ClientError):
                    validate_holder_participants(shared, [A])
                self.assertEqual([A, B], validate_holder_participants(shared, [A, B], clients))
                seen, received = set(), threading.Event()
                lock = threading.Lock()
                def drain(recipient, client, _ledger, **_kwargs):
                    for message in client.poll(recipient, status='queued')['messages']:
                        client.ack(message['id'], recipient)
                        with lock:
                            seen.add(recipient)
                            if seen == {A, B}:
                                received.set()
                    return []
                with patch('multi_dispatcher.ROOT', root):
                    dispatcher = MultiRecipientDispatcher([A, B], broker=shared, recipient_brokers=clients,
                        ledger=DeliveryLedger(root / 'ledger.sqlite3'), drain_function=drain,
                        wait_timeout=0.5, poll_interval=0.1, reprobe_interval=1).start()
                    shared.send('legacy-sender', A, 'one')
                    shared.send('legacy-sender', B, 'two')
                    self.assertTrue(received.wait(3), 'both protected recipients should receive their messages')
                    dispatcher.stop()
                    self.assertTrue(dispatcher.join(3))
                self.assertEqual({A, B}, seen)
            finally:
                if dispatcher is not None:
                    dispatcher.stop()
                    dispatcher.join(3)
                server.shutdown()
                server.server_close()
                thread.join()

    def test_scoped_clients_route_reply_and_cannot_claim_other_sender(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            broker = Broker(root / 'db.sqlite3')
            a_secret, b_secret = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
            broker.provision_session_credential(A, a_secret)
            broker.provision_session_credential(B, b_secret)
            a_file, b_file = root / 'a.token', root / 'b.token'
            a_file.write_text(a_secret + '\n', encoding='ascii')
            b_file.write_text(b_secret + '\n', encoding='ascii')
            server = ThreadingHTTPServer(('127.0.0.1', 0), handler_for(broker, 'shared-token'))
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                state = root / 'state.json'
                state.write_text(json.dumps({'port': server.server_port, 'token': 'shared-token'}), encoding='utf-8')
                with patch.dict(os.environ, {'BRIDGE_SESSION_CREDENTIAL_FILE': str(a_file)}):
                    codex = CodexClient(state)
                    codex.register(A, 'codex')
                with patch.dict(os.environ, {'BRIDGE_SESSION_CREDENTIAL_FILE': str(b_file)}):
                    claude = ClaudeClient(state)
                    claude.register(B)
                with patch.dict(os.environ, {'BRIDGE_SESSION_CREDENTIAL_FILE': str(a_file)}):
                    original = codex.send(A, B, 'question')
                    with self.assertRaises(ClientError):
                        codex.poll(B)
                with patch.dict(os.environ, {'BRIDGE_SESSION_CREDENTIAL_FILE': str(b_file)}):
                    with self.assertRaises(BrokerError):
                        claude.send(A, B, 'forged')
                    claude.acknowledge(original['id'], B)
                    reply = claude.send(B, A, 'answer', reply_to=original['id'])
                with patch.dict(os.environ, {'BRIDGE_SESSION_CREDENTIAL_FILE': str(a_file)}):
                    status = message_status(codex, B, original['id'], root / 'missing.sqlite3', actor=A)
                self.assertEqual('acknowledged', status['broker_status'])
                self.assertEqual('recorded', status['linked_reply_status'])
                self.assertEqual(reply['id'], status['reply_message_id'])
            finally:
                server.shutdown()
                server.server_close()
                thread.join()


if __name__ == '__main__':
    unittest.main()
