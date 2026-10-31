import json
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from contextlib import closing
import unittest
from pathlib import Path
from http.server import ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from broker import Broker, handler_for


class BrokerTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / 'queue.sqlite3'
        self.broker = Broker(self.db)
        self.server = ThreadingHTTPServer(('127.0.0.1', 0), handler_for(self.broker, 'test-token'))
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = 'http://127.0.0.1:' + str(self.server.server_port)

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
        self.temp.cleanup()

    def call(self, path, body=None, token='test-token'):
        if path == '/v1/register' and body is not None:
            body = {'opt_in': True, **body}
        data = json.dumps(body).encode() if body is not None else None
        request = Request(self.url + path, data=data,
                          headers={'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json'})
        with urlopen(request, timeout=3) as response:
            return json.load(response)

    def test_routing_acknowledgment_and_persistence(self):
        for ident, kind in [('claude-a', 'claude'), ('claude-b', 'claude'), ('codex-a', 'codex')]:
            self.call('/v1/register', {'id': ident, 'kind': kind})
        msg = self.call('/v1/messages', {'sender': 'codex-a', 'recipient': 'claude-a', 'text': 'hello'})
        self.assertEqual('queued', msg['status'])
        self.assertEqual([], self.call('/v1/messages?recipient=claude-b')['messages'])
        self.assertEqual(msg['id'], self.call('/v1/messages?recipient=claude-a')['messages'][0]['id'])
        with self.assertRaises(HTTPError):
            self.call('/v1/ack', {'id': msg['id'], 'recipient': 'claude-b'})
        restarted = Broker(self.db)
        self.assertEqual('queued', restarted.inbox('claude-a')['messages'][0]['status'])
        self.call('/v1/ack', {'id': msg['id'], 'recipient': 'claude-a'})
        self.assertEqual('acknowledged', restarted.inbox('claude-a')['messages'][0]['status'])
        self.assertEqual('acknowledged', self.call('/v1/ack', {'id': msg['id'], 'recipient': 'claude-a'})['status'])

    def test_authentication_and_missing_session(self):
        with self.assertRaises(HTTPError) as error:
            self.call('/v1/sessions', token='wrong')
        self.assertEqual(403, error.exception.code)
        self.call('/v1/register', {'id': 'codex-a', 'kind': 'codex'})
        with self.assertRaises(HTTPError) as error:
            self.call('/v1/messages', {'sender': 'codex-a', 'recipient': 'unknown', 'text': 'hi'})
        self.assertEqual(400, error.exception.code)

    def test_legacy_registration_requires_fresh_explicit_opt_in(self):
        old_db = Path(self.temp.name) / 'legacy.sqlite3'
        with closing(sqlite3.connect(old_db)) as db, db:
            db.execute('CREATE TABLE sessions (id TEXT PRIMARY KEY, kind TEXT NOT NULL, updated REAL NOT NULL)')
            db.execute('INSERT INTO sessions VALUES (?, ?, ?)', ('legacy', 'codex', 1))
        broker = Broker(old_db)
        broker.register({'id': 'sender', 'kind': 'claude', 'opt_in': True})
        self.assertEqual(['sender'], [s['id'] for s in broker.sessions()['sessions']])
        for value in (None, False, 1, 'true'):
            with self.subTest(value=value), self.assertRaises(ValueError):
                broker.register({'id': 'legacy', 'kind': 'codex', 'opt_in': value})
        with self.assertRaises(ValueError):
            broker.enqueue({'sender': 'sender', 'recipient': 'legacy', 'text': 'hello'})
        with self.assertRaises(ValueError):
            broker.inbox('legacy')
        broker.register({'id': 'legacy', 'kind': 'codex', 'opt_in': True})
        broker.enqueue({'sender': 'sender', 'recipient': 'legacy', 'text': 'hello'})
        self.assertEqual('hello', broker.inbox('legacy')['messages'][0]['text'])

    def test_old_queued_message_waits_for_sender_opt_in(self):
        with closing(self.broker.connect()) as db, db:
            db.execute('INSERT INTO sessions(id,kind,updated,opted_in) VALUES (?, ?, ?, 0)',
                       ('old-sender', 'claude', 0))
            db.execute('INSERT INTO messages(id,sender,recipient,text,status,created) VALUES (?,?,?,?,?,?)',
                       ('old-message', 'old-sender', 'recipient', 'old text', 'queued', 0))
        self.broker.register({'id': 'recipient', 'kind': 'codex', 'opt_in': True})
        self.assertEqual([], self.broker.inbox('recipient')['messages'])
        self.broker.register({'id': 'old-sender', 'kind': 'claude', 'opt_in': True})
        self.assertEqual(['old text'], [m['text'] for m in self.broker.inbox('recipient')['messages']])

    def test_five_sessions_route_only_to_named_recipients(self):
        for ident in ('claude-1', 'claude-2', 'claude-3', 'codex-1', 'codex-2'):
            self.call('/v1/register', {'id': ident, 'kind': ident.split('-')[0]})
        self.call('/v1/messages', {'sender': 'codex-1', 'recipient': 'claude-1', 'text': 'one'})
        self.call('/v1/messages', {'sender': 'claude-1', 'recipient': 'codex-2', 'text': 'two'})
        self.call('/v1/messages', {'sender': 'claude-2', 'recipient': 'claude-3', 'text': 'three'})
        self.assertEqual(['one'], [m['text'] for m in self.call('/v1/messages?recipient=claude-1')['messages']])
        self.assertEqual(['two'], [m['text'] for m in self.call('/v1/messages?recipient=codex-2')['messages']])
        self.assertEqual(['three'], [m['text'] for m in self.call('/v1/messages?recipient=claude-3')['messages']])
        self.assertEqual([], self.call('/v1/messages?recipient=claude-2')['messages'])
        self.assertEqual([], self.call('/v1/messages?recipient=codex-1')['messages'])

    def test_group_fanout_is_idempotent_and_acknowledged_per_recipient(self):
        for ident, kind in [('codex-sender', 'codex'), ('claude-a', 'claude'), ('claude-b', 'claude')]:
            self.call('/v1/register', {'id': ident, 'kind': kind})
        payload = {
            'batch_id': '8b891c97-f5cc-4c85-b7c6-32290bda3121',
            'sender': 'codex-sender',
            'recipients': ['claude-b', 'claude-a'],
            'text': 'group hello',
        }

        first = self.call('/v1/group-messages', payload)
        self.assertEqual(payload['batch_id'], first['batch_id'])
        self.assertEqual(['claude-a', 'claude-b'],
                         [item['recipient'] for item in first['deliveries']])
        self.assertEqual(2, len({item['id'] for item in first['deliveries']}))
        self.assertTrue(all(item['status'] == 'queued' for item in first['deliveries']))
        for delivery in first['deliveries']:
            inbox = self.call('/v1/messages?recipient=' + delivery['recipient'])['messages']
            self.assertEqual([delivery['id']], [message['id'] for message in inbox])

        first_delivery = first['deliveries'][0]
        self.call('/v1/ack', {'id': first_delivery['id'], 'recipient': first_delivery['recipient']})
        retry = self.call('/v1/group-messages', {**payload, 'recipients': list(reversed(payload['recipients']))})
        self.assertEqual(first['deliveries'][0]['id'], retry['deliveries'][0]['id'])
        self.assertEqual('acknowledged', retry['deliveries'][0]['status'])
        self.assertEqual('queued', retry['deliveries'][1]['status'])
        self.assertEqual(1, len(self.call('/v1/messages?recipient=claude-a')['messages']))
        self.assertEqual(1, len(self.call('/v1/messages?recipient=claude-b')['messages']))

        with self.assertRaises(HTTPError) as error:
            self.call('/v1/group-messages', {**payload, 'text': 'changed'})
        self.assertEqual(400, error.exception.code)

    def test_group_fanout_rolls_back_all_recipients_on_insert_failure(self):
        for ident, kind in [('codex-sender', 'codex'), ('claude-a', 'claude'), ('claude-b', 'claude')]:
            self.broker.register({'id': ident, 'kind': kind, 'opt_in': True})
        with closing(self.broker.connect()) as db, db:
            db.execute("""
                CREATE TRIGGER reject_second_group_delivery
                BEFORE INSERT ON messages WHEN NEW.recipient='claude-b'
                BEGIN SELECT RAISE(ABORT, 'injected failure'); END
            """)

        with self.assertRaises(sqlite3.IntegrityError):
            self.broker.enqueue_group({
                'batch_id': '0fce90c6-45e4-4d90-a58d-6b5f53798071',
                'sender': 'codex-sender',
                'recipients': ['claude-a', 'claude-b'],
                'text': 'atomic group hello',
            })

        self.assertEqual([], self.broker.inbox('claude-a')['messages'])
        self.assertEqual([], self.broker.inbox('claude-b')['messages'])
        with closing(self.broker.connect()) as db:
            self.assertEqual(0, db.execute('SELECT COUNT(*) FROM group_batches').fetchone()[0])
            self.assertEqual(0, db.execute('SELECT COUNT(*) FROM group_batch_deliveries').fetchone()[0])

    def test_group_fanout_rejects_unregistered_recipient_without_partial_delivery(self):
        self.broker.register({'id': 'codex-sender', 'kind': 'codex', 'opt_in': True})
        self.broker.register({'id': 'claude-a', 'kind': 'claude', 'opt_in': True})
        with self.assertRaises(ValueError):
            self.broker.enqueue_group({
                'batch_id': 'b7a4c497-3722-4f55-a635-b044af4a2691',
                'sender': 'codex-sender',
                'recipients': ['claude-a', 'unknown'],
                'text': 'hello',
            })
        self.assertEqual([], self.broker.inbox('claude-a')['messages'])

    def test_system_monitor_has_explicit_identity(self):
        self.call('/v1/register', {'id': 'system:monitor', 'kind': 'system'})
        self.call('/v1/register', {'id': 'codex-1', 'kind': 'codex'})
        self.call('/v1/messages', {'sender': 'system:monitor', 'recipient': 'codex-1', 'text': 'stopped'})
        self.assertEqual('system:monitor', self.call('/v1/messages?recipient=codex-1')['messages'][0]['sender'])

    def test_message_id_makes_retries_idempotent_and_rejects_changed_content(self):
        self.call('/v1/register', {'id': 'system:monitor', 'kind': 'system'})
        self.call('/v1/register', {'id': 'codex-1', 'kind': 'codex'})
        msg = {'id': 'c78e0e02-c062-4ddd-a26c-51392aeb1131', 'sender': 'system:monitor',
               'recipient': 'codex-1', 'text': 'stopped'}
        self.assertEqual(self.call('/v1/messages', msg), self.call('/v1/messages', msg))
        self.assertEqual(1, len(self.call('/v1/messages?recipient=codex-1')['messages']))
        with self.assertRaises(HTTPError):
            self.call('/v1/messages', {**msg, 'text': 'changed'})

    def test_recovery_reuses_endpoint_and_preserves_queued_data(self):
        script = str(Path(__file__).with_name('broker.py'))
        state = Path(self.temp.name) / 'recovery.json'
        db = Path(self.temp.name) / 'recovery.sqlite3'
        args = [sys.executable, '-B', script, '--database', str(db), '--state', str(state)]
        process = subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        recovered = None
        try:
            for _ in range(100):
                if state.exists() and state.stat().st_size:
                    try:
                        before = json.loads(state.read_text(encoding='utf-8'))
                        break
                    except json.JSONDecodeError:
                        pass
                if process.poll() is not None:
                    self.fail('test broker failed to start')
                time.sleep(.05)
            else:
                self.fail('test broker did not start')
            saved = Broker(db)
            saved.register({'id': 'codex-test', 'kind': 'codex', 'opt_in': True})
            saved.register({'id': 'claude-test', 'kind': 'claude', 'opt_in': True})
            saved.enqueue({'sender': 'codex-test', 'recipient': 'claude-test', 'text': 'pending'})
            process.terminate()
            process.wait(timeout=5)
            recovered = subprocess.Popen(args + ['--recover'], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
            for _ in range(100):
                current = json.loads(state.read_text(encoding='utf-8'))
                if current['pid'] == recovered.pid:
                    break
                if recovered.poll() is not None:
                    self.fail('test broker failed to recover')
                time.sleep(.05)
            else:
                self.fail('test broker did not recover')
            self.assertEqual(before['port'], current['port'])
            self.assertEqual(before['token'], current['token'])
            self.assertEqual('pending', Broker(db).inbox('claude-test')['messages'][0]['text'])
        finally:
            for child in (process, recovered):
                if child is not None and child.poll() is None:
                    child.terminate()
                    child.wait(timeout=5)
                if child is not None and child.stderr:
                    child.stderr.close()


if __name__ == '__main__':
    unittest.main()
