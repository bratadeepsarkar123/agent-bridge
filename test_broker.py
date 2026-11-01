import json
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from contextlib import closing
import unittest
from pathlib import Path
from http.server import ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.parse import quote
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

    def call(self, path, body=None, token='test-token', implicit_opt_in=True):
        if path == '/v1/register' and body is not None and implicit_opt_in:
            body = {'opt_in': True, **body}
        data = json.dumps(body).encode() if body is not None else None
        request = Request(self.url + path, data=data,
                          headers={'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json'})
        with urlopen(request, timeout=3) as response:
            return json.load(response)

    def register(self, data):
        return self.broker.register({'opt_in': True, **data})

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

    def test_direct_reply_requires_reverse_participants_and_is_delivered_with_link(self):
        for ident, kind in [('codex-a', 'codex'), ('claude-b', 'claude'), ('codex-c', 'codex')]:
            self.call('/v1/register', {'id': ident, 'kind': kind})
        original = self.call('/v1/messages', {
            'sender': 'codex-a', 'recipient': 'claude-b', 'text': 'question',
        })
        reply_payload = {
            'sender': 'claude-b', 'recipient': 'codex-a', 'text': 'answer',
            'reply_to': original['id'], 'id': str(uuid.uuid4()),
        }
        reply = self.call('/v1/messages', reply_payload)
        self.assertEqual(reply, self.call('/v1/messages', reply_payload))
        inbox_reply = next(item for item in self.call('/v1/messages?recipient=codex-a')['messages']
                           if item['id'] == reply['id'])
        self.assertEqual(original['id'], inbox_reply['reply_to'])
        waited = self.call('/v1/messages/wait?recipient=codex-a&after=0&timeout=0')
        delivered_reply = next(item for item in waited['messages'] if item['id'] == reply['id'])
        self.assertEqual(original['id'], delivered_reply['reply_to'])

        invalid_replies = [
            {'sender': 'codex-a', 'recipient': 'claude-b', 'text': 'same direction',
             'reply_to': original['id']},
            {'sender': 'codex-c', 'recipient': 'codex-a', 'text': 'wrong sender',
             'reply_to': original['id']},
            {'sender': 'claude-b', 'recipient': 'codex-c', 'text': 'wrong recipient',
             'reply_to': original['id']},
            {'sender': 'claude-b', 'recipient': 'codex-a', 'text': 'missing parent',
             'reply_to': str(uuid.uuid4())},
            {'sender': 'claude-b', 'recipient': 'codex-a', 'text': 'invalid parent',
             'reply_to': 'not an id'},
        ]
        for payload in invalid_replies:
            with self.subTest(payload=payload), self.assertRaises(HTTPError) as error:
                self.call('/v1/messages', payload)
            self.assertEqual(400, error.exception.code)

        second_original = self.call('/v1/messages', {
            'sender': 'codex-a', 'recipient': 'claude-b', 'text': 'another question',
        })
        idempotent_reply = {**reply_payload, 'id': str(uuid.uuid4()), 'reply_to': second_original['id']}
        self.assertEqual(self.call('/v1/messages', idempotent_reply),
                         self.call('/v1/messages', idempotent_reply))
        # Reuse one stable ID to prove reply linkage is part of idempotent content.
        with self.assertRaises(HTTPError) as error:
            self.call('/v1/messages', {**idempotent_reply, 'reply_to': original['id']})
        self.assertEqual(400, error.exception.code)

    def test_direct_reply_cannot_link_a_room_delivery(self):
        self.call('/v1/register', {'id': 'codex-owner', 'kind': 'codex'})
        self.call('/v1/register', {'id': 'claude-member', 'kind': 'claude'})
        self.call('/v1/rooms', {
            'room_id': 'room:direct-reply-check', 'name': 'Reply check',
            'owner': 'codex-owner', 'members': ['codex-owner', 'claude-member'],
        })
        room_message = self.call('/v1/rooms/room%3Adirect-reply-check/messages', {
            'sender': 'codex-owner', 'text': 'room message',
            'batch_id': 'c78e0e02-c062-4ddd-a26c-51392aeb1131',
        })
        room_delivery_id = room_message['deliveries'][0]['id']
        with self.assertRaises(HTTPError) as error:
            self.call('/v1/messages', {
                'sender': 'claude-member', 'recipient': 'codex-owner', 'text': 'direct reply',
                'reply_to': room_delivery_id,
            })
        self.assertEqual(400, error.exception.code)

    def test_inbox_status_filter_is_optional_and_available_over_http(self):
        self.call('/v1/register', {'id': 'codex-a', 'kind': 'codex'})
        self.call('/v1/register', {'id': 'claude-a', 'kind': 'claude'})
        first = self.call('/v1/messages', {
            'sender': 'codex-a', 'recipient': 'claude-a', 'text': 'acknowledged record',
        })
        second = self.call('/v1/messages', {
            'sender': 'codex-a', 'recipient': 'claude-a', 'text': 'queued record',
        })
        self.call('/v1/ack', {'id': first['id'], 'recipient': 'claude-a'})

        default_page = self.call('/v1/messages?recipient=claude-a')
        self.assertEqual([first['id'], second['id']],
                         [message['id'] for message in default_page['messages']])
        self.assertEqual(['acknowledged', 'queued'],
                         [message['status'] for message in default_page['messages']])
        queued_page = self.call('/v1/messages?recipient=claude-a&status=queued')
        self.assertEqual([second['id']], [message['id'] for message in queued_page['messages']])
        self.assertIsNone(queued_page['next_after'])
        with self.assertRaises(HTTPError) as error:
            self.call('/v1/messages?recipient=claude-a&status=unknown')
        self.assertEqual(400, error.exception.code)

    def test_inbox_queued_filter_paginates_without_skipping_queued_rows(self):
        self.register({'id': 'codex-a', 'kind': 'codex'})
        self.register({'id': 'claude-a', 'kind': 'claude'})
        rows, queued_ids = [], []
        for index in range(1005):
            queued_id = str(uuid.UUID(int=index + 1))
            queued_ids.append(queued_id)
            rows.append((queued_id, 'codex-a', 'claude-a', 'queued', 'queued', float(index)))
            if index % 200 == 0:
                rows.append((str(uuid.UUID(int=2000 + index)), 'codex-a', 'claude-a',
                             'acknowledged', 'acknowledged', float(index)))
        with closing(self.broker.connect()) as db, db:
            db.executemany(
                'INSERT INTO messages(id,sender,recipient,text,status,created) VALUES (?,?,?,?,?,?)', rows)

        first = self.broker.inbox('claude-a', status='queued')
        self.assertEqual(1000, len(first['messages']))
        self.assertTrue(all(message['status'] == 'queued' for message in first['messages']))
        self.assertIsNotNone(first['next_after'])
        second = self.broker.inbox('claude-a', after=first['next_after'], status='queued')
        self.assertEqual(5, len(second['messages']))
        self.assertIsNone(second['next_after'])
        self.assertEqual(queued_ids, [message['id'] for message in first['messages'] + second['messages']])

        default_first = self.broker.inbox('claude-a')
        self.assertEqual(1000, len(default_first['messages']))
        self.assertIsNotNone(default_first['next_after'])
        default_second = self.broker.inbox('claude-a', after=default_first['next_after'])
        self.assertEqual(len(rows) - 1000, len(default_second['messages']))
        self.assertIsNone(default_second['next_after'])

    def test_wait_endpoint_returns_queued_pages_after_cursor_and_keeps_history(self):
        for ident, kind in [('codex-a', 'codex'), ('claude-a', 'claude')]:
            self.call('/v1/register', {'id': ident, 'kind': kind})
        sent = [self.call('/v1/messages', {
            'sender': 'codex-a', 'recipient': 'claude-a', 'text': f'message {number}'
        }) for number in range(3)]
        history = self.call('/v1/messages?recipient=claude-a')['messages']
        self.call('/v1/ack', {'id': sent[0]['id'], 'recipient': 'claude-a'})

        first_page = self.call(
            '/v1/messages/wait?recipient=claude-a&after=0&timeout=0&limit=2')
        self.assertEqual([sent[1]['id'], sent[2]['id']],
                         [message['id'] for message in first_page['messages']])
        self.assertEqual(history[2]['seq'], first_page['cursor'])
        second_page = self.call(
            f"/v1/messages/wait?recipient=claude-a&after={first_page['cursor']}&timeout=0")
        self.assertEqual([], second_page['messages'])
        self.assertEqual(first_page['cursor'], second_page['cursor'])

        persisted = self.call('/v1/messages?recipient=claude-a')['messages']
        self.assertEqual(3, len(persisted))
        self.assertEqual('acknowledged', persisted[0]['status'])
        self.assertEqual('queued', persisted[1]['status'])

    def test_wait_endpoint_requires_cursor_and_rejects_out_of_bounds_options(self):
        self.call('/v1/register', {'id': 'claude-a', 'kind': 'claude'})
        for path in (
                '/v1/messages/wait?recipient=claude-a&timeout=0',
                '/v1/messages/wait?recipient=claude-a&after=0&timeout=31',
                '/v1/messages/wait?recipient=claude-a&after=0&timeout=0&limit=101'):
            with self.subTest(path=path), self.assertRaises(HTTPError) as error:
                self.call(path)
            self.assertEqual(400, error.exception.code)

    def test_wait_http_request_wakes_when_message_is_enqueued(self):
        self.call('/v1/register', {'id': 'codex-a', 'kind': 'codex'})
        self.call('/v1/register', {'id': 'claude-a', 'kind': 'claude'})
        queried = threading.Event()
        original_query = self.broker._queued_after

        def observe_query(recipient, after, limit):
            queried.set()
            return original_query(recipient, after, limit)

        self.broker._queued_after = observe_query
        results, errors = [], []

        def long_poll():
            try:
                results.append(self.call(
                    '/v1/messages/wait?recipient=claude-a&after=0&timeout=2'))
            except BaseException as exc:
                errors.append(exc)

        waiter = threading.Thread(target=long_poll)
        waiter.start()
        self.assertTrue(queried.wait(1), 'HTTP long poll did not query the inbox')
        sent = self.call('/v1/messages', {
            'sender': 'codex-a', 'recipient': 'claude-a', 'text': 'wake the HTTP poll'
        })
        waiter.join(3)
        self.assertFalse(waiter.is_alive(), 'HTTP long poll did not return after enqueue')
        self.assertEqual([], errors)
        self.assertEqual([sent['id']], [message['id'] for message in results[0]['messages']])

    def test_wait_registration_race_does_not_lose_enqueue_notification(self):
        self.register({'id': 'codex-a', 'kind': 'codex'})
        self.register({'id': 'claude-a', 'kind': 'claude'})
        queried = threading.Event()
        release_query = threading.Event()
        committed = threading.Event()
        original_query = self.broker._queued_after
        original_notify = self.broker._notify_messages
        results, errors = [], []

        def pause_after_empty_query(recipient, after, limit):
            rows = original_query(recipient, after, limit)
            if not rows and not queried.is_set():
                queried.set()
                if not release_query.wait(2):
                    raise AssertionError('test did not release the query gate')
            return rows

        def observe_committed_notification():
            committed.set()
            original_notify()

        self.broker._queued_after = pause_after_empty_query
        self.broker._notify_messages = observe_committed_notification

        def wait_for_message():
            try:
                results.append(self.broker.wait_for_messages('claude-a', 0, timeout=1))
            except BaseException as exc:
                errors.append(exc)

        waiter = threading.Thread(target=wait_for_message)
        waiter.start()
        self.assertTrue(queried.wait(1), 'waiter did not perform its initial empty query')
        sent, send_errors = [], []

        def enqueue():
            try:
                sent.append(self.broker.enqueue({
                    'sender': 'codex-a', 'recipient': 'claude-a', 'text': 'racing message'
                }))
            except BaseException as exc:
                send_errors.append(exc)

        sender = threading.Thread(target=enqueue)
        sender.start()
        try:
            self.assertTrue(committed.wait(1), 'enqueue did not commit before notification')
        finally:
            release_query.set()
        waiter.join(2)
        sender.join(2)
        self.assertFalse(waiter.is_alive(), 'long poll did not wake after enqueue')
        self.assertFalse(sender.is_alive(), 'enqueue remained blocked after waiter registration')
        self.assertEqual([], errors)
        self.assertEqual([], send_errors)
        self.assertEqual(sent[0]['id'], results[0]['messages'][0]['id'])

    def test_group_enqueue_notifies_waiting_recipient(self):
        for ident, kind in [('codex-a', 'codex'), ('claude-a', 'claude'), ('claude-b', 'claude')]:
            self.register({'id': ident, 'kind': kind})
        queried = threading.Event()
        original_query = self.broker._queued_after
        results, errors = [], []

        def observe_empty_query(recipient, after, limit):
            rows = original_query(recipient, after, limit)
            if recipient == 'claude-a' and not rows:
                queried.set()
            return rows

        self.broker._queued_after = observe_empty_query

        def wait_for_message():
            try:
                results.append(self.broker.wait_for_messages('claude-a', 0, timeout=1))
            except BaseException as exc:
                errors.append(exc)

        waiter = threading.Thread(target=wait_for_message)
        waiter.start()
        self.assertTrue(queried.wait(1), 'waiter did not perform its initial empty query')
        # Acquiring this lock proves the waiter has atomically registered in wait().
        with self.broker._message_condition:
            batch = self.broker.enqueue_group({
                'batch_id': '3c473ef3-bc27-4be8-93e5-9ed5dd35f95b',
                'sender': 'codex-a', 'recipients': ['claude-a', 'claude-b'],
                'text': 'group notification',
            })
        waiter.join(2)
        self.assertFalse(waiter.is_alive(), 'group enqueue did not wake the long poll')
        self.assertEqual([], errors)
        expected_id = next(item['id'] for item in batch['deliveries'] if item['recipient'] == 'claude-a')
        self.assertEqual([expected_id], [message['id'] for message in results[0]['messages']])

    def test_wait_timeout_rechecks_database_after_a_missed_notification(self):
        self.register({'id': 'codex-a', 'kind': 'codex'})
        self.register({'id': 'claude-a', 'kind': 'claude'})
        queried = threading.Event()
        release_query = threading.Event()
        original_query = self.broker._queued_after
        results, errors = [], []

        def pause_after_empty_query(recipient, after, limit):
            rows = original_query(recipient, after, limit)
            if not rows and not queried.is_set():
                queried.set()
                if not release_query.wait(2):
                    raise AssertionError('test did not release the query gate')
            return rows

        self.broker._queued_after = pause_after_empty_query

        def wait_for_message():
            try:
                results.append(self.broker.wait_for_messages('claude-a', 0, timeout=1))
            except BaseException as exc:
                errors.append(exc)

        waiter = threading.Thread(target=wait_for_message)
        waiter.start()
        self.assertTrue(queried.wait(1), 'waiter did not perform its initial empty query')
        ident = 'a8620daa-12fd-4689-9e8c-a6708cdb3655'
        with closing(self.broker.connect()) as db, db:
            db.execute(
                'INSERT INTO messages(id,sender,recipient,text,status,created) VALUES (?,?,?,?,?,?)',
                (ident, 'codex-a', 'claude-a', 'inserted without notification', 'queued', time.time()))
        release_query.set()
        waiter.join(3)
        self.assertFalse(waiter.is_alive(), 'waiter did not complete after its bounded timeout')
        self.assertEqual([], errors)
        self.assertEqual([ident], [message['id'] for message in results[0]['messages']])

    def test_authentication_and_missing_session(self):
        with self.assertRaises(HTTPError) as error:
            self.call('/v1/sessions', token='wrong')
        self.assertEqual(403, error.exception.code)
        self.call('/v1/register', {'id': 'codex-a', 'kind': 'codex'})
        with self.assertRaises(HTTPError) as error:
            self.call('/v1/messages', {'sender': 'codex-a', 'recipient': 'unknown', 'text': 'hi'})
        self.assertEqual(400, error.exception.code)

    def test_legacy_registration_requires_fresh_explicit_opt_in(self):
        old_db = Path(self.temp.name) / 'legacy-opt-in.sqlite3'
        with closing(sqlite3.connect(old_db)) as db, db:
            db.execute('CREATE TABLE sessions (id TEXT PRIMARY KEY, kind TEXT NOT NULL, updated REAL NOT NULL)')
            db.execute('INSERT INTO sessions VALUES (?, ?, ?)', ('legacy', 'codex', 1))
        broker = Broker(old_db)
        broker.register({'id': 'sender', 'kind': 'claude', 'opt_in': True})
        self.assertEqual(['sender'], [row['id'] for row in broker.sessions()['sessions']])

        for value in (None, False, 1, 'true'):
            with self.subTest(opt_in=value), self.assertRaises(ValueError):
                broker.register({'id': 'legacy', 'kind': 'codex', 'opt_in': value})
        with self.assertRaises(ValueError):
            broker.enqueue({'sender': 'sender', 'recipient': 'legacy', 'text': 'not opted in'})
        with self.assertRaises(ValueError):
            broker.enqueue_group({
                'batch_id': '4abf6dc9-9958-483c-81f3-ec6768dc0838',
                'sender': 'sender', 'recipients': ['legacy'], 'text': 'not opted in',
            })
        with self.assertRaises(ValueError):
            broker.inbox('legacy')
        with self.assertRaises(HTTPError) as error:
            self.call('/v1/register', {'id': 'http-legacy', 'kind': 'codex'}, implicit_opt_in=False)
        self.assertEqual(400, error.exception.code)

        broker.register({'id': 'legacy', 'kind': 'codex', 'opt_in': True})
        sent = broker.enqueue({'sender': 'sender', 'recipient': 'legacy', 'text': 'now opted in'})
        self.assertEqual(sent['id'], broker.inbox('legacy')['messages'][0]['id'])

    def test_missing_opt_in_is_heartbeat_only_for_same_opted_in_identity(self):
        self.broker.register({'id': 'opted-session', 'kind': 'codex', 'opt_in': True})
        with closing(self.broker.connect()) as db, db:
            db.execute('UPDATE sessions SET updated=0 WHERE id=?', ('opted-session',))

        heartbeat = self.call('/v1/register', {
            'id': 'opted-session', 'kind': 'codex',
        }, implicit_opt_in=False)
        self.assertEqual({'id': 'opted-session', 'kind': 'codex'}, heartbeat)
        with closing(self.broker.connect()) as db:
            row = db.execute('SELECT opted_in,updated FROM sessions WHERE id=?',
                             ('opted-session',)).fetchone()
        self.assertEqual(1, row['opted_in'])
        self.assertGreater(row['updated'], 0)

        with self.assertRaises(HTTPError) as error:
            self.call('/v1/register', {'id': 'unknown-session', 'kind': 'claude'},
                      implicit_opt_in=False)
        self.assertEqual(400, error.exception.code)
        with closing(self.broker.connect()) as db:
            self.assertIsNone(db.execute('SELECT 1 FROM sessions WHERE id=?',
                                         ('unknown-session',)).fetchone())

        with closing(self.broker.connect()) as db, db:
            db.execute('INSERT INTO sessions(id,kind,updated,opted_in) VALUES (?,?,?,0)',
                       ('legacy-session', 'claude', 0))
        with self.assertRaises(HTTPError) as error:
            self.call('/v1/register', {'id': 'legacy-session', 'kind': 'claude'},
                      implicit_opt_in=False)
        self.assertEqual(400, error.exception.code)
        with closing(self.broker.connect()) as db:
            self.assertEqual(0, db.execute('SELECT opted_in FROM sessions WHERE id=?',
                                           ('legacy-session',)).fetchone()['opted_in'])

        for data in (
                {'id': 'opted-session', 'kind': 'codex', 'opt_in': False},
                {'id': 'opted-session', 'kind': 'codex', 'opt_in': 1},
                {'id': 'opted-session', 'kind': 'codex', 'opt_in': 'true'},
                {'id': 'opted-session', 'kind': 'claude'},
                {'id': 'opted-session', 'kind': 'claude', 'opt_in': True}):
            with self.subTest(data=data), self.assertRaises(ValueError):
                self.broker.register(data)
        self.assertEqual(['opted-session'], [row['id'] for row in self.broker.sessions()['sessions']])

    def test_historical_sender_opt_in_controls_inbox_and_longpoll(self):
        with closing(self.broker.connect()) as db, db:
            db.execute('INSERT INTO sessions(id,kind,updated,opted_in) VALUES (?,?,?,0)',
                       ('old-sender', 'claude', 0))
            db.execute('INSERT INTO messages(id,sender,recipient,text,status,created) VALUES (?,?,?,?,?,?)',
                       ('old-message', 'old-sender', 'recipient', 'old queued text', 'queued', 0))
        self.register({'id': 'recipient', 'kind': 'codex'})
        self.assertEqual(['recipient'], [row['id'] for row in self.broker.sessions()['sessions']])
        self.assertEqual([], self.broker.inbox('recipient')['messages'])
        self.assertEqual([], self.broker.inbox('recipient', status='queued')['messages'])
        self.assertEqual({'messages': [], 'cursor': 0},
                         self.broker.wait_for_messages('recipient', 0, timeout=0))
        with self.assertRaises(ValueError):
            self.broker.ack({'id': 'old-message', 'recipient': 'recipient'})

        queried = threading.Event()
        original_query = self.broker._queued_after

        def observe_hidden_history(recipient, after, limit):
            rows = original_query(recipient, after, limit)
            if recipient == 'recipient' and not rows:
                queried.set()
            return rows

        self.broker._queued_after = observe_hidden_history
        results, errors = [], []

        def wait_for_history():
            try:
                results.append(self.broker.wait_for_messages('recipient', 0, timeout=1))
            except BaseException as exc:
                errors.append(exc)

        waiter = threading.Thread(target=wait_for_history)
        waiter.start()
        self.assertTrue(queried.wait(1), 'long poll did not check the hidden historical message')

        self.register({'id': 'old-sender', 'kind': 'claude'})
        waiter.join(2)
        self.assertFalse(waiter.is_alive(), 'sender opt-in did not wake the long poll')
        self.assertEqual([], errors)
        self.assertEqual(['old-message'], [row['id'] for row in results[0]['messages']])
        self.assertEqual(['old-message'], [row['id'] for row in self.broker.inbox('recipient')['messages']])
        self.assertEqual(['old-message'], [row['id'] for row in self.broker.inbox(
            'recipient', status='queued')['messages']])
        self.assertEqual(['old-message'], [row['id'] for row in self.broker.wait_for_messages(
            'recipient', 0, timeout=0)['messages']])

    def test_room_participants_and_reply_targets_must_be_opted_in(self):
        self.register({'id': 'owner', 'kind': 'codex'})
        self.register({'id': 'member', 'kind': 'claude'})
        with closing(self.broker.connect()) as db, db:
            db.execute('INSERT INTO sessions(id,kind,updated,opted_in) VALUES (?,?,?,0)',
                       ('legacy-member', 'claude', 0))

        room = {'room_id': 'room:opt-in', 'name': 'Opt in', 'owner': 'owner',
                'members': ['owner', 'member', 'legacy-member']}
        with self.assertRaises(ValueError):
            self.broker.create_room(room)
        room['members'] = ['owner', 'member']
        self.broker.create_room(room)
        with self.assertRaises(ValueError):
            self.broker.update_room_member('room:opt-in', {
                'actor': 'owner', 'member': 'legacy-member', 'action': 'add',
            })

        with closing(self.broker.connect()) as db, db:
            db.execute('INSERT INTO room_members(room_id,member_id) VALUES (?,?)',
                       ('room:opt-in', 'legacy-member'))
        self.assertEqual(['member', 'owner'], self.broker.room_info('room:opt-in', 'owner')['members'])
        with self.assertRaises(ValueError):
            self.broker.room_info('room:opt-in', 'legacy-member')
        with self.assertRaises(ValueError):
            self.broker.send_room_message('room:opt-in', {
                'sender': 'legacy-member', 'text': 'hidden sender',
                'batch_id': 'a3de89bf-b38e-41ee-a05f-8d73a8376c38',
            })
        sent = self.broker.send_room_message('room:opt-in', {
            'sender': 'owner', 'text': 'visible to opted-in member',
            'batch_id': 'b3de89bf-b38e-41ee-a05f-8d73a8376c38',
        })
        self.assertEqual(['member'], [row['recipient'] for row in sent['deliveries']])
        self.assertEqual('room:opt-in', self.broker.inbox('member', status='queued')['messages'][0]['room_id'])

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

    def test_named_room_membership_is_explicit_and_scoped(self):
        for ident, kind in [('codex-owner', 'codex'), ('claude-a', 'claude'),
                            ('claude-b', 'claude'), ('claude-outsider', 'claude')]:
            self.call('/v1/register', {'id': ident, 'kind': kind})
        room = {
            'room_id': 'room:design-team', 'name': 'Design team',
            'owner': 'codex-owner', 'members': ['codex-owner', 'claude-a'],
        }
        created = self.call('/v1/rooms', room)
        encoded_room_id = quote(room['room_id'], safe='')
        self.assertEqual(room['room_id'], created['room_id'])
        self.assertEqual(created, self.call('/v1/rooms', room))
        self.assertEqual(created, self.call(f'/v1/rooms/{encoded_room_id}?member=claude-a'))

        with self.assertRaises(HTTPError) as error:
            self.call('/v1/rooms/room:design-team?member=claude-outsider')
        self.assertEqual(400, error.exception.code)
        with self.assertRaises(HTTPError) as error:
            self.call('/v1/rooms')
        self.assertEqual(404, error.exception.code)

        added = self.call('/v1/rooms/room:design-team/members', {
            'actor': 'codex-owner', 'member': 'claude-b', 'action': 'add',
        })
        self.assertEqual(['claude-a', 'claude-b', 'codex-owner'], added['members'])
        removed = self.call('/v1/rooms/room:design-team/members', {
            'actor': 'codex-owner', 'member': 'claude-a', 'action': 'remove',
        })
        self.assertEqual(['claude-b', 'codex-owner'], removed['members'])
        with self.assertRaises(HTTPError) as error:
            self.call('/v1/rooms/room:design-team/members', {
                'actor': 'claude-b', 'member': 'claude-outsider', 'action': 'add',
            })
        self.assertEqual(400, error.exception.code)
        with self.assertRaises(HTTPError) as error:
            self.call('/v1/rooms', {**room, 'name': 'Different room'})
        self.assertEqual(400, error.exception.code)

    def test_room_message_reply_all_persists_room_and_reuses_batch_snapshot(self):
        for ident, kind in [('codex-owner', 'codex'), ('claude-a', 'claude'), ('claude-b', 'claude')]:
            self.call('/v1/register', {'id': ident, 'kind': kind})
        room_id = 'room:project-alpha'
        self.call('/v1/rooms', {
            'room_id': room_id, 'name': 'Project Alpha', 'owner': 'codex-owner',
            'members': ['codex-owner', 'claude-a', 'claude-b'],
        })
        encoded_room_id = quote(room_id, safe='')
        original_payload = {
            'sender': 'codex-owner', 'text': 'initial room message',
            'batch_id': '568b2642-2412-494a-9b0a-685b6546cab2',
        }
        original = self.call(f'/v1/rooms/{encoded_room_id}/messages', original_payload)
        self.assertEqual(room_id, original['room_id'])
        self.assertEqual(['claude-a', 'claude-b'],
                         [item['recipient'] for item in original['deliveries']])
        parent_id = original['deliveries'][0]['id']
        for recipient in ('claude-a', 'claude-b'):
            event = self.call(f'/v1/messages?recipient={recipient}')['messages'][0]
            self.assertEqual(room_id, event['room_id'])
            self.assertIsNone(event['reply_to'])

        other_room_id = 'room:other-project'
        self.call('/v1/rooms', {
            'room_id': other_room_id, 'name': 'Other project', 'owner': 'codex-owner',
            'members': ['codex-owner', 'claude-b'],
        })
        other_message = self.call(f'/v1/rooms/{other_room_id}/messages', {
            'sender': 'codex-owner', 'text': 'message from another room',
            'batch_id': 'e23a2497-1854-41d4-9dfc-71e73b8cfe76',
        })
        other_parent_id = other_message['deliveries'][0]['id']
        member_payload = {
            'sender': 'claude-b', 'text': 'member-authored message',
            'batch_id': 'e3f63e74-f533-4fd1-849b-56f1b04ef6a5',
        }
        self.call(f'/v1/rooms/{encoded_room_id}/messages', member_payload)

        self.call(f'/v1/rooms/{room_id}/members', {
            'actor': 'codex-owner', 'member': 'claude-b', 'action': 'remove',
        })
        retry = self.call(f'/v1/rooms/{encoded_room_id}/messages', original_payload)
        self.assertEqual([item['id'] for item in original['deliveries']],
                         [item['id'] for item in retry['deliveries']])
        with self.assertRaises(HTTPError) as error:
            self.call(f'/v1/rooms/{encoded_room_id}/messages', member_payload)
        self.assertEqual(400, error.exception.code)

        reply_payload = {
            'sender': 'codex-owner', 'text': 'reply all',
            'batch_id': 'cbe12cf1-cf2e-4fa8-9e56-7ca72dbd87f7',
            'reply_to': parent_id,
        }
        reply = self.call(f'/v1/rooms/{encoded_room_id}/reply-all', reply_payload)
        self.assertEqual(room_id, reply['room_id'])
        self.assertEqual(parent_id, reply['reply_to'])
        self.assertEqual(['claude-a'], [item['recipient'] for item in reply['deliveries']])
        reply_event = next(message for message in self.call(
            '/v1/messages?recipient=claude-a')['messages']
                           if message['id'] == reply['deliveries'][0]['id'])
        self.assertEqual(room_id, reply_event['room_id'])
        self.assertEqual(parent_id, reply_event['reply_to'])
        self.assertEqual(reply['deliveries'], self.call(
            f'/v1/rooms/{encoded_room_id}/reply-all', reply_payload)['deliveries'])
        missing_reply_targets = (
            {
                'sender': 'codex-owner', 'text': 'reply without a target',
                'batch_id': 'b29fd9ce-9392-488f-844b-82496f1fe24f',
            },
            {
                'sender': 'codex-owner', 'text': 'reply with an empty target',
                'batch_id': '85d2b5f9-2b2b-478f-9130-1586a8c117f8', 'reply_to': '',
            },
        )
        for missing_target in missing_reply_targets:
            with self.subTest(reply_to=missing_target.get('reply_to', '<omitted>')):
                with self.assertRaises(HTTPError) as error:
                    self.call(f'/v1/rooms/{encoded_room_id}/reply-all', missing_target)
                self.assertEqual(400, error.exception.code)
        with self.assertRaises(HTTPError) as error:
            self.call(f'/v1/rooms/{encoded_room_id}/reply-all', {
                **reply_payload, 'reply_to': other_parent_id,
            })
        self.assertEqual(400, error.exception.code)
        with self.assertRaises(HTTPError) as error:
            self.call(f'/v1/rooms/{encoded_room_id}/reply-all', {
                'sender': 'claude-b', 'text': 'removed members cannot reply',
                'batch_id': '6c5b1819-9d0d-4d1c-96c5-8cafc4e6488e',
                'reply_to': parent_id,
            })
        self.assertEqual(400, error.exception.code)

    def test_room_fanout_rolls_back_batch_and_all_messages_on_insert_failure(self):
        for ident, kind in [('codex-owner', 'codex'), ('claude-a', 'claude'), ('claude-b', 'claude')]:
            self.register({'id': ident, 'kind': kind})
        room_id = 'room:atomic'
        self.broker.create_room({
            'room_id': room_id, 'name': 'Atomic room', 'owner': 'codex-owner',
            'members': ['codex-owner', 'claude-a', 'claude-b'],
        })
        payload = {
            'sender': 'codex-owner', 'text': 'must be atomic',
            'batch_id': 'abfb1f39-2e34-4ec2-830b-65faf50bf5a3',
        }
        with closing(self.broker.connect()) as db, db:
            db.execute(f"""
                CREATE TRIGGER reject_room_delivery
                BEFORE INSERT ON messages
                WHEN NEW.recipient='claude-b' AND NEW.room_id='{room_id}'
                BEGIN SELECT RAISE(ABORT, 'injected failure'); END
            """)
        with self.assertRaises(sqlite3.IntegrityError):
            self.broker.send_room_message(room_id, payload)

        self.assertEqual([], self.broker.inbox('claude-a')['messages'])
        self.assertEqual([], self.broker.inbox('claude-b')['messages'])
        with closing(self.broker.connect()) as db, db:
            self.assertEqual(0, db.execute(
                'SELECT COUNT(*) FROM group_batches WHERE batch_id=?', (payload['batch_id'],)
            ).fetchone()[0])
            self.assertEqual(0, db.execute(
                'SELECT COUNT(*) FROM group_batch_deliveries WHERE batch_id=?', (payload['batch_id'],)
            ).fetchone()[0])
            db.execute('DROP TRIGGER reject_room_delivery')

        retried = self.broker.send_room_message(room_id, payload)
        self.assertEqual(2, len(retried['deliveries']))
        self.assertEqual(1, len(self.broker.inbox('claude-a')['messages']))
        self.assertEqual(1, len(self.broker.inbox('claude-b')['messages']))

    def test_room_schema_migrates_existing_queued_messages(self):
        legacy_db = Path(self.temp.name) / 'legacy.sqlite3'
        with closing(sqlite3.connect(legacy_db)) as db, db:
            db.executescript("""
                CREATE TABLE sessions (id TEXT PRIMARY KEY, kind TEXT NOT NULL, updated REAL NOT NULL);
                CREATE TABLE messages (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    id TEXT UNIQUE NOT NULL, sender TEXT NOT NULL, recipient TEXT NOT NULL,
                    text TEXT NOT NULL, status TEXT NOT NULL, created REAL NOT NULL, acknowledged REAL
                );
                CREATE TABLE group_batches (
                    batch_id TEXT PRIMARY KEY, sender TEXT NOT NULL, recipients_json TEXT NOT NULL,
                    text_sha256 TEXT NOT NULL, created REAL NOT NULL
                );
                CREATE TABLE group_batch_deliveries (
                    batch_id TEXT NOT NULL, recipient TEXT NOT NULL, message_id TEXT UNIQUE NOT NULL,
                    PRIMARY KEY(batch_id, recipient)
                );
            """)
            db.executemany('INSERT INTO sessions(id,kind,updated) VALUES (?,?,?)', [
                ('codex-legacy', 'codex', time.time()), ('claude-legacy', 'claude', time.time()),
            ])
            db.execute(
                'INSERT INTO messages(id,sender,recipient,text,status,created) VALUES (?,?,?,?,?,?)',
                ('legacy-message', 'codex-legacy', 'claude-legacy', 'still queued', 'queued', time.time()))

        migrated = Broker(legacy_db)
        with self.assertRaises(ValueError):
            migrated.inbox('claude-legacy')
        migrated.register({'id': 'codex-legacy', 'kind': 'codex', 'opt_in': True})
        migrated.register({'id': 'claude-legacy', 'kind': 'claude', 'opt_in': True})
        old_event = migrated.inbox('claude-legacy')['messages'][0]
        self.assertEqual('legacy-message', old_event['id'])
        self.assertIsNone(old_event['room_id'])
        self.assertIsNone(old_event['reply_to'])
        migrated.create_room({
            'room_id': 'room:legacy', 'name': 'Migrated room', 'owner': 'codex-legacy',
            'members': ['codex-legacy', 'claude-legacy'],
        })
        sent = migrated.send_room_message('room:legacy', {
            'sender': 'codex-legacy', 'text': 'new schema works',
            'batch_id': '6e97ea9e-70f3-48a0-b1b3-bb089a7420c7',
        })
        self.assertEqual('room:legacy', migrated.inbox('claude-legacy')['messages'][1]['room_id'])
        self.assertEqual(1, len(sent['deliveries']))

    def test_group_fanout_rolls_back_all_recipients_on_insert_failure(self):
        for ident, kind in [('codex-sender', 'codex'), ('claude-a', 'claude'), ('claude-b', 'claude')]:
            self.register({'id': ident, 'kind': kind})
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
        self.register({'id': 'codex-sender', 'kind': 'codex'})
        self.register({'id': 'claude-a', 'kind': 'claude'})
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
