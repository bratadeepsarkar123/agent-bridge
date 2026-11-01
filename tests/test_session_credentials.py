"""Isolated credential contract tests: ephemeral DBs and loopback HTTP only."""
from contextlib import closing, redirect_stdout, redirect_stderr
import hashlib
from http.client import HTTPConnection
from http.server import ThreadingHTTPServer
import io
import json
from pathlib import Path
import secrets
import sqlite3
import tempfile
import threading
import unittest
import uuid

from broker import Broker, handler_for


class SessionCredentialTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'isolated.sqlite3'
        self.broker = Broker(self.path)
        self.shared = secrets.token_urlsafe(32)
        self.a = secrets.token_urlsafe(32)
        self.b = secrets.token_urlsafe(32)
        self.server = ThreadingHTTPServer(('127.0.0.1', 0), handler_for(self.broker, self.shared))
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.close_server)
        self.broker.provision_session_credential('alice', self.a)
        self.broker.provision_session_credential('bob', self.b)
        for ident, credential in [('alice', self.a), ('bob', self.b), ('legacy', None), ('peer', None)]:
            self.broker.register({'id': ident, 'kind': 'codex', 'opt_in': True}, credential=credential)

    def close_server(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(2)
        self.assertFalse(self.thread.is_alive())

    def call(self, path, body=None, token=None, expected=200, headers=None):
        conn = HTTPConnection('127.0.0.1', self.server.server_port, timeout=3)
        try:
            request_headers = {'Authorization': 'Bearer ' + (self.shared if token is None else token)}
            request_headers.update(headers or {})
            conn.request('GET' if body is None else 'POST', path,
                         body=None if body is None else json.dumps(body), headers=request_headers)
            response = conn.getresponse()
            result = json.loads(response.read())
            self.assertEqual(expected, response.status, result)
            self.assertEqual('no-store', response.getheader('Cache-Control'))
            # Every response, including failures, must omit all credential material.
            encoded = json.dumps(result)
            for secret in (self.shared, self.a, self.b):
                self.assertNotIn(secret, encoded)
                self.assertNotIn(hashlib.sha256(secret.encode()).hexdigest(), encoded)
            return result
        finally:
            conn.close()

    def room(self):
        return self.call('/v1/rooms', {'room_id': 'team', 'name': 'Team', 'owner': 'alice',
                                     'members': ['alice', 'bob', 'legacy']}, self.a)

    def test_initial_binding_does_not_replace_and_default_still_rotates(self):
        fresh = secrets.token_urlsafe(32)
        replacement = secrets.token_urlsafe(32)
        self.broker.provision_session_credential('new-profile', fresh, replace=False)
        for value in (fresh, replacement):
            with self.assertRaises(ValueError):
                self.broker.provision_session_credential('new-profile', value, replace=False)
        self.assertTrue(self.broker.credential_is_valid(fresh))
        self.assertFalse(self.broker.credential_is_valid(replacement))
        self.broker.provision_session_credential('new-profile', replacement)
        self.assertFalse(self.broker.credential_is_valid(fresh))
        self.assertTrue(self.broker.credential_is_valid(replacement))
        self.assertNotIn('new-profile', [row['id'] for row in self.broker.sessions()['sessions']])
        for invalid in (None, 0, 1, 'false'):
            with self.assertRaises(ValueError):
                self.broker.provision_session_credential('invalid-option', fresh, replace=invalid)

    def test_initial_binding_race_across_broker_instances_has_one_winner(self):
        from concurrent.futures import ThreadPoolExecutor
        gate = threading.Barrier(2)
        credentials = [secrets.token_urlsafe(32), secrets.token_urlsafe(32)]
        brokers = [Broker(self.path), Broker(self.path)]
        def attempt(index):
            gate.wait(timeout=3)
            try:
                brokers[index].provision_session_credential('race-profile', credentials[index], replace=False)
                return True
            except ValueError:
                return False
        with ThreadPoolExecutor(max_workers=2) as pool:
            outcomes = list(pool.map(attempt, range(2)))
        self.assertEqual(1, sum(outcomes))
        for credential, won in zip(credentials, outcomes):
            self.assertEqual(won, self.broker.credential_is_valid(credential))

    def test_session_check_exact_identity_legacy_and_scoped_credentials(self):
        self.assertEqual({'id': 'legacy', 'kind': 'codex'},
                         self.call('/v1/sessions/check?id=legacy'))
        self.assertEqual({'id': 'alice', 'kind': 'codex'},
                         self.call('/v1/sessions/check?id=alice', token=self.a))
        self.assertEqual({'id': 'bob', 'kind': 'codex'},
                         self.call('/v1/sessions/check?id=bob', token=self.b))
        for ident, token in [('alice', self.shared), ('alice', self.b), ('bob', self.a),
                             ('legacy', self.a), ('alice', 'invalid'), ('legacy', 'invalid')]:
            with self.subTest(ident=ident):
                self.assertEqual({'error': 'unauthorized'}, self.call(
                    '/v1/sessions/check?id=' + ident, token=token, expected=403))
        with self.assertRaises(PermissionError):
            self.broker.check_session('alice')
        with self.assertRaises(PermissionError):
            self.broker.check_session('alice', credential=self.b)
        self.assertEqual({'id': 'alice', 'kind': 'codex'},
                         self.broker.check_session('alice', credential=self.a))
        self.broker.provision_session_credential('alice', secrets.token_urlsafe(32))
        self.call('/v1/sessions/check?id=alice', token=self.a, expected=403)

    def test_session_check_requires_opt_in_and_returns_no_message_data_or_mutation(self):
        self.broker.enqueue({'sender': 'legacy', 'recipient': 'alice', 'text': 'private message body'})
        with closing(self.broker.connect()) as db, db:
            db.execute("INSERT INTO sessions(id,kind,updated,opted_in) VALUES ('inactive','claude',0,0)")
        reserved = secrets.token_urlsafe(32)
        self.broker.provision_session_credential('reserved', reserved)
        with closing(self.broker.connect()) as db:
            before = '\n'.join(db.iterdump())
        self.assertEqual({'id': 'alice', 'kind': 'codex'},
                         self.call('/v1/sessions/check?id=alice', token=self.a))
        for ident, token in [('inactive', self.shared), ('unknown', self.shared), ('reserved', reserved)]:
            self.call('/v1/sessions/check?id=' + ident, token=token, expected=400)
        self.call('/v1/sessions/check?id=reserved', expected=403)
        for path in ('/v1/sessions/check', '/v1/sessions/check?id=',
                     '/v1/sessions/check?id=../bad', '/v1/sessions/check?id=alice&id=legacy'):
            self.call(path, token=self.a, expected=400)
        with closing(self.broker.connect()) as db:
            self.assertEqual(before, '\n'.join(db.iterdump()))

    def test_room_reply_evidence_is_visible_to_original_sender_and_recipient(self):
        self.room()
        sent = self.call('/v1/rooms/team/messages', {'sender': 'alice', 'text': 'question',
                                                      'batch_id': str(uuid.uuid4())}, self.a)
        original = next(item['id'] for item in sent['deliveries'] if item['recipient'] == 'bob')
        reply = self.call('/v1/rooms/team/reply-all', {'sender': 'bob', 'text': 'answer',
                          'reply_to': original, 'batch_id': str(uuid.uuid4())}, self.b)
        reply_id = next(item['id'] for item in reply['deliveries'] if item['recipient'] == 'alice')
        path = '/v1/messages/status?recipient=bob&id=' + original
        for actor, credential in [('alice', self.a), ('bob', self.b)]:
            status = self.call(path + '&actor=' + actor, token=credential)
            self.assertEqual('recorded', status['linked_reply_status'])
            self.assertEqual(reply_id, status['reply_message_id'])

    def test_registration_reserved_before_opt_in_and_cannot_be_spoofed(self):
        token = secrets.token_urlsafe(32)
        self.broker.provision_session_credential('reserved', token)
        self.assertNotIn('reserved', [s['id'] for s in self.broker.sessions()['sessions']])
        data = {'id': 'reserved', 'kind': 'codex', 'opt_in': True}
        for bearer in (self.shared, self.a, 'invalid'):
            self.call('/v1/register', data, bearer, expected=403)
        self.call('/v1/register', data, token)
        self.call('/v1/register', {'id': 'alice', 'kind': 'codex'}, self.shared, expected=403)
        self.call('/v1/register', {'id': 'alice', 'kind': 'codex'}, self.a)
        self.call('/v1/register', {'id': 'legacy', 'kind': 'codex'}, self.a, expected=403)
        self.call('/v1/register', {'id': 'new', 'kind': 'codex', 'opt_in': True})

    def test_all_sender_routes_scope_actor_and_allow_protected_destinations(self):
        self.room()
        parent = self.call('/v1/rooms/team/messages', {
            'sender': 'alice', 'text': 'room parent', 'batch_id': str(uuid.uuid4())}, self.a)
        parent_id = parent['deliveries'][0]['id']
        routes = [('/v1/messages', {'recipient': 'bob'}),
                  ('/v1/group-messages', {'recipients': ['bob', 'legacy'], 'batch_id': str(uuid.uuid4())}),
                  ('/v1/rooms/team/messages', {'batch_id': str(uuid.uuid4())}),
                  ('/v1/rooms/team/reply-all', {'batch_id': str(uuid.uuid4()), 'reply_to': parent_id})]
        for route, extra in routes:
            data = {'sender': 'alice', 'text': 'scoped content', **extra}
            with self.subTest(route=route):
                for bearer in (self.shared, self.b):
                    self.call(route, data, bearer, expected=403)
                first = self.call(route, data, self.a)
                if 'batch_id' in data:
                    self.assertEqual(first, self.call(route, data, self.a))
                self.call(route, {**data, 'sender': 'peer'}, self.a, expected=403)
        # A protected recipient does not require the sender to possess its credential.
        self.call('/v1/messages', {'sender': 'legacy', 'recipient': 'alice', 'text': 'legacy sender'})
        self.call('/v1/group-messages', {'sender': 'legacy', 'recipients': ['alice', 'bob'],
                                       'text': 'legacy group', 'batch_id': str(uuid.uuid4())})

    def test_inbox_wait_ack_and_status_are_recipient_scoped(self):
        sent = self.call('/v1/messages', {'sender': 'legacy', 'recipient': 'alice', 'text': 'private'})
        paths = ['/v1/messages?recipient=alice', '/v1/messages?recipient=alice&status=queued',
                 '/v1/messages/wait?recipient=alice&after=0&timeout=0',
                 '/v1/messages/status?recipient=alice&id=' + sent['id']]
        for path in paths:
            for bearer in (self.shared, self.b):
                with self.subTest(path=path, bearer='shared' if bearer == self.shared else 'other'):
                    self.call(path, token=bearer, expected=403)
            self.call(path, token=self.a)
        ack = {'id': sent['id'], 'recipient': 'alice'}
        for bearer in (self.shared, self.b):
            self.call('/v1/ack', ack, bearer, expected=403)
        self.assertEqual('queued', self.broker.message_status('alice', sent['id'], credential=self.a)['status'])
        self.call('/v1/ack', ack, self.a)
        self.call('/v1/ack', ack, self.a)
        status = self.call(paths[-1], token=self.a)
        self.assertEqual('acknowledged', status['status'])
        self.assertIsInstance(status['acknowledged'], float)
        self.call('/v1/messages/status?recipient=bob&id=' + sent['id'], token=self.b, expected=400)
        self.call('/v1/messages?recipient=legacy', token=self.a, expected=403)
        self.call('/v1/messages?recipient=legacy')

    def test_room_owner_member_actor_not_target_member(self):
        room = {'room_id': 'team', 'name': 'Team', 'owner': 'alice', 'members': ['alice', 'bob']}
        for bearer in (self.shared, self.b):
            self.call('/v1/rooms', room, bearer, expected=403)
        self.call('/v1/rooms', room, self.a)
        for bearer in (self.shared, self.b):
            self.call('/v1/rooms/team?member=alice', token=bearer, expected=403)
        self.call('/v1/rooms/team?member=alice', token=self.a)
        self.call('/v1/rooms/team?member=bob', token=self.b)
        for action in ('remove', 'add'):
            body = {'actor': 'alice', 'member': 'bob', 'action': action}
            for bearer in (self.shared, self.b):
                self.call('/v1/rooms/team/members', body, bearer, expected=403)
            self.call('/v1/rooms/team/members', body, self.a)
        # Correct credential still does not confer room ownership.
        self.call('/v1/rooms/team/members', {'actor': 'bob', 'member': 'legacy', 'action': 'add'},
                  self.b, expected=400)
        self.call('/v1/rooms', {'room_id': 'old', 'name': 'Legacy', 'owner': 'legacy',
                               'members': ['legacy', 'alice']})
        self.call('/v1/rooms/old/members', {'actor': 'legacy', 'member': 'bob', 'action': 'add'})

    def test_status_sender_and_recipient_can_check_outbound_and_linked_reply(self):
        sent = self.call('/v1/messages', {'sender': 'alice', 'recipient': 'bob', 'text': 'private original'}, self.a)
        path = '/v1/messages/status?recipient=bob&id=' + sent['id']
        initial = self.call(path + '&actor=alice', token=self.a)
        self.assertEqual('queued', initial['status'])
        self.assertEqual('not recorded', initial['linked_reply_status'])
        self.assertNotIn('reply_message_id', initial)
        self.call('/v1/ack', {'id': sent['id'], 'recipient': 'bob'}, self.b)
        reply = self.call('/v1/messages', {'sender': 'bob', 'recipient': 'alice', 'text': 'private answer',
                                          'reply_to': sent['id']}, self.b)
        sender_status = self.call(path + '&actor=alice', token=self.a)
        self.assertEqual(sender_status, self.call(path + '&actor=bob', token=self.b))
        self.assertEqual(sender_status, self.call(path, token=self.b))
        self.assertEqual(sender_status, self.broker.message_status('bob', sent['id'], actor='alice', credential=self.a))
        self.assertEqual({'id', 'sender', 'recipient', 'status', 'acknowledged', 'linked_reply_status',
                          'reply_message_id'}, set(sender_status))
        self.assertEqual('acknowledged', sender_status['status'])
        self.assertEqual('recorded', sender_status['linked_reply_status'])
        self.assertEqual(reply['id'], sender_status['reply_message_id'])
        self.assertNotIn('private', json.dumps(sender_status))
        # The same scoped sender can check the reply it sent, without reading
        # the other participant's inbox. Reply direction is always reciprocal.
        reply_status = self.call('/v1/messages/status?recipient=alice&id=' + reply['id'] + '&actor=bob', token=self.b)
        self.assertEqual('not recorded', reply_status['linked_reply_status'])
        self.call('/v1/messages?recipient=bob', token=self.a, expected=403)

    def test_status_actor_authorization_does_not_confuse_destination_with_actor(self):
        sent = self.call('/v1/messages', {'sender': 'alice', 'recipient': 'bob', 'text': 'private'}, self.a)
        path = '/v1/messages/status?recipient=bob&id=' + sent['id']
        for token, actor in [(self.shared, 'alice'), (self.shared, 'bob'), (self.b, 'alice'),
                             (self.a, 'bob'), (self.shared, 'legacy'), (self.a, 'legacy')]:
            with self.subTest(actor=actor):
                self.call(path + '&actor=' + actor, token=token, expected=403)
        self.call(path, token=self.a, expected=403)  # legacy default remains bob
        outsider = secrets.token_urlsafe(32)
        self.broker.provision_session_credential('peer', outsider)
        self.call(path + '&actor=peer', token=outsider, expected=403)
        with self.assertRaises(PermissionError):
            self.broker.message_status('bob', sent['id'], actor='peer', credential=outsider)
        self.call('/v1/messages/status?recipient=alice&id=' + sent['id'] + '&actor=alice', token=self.a, expected=400)
        self.call(path + '&actor=alice', token=self.a)
        self.broker.provision_session_credential('alice', secrets.token_urlsafe(32))
        self.call(path + '&actor=alice', token=self.a, expected=403)

    def test_status_legacy_default_and_explicit_unprotected_sender(self):
        sent = self.call('/v1/messages', {'sender': 'legacy', 'recipient': 'alice', 'text': 'outbound'})
        path = '/v1/messages/status?recipient=alice&id=' + sent['id']
        self.call(path, expected=403)
        self.call(path + '&actor=legacy')
        self.call(path, token=self.a)
        legacy = self.call('/v1/messages', {'sender': 'peer', 'recipient': 'legacy', 'text': 'old behavior'})
        default = self.call('/v1/messages/status?recipient=legacy&id=' + legacy['id'])
        self.assertEqual('queued', default['status'])
        for actor in ('', '../invalid'):
            self.call(path + '&actor=' + actor, token=self.a, expected=400)
        self.call(path + '&actor=alice&actor=legacy', token=self.a, expected=400)
        self.call(path + '&recipient=legacy', token=self.a, expected=400)
        with self.assertRaises(ValueError):
            self.broker.message_status('alice', sent['id'], actor='', credential=self.a)

    def test_status_reply_evidence_requires_exact_reciprocal_direct_link(self):
        sent = self.call('/v1/messages', {'sender': 'alice', 'recipient': 'bob', 'text': 'original'}, self.a)
        path = '/v1/messages/status?recipient=bob&id=' + sent['id'] + '&actor=alice'
        # Historical/invalid linkage rows must never manufacture reply evidence.
        rows = [('bob', 'alice', None, None), ('legacy', 'alice', sent['id'], None),
                ('bob', 'legacy', sent['id'], None), ('alice', 'bob', sent['id'], None),
                ('bob', 'alice', sent['id'], 'room'), ('bob', 'alice', str(uuid.uuid4()), None)]
        with closing(self.broker.connect()) as db, db:
            db.executemany('INSERT INTO messages(id,sender,recipient,text,status,created,reply_to,room_id) '
                           'VALUES (?,?,?,?,?,?,?,?)',
                           [(str(uuid.uuid4()), sender, recipient, 'hidden text', 'queued', 0, link, room)
                            for sender, recipient, link, room in rows])
        self.assertEqual('not recorded', self.call(path, token=self.a)['linked_reply_status'])
        replies = [self.call('/v1/messages', {'sender': 'bob', 'recipient': 'alice', 'text': 'hidden reply',
                                             'reply_to': sent['id']}, self.b) for _ in range(2)]
        result = self.call(path, token=self.a)
        self.assertEqual('recorded', result['linked_reply_status'])
        self.assertEqual(replies[0]['id'], result['reply_message_id'])
        self.assertNotIn('hidden', json.dumps(result))
        # Even a reciprocal row cannot turn a room original into a direct reply.
        self.room()
        room_message = self.call('/v1/rooms/team/messages', {'sender': 'alice', 'text': 'room body',
                                                           'batch_id': str(uuid.uuid4())}, self.a)
        delivery = next(item for item in room_message['deliveries'] if item['recipient'] == 'bob')
        with closing(self.broker.connect()) as db, db:
            db.execute('INSERT INTO messages(id,sender,recipient,text,status,created,reply_to) VALUES (?,?,?,?,?,?,?)',
                       (str(uuid.uuid4()), 'bob', 'alice', 'historical bad linkage', 'queued', 0, delivery['id']))
        room_status = self.call('/v1/messages/status?recipient=bob&id=' + delivery['id'] + '&actor=alice', token=self.a)
        self.assertEqual('not recorded', room_status['linked_reply_status'])
        self.assertNotIn('reply_message_id', room_status)

    def test_legacy_to_protected_migration_rotation_restart_hash_only(self):
        sent = self.call('/v1/messages', {'sender': 'peer', 'recipient': 'legacy', 'text': 'preserved'})
        token = secrets.token_urlsafe(32)
        replacement = secrets.token_urlsafe(32)
        out, err = io.StringIO(), io.StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            result = self.broker.provision_session_credential('legacy', token)
        self.assertEqual({'id': 'legacy', 'protected': True}, result)
        self.assertEqual('', out.getvalue() + err.getvalue())
        self.call('/v1/messages?recipient=legacy', expected=403)
        self.call('/v1/messages?recipient=legacy', token=token)
        restarted = Broker(self.path)
        restarted.provision_session_credential('legacy', replacement)
        self.call('/v1/messages?recipient=legacy', token=token, expected=403)
        self.call('/v1/sessions', token=token, expected=403)
        self.call('/v1/messages?recipient=legacy', token=replacement)
        self.assertEqual(sent['id'], restarted.inbox('legacy', credential=replacement)['messages'][0]['id'])
        with closing(restarted.connect()) as db:
            stored = dict(db.execute('SELECT session_id,token_sha256 FROM session_credentials').fetchall())
            dump = '\n'.join(db.iterdump())
        self.assertEqual(hashlib.sha256(replacement.encode()).hexdigest(), stored['legacy'])
        for secret in (self.a, self.b, token, replacement):
            self.assertNotIn(secret, dump)
            self.assertNotIn(secret.encode(), self.path.read_bytes())

    def test_local_api_cannot_bypass_scope_and_denials_do_not_mutate(self):
        self.room()
        parent = self.broker.enqueue({'sender': 'legacy', 'recipient': 'alice', 'text': 'parent'})
        operations = [
            lambda c: self.broker.register({'id': 'alice', 'kind': 'codex'}, credential=c),
            lambda c: self.broker.enqueue({'sender': 'alice', 'recipient': 'legacy', 'text': 'x'}, credential=c),
            lambda c: self.broker.enqueue_group({'sender': 'alice', 'recipients': ['bob'], 'text': 'x',
                                                'batch_id': str(uuid.uuid4())}, credential=c),
            lambda c: self.broker.send_room_message('team', {'sender': 'alice', 'text': 'x',
                                                            'batch_id': str(uuid.uuid4())}, credential=c),
            lambda c: self.broker.reply_all('team', {'sender': 'alice', 'text': 'x', 'reply_to': parent['id'],
                                                   'batch_id': str(uuid.uuid4())}, credential=c),
            lambda c: self.broker.inbox('alice', credential=c),
            lambda c: self.broker.wait_for_messages('alice', 0, timeout=0, credential=c),
            lambda c: self.broker.ack({'id': parent['id'], 'recipient': 'alice'}, credential=c),
            lambda c: self.broker.room_info('team', 'alice', credential=c),
            lambda c: self.broker.update_room_member('team', {'actor': 'alice', 'member': 'legacy',
                                                             'action': 'remove'}, credential=c),
            lambda c: self.broker.create_room({'room_id': 'new', 'name': 'New', 'owner': 'alice',
                                               'members': ['alice', 'bob']}, credential=c),
            lambda c: self.broker.message_status('alice', parent['id'], credential=c),
        ]
        def snapshot():
            with closing(self.broker.connect()) as db:
                return '\n'.join(db.iterdump())
        before = snapshot()
        for operation in operations:
            for credential in (None, self.b, 'bad'):
                with self.assertRaises(PermissionError):
                    operation(credential)
        self.assertEqual(before, snapshot())

    def test_validation_uniqueness_and_no_remote_provisioning(self):
        for value in (None, '', 'short', 'x' * 513, ' ' * 32, '\u2603' * 32, b'x' * 32):
            with self.assertRaises(ValueError):
                self.broker.provision_session_credential('alice', value)
        with self.assertRaises(ValueError):
            self.broker.provision_session_credential('bob', self.a)
        with self.assertRaises(ValueError):
            self.broker.provision_session_credential('../bad', secrets.token_urlsafe(32))
        with self.assertRaises(ValueError):
            self.broker.provision_session_credential('alice', self.shared)
        with self.assertRaises(ValueError):
            handler_for(self.broker, self.a)
        self.assertTrue(self.broker.credential_is_valid(self.a))
        for path in ('/v1/session-credentials', '/v1/admin/session-credentials', '/v1/provision'):
            self.call(path, {'id': 'alice', 'token': self.a}, expected=404)
        # Unknown body fields cannot supply an actor credential under legacy auth.
        self.call('/v1/register', {'id': 'alice', 'kind': 'codex', 'credential': self.a}, expected=403)
        self.call('/v1/messages?recipient=alice&token=' + self.a, expected=403)

    def test_bad_auth_is_generic_and_does_not_echo_secrets(self):
        for value in ('', 'Basic ' + self.a, 'Bearer invalid', 'Bearer ' + self.a + ' '):
            result = self.call('/v1/sessions', headers={'Authorization': value}, expected=403)
            self.assertEqual({'error': 'unauthorized'}, result)
        # Conversion errors must not reflect a token submitted as a query value.
        self.call('/v1/messages?recipient=alice&after=' + self.a, token=self.a, expected=400)
        self.call('/v1/messages/wait?recipient=alice&after=0&timeout=' + self.a,
                  token=self.a, expected=400)
        self.call('/v1/sessions', token=self.a)
        conn = HTTPConnection('127.0.0.1', self.server.server_port, timeout=3)
        try:
            conn.putrequest('GET', '/v1/sessions')
            conn.putheader('Authorization', 'Bearer ' + self.a)
            conn.putheader('Authorization', 'Bearer ' + self.shared)
            conn.endheaders()
            response = conn.getresponse()
            self.assertEqual(403, response.status)
            self.assertEqual({'error': 'unauthorized'}, json.loads(response.read()))
        finally:
            conn.close()

    def test_shared_bearer_never_becomes_scoped_even_after_external_admin_collision(self):
        # A separate local admin cannot know this listener's shared credential.
        # Accidental reuse must fail closed rather than elevate shared authority.
        Broker(self.path).provision_session_credential('alice', self.shared)
        self.call('/v1/messages?recipient=alice', expected=403)
        self.call('/v1/register', {'id': 'alice', 'kind': 'codex'}, expected=403)
        self.call('/v1/messages', {'sender': 'alice', 'recipient': 'legacy', 'text': 'denied'}, expected=403)
        self.call('/v1/messages?recipient=legacy')

    def test_direct_retry_cannot_bypass_rotation_or_change_content(self):
        body = {'id': str(uuid.uuid4()), 'sender': 'alice', 'recipient': 'bob', 'text': 'retry'}
        first = self.call('/v1/messages', body, self.a)
        self.assertEqual(first, self.call('/v1/messages', body, self.a))
        self.call('/v1/messages', {**body, 'text': 'different'}, self.a, expected=400)
        replacement = secrets.token_urlsafe(32)
        self.broker.provision_session_credential('alice', replacement)
        self.call('/v1/messages', body, self.a, expected=403)
        self.call('/v1/messages', body, expected=403)
        self.assertEqual(first, self.call('/v1/messages', body, replacement))
        self.assertEqual(1, len(self.broker.inbox('bob', credential=self.b)['messages']))

    def test_rotation_and_new_protection_recheck_inflight_waits(self):
        # Gate only the observer, after its read transaction has finished. This
        # exercises the check/wait race without arbitrary timing sleeps.
        for recipient, old_credential in [('alice', self.a), ('legacy', None)]:
            with self.subTest(recipient=recipient):
                observed, release = threading.Event(), threading.Event()
                original = self.broker._queued_after
                failures = []
                def observe(*args, **kwargs):
                    rows = original(*args, **kwargs)
                    if not observed.is_set():
                        observed.set()
                        if not release.wait(2):
                            raise AssertionError('query gate timed out')
                    return rows
                self.broker._queued_after = observe
                def wait():
                    try:
                        self.broker.wait_for_messages(recipient, 0, timeout=0.25, credential=old_credential)
                    except BaseException as exc:
                        failures.append(exc)
                waiter = threading.Thread(target=wait)
                waiter.start()
                try:
                    self.assertTrue(observed.wait(1))
                    # Another Broker simulates a different local admin process;
                    # timeout must revalidate even without its notification.
                    Broker(self.path).provision_session_credential(recipient, secrets.token_urlsafe(32))
                finally:
                    release.set()
                    waiter.join(2)
                    self.broker._queued_after = original
                self.assertFalse(waiter.is_alive())
                self.assertEqual(1, len(failures))
                self.assertIsInstance(failures[0], PermissionError)

    def test_protection_serializes_with_already_authorized_write(self):
        authorized, release, admin_attempt = threading.Event(), threading.Event(), threading.Event()
        original = self.broker._authorize
        failures, results = [], []
        def gate(db, actor, credential):
            original(db, actor, credential)
            authorized.set()
            if not release.wait(2):
                raise AssertionError('authorization gate timed out')
        self.broker._authorize = gate
        admin = Broker(self.path)
        original_connect = admin.connect
        def connect():
            db = original_connect()
            db.set_trace_callback(lambda sql: admin_attempt.set() if sql == 'BEGIN IMMEDIATE' else None)
            return db
        admin.connect = connect
        token = secrets.token_urlsafe(32)
        def write():
            try:
                results.append(self.broker.enqueue({'sender': 'legacy', 'recipient': 'alice', 'text': 'before protection'}))
            except BaseException as exc:
                failures.append(exc)
        def protect():
            try:
                admin.provision_session_credential('legacy', token)
            except BaseException as exc:
                failures.append(exc)
        writer, provisioner = threading.Thread(target=write), threading.Thread(target=protect)
        writer.start()
        try:
            self.assertTrue(authorized.wait(1))
            provisioner.start()
            self.assertTrue(admin_attempt.wait(1))
        finally:
            release.set()
            writer.join(3)
            if provisioner.ident is not None:
                provisioner.join(3)
            self.broker._authorize = original
        self.assertFalse(writer.is_alive())
        self.assertFalse(provisioner.is_alive())
        self.assertEqual([], failures)
        self.assertEqual(1, len(results))
        with self.assertRaises(PermissionError):
            self.broker.enqueue({'sender': 'legacy', 'recipient': 'alice', 'text': 'after protection'})
        self.assertEqual(1, len(self.broker.inbox('alice', credential=self.a)['messages']))

    def test_additive_migration_keeps_legacy_rows(self):
        path = Path(self.temp.name) / 'old.sqlite3'
        with closing(sqlite3.connect(path)) as db, db:
            db.execute('CREATE TABLE sessions(id TEXT PRIMARY KEY, kind TEXT NOT NULL, updated REAL NOT NULL)')
            db.execute("INSERT INTO sessions VALUES ('old', 'codex', 1)")
        broker = Broker(path)
        self.assertEqual([], broker.sessions()['sessions'])
        broker.register({'id': 'old', 'kind': 'codex', 'opt_in': True})
        token = secrets.token_urlsafe(32)
        broker.provision_session_credential('old', token)
        with self.assertRaises(PermissionError):
            broker.inbox('old')
        self.assertEqual([], broker.inbox('old', credential=token)['messages'])


if __name__ == '__main__':
    unittest.main()
