import io
from contextlib import closing
import json
import sqlite3
import tempfile
import threading
import time
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path

from broker import Broker, handler_for
from claude_adapter import BrokerClient as ClaudeBrokerClient, ClaudeAdapter, BrokerError
from codex_client import BrokerClient as CodexBrokerClient
from codex_mcp import respond


class IntegrationTest(unittest.TestCase):
    def test_queued_reconciliation_finds_older_newly_opted_in_sender(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            broker = Broker(root / 'queue.sqlite3')
            broker.register({'id': 'claude-test', 'kind': 'claude', 'opt_in': True})
            broker.register({'id': 'current', 'kind': 'codex', 'opt_in': True})
            with closing(broker.connect()) as db, db:
                db.execute('INSERT INTO sessions(id,kind,updated,opted_in) VALUES (?,?,?,0)', ('legacy','codex',0))
                db.execute('INSERT INTO messages(id,sender,recipient,text,status,created) VALUES (?,?,?,?,?,?)',
                           ('old-message','legacy','claude-test','older','queued',0))
            broker.enqueue({'sender': 'current', 'recipient': 'claude-test', 'text': 'newer'})
            http = ThreadingHTTPServer(('127.0.0.1', 0), handler_for(broker, 'token'))
            server = threading.Thread(target=http.serve_forever, daemon=True)
            server.start()
            try:
                state = root / 'session.json'
                state.write_text(json.dumps({'port': http.server_port, 'token': 'token'}), encoding='utf-8')
                client = ClaudeBrokerClient(state)
                output = io.StringIO()
                adapter = ClaudeAdapter('claude-test', 'current', client, stdout=output)
                page = client.wait_for_messages('claude-test', 0, 0)
                adapter.deliver_messages(page['messages'])
                broker.register({'id': 'legacy', 'kind': 'codex', 'opt_in': True})
                self.assertEqual([], client.wait_for_messages('claude-test', page['cursor'], 0)['messages'])
                adapter.deliver_messages(client.messages('claude-test', status='queued')['messages'])
                events = [json.loads(line)['params']['content'] for line in output.getvalue().splitlines()]
                self.assertEqual(['newer', 'older'], events)
            finally:
                http.shutdown()
                http.server_close()
                server.join()

    def test_five_opted_in_participants_can_address_each_other(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            broker = Broker(root / 'queue.sqlite3')
            http = ThreadingHTTPServer(('127.0.0.1', 0), handler_for(broker, 'token'))
            server = threading.Thread(target=http.serve_forever, daemon=True)
            server.start()
            try:
                state = root / 'session.json'
                state.write_text(json.dumps({'port': http.server_port, 'token': 'token'}), encoding='utf-8')
                client = CodexBrokerClient(state)
                members = [('claude-a', 'claude'), ('claude-b', 'claude'), ('claude-c', 'claude'),
                           ('codex-a', 'codex'), ('codex-b', 'codex')]
                for ident, kind in members:
                    client.register(ident, kind)
                for sender, _ in members:
                    for recipient, _ in members:
                        if recipient != sender:
                            client.send(sender, recipient, sender + ' to ' + recipient)
                for recipient, _ in members:
                    messages = client.poll(recipient)['messages']
                    self.assertEqual({sid for sid, _ in members if sid != recipient},
                                     {item['sender'] for item in messages})
                    self.assertTrue(all(item['recipient'] == recipient for item in messages))
            finally:
                http.shutdown()
                http.server_close()
                server.join()

    def test_old_claude_heartbeat_after_explicit_migration(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            db_path = root / 'queue.sqlite3'
            with closing(sqlite3.connect(db_path)) as db, db:
                db.execute('CREATE TABLE sessions(id TEXT PRIMARY KEY, kind TEXT NOT NULL, updated REAL NOT NULL)')
                db.execute('INSERT INTO sessions VALUES (?, ?, ?)', ('claude-test', 'claude', 0))
            broker = Broker(db_path)
            broker.register({'id': 'claude-test', 'kind': 'claude', 'opt_in': True})
            http = ThreadingHTTPServer(('127.0.0.1', 0), handler_for(broker, 'token'))
            server = threading.Thread(target=http.serve_forever, daemon=True)
            server.start()
            try:
                state = root / 'session.json'
                state.write_text(json.dumps({'port': http.server_port, 'token': 'token'}), encoding='utf-8')
                # The running old adapter sends no opt_in field.
                legacy = ClaudeBrokerClient(state)
                result = legacy._request('POST', '/v1/register', {'id': 'claude-test', 'kind': 'claude'})
                self.assertEqual('claude-test', result['id'])
                with self.assertRaises(BrokerError):
                    legacy._request('POST', '/v1/register', {'id': 'new-session', 'kind': 'claude'})
            finally:
                http.shutdown()
                http.server_close()
                server.join()

    def test_room_reply_reaches_other_members_with_room_context(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            broker = Broker(root / 'queue.sqlite3')
            http = ThreadingHTTPServer(('127.0.0.1', 0), handler_for(broker, 'token'))
            server = threading.Thread(target=http.serve_forever, daemon=True)
            server.start()
            try:
                state = root / 'session.json'
                state.write_text(json.dumps({'port': http.server_port, 'token': 'token'}), encoding='utf-8')
                codex = CodexBrokerClient(state)
                claude = ClaudeBrokerClient(state)
                codex.register('codex-a', 'codex')
                codex.register('codex-b', 'codex')
                claude.register('claude-a')
                claude.register('claude-b')
                claude.register('claude-c')
                codex.create_room('team', 'Team', 'codex-a',
                                  ['codex-a', 'codex-b', 'claude-a', 'claude-b', 'claude-c'])
                original = codex.send_room_message('team', 'codex-a', 'hello',
                                                   '123e4567-e89b-12d3-a456-426614174000')
                output = io.StringIO()
                adapter = ClaudeAdapter('claude-a', 'codex-a', claude, stdout=output)
                adapter.deliver_messages(claude.messages('claude-a')['messages'])
                event = json.loads(output.getvalue())['params']
                self.assertEqual('team', event['meta']['room_id'])
                response = adapter.call_tool({'name': 'bridge_room_reply', 'arguments': {
                    'room_id': 'team', 'text': 'hello all', 'reply_to': event['meta']['message_id'],
                    'batch_id': '123e4567-e89b-12d3-a456-426614174001'}})
                self.assertNotIn('isError', response)
                for recipient in ('codex-a', 'codex-b', 'claude-b', 'claude-c'):
                    messages = codex.poll(recipient)['messages']
                    reply = next(item for item in messages if item['text'] == 'hello all')
                    self.assertEqual('team', reply['room_id'])
                    self.assertEqual(event['meta']['message_id'], reply['reply_to'])
                self.assertEqual(4, len(original['deliveries']))
            finally:
                http.shutdown()
                http.server_close()
                server.join()

    def test_wait_notifies_both_client_types_without_periodic_polling(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            broker = Broker(root / 'queue.sqlite3')
            http = ThreadingHTTPServer(('127.0.0.1', 0), handler_for(broker, 'token'))
            server = threading.Thread(target=http.serve_forever, daemon=True)
            server.start()
            try:
                state = root / 'session.json'
                state.write_text(json.dumps({'port': http.server_port, 'token': 'token'}), encoding='utf-8')
                codex = CodexBrokerClient(state)
                claude = ClaudeBrokerClient(state)
                codex.register('codex-test', 'codex')
                claude.register('claude-test')
                result = {}
                def receive(name, client):
                    result[name] = client.wait_for_messages(name, 0, 4)
                waiters = [threading.Thread(target=receive, args=('codex-test', codex)),
                           threading.Thread(target=receive, args=('claude-test', claude))]
                for waiter in waiters:
                    waiter.start()
                time.sleep(0.1)
                codex.send('codex-test', 'claude-test', 'hello')
                claude.send('claude-test', 'codex-test', 'hello back')
                for waiter in waiters:
                    waiter.join(2)
                    self.assertFalse(waiter.is_alive(), 'long-poll waiter was not notified')
                self.assertEqual('hello', result['claude-test']['messages'][0]['text'])
                self.assertEqual('hello back', result['codex-test']['messages'][0]['text'])
            finally:
                http.shutdown()
                http.server_close()
                server.join()

    def test_inbox_paginates_beyond_one_thousand(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            broker = Broker(root / 'queue.sqlite3')
            broker.register({'id': 'codex-test', 'kind': 'codex', 'opt_in': True})
            broker.register({'id': 'claude-test', 'kind': 'claude', 'opt_in': True})
            with closing(broker.connect()) as db, db:
                db.executemany('INSERT INTO messages(id,sender,recipient,text,status,created) VALUES (?,?,?,?,?,?)',
                               [(str(i), 'claude-test', 'codex-test', str(i), 'queued', 0) for i in range(1001)])
            http = ThreadingHTTPServer(('127.0.0.1', 0), handler_for(broker, 'token'))
            thread = threading.Thread(target=http.serve_forever, daemon=True)
            thread.start()
            try:
                state = root / 'session.json'
                state.write_text(json.dumps({'port': http.server_port, 'token': 'token'}), encoding='utf-8')
                self.assertEqual(1001, len(CodexBrokerClient(state).poll('codex-test')['messages']))
                self.assertEqual(1001, len(ClaudeBrokerClient(state).messages('codex-test')['messages']))
            finally:
                http.shutdown()
                http.server_close()
                thread.join()

    def test_adapter_reloads_broker_after_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            db = root / 'queue.sqlite3'
            state = root / 'broker-session.json'
            first = ThreadingHTTPServer(('127.0.0.1', 0), handler_for(Broker(db), 'first-token'))
            worker = threading.Thread(target=first.serve_forever, daemon=True)
            worker.start()
            state.write_text(json.dumps({'port': first.server_port, 'token': 'first-token'}), encoding='utf-8')
            client = ClaudeBrokerClient(state)
            client.register('claude-test')
            first.shutdown()
            first.server_close()
            worker.join()
            second = ThreadingHTTPServer(('127.0.0.1', 0), handler_for(Broker(db), 'second-token'))
            worker = threading.Thread(target=second.serve_forever, daemon=True)
            worker.start()
            try:
                state.write_text(json.dumps({'port': second.server_port, 'token': 'second-token'}), encoding='utf-8')
                self.assertEqual([], client.messages('claude-test')['messages'])
            finally:
                second.shutdown()
                second.server_close()
                worker.join()

    def test_two_way_routing_and_acknowledgment(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            broker = Broker(root / 'queue.sqlite3')
            http = ThreadingHTTPServer(('127.0.0.1', 0), handler_for(broker, 'integration-token'))
            thread = threading.Thread(target=http.serve_forever, daemon=True)
            thread.start()
            try:
                state = root / 'broker-session.json'
                state.write_text(json.dumps({'port': http.server_port, 'token': 'integration-token'}), encoding='utf-8')
                codex = CodexBrokerClient(state)
                claude = ClaudeBrokerClient(state)
                codex.register('codex-test', 'codex')
                codex.register('other-test', 'codex')
                claude.register('claude-test')
                outbound = codex.send('codex-test', 'claude-test', 'Hello Claude')
                output = io.StringIO()
                adapter = ClaudeAdapter('claude-test', 'codex-test', claude, stdout=output)
                adapter.deliver_messages(claude.messages('claude-test')['messages'])
                event = json.loads(output.getvalue())
                self.assertEqual(outbound['id'], event['params']['meta']['message_id'])
                self.assertEqual('codex-test', event['params']['meta']['sender'])
                adapter.deliver_messages(claude.messages('claude-test')['messages'])
                self.assertEqual(1, len(output.getvalue().splitlines()))
                result = adapter.call_tool({'name': 'bridge_ack', 'arguments': {'message_id': outbound['id']}})
                self.assertNotIn('isError', result)
                reply = adapter.call_tool({'name': 'bridge_send', 'arguments': {'text': 'Hello Codex', 'recipient': 'codex-test'}})
                self.assertNotIn('isError', reply)
                inbox = codex.poll('codex-test')['messages']
                self.assertEqual('Hello Codex', inbox[0]['text'])
                waited = respond({'id': 2, 'method': 'tools/call', 'params': {
                    'name': 'bridge_wait', 'arguments': {'recipient': 'codex-test', 'timeout_seconds': 0}}}, codex)
                self.assertFalse(json.loads(waited['result']['content'][0]['text'])['timed_out'])
                self.assertEqual([], codex.poll('other-test')['messages'])
                codex.ack(inbox[0]['id'], 'codex-test')
                waited = respond({'id': 3, 'method': 'tools/call', 'params': {
                    'name': 'bridge_wait', 'arguments': {'recipient': 'codex-test', 'timeout_seconds': 0}}}, codex)
                self.assertTrue(json.loads(waited['result']['content'][0]['text'])['timed_out'])
                self.assertEqual('acknowledged', codex.poll('codex-test')['messages'][0]['status'])
                self.assertEqual('acknowledged', broker.inbox('claude-test')['messages'][0]['status'])
                tool = respond({'id': 1, 'method': 'tools/call', 'params': {
                    'name': 'bridge_inbox', 'arguments': {'recipient': 'codex-test'}}}, codex)
                self.assertEqual('acknowledged', json.loads(tool['result']['content'][0]['text'])['messages'][0]['status'])
            finally:
                http.shutdown()
                http.server_close()
                thread.join()


if __name__ == '__main__':
    unittest.main()
