import io
from contextlib import closing
import json
import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path

from broker import Broker, handler_for
from claude_adapter import BrokerClient as ClaudeBrokerClient, ClaudeAdapter
from codex_client import BrokerClient as CodexBrokerClient
from codex_mcp import respond


class IntegrationTest(unittest.TestCase):
    def test_inbox_paginates_beyond_one_thousand(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            broker = Broker(root / 'queue.sqlite3')
            broker.register({'id': 'codex-test', 'kind': 'codex'})
            broker.register({'id': 'claude-test', 'kind': 'claude'})
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
                reply = adapter.call_tool({'name': 'bridge_send', 'arguments': {'text': 'Hello Codex'}})
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
