"""Retries carrying the same message ID remain safe across a broker restart."""
import tempfile
import threading
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from broker import Broker


class DirectRetryPersistenceTests(unittest.TestCase):
    def test_retry_after_lost_response_and_restart_does_not_duplicate(self):
        with tempfile.TemporaryDirectory() as folder:
            database = Path(folder) / 'broker.sqlite3'
            broker = Broker(database)
            broker.register({'id': 'sender', 'kind': 'codex', 'opt_in': True})
            broker.register({'id': 'recipient', 'kind': 'claude', 'opt_in': True})
            payload = {'sender': 'sender', 'recipient': 'recipient',
                       'text': 'synthetic retry', 'id': str(uuid.uuid4())}
            # Simulate a response lost after the broker committed the message.
            broker.enqueue(payload)
            restarted = Broker(database)
            self.assertEqual(payload['id'], restarted.enqueue(payload)['id'])
            self.assertEqual(1, len(restarted.inbox('recipient')['messages']))
            with self.assertRaises(ValueError):
                restarted.enqueue({**payload, 'text': 'different content'})

    def test_simultaneous_same_id_retries_create_one_message(self):
        with tempfile.TemporaryDirectory() as folder:
            database = Path(folder) / 'broker.sqlite3'
            first, second = Broker(database), Broker(database)
            first.register({'id': 'sender', 'kind': 'codex', 'opt_in': True})
            first.register({'id': 'recipient', 'kind': 'claude', 'opt_in': True})
            payload = {'sender': 'sender', 'recipient': 'recipient',
                       'text': 'synthetic retry', 'id': str(uuid.uuid4())}
            barrier = threading.Barrier(2)
            def send(broker):
                barrier.wait(timeout=5)
                return broker.enqueue(payload)
            with ThreadPoolExecutor(max_workers=2) as pool:
                results = list(pool.map(send, (first, second)))
            self.assertEqual([payload['id'], payload['id']], [item['id'] for item in results])
            self.assertEqual(1, len(first.inbox('recipient')['messages']))


if __name__ == '__main__':
    unittest.main()
