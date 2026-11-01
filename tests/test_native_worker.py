import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from codex_queue import DeliveryLedger
from codex_native import RPCRejected, ProxyError
import native_worker
from native_worker import drain_once, start_holder

THREAD = '123e4567-e89b-12d3-a456-426614174000'
MESSAGE = '987e6543-e21b-12d3-a456-426614174000'


class NativeWorkerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.ledger = DeliveryLedger(Path(self.tmp.name) / 'ledger.sqlite3')
        self.broker = Mock()
        self.broker.poll.return_value = {'messages': [{'id': MESSAGE, 'recipient': THREAD,
            'sender': 'claude-test', 'text': 'hello', 'status': 'queued'}]}

    def test_holder_is_only_started_for_explicit_opt_in(self):
        factory, proxy_factory = Mock(), Mock()
        self.assertIsNone(start_holder(THREAD, False, factory, proxy_factory))
        factory.assert_not_called()
        holder = start_holder(THREAD, True, factory, proxy_factory)
        factory.assert_called_once_with(proxy_factory, THREAD)
        holder.connect.assert_called_once_with(resume=True)

    def test_worker_does_not_register_or_opt_in_its_recipient(self):
        broker = Mock()
        broker.wait_for_messages.side_effect = KeyboardInterrupt
        with patch('sys.argv', ['native_worker.py', '--recipient', THREAD]), \
             patch.object(native_worker, 'acquire'), patch.object(native_worker, 'release'), \
             patch.object(native_worker, 'BrokerClient', return_value=broker), \
             patch.object(native_worker, 'DeliveryLedger'), \
             patch.object(native_worker, 'drain_once', return_value=[]):
            with self.assertRaises(KeyboardInterrupt):
                native_worker.main()
        broker.register.assert_not_called()

    def test_waits_for_approval_without_claiming_or_sending(self):
        proxy = Mock()
        proxy.request.return_value = {'thread': {'id': THREAD,
            'status': {'type': 'active', 'activeFlags': ['waitingOnApproval']}}}
        result = drain_once(THREAD, self.broker, self.ledger, lambda: proxy)
        self.assertEqual('waiting_for_target', result[0]['status'])
        self.assertIsNone(self.ledger.get(MESSAGE))
        self.assertEqual(['thread/read'], [c.args[0] for c in proxy.request.call_args_list])

    def test_submits_once_without_acknowledging(self):
        proxy = Mock()
        thread = {'thread': {'id': THREAD, 'status': {'type': 'active', 'activeFlags': []}}}
        proxy.request.side_effect = [thread, thread, {'turn': {'id': 'turn-1'}}]
        result = drain_once(THREAD, self.broker, self.ledger, lambda: proxy)
        self.assertEqual('submitted', result[0]['status'])
        self.assertEqual('accepted', self.ledger.get(MESSAGE)['state'])
        self.assertEqual([], drain_once(THREAD, self.broker, self.ledger, lambda: proxy))
        self.broker.ack.assert_not_called()

    def test_unloaded_thread_is_not_resumed(self):
        proxy = Mock()
        proxy.request.return_value = {'thread': {'id': THREAD, 'status': {'type': 'notLoaded'}}}
        self.assertEqual('waiting_for_target', drain_once(THREAD, self.broker, self.ledger, lambda: proxy)[0]['status'])
        self.assertEqual(['thread/read'], [c.args[0] for c in proxy.request.call_args_list])

    def test_waiting_on_user_input_is_not_steered(self):
        proxy = Mock()
        proxy.request.return_value = {'thread': {'id': THREAD,
            'status': {'type': 'active', 'activeFlags': ['waitingOnUserInput']}}}
        self.assertEqual('waiting_for_target', drain_once(THREAD, self.broker, self.ledger, lambda: proxy)[0]['status'])
        self.assertIsNone(self.ledger.get(MESSAGE))

    def test_idle_only_holds_active_turn_then_delivers_when_idle(self):
        proxy = Mock()
        active = {'thread': {'id': THREAD, 'status': {'type': 'active', 'activeFlags': []}}}
        proxy.request.return_value = active
        self.assertEqual('waiting_for_target', drain_once(THREAD, self.broker, self.ledger, lambda: proxy, idle_only=True)[0]['status'])
        self.assertIsNone(self.ledger.get(MESSAGE))
        idle = {'thread': {'id': THREAD, 'status': {'type': 'idle'}}}
        proxy.request.side_effect = [idle, idle, {'turn': {'id': 'turn-id'}}]
        self.assertEqual('submitted', drain_once(THREAD, self.broker, self.ledger, lambda: proxy, idle_only=True)[0]['status'])

    def test_definite_rejection_releases_claim_for_retry(self):
        proxy = Mock()
        thread = {'thread': {'id': THREAD, 'status': {'type': 'active', 'activeFlags': []}}}
        proxy.request.side_effect = [thread, thread, RPCRejected('rejected')]
        result = drain_once(THREAD, self.broker, self.ledger, lambda: proxy)
        self.assertEqual('waiting_for_target', result[0]['status'])
        self.assertIsNone(self.ledger.get(MESSAGE))

    def test_unknown_transport_failure_stays_uncertain(self):
        proxy = Mock()
        thread = {'thread': {'id': THREAD, 'status': {'type': 'active', 'activeFlags': []}}}
        proxy.request.side_effect = [thread, thread, ProxyError('timeout')]
        result = drain_once(THREAD, self.broker, self.ledger, lambda: proxy)
        self.assertEqual('uncertain', result[0]['status'])
        self.assertEqual('uncertain', self.ledger.get(MESSAGE)['state'])


if __name__ == '__main__':
    unittest.main()
