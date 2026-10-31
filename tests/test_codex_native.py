import json
import unittest
from unittest.mock import Mock

from codex_native import deliver_message


THREAD = '123e4567-e89b-12d3-a456-426614174000'
MESSAGE = '987e6543-e21b-12d3-a456-426614174000'


class NativeDeliveryTests(unittest.TestCase):
    def test_sends_external_tool_output_without_user_input_or_resume(self):
        proxy = Mock()
        proxy.request.side_effect = [{'thread': {'id': THREAD, 'status': {'type': 'active', 'activeFlags': []}}}, {'turn': {'id': 'turn-1'}}]
        message = {'id': MESSAGE, 'sender': 'claude-test', 'recipient': THREAD,
                   'text': 'hello', 'status': 'queued'}
        self.assertEqual('turn-1', deliver_message(proxy, THREAD, message)['turn']['id'])
        self.assertEqual(['thread/read', 'turn/start'], [c.args[0] for c in proxy.request.call_args_list])
        params = proxy.request.call_args_list[1].args[1]
        self.assertEqual([], params['input'])
        payload = json.loads(params['toolOutput']['output'])
        self.assertEqual('claude-test', payload['sender'])
        self.assertEqual(MESSAGE, payload['message_id'])
        self.assertIn('not a user', payload['notice'])

    def test_rejects_message_for_a_different_thread(self):
        proxy = Mock()
        message = {'id': MESSAGE, 'sender': 'claude-test', 'recipient': 'other',
                   'text': 'hello', 'status': 'queued'}
        with self.assertRaises(ValueError):
            deliver_message(proxy, THREAD, message)
        proxy.request.assert_not_called()

    def test_loaded_idle_thread_is_eligible_without_resuming(self):
        proxy = Mock()
        proxy.request.side_effect = [{'thread': {'id': THREAD, 'status': {'type': 'idle'}}}, {'turn': {'id': 'turn-2'}}]
        message = {'id': MESSAGE, 'sender': 'claude-test', 'recipient': THREAD,
                   'text': 'hello', 'status': 'queued'}
        deliver_message(proxy, THREAD, message)
        self.assertEqual(['thread/read', 'turn/start'], [c.args[0] for c in proxy.request.call_args_list])


if __name__ == '__main__':
    unittest.main()
