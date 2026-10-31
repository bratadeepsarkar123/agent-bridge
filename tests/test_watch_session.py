import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from codex_client import ClientError
from watch_session import watch_once


class WatchTests(unittest.TestCase):
    def test_status_transition_sends_once_and_never_restarts(self):
        with tempfile.TemporaryDirectory() as folder:
            db = Path(folder) / 'watch.sqlite3'
            client = Mock()
            client.send.return_value = {'status': 'queued'}
            with patch('watch_session.process_status', side_effect=['alive', 'not_running', 'not_running']):
                self.assertEqual('none', watch_once(123, 'codex-test', db, client)['notification'])
                self.assertEqual('queued', watch_once(123, 'codex-test', db, client)['notification'])
                self.assertFalse(watch_once(123, 'codex-test', db, client)['change'])
            client.register.assert_called_once_with('system:process-monitor', 'system')
            client.send.assert_called_once()
            self.assertIn('Do not restart', client.send.call_args.args[2])

    def test_uncertain_notification_retries_with_same_message_id(self):
        with tempfile.TemporaryDirectory() as folder:
            db = Path(folder) / 'watch.sqlite3'
            client = Mock()
            client.send.side_effect = [ClientError('transport_error', 'timeout'), {'status': 'queued'}]
            with patch('watch_session.process_status', return_value='not_running'):
                self.assertEqual('pending', watch_once(123, 'codex-test', db, client)['notification'])
                second = watch_once(123, 'codex-test', db, client)
                self.assertFalse(second['change'])
                self.assertEqual('queued', second['notification'])
            self.assertEqual(2, client.send.call_count)
            self.assertEqual(client.send.call_args_list[0].kwargs['message_id'],
                             client.send.call_args_list[1].kwargs['message_id'])

    def test_concurrent_watchers_create_one_transition(self):
        with tempfile.TemporaryDirectory() as folder:
            db = Path(folder) / 'watch.sqlite3'
            client = Mock()
            client.send.return_value = {'status': 'queued'}
            errors = []
            def run():
                try:
                    watch_once(123, 'codex-test', db, client)
                except Exception as exc:
                    errors.append(exc)
            with patch('watch_session.process_status', return_value='not_running'):
                threads = [threading.Thread(target=run) for _ in range(2)]
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join()
            self.assertEqual([], errors)
            # Duplicate sends may race, but they share an idempotency key.
            ids = {call.kwargs['message_id'] for call in client.send.call_args_list}
            self.assertEqual(1, len(ids))

    def test_reused_pid_does_not_generate_a_recovery_alert(self):
        with tempfile.TemporaryDirectory() as folder:
            db = Path(folder) / 'watch.sqlite3'
            client = Mock()
            client.send.return_value = {'status': 'queued'}
            with patch('watch_session.process_status', side_effect=['not_running', 'alive']):
                watch_once(123, 'codex-test', db, client)
                self.assertEqual('none', watch_once(123, 'codex-test', db, client)['notification'])
            client.send.assert_called_once()


if __name__ == '__main__':
    unittest.main()
