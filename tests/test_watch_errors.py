import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from codex_client import ClientError
from watch_errors import poll_once


class ErrorWatchTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.transcript = self.root / 'session.jsonl'
        self.db = self.root / 'watch.sqlite3'
        self.transcript.write_text('', encoding='utf-8')
        self.client = Mock()
        self.client.send.return_value = {'status': 'queued'}

    def append_error(self, newline=True):
        record = {'type': 'user', 'message': {'content': [{'type': 'tool_result', 'is_error': True,
                                            'content': 'SECRET ERROR DETAILS'}]}}
        with self.transcript.open('a', encoding='utf-8') as stream:
            stream.write(json.dumps(record) + ('\n' if newline else ''))

    def test_new_errors_only_rate_limited_without_leaking_output(self):
        self.append_error()
        self.assertEqual(0, poll_once(self.transcript, 'claude-test', self.db, self.client, now=1000)['new_tool_errors'])
        self.append_error()
        self.assertEqual(1, poll_once(self.transcript, 'claude-test', self.db, self.client, now=1001)['alerts_queued'])
        self.assertNotIn('SECRET', self.client.send.call_args.args[2])
        self.append_error()
        result = poll_once(self.transcript, 'claude-test', self.db, self.client, now=1002)
        self.assertEqual(1, result['new_tool_errors'])
        self.assertEqual(0, result['alerts_queued'])
        self.client.send.assert_called_once()

    def test_incomplete_lines_wait_until_completed(self):
        poll_once(self.transcript, 'claude-test', self.db, self.client, now=1000)
        self.append_error(newline=False)
        self.assertEqual(0, poll_once(self.transcript, 'claude-test', self.db, self.client, now=1001)['new_tool_errors'])
        with self.transcript.open('a', encoding='utf-8') as stream:
            stream.write('\n')
        self.assertEqual(1, poll_once(self.transcript, 'claude-test', self.db, self.client, now=1002)['alerts_queued'])

    def test_pending_alert_retries_with_same_id(self):
        poll_once(self.transcript, 'claude-test', self.db, self.client, now=1000)
        self.append_error()
        self.client.send.side_effect = [ClientError('transport_error', 'timeout'), {'status': 'queued'}]
        self.assertEqual(0, poll_once(self.transcript, 'claude-test', self.db, self.client, now=1001)['alerts_queued'])
        self.assertEqual(1, poll_once(self.transcript, 'claude-test', self.db, self.client, now=1002)['alerts_queued'])
        self.assertEqual(self.client.send.call_args_list[0].kwargs['message_id'],
                         self.client.send.call_args_list[1].kwargs['message_id'])

    def test_api_history_replay_is_not_a_new_tool_error(self):
        poll_once(self.transcript, 'claude-test', self.db, self.client, now=1000)
        record = {'type': 'api-request-blob', 'message': {'content': [
            {'type': 'tool_result', 'is_error': True, 'content': 'old error'}]}}
        with self.transcript.open('a', encoding='utf-8') as stream:
            stream.write(json.dumps(record) + '\n')
        result = poll_once(self.transcript, 'claude-test', self.db, self.client, now=1001)
        self.assertEqual(0, result['new_tool_errors'])
        self.client.send.assert_not_called()

    def test_replacement_file_reads_new_records_from_start(self):
        poll_once(self.transcript, 'claude-test', self.db, self.client, now=1000)
        self.transcript.rename(self.root / 'old.jsonl')
        self.append_error()
        result = poll_once(self.transcript, 'claude-test', self.db, self.client, now=1001)
        self.assertEqual(1, result['alerts_queued'])


if __name__ == '__main__':
    unittest.main()
