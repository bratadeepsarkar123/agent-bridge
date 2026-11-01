import sqlite3
from contextlib import closing
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from message_status import message_status
from codex_client import ClientError

RECIPIENT = '123e4567-e89b-12d3-a456-426614174000'
MESSAGE = '123e4567-e89b-12d3-a456-426614174001'


class MessageStatusTests(unittest.TestCase):
    def test_sender_can_query_status_without_reading_recipient_inbox(self):
        client = Mock()
        client.message_status.return_value = {'id': MESSAGE, 'sender': 'peer', 'recipient': RECIPIENT,
                                              'status': 'acknowledged', 'linked_reply_status': 'recorded',
                                              'reply_message_id': 'reply'}
        with tempfile.TemporaryDirectory() as folder:
            result = message_status(client, RECIPIENT, MESSAGE, Path(folder) / 'missing.sqlite3', actor='peer')
        client.message_status.assert_called_once_with(RECIPIENT, MESSAGE, actor='peer')
        client.poll.assert_not_called()
        self.assertEqual('recorded', result['linked_reply_status'])

    def test_denied_status_never_falls_back_to_legacy_inboxes(self):
        client = Mock()
        client.message_status.side_effect = ClientError('broker_error', 'denied', 403)
        with tempfile.TemporaryDirectory() as folder:
            with self.assertRaises(ClientError):
                message_status(client, RECIPIENT, MESSAGE, Path(folder) / 'missing.sqlite3', actor='peer')
        client.poll.assert_not_called()

    def test_ack_and_submission_are_distinct_evidence(self):
        client = Mock()
        client.message_status.side_effect = ClientError('broker_error', 'not found', 404)
        client.poll.side_effect = [
            {'messages': [{'id': MESSAGE, 'sender': 'peer', 'recipient': RECIPIENT, 'status': 'queued'}]},
            {'messages': []},
        ]
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'ledger.sqlite3'
            with closing(sqlite3.connect(path)) as db, db:
                db.execute('CREATE TABLE codex_queue_attempts(message_id TEXT, recipient TEXT, state TEXT)')
                db.execute('INSERT INTO codex_queue_attempts VALUES (?, ?, ?)', (MESSAGE, RECIPIENT, 'accepted'))
            result = message_status(client, RECIPIENT, MESSAGE, path)
        self.assertEqual('queued', result['broker_status'])
        self.assertEqual('accepted', result['native_submission'])
        self.assertEqual('not recorded', result['linked_reply_status'])

    def test_reports_linked_reply_recorded_only_for_exact_reciprocal_link(self):
        reply_id = '123e4567-e89b-12d3-a456-426614174002'
        client = Mock()
        client.message_status.side_effect = ClientError('broker_error', 'not found', 404)
        client.poll.side_effect = [
            {'messages': [{'id': MESSAGE, 'sender': 'peer', 'recipient': RECIPIENT,
                           'status': 'acknowledged'}]},
            {'messages': [
                {'id': '123e4567-e89b-12d3-a456-426614174003', 'sender': 'someone-else',
                 'recipient': 'peer', 'reply_to': MESSAGE},
                {'id': '123e4567-e89b-12d3-a456-426614174004', 'sender': RECIPIENT,
                 'recipient': 'someone-else', 'reply_to': MESSAGE},
                {'id': reply_id, 'sender': RECIPIENT, 'recipient': 'peer', 'reply_to': MESSAGE},
            ]},
        ]
        with tempfile.TemporaryDirectory() as folder:
            result = message_status(client, RECIPIENT, MESSAGE, Path(folder) / 'missing.sqlite3')
        self.assertEqual('recorded', result['linked_reply_status'])
        self.assertEqual(reply_id, result['reply_message_id'])

    def test_unlinked_or_wrong_direction_messages_are_reported_as_not_recorded(self):
        client = Mock()
        client.message_status.side_effect = ClientError('broker_error', 'not found', 404)
        client.poll.side_effect = [
            {'messages': [{'id': MESSAGE, 'sender': 'peer', 'recipient': RECIPIENT,
                           'status': 'queued'}]},
            {'messages': [
                {'id': 'reply-1', 'sender': RECIPIENT, 'recipient': 'someone-else',
                 'reply_to': MESSAGE},
                {'id': 'reply-2', 'sender': 'someone-else', 'recipient': 'peer',
                 'reply_to': MESSAGE},
                {'id': 'reply-3', 'sender': RECIPIENT, 'recipient': 'peer',
                 'reply_to': 'another-message'},
            ]},
        ]
        with tempfile.TemporaryDirectory() as folder:
            result = message_status(client, RECIPIENT, MESSAGE, Path(folder) / 'missing.sqlite3')
        self.assertEqual('not recorded', result['linked_reply_status'])
        self.assertNotIn('reply_message_id', result)

    def test_absent_message_is_not_reported_as_delivered(self):
        client = Mock()
        client.message_status.side_effect = ClientError('broker_error', 'not found', 404)
        client.poll.return_value = {'messages': []}
        with tempfile.TemporaryDirectory() as folder:
            with self.assertRaisesRegex(ValueError, 'not found'):
                message_status(client, RECIPIENT, MESSAGE, Path(folder) / 'missing.sqlite3')


if __name__ == '__main__':
    unittest.main()
