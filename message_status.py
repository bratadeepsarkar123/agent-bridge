"""Read broker, submission, and explicit linked-reply evidence for a bridge message.

An unrecorded reply link is not evidence that no reply happened: older clients
may send replies without the optional ``reply_to`` field.
"""
import argparse
from contextlib import closing
import json
import sqlite3
from pathlib import Path

from codex_client import BrokerClient, ClientError
from codex_queue import canonical_uuid

LEDGER = Path(__file__).with_name('codex-native-ledger.sqlite3')


def message_status(client, recipient, message_id, ledger=LEDGER, actor=None):
    recipient = canonical_uuid(recipient, 'recipient')
    message_id = canonical_uuid(message_id, 'message_id')
    try:
        message = client.message_status(recipient, message_id, actor=actor)
    except ClientError as exc:
        if exc.status != 404:
            raise
        # Legacy brokers have no dedicated status endpoint. Accessing both
        # inboxes is possible only when their existing policy permits it.
        messages = client.poll(recipient).get('messages', [])
        message = next((item for item in messages if item.get('id') == message_id), None)
        if message is None:
            raise ValueError('message not found for recipient')
        sender = message.get('sender')
        if actor is not None and actor not in (sender, recipient):
            raise ValueError('actor is not a message participant')
        replies = client.poll(sender).get('messages', []) if isinstance(sender, str) else []
        reply = next((item for item in replies if isinstance(item, dict)
                      and isinstance(item.get('id'), str) and item.get('id')
                      and item.get('reply_to') == message_id
                      and item.get('sender') == recipient and item.get('recipient') == sender), None)
        message['linked_reply_status'] = 'recorded' if reply is not None else 'not recorded'
        if reply is not None:
            message['reply_message_id'] = reply['id']
    if not isinstance(message, dict) or message.get('id') != message_id or message.get('recipient') != recipient:
        raise ValueError('broker returned invalid message status')
    evidence = {'message_id': message_id, 'sender': message['sender'], 'recipient': recipient,
                'broker_status': message['status'], 'native_submission': 'not recorded',
                'linked_reply_status': message.get('linked_reply_status', 'not recorded')}
    if message.get('reply_message_id') is not None:
        evidence['reply_message_id'] = message['reply_message_id']
    path = Path(ledger)
    if path.is_file():
        with closing(sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True)) as db:
            row = db.execute('SELECT state FROM codex_queue_attempts WHERE message_id=? AND recipient=?',
                             (message_id, recipient)).fetchone()
            if row:
                evidence['native_submission'] = row[0]
    return evidence


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--recipient', required=True)
    parser.add_argument('--id', required=True)
    parser.add_argument('--actor', help='the opted-in sender or recipient requesting status')
    args = parser.parse_args()
    try:
        print(json.dumps(message_status(BrokerClient(), args.recipient, args.id, actor=args.actor)))
    except (ClientError, ValueError, sqlite3.Error) as exc:
        parser.exit(1, 'Status unavailable: {}\n'.format(exc))


if __name__ == '__main__':
    main()
