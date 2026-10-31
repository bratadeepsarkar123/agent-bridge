"""Local, opt-in message broker for participating agent sessions.

Run this as a single process. The discovery file contains a bearer secret and
must stay in a directory accessible only to the local user. Acknowledgement is
explicit: an HTTP success means queued, not that an agent read the message.
"""
import argparse
from contextlib import closing
import hashlib
import hmac
import json
import os
import re
import secrets
import sqlite3
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

ROOT = Path(__file__).resolve().parent
ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
MAX_GROUP_RECIPIENTS = 100


class Broker:
    def __init__(self, database):
        self.database = str(database)
        with closing(self.connect()) as db, db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS sessions (
                    id TEXT PRIMARY KEY, kind TEXT NOT NULL,
                    updated REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS messages (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    id TEXT UNIQUE NOT NULL, sender TEXT NOT NULL,
                    recipient TEXT NOT NULL, text TEXT NOT NULL,
                    status TEXT NOT NULL, created REAL NOT NULL,
                    acknowledged REAL
                );
                CREATE INDEX IF NOT EXISTS recipient_queue
                    ON messages(recipient, status, seq);
                CREATE TABLE IF NOT EXISTS group_batches (
                    batch_id TEXT PRIMARY KEY,
                    sender TEXT NOT NULL,
                    recipients_json TEXT NOT NULL,
                    text_sha256 TEXT NOT NULL,
                    created REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS group_batch_deliveries (
                    batch_id TEXT NOT NULL,
                    recipient TEXT NOT NULL,
                    message_id TEXT UNIQUE NOT NULL,
                    PRIMARY KEY(batch_id, recipient),
                    FOREIGN KEY(batch_id) REFERENCES group_batches(batch_id)
                );
            """)
            # Older registrations were sometimes created on another session's
            # behalf. They are not evidence of that session opting in.
            columns = {row['name'] for row in db.execute('PRAGMA table_info(sessions)')}
            if 'opted_in' not in columns:
                db.execute('ALTER TABLE sessions ADD COLUMN opted_in INTEGER NOT NULL DEFAULT 0')

    def connect(self):
        db = sqlite3.connect(self.database, timeout=10)
        db.row_factory = sqlite3.Row
        return db

    @staticmethod
    def valid_id(value):
        return isinstance(value, str) and ID_RE.fullmatch(value) is not None

    def register(self, data):
        ident, kind = data.get('id'), data.get('kind')
        if not self.valid_id(ident) or kind not in ('claude', 'codex', 'system'):
            raise ValueError('invalid session id or kind')
        if data.get('opt_in') is not True:
            raise ValueError('explicit opt_in is required')
        with closing(self.connect()) as db, db:
            old = db.execute('SELECT kind FROM sessions WHERE id=?', (ident,)).fetchone()
            if old and old['kind'] != kind:
                raise ValueError('session id belongs to another kind')
            db.execute('INSERT INTO sessions(id,kind,updated,opted_in) VALUES (?, ?, ?, 1) ON CONFLICT(id) '
                       'DO UPDATE SET updated=excluded.updated,opted_in=1', (ident, kind, time.time()))
        return {'id': ident, 'kind': kind}

    def sessions(self):
        with closing(self.connect()) as db, db:
            return {'sessions': [dict(row) for row in db.execute(
                'SELECT id, kind, updated FROM sessions WHERE opted_in=1 ORDER BY id')]}

    def enqueue(self, data):
        sender, recipient, text = (data.get(k) for k in ('sender', 'recipient', 'text'))
        if not self.valid_id(sender) or not self.valid_id(recipient):
            raise ValueError('invalid sender or recipient')
        if sender == recipient:
            raise ValueError('sender and recipient must differ')
        if not isinstance(text, str) or not text.strip() or len(text) > 4000:
            raise ValueError('text must contain 1 to 4000 characters')
        with closing(self.connect()) as db, db:
            known = {r['id'] for r in db.execute(
                'SELECT id FROM sessions WHERE opted_in=1 AND id IN (?, ?)', (sender, recipient))}
            if sender not in known or recipient not in known:
                raise ValueError('sender and recipient must be registered')
            requested = data.get('id')
            if requested is None:
                ident = str(uuid.uuid4())
            else:
                try:
                    ident = str(uuid.UUID(requested))
                except (ValueError, TypeError, AttributeError) as exc:
                    raise ValueError('message id must be a UUID') from exc
                if requested != ident:
                    raise ValueError('message id must be a canonical UUID')
            db.execute('INSERT OR IGNORE INTO messages(id,sender,recipient,text,status,created) '
                       'VALUES (?,?,?,?,?,?)', (ident, sender, recipient, text, 'queued', time.time()))
            row = db.execute('SELECT sender,recipient,text,status FROM messages WHERE id=?', (ident,)).fetchone()
            if (row['sender'], row['recipient'], row['text']) != (sender, recipient, text):
                raise ValueError('message id already belongs to different content')
            result = {'id': ident, 'status': row['status']}
        return result

    def enqueue_group(self, data):
        """Atomically fan out one explicit, idempotent batch to individual inboxes."""
        sender, recipients, text, batch_id = (
            data.get(k) for k in ('sender', 'recipients', 'text', 'batch_id'))
        if not self.valid_id(sender):
            raise ValueError('invalid sender')
        if not isinstance(recipients, list) or not 1 <= len(recipients) <= MAX_GROUP_RECIPIENTS:
            raise ValueError(f'recipients must contain 1 to {MAX_GROUP_RECIPIENTS} session IDs')
        if any(not self.valid_id(recipient) for recipient in recipients):
            raise ValueError('invalid recipient')
        if len(set(recipients)) != len(recipients):
            raise ValueError('recipients must be unique')
        if sender in recipients:
            raise ValueError('sender must not be a recipient')
        if not isinstance(text, str) or not text.strip() or len(text) > 4000:
            raise ValueError('text must contain 1 to 4000 characters')
        if not isinstance(batch_id, str):
            raise ValueError('batch_id must be a canonical UUID')
        try:
            parsed_batch_id = uuid.UUID(batch_id)
        except (ValueError, TypeError, AttributeError) as exc:
            raise ValueError('batch_id must be a canonical UUID') from exc
        if str(parsed_batch_id) != batch_id:
            raise ValueError('batch_id must be a canonical UUID')

        # Canonical ordering makes recipient order irrelevant to idempotency.
        ordered_recipients = sorted(recipients)
        recipients_json = json.dumps(ordered_recipients, separators=(',', ':'))
        text_sha256 = hashlib.sha256(text.encode('utf-8')).hexdigest()
        all_ids = [sender, *ordered_recipients]
        all_placeholders = ','.join('?' for _ in all_ids)

        # Serialize concurrent retries before checking or creating the batch.
        with closing(self.connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            known = {row['id'] for row in db.execute(
                f'SELECT id FROM sessions WHERE opted_in=1 AND id IN ({all_placeholders})', all_ids)}
            if sender not in known:
                raise ValueError('sender must be registered')
            missing = [recipient for recipient in ordered_recipients if recipient not in known]
            if missing:
                raise ValueError('all recipients must be registered')

            batch = db.execute(
                'SELECT sender,recipients_json,text_sha256 FROM group_batches WHERE batch_id=?',
                (batch_id,)).fetchone()
            is_new = batch is None
            if batch is not None:
                if (batch['sender'], batch['recipients_json'], batch['text_sha256']) != (
                        sender, recipients_json, text_sha256):
                    raise ValueError('batch_id already belongs to different content or recipients')
            else:
                db.execute('INSERT INTO group_batches(batch_id,sender,recipients_json,text_sha256,created) '
                           'VALUES (?,?,?,?,?)',
                           (batch_id, sender, recipients_json, text_sha256, time.time()))

            for recipient in ordered_recipients:
                delivery = db.execute(
                    'SELECT message_id FROM group_batch_deliveries WHERE batch_id=? AND recipient=?',
                    (batch_id, recipient)).fetchone()
                if delivery is not None:
                    continue
                if not is_new:
                    raise ValueError('stored group batch is incomplete')

                message_id = str(uuid.uuid4())
                db.execute('INSERT INTO messages(id,sender,recipient,text,status,created) '
                           'VALUES (?,?,?,?,?,?)',
                           (message_id, sender, recipient, text, 'queued', time.time()))
                db.execute('INSERT INTO group_batch_deliveries(batch_id,recipient,message_id) '
                           'VALUES (?,?,?)', (batch_id, recipient, message_id))

            rows = db.execute(
                'SELECT m.id,m.recipient,m.status FROM group_batch_deliveries d '
                'JOIN messages m ON m.id=d.message_id WHERE d.batch_id=? ORDER BY m.recipient',
                (batch_id,)).fetchall()
            if len(rows) != len(ordered_recipients):
                raise ValueError('stored group batch is incomplete')
            return {
                'batch_id': batch_id,
                'deliveries': [
                    {'id': row['id'], 'recipient': row['recipient'], 'status': row['status']}
                    for row in rows
                ],
            }

    def inbox(self, recipient, after=0):
        if not self.valid_id(recipient):
            raise ValueError('invalid recipient')
        if isinstance(after, bool) or not isinstance(after, int) or after < 0:
            raise ValueError('invalid page cursor')
        with closing(self.connect()) as db, db:
            if not db.execute('SELECT 1 FROM sessions WHERE id=? AND opted_in=1', (recipient,)).fetchone():
                raise ValueError('recipient has not opted in')
            rows = [dict(row) for row in db.execute(
                'SELECT m.seq,m.id,m.sender,m.recipient,m.text,m.status,m.created,m.acknowledged '
                'FROM messages m JOIN sessions s ON s.id=m.sender AND s.opted_in=1 '
                'WHERE m.recipient=? AND m.seq>? ORDER BY m.seq LIMIT 1000', (recipient, after))]
            return {'messages': rows, 'next_after': rows[-1]['seq'] if len(rows) == 1000 else None}

    def ack(self, data):
        ident, recipient = data.get('id'), data.get('recipient')
        if not isinstance(ident, str) or not self.valid_id(recipient):
            raise ValueError('invalid message or recipient')
        with closing(self.connect()) as db, db:
            row = db.execute('SELECT status FROM messages WHERE id=? AND recipient=?',
                             (ident, recipient)).fetchone()
            if not row:
                raise ValueError('message not found for recipient')
            if row['status'] == 'queued':
                db.execute('UPDATE messages SET status=?, acknowledged=? WHERE id=? AND recipient=?',
                           ('acknowledged', time.time(), ident, recipient))
        return {'id': ident, 'status': 'acknowledged'}


def handler_for(broker, token):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def response(self, status, value):
            raw = json.dumps(value, ensure_ascii=False).encode('utf-8')
            self.send_response(status)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('Content-Length', str(len(raw)))
            self.send_header('Cache-Control', 'no-store')
            self.end_headers()
            self.wfile.write(raw)

        def authorized(self):
            if not hmac.compare_digest(self.headers.get('Authorization', ''), 'Bearer ' + token):
                self.response(403, {'error': 'unauthorized'})
                return False
            return True

        def do_GET(self):
            if not self.authorized():
                return
            try:
                path = urlsplit(self.path)
                if path.path == '/v1/sessions':
                    return self.response(200, broker.sessions())
                if path.path == '/v1/messages':
                    query = parse_qs(path.query)
                    recipient = query.get('recipient', [''])[0]
                    after = int(query.get('after', ['0'])[0])
                    return self.response(200, broker.inbox(recipient, after))
                self.response(404, {'error': 'not found'})
            except ValueError as exc:
                self.response(400, {'error': str(exc)})

        def do_POST(self):
            if not self.authorized():
                return
            try:
                length = int(self.headers.get('Content-Length', '0'))
                if not 0 < length <= 20000:
                    raise ValueError('invalid request size')
                data = json.loads(self.rfile.read(length))
                if not isinstance(data, dict):
                    raise ValueError('expected a JSON object')
                routes = {'/v1/register': broker.register, '/v1/messages': broker.enqueue,
                          '/v1/group-messages': broker.enqueue_group, '/v1/ack': broker.ack}
                fn = routes.get(urlsplit(self.path).path)
                if fn is None:
                    return self.response(404, {'error': 'not found'})
                self.response(200, fn(data))
            except (ValueError, UnicodeError) as exc:
                self.response(400, {'error': str(exc)})

    return Handler


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--database', type=Path, default=ROOT / 'broker.sqlite3')
    parser.add_argument('--state', type=Path, default=ROOT / 'broker-session.json')
    parser.add_argument('--recover', action='store_true',
                        help='reuse stale endpoint credentials after checking the old process is gone')
    args = parser.parse_args()
    token, port = secrets.token_urlsafe(32), 0
    if args.recover:
        from status import _process_is_alive
        try:
            previous = json.loads(args.state.read_text(encoding='utf-8'))
            old_pid, port, token = previous['pid'], previous['port'], previous['token']
            if (isinstance(old_pid, bool) or not isinstance(old_pid, int) or old_pid <= 0 or
                isinstance(port, bool) or not isinstance(port, int) or not 0 < port <= 65535 or
                not isinstance(token, str) or len(token) < 32):
                raise ValueError('invalid state')
        except (OSError, ValueError, KeyError, TypeError) as exc:
            raise SystemExit('Cannot recover: broker state is unavailable or invalid.') from exc
        if _process_is_alive(old_pid):
            raise SystemExit('Cannot recover: recorded broker process is still running.')
    broker = Broker(args.database)
    server = ThreadingHTTPServer(('127.0.0.1', port), handler_for(broker, token))
    if args.recover:
        # A successful bind on the original port prevents two recoveries from
        # both claiming the same endpoint. Atomically replace only the PID.
        temp = args.state.with_name(args.state.name + '.' + str(os.getpid()) + '.tmp')
        try:
            fd = os.open(str(temp), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                json.dump({'port': server.server_port, 'token': token, 'pid': os.getpid()}, f)
            os.replace(temp, args.state)
        except BaseException:
            server.server_close()
            try:
                temp.unlink()
            except FileNotFoundError:
                pass
            raise
    else:
        # Exclusive create avoids replacing another live server's credentials.
        try:
            fd = os.open(str(args.state), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            server.server_close()
            raise SystemExit('Broker state file already exists; check for a running broker.')
    try:
        if not args.recover:
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                json.dump({'port': server.server_port, 'token': token, 'pid': os.getpid()}, f)
        print('Agent bridge broker listening on localhost.', flush=True)
        server.serve_forever()
    finally:
        server.server_close()
        try:
            args.state.unlink()
        except FileNotFoundError:
            pass


if __name__ == '__main__':
    main()
