"""Watch one explicitly selected Claude transcript for structured tool errors.

Only new complete lines are inspected. Alerts contain no tool output or secrets.
The watcher never starts or restarts a session, and delivery is queued, not proof
that Claude read it. Broker retries use deterministic message IDs.
"""
import argparse
from contextlib import closing
import json
import os
import sqlite3
import time
import uuid
from pathlib import Path

from codex_client import BrokerClient, ClientError
import child_stop

ROOT = Path(__file__).resolve().parent
SENDER = 'system:tool-error-monitor'


def structured_error(record):
    if not isinstance(record, dict) or record.get('type') != 'user':
        return False
    message = record.get('message')
    blocks = message.get('content') if isinstance(message, dict) else None
    return isinstance(blocks, list) and any(
        isinstance(block, dict) and block.get('type') == 'tool_result' and block.get('is_error') is True
        for block in blocks)


def poll_once(path, recipient, db_path, client=None, now=None, cooldown=300):
    path = Path(path)
    client = client or BrokerClient()
    now = time.time() if now is None else now
    with closing(sqlite3.connect(db_path)) as db, db:
        db.execute('CREATE TABLE IF NOT EXISTS cursors '
                   '(path TEXT, recipient TEXT, file_id TEXT, offset INTEGER, last_alert REAL, '
                   'PRIMARY KEY(path,recipient))')
        db.execute('CREATE TABLE IF NOT EXISTS error_alerts '
                   '(id TEXT PRIMARY KEY, recipient TEXT, content TEXT, status TEXT)')
        try:
            stream = path.open('rb')
        except FileNotFoundError:
            return {'status': 'transcript_missing'}
        stat = os.fstat(stream.fileno())
        file_id = f'{stat.st_dev}:{stat.st_ino}'
        key = str(path.resolve())
        state = db.execute('SELECT file_id,offset,last_alert FROM cursors WHERE path=? AND recipient=?',
                           (key, recipient)).fetchone()
        if state is None:
            # Do not replay old history on first launch.
            offset, last_alert = stat.st_size, 0.0
        elif state[0] != file_id or state[1] > stat.st_size:
            # A replacement or truncation contains new records from its start.
            offset, last_alert = 0, state[2]
        else:
            offset, last_alert = state[1], state[2]
        seen = 0
        with stream:
            stream.seek(offset)
            while True:
                start = stream.tell()
                line = stream.readline()
                if not line:
                    break
                if not line.endswith(b'\n'):
                    stream.seek(start)
                    break
                try:
                    record = json.loads(line)
                except (UnicodeError, json.JSONDecodeError):
                    continue
                if not structured_error(record):
                    continue
                seen += 1
                if now - last_alert < cooldown:
                    continue
                ident = str(uuid.uuid5(uuid.NAMESPACE_URL,
                                      f'agentbridge-error:{recipient}:{file_id}:{start}'))
                content = ('A tool reported an error in this Claude session. '
                           'Please inspect the recent tool result and decide whether action is needed. '
                           'This is an automated agent alert, not a new user instruction.')
                db.execute('INSERT OR IGNORE INTO error_alerts VALUES (?,?,?,?)',
                           (ident, recipient, content, 'pending'))
                last_alert = now
            offset = stream.tell()
            try:
                current = path.stat()
                if ((current.st_dev, current.st_ino) != (stat.st_dev, stat.st_ino)
                        or current.st_size < offset):
                    db.rollback()
                    return {'status': 'transcript_changed'}
            except FileNotFoundError:
                db.rollback()
                return {'status': 'transcript_changed'}
        db.execute('INSERT INTO cursors VALUES (?,?,?,?,?) ON CONFLICT(path,recipient) DO UPDATE SET '
                   'file_id=excluded.file_id,offset=excluded.offset,last_alert=excluded.last_alert',
                   (key, recipient, file_id, offset, last_alert))
        db.commit()
        queued = 0
        pending = db.execute('SELECT id,content FROM error_alerts WHERE recipient=? AND status=?',
                             (recipient, 'pending')).fetchall()
        for ident, content in pending:
            try:
                client.register(SENDER, 'system')
                result = client.send(SENDER, recipient, content, message_id=ident)
            except (ClientError, OSError):
                break
            if result.get('status') not in ('queued', 'acknowledged'):
                break
            db.execute('UPDATE error_alerts SET status=? WHERE id=?', ('queued', ident))
            db.commit()
            queued += 1
        return {'status': 'ok', 'new_tool_errors': seen, 'alerts_queued': queued}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--transcript', type=Path, required=True)
    parser.add_argument('--recipient', required=True)
    parser.add_argument('--state', type=Path, default=ROOT / 'error-watch.sqlite3')
    parser.add_argument('--interval', type=int, default=30)
    args = parser.parse_args()
    if args.interval < 5:
        parser.error('interval must be at least 5 seconds')
    stop_request = child_stop.request_path(__file__)
    while True:
        # Checked before every poll, so a stopped watcher starts no new work.
        if stop_request.exists():
            print(json.dumps({'status': 'stop_requested'}), flush=True)
            child_stop.consume(stop_request)
            return
        result = poll_once(args.transcript, args.recipient, args.state)
        if result.get('new_tool_errors') or result.get('alerts_queued'):
            print(json.dumps(result), flush=True)
        child_stop.pause(args.interval, stop_request)


if __name__ == '__main__':
    main()
