"""Opt-in watcher: queue process-status alerts to one explicit Codex inbox.

This does not restart the watched session, judge task correctness, or wake Codex.
It retries pending alerts with a stable message ID to avoid duplicate messages.
"""
import argparse
from contextlib import closing
import json
import os
import sqlite3
import sys
import time
import uuid
from pathlib import Path

import child_stop
from codex_client import BrokerClient, ClientError
from monitor import process_status

ROOT = Path(__file__).resolve().parent


def watch_once(pid, recipient, db_path, broker=None):
    sender = 'system:process-monitor'
    client = broker or BrokerClient()
    with closing(sqlite3.connect(db_path)) as db, db:
        db.execute('BEGIN IMMEDIATE')
        db.execute('CREATE TABLE IF NOT EXISTS watch_state '
                   '(pid INTEGER, recipient TEXT, status TEXT, PRIMARY KEY(pid,recipient))')
        db.execute('CREATE TABLE IF NOT EXISTS watch_events '
                   '(id INTEGER PRIMARY KEY AUTOINCREMENT, pid INTEGER, recipient TEXT, '
                   'status TEXT, delivery TEXT, created REAL)')
        columns = {row[1] for row in db.execute('PRAGMA table_info(watch_events)')}
        if 'content' not in columns:
            db.execute('ALTER TABLE watch_events ADD COLUMN content TEXT')
        previous = db.execute('SELECT status FROM watch_state WHERE pid=? AND recipient=?',
                              (pid, recipient)).fetchone()
        old = previous[0] if previous else None
        # Once the original process has exited, the same PID can be reused by
        # an unrelated process. Retire this watch instead of sending false
        # recovery or repeated exit alerts. A resumed session needs a new watch.
        current = 'not_running' if old == 'not_running' else process_status(pid)
        changed = old != current
        event_id = None
        if changed:
            db.execute('INSERT INTO watch_state VALUES (?,?,?) ON CONFLICT(pid,recipient) '
                       'DO UPDATE SET status=excluded.status', (pid, recipient, current))
            # A reappearing PID can belong to another process. Never announce
            # that the original agent recovered on PID presence alone.
            if current != 'alive':
                cursor = db.execute('INSERT INTO watch_events(pid,recipient,status,delivery,created) '
                                    'VALUES (?,?,?,?,?)', (pid, recipient, current, 'pending', time.time()))
                event_id = cursor.lastrowid
                content = (f'Process monitor event {event_id}: watched Claude process {pid} '
                           f'is {current}. Process presence does not establish agent health. '
                           'PIDs may be reused; a present PID is not proof the original session resumed. '
                           'Do not restart the session without the user\'s instruction.')
                db.execute('UPDATE watch_events SET content=? WHERE id=?', (content, event_id))
        db.commit()
        pending = db.execute('SELECT id,status,content FROM watch_events WHERE pid=? AND recipient=? '
                             'AND delivery=? ORDER BY id', (pid, recipient, 'pending')).fetchall()
        delivery = 'none'
        for pending_id, status, content in pending:
            if not content:
                # Old incomplete events cannot be safely reconstructed: a previous
                # version may already have sent a different payload with this ID.
                db.execute('UPDATE watch_events SET delivery=? WHERE id=?', ('unknown', pending_id))
                db.commit()
                continue
            message_id = str(uuid.uuid5(uuid.NAMESPACE_URL,
                                        f'agentbridge-monitor:{pid}:{recipient}:{pending_id}'))
            try:
                client.register(sender, 'system')
                result = client.send(sender, recipient, content, message_id=message_id)
                delivery = 'queued' if result.get('status') in ('queued', 'acknowledged') else 'pending'
            except (ClientError, OSError):
                delivery = 'pending'
            if delivery == 'queued':
                db.execute('UPDATE watch_events SET delivery=? WHERE id=?', (delivery, pending_id))
                db.commit()
            else:
                break
        response = {'process_status': current, 'change': changed, 'notification': delivery}
        if event_id is not None:
            response['event_id'] = event_id
        return response


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--pid', type=int, required=True)
    parser.add_argument('--recipient', required=True)
    parser.add_argument('--interval', type=int, default=30)
    parser.add_argument('--state', type=Path, default=ROOT / 'watch-state.sqlite3')
    args = parser.parse_args()
    if args.pid <= 0 or args.interval < 5:
        parser.error('PID must be positive and interval at least 5 seconds')
    stop_request = child_stop.request_path(__file__)
    while True:
        # Checked before every pass, so a stopped watcher starts no new work.
        if stop_request.exists():
            print(json.dumps({'status': 'stop_requested'}), flush=True)
            child_stop.consume(stop_request)
            return
        result = watch_once(args.pid, args.recipient, args.state)
        if result.get('change'):
            print(json.dumps(result), flush=True)
        child_stop.pause(args.interval, stop_request)


if __name__ == '__main__':
    main()
