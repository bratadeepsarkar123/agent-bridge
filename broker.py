"""Local, opt-in message broker for participating agent sessions.

Run this as a single process. The discovery file contains a bearer secret and
must stay in a directory accessible only to the local user. Acknowledgement is
explicit: an HTTP success means queued, not that an agent read the message.

Staged session credential contract:
* Local administrators call Broker.provision_session_credential(id, token) with a
  separately generated secrets.token_urlsafe(32) value. It reserves/protects the
  profile before registration, rotates on repeat calls, stores only SHA-256, and
  returns no secret. There is deliberately no HTTP or model MCP provisioning API.
  Passing replace=False atomically requires an absent binding; the default
  replace=True preserves explicit local rotation semantics.
* HTTP Authorization: Bearer <shared-token> retains legacy access to unprotected
  actors. Authorization: Bearer <session-token> acts only as its protected profile.
  No body/query token or identity fallback is accepted. Failures are HTTP 403 with
  a generic error. Ordinary malformed application requests remain HTTP 400.
* Actor fields: register.id; messages/group/room/reply-all.sender; ack.recipient;
  inbox/wait query recipient; status query actor (defaults to recipient when
  omitted); rooms.owner; room-info query member; room
  membership changes.actor. Destination recipients/members are not actors.
* GET /v1/sessions/check?id=<id> authorizes that exact opted-in session and
  returns only id and kind. Shared credentials work only for unprotected IDs.
* GET /v1/messages/status?recipient=<destination>&id=<message-id>&actor=<id>
  authorizes actor, which must equal the stored sender or recipient. Omitting
  actor preserves recipient-as-actor requests. Returns id, sender, recipient,
  status, acknowledged, linked_reply_status ('recorded' or 'not recorded'), and
  reply_message_id only when an exact reciprocal direct reply is recorded.
  Both original and reply must be outside rooms. The earliest matching reply
  by sequence is reported. No body or native submission evidence is returned.
  'not recorded' does not establish that no unlinked reply happened. Recipient
  always selects the original destination, never the requesting actor.
* Python operations accept keyword-only credential=None for legacy calls, or
  the raw scoped bearer. The in-process API is trusted local code, not a boundary
  against same-user malware. Credentials bind a configured profile, not an actual
  Codex thread. Existing clients require staged credential configuration before
  their profile is protected. Provisioning never implies opt-in.

"""
import argparse
from contextlib import closing, contextmanager
import hashlib
import hmac
import json
import math
import os
import re
import secrets
import socket
import sqlite3
import sys
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit

ROOT = Path(__file__).resolve().parent
ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
MAX_GROUP_RECIPIENTS = 100
MAX_ROOM_MEMBERS = 100
MAX_ROOM_NAME = 120
DEFAULT_WAIT_SECONDS = 25
MAX_WAIT_SECONDS = 30
MAX_WAIT_MESSAGES = 100


def stop_marker_path(state):
    """Persistent operator-owned recovery inhibit, separate from discovery.

    Operator recovery is intentionally not automated: establish that the old
    process and every handler/writer have exited, preserve/reconcile the latest
    database and credential generations, and review discovery/marker ownership
    before an explicit recovery decision. Never delete a marker merely because
    its PID exited, its digest differs, or its contents cannot be parsed.
    Losing/rolling back this directory or bypassing this protocol defeats the
    fence. fsync/hard-link publication handles ordinary process crashes, not an
    unconditional power-loss guarantee (especially Windows directory metadata).

    ROLLOUT BLOCKER: if finalization cannot publish an inhibit before the process
    exits, discovery can remain without a marker. The supervisor can then regard
    that exit as a crash and auto-recover. Refusing a controller stop while still
    serving does not solve startup/finalization exceptions or subsequent process
    death. A durable pre-armed recovery policy/independent supervisor protocol is
    required to close this gap; stderr, exit status, or an in-memory flag cannot
    substitute for persistent coordination. Do not deploy this as a complete
    fail-closed stop/recovery protocol until that design is implemented.
    """
    return Path(state).with_name(Path(state).name + '.stop-pending')


def require_recovery_allowed(state):
    # Presence alone inhibits: stale, partial, unreadable and foreign markers
    # never grant authority. lstat also catches dangling links.
    try:
        stop_marker_path(state).lstat()
    except FileNotFoundError:
        return
    raise RuntimeError('Stop pending; explicit operator recovery required')


@contextmanager
def recovery_gate(state):
    """Serialize startup and stop publication, not a same-user security boundary.

    The lock file persists; OS locks release on process death. Never unlink it:
    replacing its inode would split the lock domain. Paths/ACLs and all launchers
    must be trusted. Windows file fsync does not prove directory-entry durability
    across power loss; this protocol is not rollback-resistant storage.
    """
    path = Path(state).with_name(Path(state).name + '.recovery-lock')
    with open(path, 'a+b') as stream:
        stream.seek(0, os.SEEK_END)
        if stream.tell() == 0:
            stream.write(b'\0')
            stream.flush()
        stream.seek(0)
        if os.name == 'nt':
            import msvcrt
            msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            yield
        finally:
            stream.seek(0)
            if os.name == 'nt':
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


def _sync_directory(path):
    if os.name != 'nt':
        fd = os.open(str(path), os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def publish_stop_inhibit(state, owned_state):
    """Publish complete, flushed inhibit before draining; return ownership bytes.

    Atomic exclusive hard-link publication avoids replacing another marker. Any
    failure prevents drain and cleanup. Crash leftovers require operator review.
    The marker contains no bearer. Matching uses exact discovery bytes, including
    legacy discovery without an instance UUID. No automatic stale-marker removal.
    """
    state = Path(state)
    with recovery_gate(state):
        require_recovery_allowed(state)
        if state.read_bytes() != owned_state:
            raise RuntimeError('Discovery ownership changed')
        raw = json.dumps({'version': 1, 'operation': uuid.uuid4().hex,
                          'state_sha256': hashlib.sha256(owned_state).hexdigest()},
                         sort_keys=True).encode('ascii')
        temporary = state.with_name(state.name + '.stop-' + uuid.uuid4().hex + '.tmp')
        try:
            fd = os.open(str(temporary), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, 'wb') as stream:
                stream.write(raw)
                stream.flush()
                os.fsync(stream.fileno())
            os.link(temporary, stop_marker_path(state))
            _sync_directory(state.parent)
        finally:
            temporary.unlink(missing_ok=True)
        return raw


def complete_stop(state, owned_state, owned_marker):
    """Only the successful stopper may remove its exact state and marker."""
    state = Path(state)
    with recovery_gate(state):
        if (state.read_bytes() != owned_state or
                stop_marker_path(state).read_bytes() != owned_marker):
            raise RuntimeError('Stop ownership changed; operator recovery required')
        state.unlink()
        _sync_directory(state.parent)
        stop_marker_path(state).unlink()
        _sync_directory(state.parent)


def require_current_schema_or_empty(database, *, require_existing=False):
    """Reject old schemas before any migration; does not detect stale backups."""
    database = Path(database)
    if not database.exists():
        if require_existing:
            raise ValueError('recovery requires an existing database')
        return
    uri = database.resolve().as_uri() + '?mode=ro'
    with closing(sqlite3.connect(uri, uri=True)) as db:
        tables = {row[0] for row in db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")}
        if not tables:
            if require_existing:
                raise ValueError('recovery requires a nonempty database')
            return
        required = {
            'sessions': {'id', 'kind', 'updated', 'opted_in'},
            'messages': {'seq', 'id', 'sender', 'recipient', 'text', 'status', 'created', 'acknowledged', 'room_id', 'reply_to'},
            'session_credentials': {'session_id', 'token_sha256'},
            'rooms': {'room_id', 'name', 'owner', 'created'},
            'room_members': {'room_id', 'member_id'},
            'group_batches': {'batch_id', 'sender', 'recipients_json', 'text_sha256', 'created', 'room_id', 'reply_to'},
            'group_batch_deliveries': {'batch_id', 'recipient', 'message_id'},
        }
        if not required.keys() <= tables:
            raise ValueError('database schema requires explicit migration')
        unique_keys = {
            'sessions': {('id',)},
            'messages': {('seq',), ('id',)},
            'session_credentials': {('session_id',), ('token_sha256',)},
            'rooms': {('room_id',)},
            'room_members': {('room_id', 'member_id')},
            'group_batches': {('batch_id',)},
            'group_batch_deliveries': {('batch_id', 'recipient'), ('message_id',)},
        }
        for table, columns in required.items():
            info = list(db.execute(f'PRAGMA table_info({table})'))
            actual = {row[1] for row in info}
            if not columns <= actual:
                raise ValueError('database schema requires explicit migration')
            primary = tuple(row[1] for row in sorted((row for row in info if row[5]), key=lambda row: row[5]))
            keys = {primary} if primary else set()
            for index in db.execute(f'PRAGMA index_list({table})'):
                if index[2] and not index[4]:  # unique and not partial
                    name = index[1].replace("'", "''")
                    keys.add(tuple(row[2] for row in db.execute(f"PRAGMA index_info('{name}')")))
            if not unique_keys[table] <= keys:
                raise ValueError('database schema lacks required uniqueness constraints')


class Broker:
    def __init__(self, database):
        self.database = str(database)
        self._message_condition = threading.Condition()
        self._shared_token_hashes = set()
        with closing(self.connect()) as db, db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS session_credentials (
                    session_id TEXT PRIMARY KEY, token_sha256 TEXT UNIQUE NOT NULL
                );
                CREATE TABLE IF NOT EXISTS sessions (
                    id TEXT PRIMARY KEY, kind TEXT NOT NULL,
                    updated REAL NOT NULL, opted_in INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS messages (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    id TEXT UNIQUE NOT NULL, sender TEXT NOT NULL,
                    recipient TEXT NOT NULL, text TEXT NOT NULL,
                    status TEXT NOT NULL, created REAL NOT NULL,
                    acknowledged REAL, room_id TEXT, reply_to TEXT
                );
                CREATE INDEX IF NOT EXISTS recipient_queue
                    ON messages(recipient, status, seq);
                CREATE TABLE IF NOT EXISTS group_batches (
                    batch_id TEXT PRIMARY KEY,
                    sender TEXT NOT NULL,
                    recipients_json TEXT NOT NULL,
                    text_sha256 TEXT NOT NULL,
                    created REAL NOT NULL,
                    room_id TEXT, reply_to TEXT
                );
                CREATE TABLE IF NOT EXISTS group_batch_deliveries (
                    batch_id TEXT NOT NULL,
                    recipient TEXT NOT NULL,
                    message_id TEXT UNIQUE NOT NULL,
                    PRIMARY KEY(batch_id, recipient),
                    FOREIGN KEY(batch_id) REFERENCES group_batches(batch_id)
                );
                CREATE TABLE IF NOT EXISTS rooms (
                    room_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    owner TEXT NOT NULL,
                    created REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS room_members (
                    room_id TEXT NOT NULL,
                    member_id TEXT NOT NULL,
                    PRIMARY KEY(room_id, member_id),
                    FOREIGN KEY(room_id) REFERENCES rooms(room_id)
                );
            """)
            columns = {row['name'] for row in db.execute('PRAGMA table_info(sessions)')}
            if 'opted_in' not in columns:
                # Legacy registrations made on another session's behalf do not
                # imply participation in the bridge.
                db.execute('ALTER TABLE sessions ADD COLUMN opted_in INTEGER NOT NULL DEFAULT 0')
            # Additive migration keeps existing broker databases and queued rows.
            for table, column in (('messages', 'room_id'), ('messages', 'reply_to'),
                                  ('group_batches', 'room_id'), ('group_batches', 'reply_to')):
                columns = {row['name'] for row in db.execute(f'PRAGMA table_info({table})')}
                if column not in columns:
                    db.execute(f'ALTER TABLE {table} ADD COLUMN {column} TEXT')

    def connect(self):
        db = sqlite3.connect(self.database, timeout=10)
        db.row_factory = sqlite3.Row
        return db

    @staticmethod
    def valid_id(value):
        return isinstance(value, str) and ID_RE.fullmatch(value) is not None

    @staticmethod
    def _credential_hash(credential):
        if (not isinstance(credential, str) or not 32 <= len(credential) <= 512
                or not re.fullmatch(r'[A-Za-z0-9_-]+', credential)):
            raise ValueError('credential must be a 32 to 512 character URL-safe token')
        return hashlib.sha256(credential.encode('ascii')).hexdigest()

    def provision_session_credential(self, session_id, credential, *, replace=True):
        """Local admin only; replace=False atomically requires no prior binding.

        The default preserves explicit rotation semantics. Initial binding rejects
        any existing binding, even an identical token, without changing opt-in.
        """
        if not self.valid_id(session_id):
            raise ValueError('invalid session id')
        if not isinstance(replace, bool):
            raise ValueError('replace must be a boolean')
        digest = self._credential_hash(credential)
        with closing(self.connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            if not replace and db.execute(
                    'SELECT 1 FROM session_credentials WHERE session_id=?', (session_id,)).fetchone():
                raise ValueError('session credential already provisioned')
            if digest in self._shared_token_hashes:
                raise ValueError('session credential must differ from shared token')
            other = db.execute('SELECT session_id FROM session_credentials '
                               'WHERE token_sha256=?', (digest,)).fetchone()
            if other is not None and other['session_id'] != session_id:
                raise ValueError('credential already assigned')
            db.execute('INSERT INTO session_credentials(session_id,token_sha256) VALUES (?,?) '
                       'ON CONFLICT(session_id) DO UPDATE SET token_sha256=excluded.token_sha256',
                       (session_id, digest))
        self._notify_messages()
        return {'id': session_id, 'protected': True}

    def _authorize(self, db, actor, credential):
        row = db.execute('SELECT token_sha256 FROM session_credentials WHERE session_id=?',
                         (actor,)).fetchone()
        if credential is None:
            if row is None:
                return
        else:
            try:
                digest = self._credential_hash(credential)
            except ValueError:
                raise PermissionError('unauthorized') from None
            if row is not None and hmac.compare_digest(row['token_sha256'], digest):
                return
        raise PermissionError('unauthorized')

    def credential_is_valid(self, credential):
        try:
            digest = self._credential_hash(credential)
        except ValueError:
            return False
        with closing(self.connect()) as db:
            return db.execute('SELECT 1 FROM session_credentials WHERE token_sha256=?',
                              (digest,)).fetchone() is not None

    def register(self, data, *, credential=None):
        ident, kind = data.get('id'), data.get('kind')
        if not self.valid_id(ident) or kind not in ('claude', 'codex', 'system'):
            raise ValueError('invalid session id or kind')
        requested_opt_in = data.get('opt_in') is True
        if 'opt_in' in data and not requested_opt_in:
            raise ValueError('opt_in must be true')
        with closing(self.connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            self._authorize(db, ident, credential)
            old = db.execute('SELECT kind,opted_in FROM sessions WHERE id=?', (ident,)).fetchone()
            if old and old['kind'] != kind:
                raise ValueError('session id belongs to another kind')
            if not requested_opt_in:
                if old is None or not old['opted_in']:
                    raise ValueError('explicit opt_in is required')
                db.execute('UPDATE sessions SET updated=? WHERE id=?', (time.time(), ident))
            else:
                db.execute('INSERT INTO sessions(id,kind,updated,opted_in) VALUES (?, ?, ?, 1) '
                           'ON CONFLICT(id) DO UPDATE SET updated=excluded.updated,opted_in=1',
                           (ident, kind, time.time()))
        if requested_opt_in and (old is None or not old['opted_in']):
            # Historical queued messages may become visible when their sender
            # first opts in; wake long polls so they can recheck the inbox.
            self._notify_messages()
        return {'id': ident, 'kind': kind}

    def sessions(self):
        with closing(self.connect()) as db, db:
            return {'sessions': [dict(row) for row in db.execute(
                'SELECT id, kind, updated FROM sessions WHERE opted_in=1 ORDER BY id')]}

    def check_session(self, ident, *, credential=None):
        """Check profile authority and opt-in without reading any messages."""
        if not self.valid_id(ident):
            raise ValueError('invalid session id')
        with closing(self.connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            self._authorize(db, ident, credential)
            row = db.execute('SELECT id,kind FROM sessions WHERE id=? AND opted_in=1',
                             (ident,)).fetchone()
            if row is None:
                raise ValueError('session has not opted in')
            return dict(row)

    def create_room(self, data, *, credential=None):
        room_id, name, owner, members = (
            data.get(k) for k in ('room_id', 'name', 'owner', 'members'))
        if not self.valid_id(room_id):
            raise ValueError('invalid room id')
        if not isinstance(name, str) or not name.strip() or len(name.strip()) > MAX_ROOM_NAME:
            raise ValueError(f'room name must contain 1 to {MAX_ROOM_NAME} characters')
        name = name.strip()
        if not self.valid_id(owner):
            raise ValueError('invalid room owner')
        if (not isinstance(members, list) or not 1 <= len(members) <= MAX_ROOM_MEMBERS or
                any(not self.valid_id(member) for member in members)):
            raise ValueError(f'members must contain 1 to {MAX_ROOM_MEMBERS} registered session IDs')
        if len(set(members)) != len(members):
            raise ValueError('room members must be unique')
        if owner not in members:
            raise ValueError('room owner must be an explicit member')

        ordered_members = sorted(members)
        placeholders = ','.join('?' for _ in ordered_members)
        with closing(self.connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            self._authorize(db, owner, credential)
            known = {row['id'] for row in db.execute(
                f'SELECT id FROM sessions WHERE opted_in=1 AND id IN ({placeholders})', ordered_members)}
            if known != set(ordered_members):
                raise ValueError('all room members must be registered')
            room = db.execute('SELECT name,owner FROM rooms WHERE room_id=?', (room_id,)).fetchone()
            if room is not None:
                stored_members = [row['member_id'] for row in db.execute(
                    'SELECT member_id FROM room_members WHERE room_id=? ORDER BY member_id',
                    (room_id,))]
                if (room['name'], room['owner'], stored_members) != (
                        name, owner, ordered_members):
                    raise ValueError('room id already belongs to different room details')
            else:
                db.execute('INSERT INTO rooms(room_id,name,owner,created) VALUES (?,?,?,?)',
                           (room_id, name, owner, time.time()))
                db.executemany('INSERT INTO room_members(room_id,member_id) VALUES (?,?)',
                               [(room_id, member) for member in ordered_members])
        return {'room_id': room_id, 'name': name, 'owner': owner, 'members': ordered_members}

    def room_info(self, room_id, member, *, credential=None):
        if not self.valid_id(room_id) or not self.valid_id(member):
            raise ValueError('invalid room id or member')
        with closing(self.connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            self._authorize(db, member, credential)
            room = db.execute(
                'SELECT r.room_id,r.name,r.owner FROM rooms r WHERE r.room_id=? AND EXISTS '
                '(SELECT 1 FROM room_members rm JOIN sessions s ON s.id=rm.member_id '
                'WHERE rm.room_id=r.room_id AND rm.member_id=? AND s.opted_in=1)',
                (room_id, member)).fetchone()
            if room is None:
                raise ValueError('room not found or requester is not a member')
            result = dict(room)
            result['members'] = [row['member_id'] for row in db.execute(
                'SELECT rm.member_id FROM room_members rm JOIN sessions s ON s.id=rm.member_id '
                'WHERE rm.room_id=? AND s.opted_in=1 ORDER BY rm.member_id',
                (room_id,))]
            return result

    def update_room_member(self, room_id, data, *, credential=None):
        actor, member, action = (data.get(k) for k in ('actor', 'member', 'action'))
        if not self.valid_id(room_id) or not self.valid_id(actor) or not self.valid_id(member):
            raise ValueError('invalid room id, actor, or member')
        if action not in ('add', 'remove'):
            raise ValueError("action must be 'add' or 'remove'")
        with closing(self.connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            self._authorize(db, actor, credential)
            room = db.execute(
                'SELECT r.owner FROM rooms r JOIN sessions s ON s.id=r.owner '
                'WHERE r.room_id=? AND s.opted_in=1', (room_id,)).fetchone()
            if room is None:
                raise ValueError('room not found')
            if room['owner'] != actor:
                raise ValueError('only the room owner can change membership')
            if action == 'add':
                if not db.execute('SELECT 1 FROM sessions WHERE id=? AND opted_in=1', (member,)).fetchone():
                    raise ValueError('room member must be opted in')
                exists = db.execute(
                    'SELECT 1 FROM room_members WHERE room_id=? AND member_id=?',
                    (room_id, member)).fetchone()
                count = db.execute(
                    'SELECT COUNT(*) FROM room_members WHERE room_id=?', (room_id,)).fetchone()[0]
                if not exists and count >= MAX_ROOM_MEMBERS:
                    raise ValueError(f'room cannot exceed {MAX_ROOM_MEMBERS} members')
                db.execute('INSERT OR IGNORE INTO room_members(room_id,member_id) VALUES (?,?)',
                           (room_id, member))
            else:
                if member == room['owner']:
                    raise ValueError('room owner cannot be removed')
                db.execute('DELETE FROM room_members WHERE room_id=? AND member_id=?',
                           (room_id, member))
            members = [row['member_id'] for row in db.execute(
                'SELECT rm.member_id FROM room_members rm JOIN sessions s ON s.id=rm.member_id '
                'WHERE rm.room_id=? AND s.opted_in=1 ORDER BY rm.member_id',
                (room_id,))]
        return {'room_id': room_id, 'members': members}

    def enqueue(self, data, *, credential=None):
        sender, recipient, text, reply_to = (
            data.get(k) for k in ('sender', 'recipient', 'text', 'reply_to'))
        if not self.valid_id(sender) or not self.valid_id(recipient):
            raise ValueError('invalid sender or recipient')
        if sender == recipient:
            raise ValueError('sender and recipient must differ')
        if not isinstance(text, str) or not text.strip() or len(text) > 4000:
            raise ValueError('text must contain 1 to 4000 characters')
        if reply_to is not None and not self.valid_id(reply_to):
            raise ValueError('invalid reply_to message id')
        with closing(self.connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            self._authorize(db, sender, credential)
            known = {r['id'] for r in db.execute(
                'SELECT id FROM sessions WHERE opted_in=1 AND id IN (?, ?)', (sender, recipient))}
            if sender not in known or recipient not in known:
                raise ValueError('sender and recipient must be registered')
            if reply_to is not None:
                parent = db.execute(
                    'SELECT sender,recipient,room_id FROM messages WHERE id=?', (reply_to,)).fetchone()
                if (parent is None or parent['sender'] != recipient or parent['recipient'] != sender
                        or parent['room_id'] is not None):
                    raise ValueError('reply_to must identify a direct message from recipient to sender')
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
            inserted = db.execute(
                'INSERT OR IGNORE INTO messages(id,sender,recipient,text,status,created,reply_to) '
                'VALUES (?,?,?,?,?,?,?)',
                (ident, sender, recipient, text, 'queued', time.time(), reply_to)).rowcount == 1
            row = db.execute(
                'SELECT sender,recipient,text,status,reply_to,room_id, '
                'EXISTS(SELECT 1 FROM group_batch_deliveries WHERE message_id=messages.id) AS is_group '
                'FROM messages WHERE id=?', (ident,)).fetchone()
            if (row['sender'], row['recipient'], row['text'], row['reply_to'], row['room_id'], row['is_group']) != (
                    sender, recipient, text, reply_to, None, 0):
                raise ValueError('message id already belongs to different content')
            result = {'id': ident, 'status': row['status']}
        if inserted:
            self._notify_messages()
        return result

    def _notify_messages(self):
        # Senders notify only after committing, so awakened readers can see the row.
        with self._message_condition:
            self._message_condition.notify_all()

    def enqueue_group(self, data, *, credential=None):
        """Atomically fan out one explicit, idempotent batch to individual inboxes."""
        return self._enqueue_fanout(
            data.get('sender'), data.get('recipients'), data.get('text'), data.get('batch_id'), credential=credential)

    def send_room_message(self, room_id, data, *, credential=None):
        return self._enqueue_fanout(
            data.get('sender'), None, data.get('text'), data.get('batch_id'), room_id=room_id, credential=credential)

    def reply_all(self, room_id, data, *, credential=None):
        reply_to = data.get('reply_to')
        if not self.valid_id(reply_to):
            raise ValueError('reply_to message id is required')
        return self._enqueue_fanout(
            data.get('sender'), None, data.get('text'), data.get('batch_id'),
            room_id=room_id, reply_to=reply_to, credential=credential)

    def _enqueue_fanout(self, sender, recipients, text, batch_id, room_id=None, reply_to=None, *, credential=None):
        """Share one transaction and idempotency ledger for explicit groups and rooms."""
        if not self.valid_id(sender):
            raise ValueError('invalid sender')
        if not isinstance(text, str) or not text.strip() or len(text) > 4000:
            raise ValueError('text must contain 1 to 4000 characters')
        if room_id is None:
            if not isinstance(recipients, list) or not 1 <= len(recipients) <= MAX_GROUP_RECIPIENTS:
                raise ValueError(f'recipients must contain 1 to {MAX_GROUP_RECIPIENTS} session IDs')
            if any(not self.valid_id(recipient) for recipient in recipients):
                raise ValueError('invalid recipient')
            if len(set(recipients)) != len(recipients):
                raise ValueError('recipients must be unique')
            if sender in recipients:
                raise ValueError('sender must not be a recipient')
            if reply_to is not None:
                raise ValueError('reply_to requires a room')
        else:
            if not self.valid_id(room_id):
                raise ValueError('invalid room id')
            if recipients is not None:
                raise ValueError('room recipients are determined by membership')
            if reply_to is not None and not self.valid_id(reply_to):
                raise ValueError('invalid reply_to message id')
        if not isinstance(batch_id, str):
            raise ValueError('batch_id must be a canonical UUID')
        try:
            parsed_batch_id = uuid.UUID(batch_id)
        except (ValueError, TypeError, AttributeError) as exc:
            raise ValueError('batch_id must be a canonical UUID') from exc
        if str(parsed_batch_id) != batch_id:
            raise ValueError('batch_id must be a canonical UUID')

        text_sha256 = hashlib.sha256(text.encode('utf-8')).hexdigest()

        # Serialize membership snapshots, first sends, and retries together.
        with closing(self.connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            self._authorize(db, sender, credential)
            batch = db.execute('SELECT sender,recipients_json,text_sha256,room_id,reply_to '
                               'FROM group_batches WHERE batch_id=?', (batch_id,)).fetchone()
            is_new = batch is None

            if room_id is not None:
                if not db.execute('SELECT 1 FROM rooms WHERE room_id=?', (room_id,)).fetchone():
                    raise ValueError('room not found')
                members = [row['member_id'] for row in db.execute(
                    'SELECT rm.member_id FROM room_members rm JOIN sessions s ON s.id=rm.member_id '
                    'WHERE rm.room_id=? AND s.opted_in=1 ORDER BY rm.member_id',
                    (room_id,))]
                if sender not in members:
                    raise ValueError('sender must be an opted-in room member')
                if reply_to is not None and not db.execute(
                        'SELECT 1 FROM messages m JOIN sessions s ON s.id=m.sender AND s.opted_in=1 '
                        'WHERE m.id=? AND m.room_id=?',
                        (reply_to, room_id)).fetchone():
                    raise ValueError('reply_to must identify a message in this room')

                if batch is not None:
                    # A retry uses the original recipient snapshot even if room
                    # membership has changed since its first committed send.
                    if (batch['sender'], batch['text_sha256'], batch['room_id'], batch['reply_to']) != (
                            sender, text_sha256, room_id, reply_to):
                        raise ValueError('batch_id already belongs to different room message content')
                    ordered_recipients = json.loads(batch['recipients_json'])
                else:
                    ordered_recipients = [member for member in members if member != sender]
                    if not ordered_recipients:
                        raise ValueError('room message requires at least one other member')
            else:
                ordered_recipients = sorted(recipients)

            all_ids = [sender, *ordered_recipients]
            all_placeholders = ','.join('?' for _ in all_ids)
            known = {row['id'] for row in db.execute(
                f'SELECT id FROM sessions WHERE opted_in=1 AND id IN ({all_placeholders})', all_ids)}
            if len(known) != len(set(all_ids)):
                raise ValueError('sender and all recipients must be opted in')

            # Existing batches are validated before creating any delivery rows.
            recipients_json = json.dumps(ordered_recipients, separators=(',', ':'))
            if batch is not None:
                expected = (sender, recipients_json, text_sha256, room_id, reply_to)
                actual = (batch['sender'], batch['recipients_json'], batch['text_sha256'],
                          batch['room_id'], batch['reply_to'])
                if actual != expected:
                    raise ValueError('batch_id already belongs to different content, recipients, or room')
            else:
                db.execute(
                    'INSERT INTO group_batches(batch_id,sender,recipients_json,text_sha256,created,room_id,reply_to) '
                    'VALUES (?,?,?,?,?,?,?)',
                    (batch_id, sender, recipients_json, text_sha256, time.time(), room_id, reply_to))

            for recipient in ordered_recipients:
                delivery = db.execute(
                    'SELECT message_id FROM group_batch_deliveries WHERE batch_id=? AND recipient=?',
                    (batch_id, recipient)).fetchone()
                if delivery is not None:
                    continue
                if not is_new:
                    raise ValueError('stored group batch is incomplete')

                message_id = str(uuid.uuid4())
                db.execute('INSERT INTO messages(id,sender,recipient,text,status,created,room_id,reply_to) '
                           'VALUES (?,?,?,?,?,?,?,?)',
                           (message_id, sender, recipient, text, 'queued', time.time(), room_id, reply_to))
                db.execute('INSERT INTO group_batch_deliveries(batch_id,recipient,message_id) '
                           'VALUES (?,?,?)', (batch_id, recipient, message_id))

            rows = db.execute(
                'SELECT m.id,m.recipient,m.status FROM group_batch_deliveries d '
                'JOIN messages m ON m.id=d.message_id WHERE d.batch_id=? ORDER BY m.recipient',
                (batch_id,)).fetchall()
            if len(rows) != len(ordered_recipients):
                raise ValueError('stored group batch is incomplete')
            result = {
                'batch_id': batch_id,
                'deliveries': [
                    {'id': row['id'], 'recipient': row['recipient'], 'status': row['status']}
                    for row in rows
                ],
            }
            if room_id is not None:
                result.update({'room_id': room_id, 'reply_to': reply_to})
        if is_new:
            self._notify_messages()
        return result

    def inbox(self, recipient, after=0, status=None, *, credential=None):
        if not self.valid_id(recipient):
            raise ValueError('invalid recipient')
        if isinstance(after, bool) or not isinstance(after, int) or after < 0:
            raise ValueError('invalid page cursor')
        if status is not None and status != 'queued':
            raise ValueError("status filter must be 'queued'")
        with closing(self.connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            self._authorize(db, recipient, credential)
            if not db.execute('SELECT 1 FROM sessions WHERE id=? AND opted_in=1', (recipient,)).fetchone():
                raise ValueError('recipient has not opted in')
            query = ('SELECT m.seq,m.id,m.sender,m.recipient,m.text,m.status,m.created,m.acknowledged,'
                     'm.room_id,m.reply_to FROM messages m '
                     'JOIN sessions sender_session ON sender_session.id=m.sender '
                     'AND sender_session.opted_in=1 WHERE m.recipient=? AND m.seq>?')
            params = [recipient, after]
            if status is not None:
                query += ' AND m.status=?'
                params.append(status)
            query += ' ORDER BY m.seq LIMIT 1000'
            rows = [dict(row) for row in db.execute(
                query, params)]
            return {'messages': rows, 'next_after': rows[-1]['seq'] if len(rows) == 1000 else None}

    def _queued_after(self, recipient, after, limit, *, credential=None):
        if not self.valid_id(recipient):
            raise ValueError('invalid recipient')
        if isinstance(after, bool) or not isinstance(after, int) or after < 0:
            raise ValueError('invalid page cursor')
        with closing(self.connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            self._authorize(db, recipient, credential)
            if not db.execute('SELECT 1 FROM sessions WHERE id=? AND opted_in=1', (recipient,)).fetchone():
                raise ValueError('recipient has not opted in')
            return [dict(row) for row in db.execute(
                'SELECT m.seq,m.id,m.sender,m.recipient,m.text,m.status,m.created,m.acknowledged,'
                'm.room_id,m.reply_to FROM messages m '
                'JOIN sessions sender_session ON sender_session.id=m.sender '
                'AND sender_session.opted_in=1 WHERE m.recipient=? AND m.status=? AND m.seq>? '
                'ORDER BY m.seq LIMIT ?', (recipient, 'queued', after, limit))]

    def wait_for_messages(self, recipient, after, timeout=DEFAULT_WAIT_SECONDS,
                          limit=MAX_WAIT_MESSAGES, *, credential=None):
        """Block for queued messages newer than an explicit sequence cursor.

        A timeout or a missed in-process notification still triggers a final
        database check. Message rows remain in the inbox for reconnect/history.
        """
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
            raise ValueError('invalid wait timeout')
        try:
            finite_timeout = math.isfinite(timeout)
        except OverflowError:
            finite_timeout = False
        if not finite_timeout:
            raise ValueError('invalid wait timeout')
        if not 0 <= timeout <= MAX_WAIT_SECONDS:
            raise ValueError(f'wait timeout must be between 0 and {MAX_WAIT_SECONDS} seconds')
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= MAX_WAIT_MESSAGES:
            raise ValueError(f'page limit must be between 1 and {MAX_WAIT_MESSAGES}')

        deadline = time.monotonic() + timeout
        with self._message_condition:
            while True:
                # Preserve the legacy internal query call shape for existing
                # local observers; protected polls always carry their credential.
                rows = (self._queued_after(recipient, after, limit) if credential is None else
                        self._queued_after(recipient, after, limit, credential=credential))
                if rows:
                    return {'messages': rows, 'cursor': rows[-1]['seq']}
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return {'messages': [], 'cursor': after}
                # Condition.wait atomically releases the same lock senders need
                # to notify, preventing a commit between the check and wait
                # from becoming a lost wakeup.
                self._message_condition.wait(remaining)

    def ack(self, data, *, credential=None):
        ident, recipient = data.get('id'), data.get('recipient')
        if not isinstance(ident, str) or not self.valid_id(recipient):
            raise ValueError('invalid message or recipient')
        with closing(self.connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            self._authorize(db, recipient, credential)
            row = db.execute(
                'SELECT m.status FROM messages m '
                'JOIN sessions recipient_session ON recipient_session.id=m.recipient '
                'AND recipient_session.opted_in=1 '
                'JOIN sessions sender_session ON sender_session.id=m.sender AND sender_session.opted_in=1 '
                'WHERE m.id=? AND m.recipient=?',
                             (ident, recipient)).fetchone()
            if not row:
                raise ValueError('message not found for opted-in recipient')
            if row['status'] == 'queued':
                db.execute('UPDATE messages SET status=?, acknowledged=? WHERE id=? AND recipient=?',
                           ('acknowledged', time.time(), ident, recipient))
        return {'id': ident, 'status': 'acknowledged'}

    def message_status(self, recipient, ident, *, actor=None, credential=None):
        actor = recipient if actor is None else actor
        if not all(self.valid_id(value) for value in (recipient, ident, actor)):
            raise ValueError('invalid message, recipient, or actor')
        with closing(self.connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            self._authorize(db, actor, credential)
            row = db.execute(
                'SELECT m.id,m.sender,m.recipient,m.status,m.acknowledged,m.room_id FROM messages m '
                'JOIN sessions r ON r.id=m.recipient AND r.opted_in=1 '
                'JOIN sessions s ON s.id=m.sender AND s.opted_in=1 '
                'WHERE m.id=? AND m.recipient=?', (ident, recipient)).fetchone()
            if row is None:
                raise ValueError('message not found for opted-in recipient')
            if actor not in (row['sender'], row['recipient']):
                raise PermissionError('unauthorized')
            result = {key: row[key] for key in ('id', 'sender', 'recipient', 'status', 'acknowledged')}
            reply = None
            if row['room_id'] is None:
                reply = db.execute(
                    'SELECT id FROM messages WHERE reply_to=? AND sender=? AND recipient=? '
                    'AND room_id IS NULL ORDER BY seq LIMIT 1',
                    (row['id'], row['recipient'], row['sender'])).fetchone()
            else:
                reply = db.execute(
                    'SELECT id FROM messages WHERE reply_to=? AND sender=? AND recipient=? '
                    'AND room_id=? ORDER BY seq LIMIT 1',
                    (row['id'], row['recipient'], row['sender'], row['room_id'])).fetchone()
            result['linked_reply_status'] = 'recorded' if reply is not None else 'not recorded'
            if reply is not None:
                result['reply_message_id'] = reply['id']
            return result


class DrainingHTTPServer(ThreadingHTTPServer):
    """Track connections before thread launch, including incomplete HTTP reads.

    begin_drain() atomically closes application admission. wait_for_drain(seconds)
    returns False on deadline, never claims stuck handlers are safe. Call shutdown()
    from outside serve_forever's thread, then server_close(). Non-daemon handlers
    deliberately keep the process alive if application work persists. Closing a
    socket never cancels a SQLite transaction or rolls back a committed write.
    """
    daemon_threads = False
    block_on_close = False  # Our explicit bounded drain replaces unbounded join.

    def __init__(self, address, handler, *, request_timeout=5):
        if not math.isfinite(request_timeout) or not 0 < request_timeout <= 300:
            raise ValueError('request_timeout must be in (0, 300]')
        self.request_timeout = request_timeout
        self._drain_condition = threading.Condition()
        self._connections = {}
        self._header_deadlines = {}
        self.draining = False
        super().__init__(address, handler)

    def process_request(self, request, client_address):
        request.settimeout(self.request_timeout)
        with self._drain_condition:
            self._connections[request] = False
            if self.draining:
                # Give complete requests a brief chance to receive HTTP 503,
                # but bound incomplete headers even if the client trickles data.
                timer = threading.Timer(1.0, self._expire_draining_header, (request,))
                timer.daemon = True
                self._header_deadlines[request] = timer
                timer.start()
        try:
            super().process_request(request, client_address)
        except BaseException:
            with self._drain_condition:
                self._connections.pop(request, None)
                timer = self._header_deadlines.pop(request, None)
                if timer is not None:
                    timer.cancel()
                self._drain_condition.notify_all()
            raise

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            with self._drain_condition:
                self._connections.pop(request, None)
                timer = self._header_deadlines.pop(request, None)
                if timer is not None:
                    timer.cancel()
                self._drain_condition.notify_all()

    def _expire_draining_header(self, request):
        with self._drain_condition:
            if request in self._connections and not self._connections[request]:
                try:
                    request.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass

    def admit(self, request):
        with self._drain_condition:
            if self.draining:
                return False
            self._connections[request] = True
            return True

    def begin_drain(self):
        with self._drain_condition:
            self.draining = True
            # Interrupt connections that have not completed HTTP headers. They
            # have not entered application code and cannot have durable effects.
            for request, admitted in self._connections.items():
                if not admitted:
                    try:
                        request.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
            self._drain_condition.notify_all()

    @property
    def active_handlers(self):
        with self._drain_condition:
            return len(self._connections)

    def wait_for_drain(self, timeout):
        if not math.isfinite(timeout) or not 0 < timeout <= 300:
            raise ValueError('drain timeout must be in (0, 300]')
        deadline = time.monotonic() + timeout
        with self._drain_condition:
            if not self.draining:
                raise RuntimeError('begin_drain must be called first')
            while self._connections:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    # Release blocked body reads/writes, but do not interrupt
                    # application/SQLite code or pretend its handler has exited.
                    for request in self._connections:
                        try:
                            request.shutdown(socket.SHUT_RDWR)
                        except OSError:
                            pass
                    return False
                self._drain_condition.wait(remaining)
            return True


def handler_for(broker, token):
    shared_digest = hashlib.sha256(token.encode('utf-8')).hexdigest()
    with closing(broker.connect()) as db, db:
        db.execute('BEGIN IMMEDIATE')
        if db.execute('SELECT 1 FROM session_credentials WHERE token_sha256=?',
                      (shared_digest,)).fetchone():
            raise ValueError('shared token conflicts with a session credential')
        broker._shared_token_hashes.add(shared_digest)
    class Handler(BaseHTTPRequestHandler):
        def handle_one_request(self):
            # Parse headers before rejection: closing a socket with unread HTTP
            # headers can reset the connection on Windows and hide the 503.
            # Admission still precedes authorization and reading any body.
            self.close_connection = True
            try:
                super().handle_one_request()
            except OSError:
                # Socket inactivity deadlines and drain interruption are normal
                # disconnects. The server still tracks us through final cleanup.
                self.close_connection = True

        def parse_request(self):
            if not super().parse_request():
                return False
            admit = getattr(self.server, 'admit', None)
            if admit is not None and not admit(self.connection):
                self.response(503, {'error': 'broker draining'})
                self.close_connection = True
                return False
            return True

        def log_message(self, *args):
            pass

        def response(self, status, value):
            raw = json.dumps(value, ensure_ascii=False).encode('utf-8')
            try:
                self.send_response(status)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Content-Length', str(len(raw)))
                self.send_header('Cache-Control', 'no-store')
                self.end_headers()
                self.wfile.write(raw)
            except OSError:
                # A long-poll client may disconnect while its handler is blocked.
                pass

        def authorized(self):
            headers = self.headers.get_all('Authorization', [])
            header = headers[0] if len(headers) == 1 else ''
            self.credential = None
            # Shared bearer always means legacy authority, even if a separate
            # admin process accidentally provisions that same secret as scoped.
            if hmac.compare_digest(header.encode('utf-8'), ('Bearer ' + token).encode('utf-8')):
                return True
            bearer = header[7:] if header.startswith('Bearer ') else ''
            if broker.credential_is_valid(bearer):
                self.credential = bearer
                return True
            self.response(403, {'error': 'unauthorized'})
            return False

        def do_GET(self):
            if not self.authorized():
                return
            try:
                path = urlsplit(self.path)
                if path.path == '/v1/sessions':
                    return self.response(200, broker.sessions())
                if path.path == '/v1/sessions/check':
                    query = parse_qs(path.query, keep_blank_values=True)
                    if len(query.get('id', [])) != 1:
                        raise ValueError('one session id is required')
                    return self.response(200, broker.check_session(
                        query['id'][0], credential=self.credential))
                if path.path == '/v1/messages':
                    query = parse_qs(path.query)
                    recipient = query.get('recipient', [''])[0]
                    after = int(query.get('after', ['0'])[0])
                    status = query.get('status', [None])[0]
                    return self.response(200, broker.inbox(recipient, after, status, credential=self.credential))
                if path.path == '/v1/messages/status':
                    query = parse_qs(path.query, keep_blank_values=True)
                    if any(len(query.get(key, [])) > 1 for key in ('recipient', 'id', 'actor')):
                        raise ValueError('duplicate status parameter')
                    return self.response(200, broker.message_status(
                        query.get('recipient', [''])[0], query.get('id', [''])[0],
                        actor=query.get('actor', [None])[0], credential=self.credential))
                if path.path == '/v1/messages/wait':
                    query = parse_qs(path.query)
                    if 'after' not in query:
                        raise ValueError('after cursor is required')
                    recipient = query.get('recipient', [''])[0]
                    after = int(query['after'][0])
                    timeout = float(query.get('timeout', [str(DEFAULT_WAIT_SECONDS)])[0])
                    limit = int(query.get('limit', [str(MAX_WAIT_MESSAGES)])[0])
                    return self.response(200, broker.wait_for_messages(
                        recipient, after, timeout, limit, credential=self.credential))
                parts = path.path.strip('/').split('/')
                if len(parts) == 3 and parts[:2] == ['v1', 'rooms']:
                    query = parse_qs(path.query)
                    if 'member' not in query:
                        raise ValueError('member query parameter is required')
                    return self.response(200, broker.room_info(unquote(parts[2]), query['member'][0], credential=self.credential))
                self.response(404, {'error': 'not found'})
            except PermissionError:
                self.response(403, {'error': 'unauthorized'})
            except (ValueError, UnicodeError):
                self.response(400, {'error': 'invalid request'})

        def do_POST(self):
            if not self.authorized():
                return
            try:
                length = int(self.headers.get('Content-Length', '0'))
                if not 0 < length <= 20000:
                    raise ValueError('invalid request size')
                body = self.rfile.read(length)
                if len(body) != length:
                    raise ValueError('incomplete request body')
                data = json.loads(body)
                if not isinstance(data, dict):
                    raise ValueError('expected a JSON object')
                path = urlsplit(self.path).path
                if path == '/v1/rooms':
                    return self.response(200, broker.create_room(data, credential=self.credential))
                parts = path.strip('/').split('/')
                if len(parts) == 4 and parts[:2] == ['v1', 'rooms']:
                    room_id, action = unquote(parts[2]), parts[3]
                    if action == 'members':
                        return self.response(200, broker.update_room_member(room_id, data, credential=self.credential))
                    if action == 'messages':
                        return self.response(200, broker.send_room_message(room_id, data, credential=self.credential))
                    if action == 'reply-all':
                        return self.response(200, broker.reply_all(room_id, data, credential=self.credential))
                routes = {'/v1/register': broker.register, '/v1/messages': broker.enqueue,
                          '/v1/group-messages': broker.enqueue_group, '/v1/ack': broker.ack}
                fn = routes.get(path)
                if fn is None:
                    return self.response(404, {'error': 'not found'})
                self.response(200, fn(data, credential=self.credential))
            except PermissionError:
                self.response(403, {'error': 'unauthorized'})
            except (ValueError, UnicodeError):
                self.response(400, {'error': 'invalid request'})

    return Handler


def _prepare_server(args):
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
    server = DrainingHTTPServer(('127.0.0.1', port), handler_for(broker, token),
                                request_timeout=args.request_timeout)
    if args.recover:
        # A successful bind on the original port prevents two recoveries from
        # both claiming the same endpoint. Atomically replace only the PID.
        temp = args.state.with_name(args.state.name + '.' + str(os.getpid()) + '.tmp')
        try:
            fd = os.open(str(temp), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                json.dump({'port': server.server_port, 'token': token, 'pid': os.getpid()}, f)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp, args.state)
            _sync_directory(args.state.parent)
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
    if not args.recover:
        with os.fdopen(fd, 'w', encoding='utf-8') as stream:
            json.dump({'port': server.server_port, 'token': token, 'pid': os.getpid()}, stream)
            stream.flush()
            os.fsync(stream.fileno())
    return server, args.state.read_bytes()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--database', type=Path, default=ROOT / 'broker.sqlite3')
    parser.add_argument('--state', type=Path, default=ROOT / 'broker-session.json')
    parser.add_argument('--recover', action='store_true',
                        help='reuse stale endpoint credentials after checking the old process is gone')
    parser.add_argument('--allow-legacy-migration', action='store_true',
                        help='explicitly migrate a legacy database; take a backup first')
    parser.add_argument('--control-stdin', action='store_true',
                        help='accept stop on a private stdin pipe owned by the launcher')
    parser.add_argument('--drain-timeout', type=float, default=35,
                        help='maximum handler drain wait in seconds (0 < value <= 300)')
    parser.add_argument('--request-timeout', type=float, default=5,
                        help='HTTP socket inactivity timeout in seconds (0 < value <= 300)')
    args = parser.parse_args()
    for name in ('drain_timeout', 'request_timeout'):
        if not math.isfinite(getattr(args, name)) or not 0 < getattr(args, name) <= 300:
            parser.error(name.replace('_', '-') + ' must be in (0, 300]')
    if args.recover and args.allow_legacy_migration:
        raise SystemExit('Recovery cannot perform a legacy migration.')
    if not args.allow_legacy_migration:
        try:
            require_current_schema_or_empty(args.database, require_existing=args.recover)
        except (ValueError, OSError, sqlite3.Error) as exc:
            raise SystemExit('Broker refused database: missing current schema or unreadable. '
                             'Use --allow-legacy-migration only for a planned migration.') from exc
    try:
        with recovery_gate(args.state):
            require_recovery_allowed(args.state)
            server, owned_state = _prepare_server(args)
    except (OSError, RuntimeError):
        raise SystemExit('Broker startup inhibited or coordination unavailable; operator review required.') from None
    drain_result = []
    owned_marker = []
    fence_mutex = threading.Lock()
    drain_mutex = threading.Lock()
    stop_finalized = threading.Event()
    control_thread = None
    stop_serving = threading.Event()
    server.timeout = 0.1

    def fence_stop():
        with fence_mutex:
            if not owned_marker:
                owned_marker.append(publish_stop_inhibit(args.state, owned_state))

    try:
        if args.control_stdin:
            def control():
                try:
                    for line in sys.stdin:
                        if line.strip() == 'stop':
                            break
                finally:
                    try:
                        fence_stop()  # Must persist before the first begin_drain.
                    except (OSError, RuntimeError):
                        print('Stop inhibit could not be persisted; stop refused. '
                              'Broker remains running; operator review required.',
                              file=sys.stderr, flush=True)
                        return
                    with drain_mutex:
                        if stop_finalized.is_set():
                            return
                        try:
                            server.begin_drain()
                            drain_result.append(server.wait_for_drain(args.drain_timeout))
                        finally:
                            # Safe even before the request loop starts.
                            stop_serving.set()
            control_thread = threading.Thread(target=control, daemon=True)
            control_thread.start()
        print('Agent bridge broker listening on localhost.', flush=True)
        while not stop_serving.is_set():
            server.handle_request()
    finally:
        try:
            fence_stop()
        except (OSError, RuntimeError):
            server.server_close()
            raise SystemExit('ROLLOUT BLOCKER: stop inhibit unavailable; state retained '
                             'but process may exit unfenced and supervisor may auto-recover. '
                             'Stop is not proven; do not deploy this recovery protocol.') from None
        # Serialize results and cleanup with a controller already draining;
        # never join a thread that might still be blocked reading stdin.
        with drain_mutex:
            try:
                server.begin_drain()
                # A controller deadline is terminal, even if its handlers exit
                # just afterwards. Do not spend a second full timeout or promote
                # that failed stop into success during finalization.
                if drain_result and not all(drain_result):
                    drained = False
                else:
                    drained = server.wait_for_drain(args.drain_timeout)
                server.server_close()
                if drained and all(drain_result) and server.active_handlers == 0:
                    try:
                        complete_stop(args.state, owned_state, owned_marker[0])
                    except (OSError, RuntimeError):
                        raise SystemExit('Stop cleanup uncertain; operator recovery required.') from None
                else:
                    print('Broker drain failed; discovery state and stop inhibit retained. '
                          'Persistent non-daemon handlers may keep this process alive; '
                          'do not restart or replace its database.', file=sys.stderr, flush=True)
                    raise SystemExit(2)
            finally:
                stop_finalized.set()


if __name__ == '__main__':
    main()
