"""Rehearse SQLite API backup/restore of the complete upgraded broker schema.

All rows and databases are synthetic and live under TemporaryDirectory. The
test keeps a WAL connection open while later commits are backed up, so it checks
the backup API's view of committed WAL pages as well as the main database file.
"""

from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
import uuid

from broker import Broker


def sqlite_backup(source_path, destination_path):
    """Copy a database through SQLite's online backup API, including its WAL."""
    with closing(sqlite3.connect(source_path)) as source, \
            closing(sqlite3.connect(destination_path)) as destination:
        source.backup(destination)


def query_one(database_path, sql, parameters=()):
    with closing(sqlite3.connect(database_path)) as db:
        return db.execute(sql, parameters).fetchone()


def write_wal_marker(connection, session_id):
    """Commit a recognizable synthetic update without checkpointing the WAL."""
    cursor = connection.execute(
        "UPDATE sessions SET updated=updated + 1 WHERE id=?", (session_id,)
    )
    if cursor.rowcount != 1:
        raise AssertionError("synthetic WAL marker session is missing")
    connection.commit()
    return connection.execute(
        "SELECT updated FROM sessions WHERE id=?", (session_id,)
    ).fetchone()[0]


class SQLiteBackupRestoreRehearsalTests(unittest.TestCase):
    def test_restores_full_upgraded_state_and_committed_wal_changes(self):
        with tempfile.TemporaryDirectory(prefix="bridge-sqlite-backup-rehearsal-") as folder:
            root = Path(folder)
            active_path = root / "synthetic-active.sqlite3"
            earlier_snapshot = root / "synthetic-earlier-snapshot.sqlite3"
            latest_snapshot = root / "synthetic-latest-snapshot.sqlite3"
            restored_earlier = root / "synthetic-restored-earlier.sqlite3"
            restored_latest = root / "synthetic-restored-latest.sqlite3"

            broker = Broker(active_path)
            # Keep one connection open and disable its automatic checkpoint;
            # marker updates through this connection guarantee WAL pages at
            # both backup points.
            wal_keeper = sqlite3.connect(active_path)
            try:
                self.assertEqual("wal", wal_keeper.execute(
                    "PRAGMA journal_mode=WAL").fetchone()[0].lower())
                wal_keeper.execute("PRAGMA wal_autocheckpoint=0")

                alpha_credential = "SYNTHETIC_ALPHA_CREDENTIAL_" + "a" * 48
                broker.provision_session_credential("alpha", alpha_credential, replace=False)
                broker.register(
                    {"id": "alpha", "kind": "claude", "opt_in": True},
                    credential=alpha_credential,
                )
                broker.register({"id": "beta", "kind": "codex", "opt_in": True})
                with closing(broker.connect()) as db, db:
                    db.execute(
                        "INSERT INTO sessions(id, kind, updated, opted_in) VALUES (?, ?, ?, 0)",
                        ("synthetic-not-opted-in", "codex", 1.0),
                    )

                baseline_message_id = str(uuid.uuid4())
                broker.enqueue(
                    {
                        "id": baseline_message_id,
                        "sender": "alpha",
                        "recipient": "beta",
                        "text": "synthetic message present in earlier snapshot",
                    },
                    credential=alpha_credential,
                )

                earlier_wal_value = write_wal_marker(wal_keeper, "alpha")
                wal_path = Path(str(active_path) + "-wal")
                self.assertTrue(wal_path.is_file(), "the earlier snapshot should include WAL pages")
                self.assertGreater(wal_path.stat().st_size, 32, "WAL should contain committed pages")

                # This first snapshot is the recovery point before later accepted
                # messages, acknowledgments, opt-ins, and room/group metadata.
                sqlite_backup(active_path, earlier_snapshot)

                gamma_credential = "SYNTHETIC_GAMMA_CREDENTIAL_" + "c" * 48
                broker.provision_session_credential("gamma", gamma_credential, replace=False)
                broker.register(
                    {"id": "gamma", "kind": "codex", "opt_in": True},
                    credential=gamma_credential,
                )

                after_snapshot_message_id = str(uuid.uuid4())
                broker.enqueue(
                    {
                        "id": after_snapshot_message_id,
                        "sender": "alpha",
                        "recipient": "gamma",
                        "text": "synthetic message accepted after earlier snapshot",
                    },
                    credential=alpha_credential,
                )

                group_batch_id = str(uuid.uuid4())
                group_result = broker.enqueue_group(
                    {
                        "sender": "alpha",
                        "recipients": ["beta", "gamma"],
                        "text": "synthetic group batch",
                        "batch_id": group_batch_id,
                    },
                    credential=alpha_credential,
                )

                room_id = "synthetic-room"
                broker.create_room(
                    {
                        "room_id": room_id,
                        "name": "Synthetic rehearsal room",
                        "owner": "alpha",
                        "members": ["alpha", "beta", "gamma"],
                    },
                    credential=alpha_credential,
                )
                room_batch_id = str(uuid.uuid4())
                room_result = broker.send_room_message(
                    room_id,
                    {
                        "sender": "alpha",
                        "text": "synthetic room batch",
                        "batch_id": room_batch_id,
                    },
                    credential=alpha_credential,
                )

                # Acknowledge one pre-snapshot and one post-snapshot message to
                # prove the latest copy contains changed status and timestamps.
                broker.ack({"id": baseline_message_id, "recipient": "beta"})
                broker.ack(
                    {"id": after_snapshot_message_id, "recipient": "gamma"},
                    credential=gamma_credential,
                )

                latest_wal_value = write_wal_marker(wal_keeper, "alpha")
                self.assertTrue(wal_path.is_file(), "committed changes should remain in WAL")
                self.assertGreater(wal_path.stat().st_size, 32, "WAL should contain committed pages")
                self.assertEqual("wal", wal_keeper.execute(
                    "PRAGMA journal_mode").fetchone()[0].lower())

                # Snapshot through sqlite3.Connection.backup while the WAL is
                # present, then restore both recovery points through the same API.
                sqlite_backup(active_path, latest_snapshot)
                sqlite_backup(earlier_snapshot, restored_earlier)
                sqlite_backup(latest_snapshot, restored_latest)
            finally:
                wal_keeper.close()

            self.assertEqual(query_one(active_path, 'SELECT COUNT(*) FROM messages'),
                             query_one(restored_latest, 'SELECT COUNT(*) FROM messages'))
            restored = Broker(restored_latest)
            self.assertEqual('alpha', restored.check_session('alpha', credential=alpha_credential)['id'])
            self.assertEqual('alpha', Broker(restored_earlier).check_session('alpha', credential=alpha_credential)['id'])
            with self.assertRaises(PermissionError):
                restored.check_session('alpha', credential=gamma_credential)
            with self.assertRaises(ValueError):
                restored.check_session('synthetic-not-opted-in')
            self.assertEqual(['alpha', 'beta', 'gamma'],
                             restored.room_info(room_id, 'alpha', credential=alpha_credential)['members'])

            # The earlier recovery point remains a point-in-time copy.
            self.assertEqual(
                ("queued", None),
                query_one(
                    restored_earlier,
                    "SELECT status, acknowledged FROM messages WHERE id=?",
                    (baseline_message_id,),
                ),
            )
            self.assertIsNone(query_one(
                restored_earlier,
                "SELECT id FROM messages WHERE id=?",
                (after_snapshot_message_id,),
            ))
            self.assertIsNone(query_one(
                restored_earlier,
                "SELECT id FROM sessions WHERE id='gamma'",
            ))
            self.assertEqual(0, query_one(restored_earlier, "SELECT COUNT(*) FROM rooms")[0])
            self.assertEqual(0, query_one(restored_earlier, "SELECT COUNT(*) FROM group_batches")[0])
            self.assertEqual(
                earlier_wal_value,
                query_one(restored_earlier, "SELECT updated FROM sessions WHERE id='alpha'")[0],
            )
            self.assertEqual(
                0,
                query_one(
                    restored_earlier,
                    "SELECT opted_in FROM sessions WHERE id='synthetic-not-opted-in'",
                )[0],
            )

            # The latest restored copy contains every upgraded schema area.
            with closing(sqlite3.connect(restored_latest)) as db:
                db.row_factory = sqlite3.Row
                self.assertEqual('ok', db.execute('PRAGMA integrity_check').fetchone()[0])
                self.assertEqual([], list(db.execute('PRAGMA foreign_key_check')))
                table_names = {row[0] for row in db.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'")}
                self.assertTrue(
                    {
                        "sessions",
                        "session_credentials",
                        "messages",
                        "group_batches",
                        "group_batch_deliveries",
                        "rooms",
                        "room_members",
                    }.issubset(table_names)
                )

                expected_columns = {
                    "sessions": {"id", "kind", "updated", "opted_in"},
                    "session_credentials": {"session_id", "token_sha256"},
                    "messages": {
                        "seq", "id", "sender", "recipient", "text", "status",
                        "created", "acknowledged", "room_id", "reply_to",
                    },
                    "group_batches": {
                        "batch_id", "sender", "recipients_json", "text_sha256",
                        "created", "room_id", "reply_to",
                    },
                    "group_batch_deliveries": {"batch_id", "recipient", "message_id"},
                    "rooms": {"room_id", "name", "owner", "created"},
                    "room_members": {"room_id", "member_id"},
                }
                for table, expected in expected_columns.items():
                    actual = {row[1] for row in db.execute(f"PRAGMA table_info({table})")}
                    self.assertTrue(expected.issubset(actual), f"missing columns in {table}")
                index_names = {row[0] for row in db.execute(
                    "SELECT name FROM sqlite_master WHERE type='index'")}
                self.assertIn("recipient_queue", index_names)

                # Credential rows contain SHA-256 digests only, for both profiles.
                credential_rows = dict(db.execute(
                    "SELECT session_id, token_sha256 FROM session_credentials"))
                expected_hashes = {
                    "alpha": hashlib.sha256(alpha_credential.encode("ascii")).hexdigest(),
                    "gamma": hashlib.sha256(gamma_credential.encode("ascii")).hexdigest(),
                }
                self.assertEqual(expected_hashes, credential_rows)
                for token in (alpha_credential, gamma_credential):
                    self.assertNotIn(token, credential_rows.values())

                # Opt-in state and the new profile's credential binding survive.
                self.assertEqual(
                    {
                        "alpha": 1,
                        "beta": 1,
                        "gamma": 1,
                        "synthetic-not-opted-in": 0,
                    },
                    dict(db.execute("SELECT id, opted_in FROM sessions")),
                )
                self.assertEqual(
                    latest_wal_value,
                    db.execute("SELECT updated FROM sessions WHERE id='alpha'").fetchone()[0],
                )

                # Messages accepted after the earlier snapshot and their ack
                # changes are in the latest backup.
                baseline = db.execute(
                    "SELECT status, acknowledged FROM messages WHERE id=?",
                    (baseline_message_id,),
                ).fetchone()
                after_snapshot = db.execute(
                    "SELECT status, acknowledged FROM messages WHERE id=?",
                    (after_snapshot_message_id,),
                ).fetchone()
                self.assertEqual("acknowledged", baseline["status"])
                self.assertIsNotNone(baseline["acknowledged"])
                self.assertEqual("acknowledged", after_snapshot["status"])
                self.assertIsNotNone(after_snapshot["acknowledged"])

                group = db.execute(
                    "SELECT sender, recipients_json, room_id, reply_to "
                    "FROM group_batches WHERE batch_id=?",
                    (group_batch_id,),
                ).fetchone()
                self.assertEqual("alpha", group["sender"])
                self.assertEqual(["beta", "gamma"], json.loads(group["recipients_json"]))
                self.assertIsNone(group["room_id"])
                group_deliveries = list(db.execute(
                    "SELECT recipient, message_id FROM group_batch_deliveries "
                    "WHERE batch_id=? ORDER BY recipient",
                    (group_batch_id,),
                ))
                self.assertEqual(["beta", "gamma"], [row["recipient"] for row in group_deliveries])
                self.assertEqual(
                    sorted(item["id"] for item in group_result["deliveries"]),
                    sorted(row["message_id"] for row in group_deliveries),
                )

                room = db.execute(
                    "SELECT name, owner FROM rooms WHERE room_id=?", (room_id,)
                ).fetchone()
                self.assertEqual(("Synthetic rehearsal room", "alpha"), tuple(room))
                self.assertEqual(
                    ["alpha", "beta", "gamma"],
                    [row[0] for row in db.execute(
                        "SELECT member_id FROM room_members WHERE room_id=? ORDER BY member_id",
                        (room_id,),
                    )],
                )
                room_batch = db.execute(
                    "SELECT sender, recipients_json, room_id, reply_to "
                    "FROM group_batches WHERE batch_id=?",
                    (room_batch_id,),
                ).fetchone()
                self.assertEqual("alpha", room_batch["sender"])
                self.assertEqual(room_id, room_batch["room_id"])
                self.assertEqual(["beta", "gamma"], json.loads(room_batch["recipients_json"]))
                room_messages = list(db.execute(
                    "SELECT recipient, room_id FROM messages WHERE id IN (%s) ORDER BY recipient"
                    % ",".join("?" for _ in room_result["deliveries"]),
                    [item["id"] for item in room_result["deliveries"]],
                ))
                self.assertEqual(["beta", "gamma"], [row["recipient"] for row in room_messages])
                self.assertEqual([room_id, room_id], [row["room_id"] for row in room_messages])


if __name__ == "__main__":
    unittest.main()
