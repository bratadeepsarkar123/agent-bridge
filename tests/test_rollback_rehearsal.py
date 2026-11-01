"""Isolated activation and rollback rehearsal for the opt-in broker schema.

The legacy implementation is loaded from ``git show f57d010^:broker.py``.
Every database in this test lives in a TemporaryDirectory; no configured broker
database, listener, or agent session is read or contacted.
"""

from contextlib import closing
from pathlib import Path
import sqlite3
import subprocess
import tempfile
import types
import unittest

from broker import Broker as OptInBroker


REPO_ROOT = Path(__file__).resolve().parents[1]
LEGACY_SOURCE = "f57d010^:broker.py"
PRESERVED_MESSAGE_FIELDS = (
    "seq", "id", "sender", "recipient", "text", "status", "created", "acknowledged",
)


def core_message_snapshot(messages):
    return [tuple(message[field] for field in PRESERVED_MESSAGE_FIELDS) for message in messages]


def load_pre_opt_in_broker():
    source = subprocess.run(
        ["git", "show", LEGACY_SOURCE],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    module = types.ModuleType("legacy_pre_opt_in_broker")
    module.__file__ = str(REPO_ROOT / "broker.py")
    exec(compile(source, LEGACY_SOURCE, "exec"), module.__dict__)
    return module.Broker


def load_routing_broker():
    source = subprocess.run(
        ["git", "show", "f57d010:broker.py"], cwd=REPO_ROOT,
        check=True, capture_output=True, text=True,
    ).stdout
    module = types.ModuleType("earlier_routing_broker")
    module.__file__ = str(REPO_ROOT / "broker.py")
    exec(compile(source, "f57d010:broker.py", "exec"), module.__dict__)
    return module.Broker


class RollbackRehearsalTests(unittest.TestCase):
    def test_opt_in_routing_fallback_uses_current_database_copy(self):
        """A fresh backup retains recent direct messages and opt-in gating.

        This is not a production rollback: the earlier broker lacks scoped
        credentials and room semantics, which require separate safeguards.
        """
        with tempfile.TemporaryDirectory(prefix="bridge-routing-fallback-") as folder:
            root = Path(folder)
            active_db = root / "active.sqlite3"
            active = OptInBroker(active_db)
            for ident, kind in (("sender", "claude"), ("recipient", "codex")):
                active.register({"id": ident, "kind": kind, "opt_in": True})
            active.enqueue({"sender": "sender", "recipient": "recipient", "text": "before snapshot"})
            old_snapshot = root / "old-snapshot.sqlite3"
            with closing(sqlite3.connect(active_db)) as source, closing(sqlite3.connect(old_snapshot)) as dest:
                source.backup(dest)
            active.enqueue({"sender": "sender", "recipient": "recipient", "text": "after snapshot"})
            expected = core_message_snapshot(active.inbox("recipient")["messages"])
            current_snapshot = root / "current-snapshot.sqlite3"
            with closing(sqlite3.connect(active_db)) as source, closing(sqlite3.connect(current_snapshot)) as dest:
                source.backup(dest)

            fallback = load_routing_broker()(current_snapshot)
            self.assertEqual(expected, core_message_snapshot(fallback.inbox("recipient")["messages"]))
            with self.assertRaises(ValueError):
                fallback.register({"id": "unconsented", "kind": "codex"})
            with self.assertRaises(ValueError):
                fallback.enqueue({"sender": "sender", "recipient": "unconsented", "text": "blocked"})
            with self.assertRaises(ValueError):
                fallback.inbox("unconsented")

    def test_legacy_downgrade_preserves_messages_but_loses_enforcement(self):
        LegacyBroker = load_pre_opt_in_broker()

        with tempfile.TemporaryDirectory(prefix="bridge-rollback-rehearsal-") as folder:
            root = Path(folder)
            active_db = root / "synthetic-active.sqlite3"

            # Build real pre-opt-in state and a pending message using the exact
            # pre-opt-in source, rather than approximating its schema.
            legacy = LegacyBroker(active_db)
            legacy.register({"id": "sender", "kind": "claude"})
            legacy.register({"id": "recipient", "kind": "codex"})
            legacy.enqueue({
                "sender": "sender",
                "recipient": "recipient",
                "text": "queued before activation",
            })

            # Activation adds opt-in state without deleting the historical row.
            active = OptInBroker(active_db)
            with closing(active.connect()) as db:
                self.assertIn(
                    "opted_in",
                    {row["name"] for row in db.execute("PRAGMA table_info(sessions)")},
                )
                self.assertEqual(
                    1,
                    db.execute("SELECT COUNT(*) FROM messages WHERE text=?",
                               ("queued before activation",)).fetchone()[0],
                )

            # Both ends must opt in; opting in only the recipient leaves the
            # historical sender's message safely stored and hidden.
            active.register({"id": "recipient", "kind": "codex", "opt_in": True})
            self.assertEqual([], active.inbox("recipient")["messages"])
            active.register({"id": "sender", "kind": "claude", "opt_in": True})
            self.assertEqual(
                ["queued before activation"],
                [row["text"] for row in active.inbox("recipient")["messages"]],
            )
            active.enqueue({
                "sender": "sender",
                "recipient": "recipient",
                "text": "queued after activation",
            })
            preserved_messages = core_message_snapshot(active.inbox("recipient")["messages"])
            self.assertEqual(2, len(preserved_messages))

            # Directly opening the upgraded schema with the old source is not a
            # supported rollback: its positional three-column INSERT fails.
            in_place_legacy = LegacyBroker(active_db)
            with self.assertRaises(sqlite3.OperationalError):
                in_place_legacy.register({"id": "another-session", "kind": "codex"})
            self.assertEqual(
                preserved_messages,
                core_message_snapshot(active.inbox("recipient")["messages"]),
            )

            # Demonstrate the unsafe legacy projection on an isolated copy:
            # message rows survive, but legacy enforcement is absent.
            downgraded_db = root / "synthetic-downgraded.sqlite3"
            with closing(sqlite3.connect(active_db)) as source_db, \
                    closing(sqlite3.connect(downgraded_db)) as backup_db:
                source_db.backup(backup_db)
            with closing(sqlite3.connect(downgraded_db)) as db, db:
                db.execute("BEGIN IMMEDIATE")
                db.execute("ALTER TABLE sessions RENAME TO sessions_with_opt_in")
                db.execute(
                    "CREATE TABLE sessions ("
                    "id TEXT PRIMARY KEY, kind TEXT NOT NULL, updated REAL NOT NULL)"
                )
                db.execute(
                    "INSERT INTO sessions(id,kind,updated) "
                    "SELECT id,kind,updated FROM sessions_with_opt_in"
                )
                db.execute("DROP TABLE sessions_with_opt_in")

            downgraded = LegacyBroker(downgraded_db)
            self.assertEqual(
                preserved_messages,
                core_message_snapshot(downgraded.inbox("recipient")["messages"]),
            )
            downgraded.register({"id": "unconsented", "kind": "codex", "opt_in": False})
            downgraded.enqueue({"sender": "sender", "recipient": "unconsented", "text": "not gated"})
            self.assertEqual(["not gated"], [row["text"] for row in downgraded.inbox("unconsented")["messages"]])

            # The legacy schema cannot represent opt-in consent. Reactivation
            # therefore gates the preserved messages until both parties opt in
            # again. This rehearsal covers direct messages, not room semantics.
            reactivated = OptInBroker(downgraded_db)
            with self.assertRaises(ValueError):
                reactivated.inbox("recipient")
            reactivated.register({"id": "recipient", "kind": "codex", "opt_in": True})
            self.assertEqual([], reactivated.inbox("recipient")["messages"])
            reactivated.register({"id": "sender", "kind": "claude", "opt_in": True})
            self.assertEqual(
                preserved_messages,
                core_message_snapshot(reactivated.inbox("recipient")["messages"]),
            )


if __name__ == "__main__":
    unittest.main()
