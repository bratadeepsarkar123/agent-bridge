"""Direct-message idempotency conflicts using only temporary SQLite data."""

from contextlib import closing
from pathlib import Path
import tempfile
import unittest
import uuid

from broker import Broker


class DirectRetryConflictTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="bridge-direct-retry-conflicts-")
        self.addCleanup(self.temp.cleanup)
        self.broker = Broker(Path(self.temp.name) / "synthetic.sqlite3")
        for session_id, kind in (
            ("sender-a", "codex"),
            ("sender-b", "claude"),
            ("recipient-a", "claude"),
            ("recipient-b", "codex"),
        ):
            self.broker.register({"id": session_id, "kind": kind, "opt_in": True})

    def direct(self, message_id, sender, recipient, text, reply_to=None):
        payload = {
            "id": message_id,
            "sender": sender,
            "recipient": recipient,
            "text": text,
        }
        if reply_to is not None:
            payload["reply_to"] = reply_to
        return self.broker.enqueue(payload)

    def message_count(self):
        with closing(self.broker.connect()) as db:
            return db.execute("SELECT COUNT(*) FROM messages").fetchone()[0]

    def test_group_delivery_id_cannot_be_reused_as_direct_message_id(self):
        text = "same payload across delivery kinds"
        group = self.broker.enqueue_group({
            "sender": "sender-a",
            "recipients": ["recipient-a"],
            "text": text,
            "batch_id": str(uuid.uuid4()),
        })
        group_delivery = group["deliveries"][0]
        before = self.message_count()

        with self.assertRaises(ValueError):
            self.direct(
                group_delivery["id"], "sender-a", "recipient-a", text
            )

        self.assertEqual(before, self.message_count())

    def test_room_delivery_id_cannot_be_reused_as_direct_message_id(self):
        room_id = "room-direct-retry-conflict"
        text = "same payload across delivery kinds"
        self.broker.create_room({
            "room_id": room_id,
            "name": "Synthetic room",
            "owner": "sender-a",
            "members": ["sender-a", "recipient-a"],
        })
        room = self.broker.send_room_message(room_id, {
            "sender": "sender-a",
            "text": text,
            "batch_id": str(uuid.uuid4()),
        })
        room_delivery = room["deliveries"][0]
        before = self.message_count()

        with self.assertRaises(ValueError):
            self.direct(
                room_delivery["id"], "sender-a", "recipient-a", text
            )

        self.assertEqual(before, self.message_count())

    def test_direct_id_reuse_rejects_changed_sender_recipient_or_text(self):
        message_id = str(uuid.uuid4())
        original = {
            "id": message_id,
            "sender": "sender-a",
            "recipient": "recipient-a",
            "text": "original direct message",
        }
        self.assertEqual(
            {"id": message_id, "status": "queued"},
            self.broker.enqueue(original),
        )

        conflicts = (
            ("sender", {**original, "sender": "sender-b"}),
            ("recipient", {**original, "recipient": "recipient-b"}),
            ("text", {**original, "text": "changed direct message"}),
        )
        for field, payload in conflicts:
            with self.subTest(field=field):
                before = self.message_count()
                with self.assertRaises(ValueError):
                    self.broker.enqueue(payload)
                self.assertEqual(before, self.message_count())

        with closing(self.broker.connect()) as db:
            row = db.execute(
                "SELECT sender, recipient, text, reply_to, room_id FROM messages WHERE id=?",
                (message_id,),
            ).fetchone()
        self.assertEqual(
            ("sender-a", "recipient-a", "original direct message", None, None),
            tuple(row),
        )

    def test_direct_id_reuse_rejects_a_different_valid_reply_to(self):
        parent_one = self.broker.enqueue({
            "sender": "recipient-a",
            "recipient": "sender-a",
            "text": "first parent",
        })
        parent_two = self.broker.enqueue({
            "sender": "recipient-a",
            "recipient": "sender-a",
            "text": "second parent",
        })
        message_id = str(uuid.uuid4())
        original = {
            "id": message_id,
            "sender": "sender-a",
            "recipient": "recipient-a",
            "text": "reply with stable id",
            "reply_to": parent_one["id"],
        }
        self.broker.enqueue(original)
        before = self.message_count()

        with self.assertRaises(ValueError):
            self.broker.enqueue({**original, "reply_to": parent_two["id"]})

        self.assertEqual(before, self.message_count())
        with closing(self.broker.connect()) as db:
            row = db.execute(
                "SELECT sender, recipient, text, reply_to, room_id FROM messages WHERE id=?",
                (message_id,),
            ).fetchone()
        self.assertEqual(
            ("sender-a", "recipient-a", "reply with stable id", parent_one["id"], None),
            tuple(row),
        )


if __name__ == "__main__":
    unittest.main()
