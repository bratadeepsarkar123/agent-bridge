"""Codex fallback tests; broker and process interactions are always mocked."""

import io
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import codex_queue


THREAD_ID = "123e4567-e89b-12d3-a456-426614174000"
MESSAGE_ID = "987e6543-e21b-12d3-a456-426614174000"


def queued_message(message_id=MESSAGE_ID, recipient=THREAD_ID, text="hello from the bridge"):
    return {
        "id": message_id,
        "sender": "claude-session",
        "recipient": recipient,
        "text": text,
        "status": "queued",
    }


class FakeBroker:
    def __init__(self, messages=None):
        self.messages = list(messages or [])
        self.polled = []
        self.acks = []

    def poll(self, recipient):
        self.polled.append(recipient)
        return {"messages": list(self.messages)}

    def ack(self, message_id, recipient):
        self.acks.append((message_id, recipient))
        raise AssertionError("Codex queue fallback must not acknowledge broker messages")


class CodexQueueAdapterTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.ledger_path = Path(self.temp.name) / "ledger.sqlite3"
        self.ledger = codex_queue.DeliveryLedger(self.ledger_path)
        self.output = io.StringIO()
        self.broker = FakeBroker([queued_message()])

    def adapter(self, runner):
        return codex_queue.CodexQueueAdapter(
            self.broker,
            THREAD_ID,
            self.ledger,
            runner=runner,
            stdout=self.output,
        )

    def test_delivery_is_disabled_by_default_and_only_reports_follow_up_label(self):
        runner = unittest.mock.Mock()
        adapter = self.adapter(runner)

        self.assertEqual(adapter.poll_once(), 1)

        runner.assert_not_called()
        self.assertIsNone(self.ledger.get(MESSAGE_ID))
        self.assertIn("would be queued as follow-up input", self.output.getvalue())
        self.assertIn("not native agent message", self.output.getvalue())
        self.assertEqual(self.broker.polled, [THREAD_ID])
        self.assertEqual(self.broker.acks, [])

    def test_deliver_uses_argv_without_shell_and_explicit_thread_and_message_ids(self):
        runner = unittest.mock.Mock(return_value=SimpleNamespace(returncode=0))
        adapter = self.adapter(runner)

        adapter.poll_once(deliver=True)

        args, kwargs = runner.call_args
        argv = args[0]
        self.assertEqual(argv[:5], [codex_queue.DEFAULT_CODEX, "queue", "--thread", THREAD_ID, "--message"])
        self.assertIn(MESSAGE_ID, argv[5])
        self.assertIn("hello from the bridge", argv[5])
        self.assertIs(kwargs["shell"], False)
        self.assertTrue(kwargs["capture_output"])
        self.assertEqual(kwargs["timeout"], codex_queue.DEFAULT_TIMEOUT_SECONDS)
        self.assertEqual(self.ledger.get(MESSAGE_ID)["state"], "accepted")
        self.assertIn(codex_queue.USER_FACING_LABEL, self.output.getvalue())
        self.assertEqual(self.broker.acks, [])

    def test_accepted_message_id_is_persistent_and_never_retried(self):
        runner = unittest.mock.Mock(return_value=SimpleNamespace(returncode=0))
        self.adapter(runner).poll_once(deliver=True)

        # A new adapter and ledger instance model a process restart.
        restarted_ledger = codex_queue.DeliveryLedger(self.ledger_path)
        restarted = codex_queue.CodexQueueAdapter(
            self.broker, THREAD_ID, restarted_ledger, runner=runner, stdout=self.output
        )
        restarted.poll_once(deliver=True)

        runner.assert_called_once()
        self.assertIn("already attempted (state: accepted); not retrying", self.output.getvalue())
        self.assertEqual(self.broker.acks, [])

    def test_uncertain_timeout_is_ledgered_and_not_retried(self):
        runner = unittest.mock.Mock(
            side_effect=subprocess.TimeoutExpired(["codex", "exec"], timeout=1)
        )
        adapter = self.adapter(runner)

        adapter.poll_once(deliver=True)
        adapter.poll_once(deliver=True)

        runner.assert_called_once()
        self.assertEqual(self.ledger.get(MESSAGE_ID)["state"], "uncertain")
        self.assertIn("uncertain delivery", self.output.getvalue())
        self.assertIn("not retrying", self.output.getvalue())
        self.assertEqual(self.broker.acks, [])

    def test_nonzero_cli_exit_is_uncertain_and_not_retried(self):
        runner = unittest.mock.Mock(return_value=SimpleNamespace(returncode=9))
        adapter = self.adapter(runner)

        adapter.poll_once(deliver=True)
        adapter.poll_once(deliver=True)

        runner.assert_called_once()
        self.assertEqual(self.ledger.get(MESSAGE_ID)["state"], "uncertain")
        self.assertEqual(self.ledger.get(MESSAGE_ID)["return_code"], 9)
        self.assertIn("exit code 9", self.output.getvalue())
        self.assertEqual(self.broker.acks, [])

    def test_invalid_or_other_recipient_messages_are_skipped(self):
        self.broker.messages = [
            queued_message(message_id="not-a-uuid"),
            queued_message(recipient="a-different-thread"),
            {**queued_message(), "status": "acknowledged"},
        ]
        runner = unittest.mock.Mock()

        handled = self.adapter(runner).poll_once(deliver=True)

        self.assertEqual(handled, 0)
        runner.assert_not_called()
        self.assertEqual(self.broker.acks, [])

    def test_explicit_recipient_must_be_uuid_before_poll_or_process(self):
        runner = unittest.mock.Mock()
        with self.assertRaises(codex_queue.QueueError):
            codex_queue.CodexQueueAdapter(
                self.broker, "latest", self.ledger, runner=runner, stdout=self.output
            )
        runner.assert_not_called()
        self.assertEqual(self.broker.polled, [])

    def test_cli_default_does_not_deliver_and_invalid_id_does_not_create_client(self):
        runner = unittest.mock.Mock()
        with patch.object(codex_queue.codex_client, "BrokerClient") as broker_factory, patch(
            "sys.stderr", io.StringIO()
        ):
            code = codex_queue.main(
                ["--recipient", THREAD_ID, "--ledger", str(self.ledger_path)],
                broker=self.broker,
                runner=runner,
                stdout=self.output,
            )
            invalid_code = codex_queue.main(
                ["--recipient", "latest", "--ledger", str(self.ledger_path)],
                runner=runner,
                stdout=self.output,
            )

        self.assertEqual(code, 0)
        self.assertEqual(invalid_code, 2)
        broker_factory.assert_not_called()
        runner.assert_not_called()
        self.assertIn("delivery is disabled", self.output.getvalue())

    def test_cli_requires_deliver_flag_to_launch_codex(self):
        runner = unittest.mock.Mock(return_value=SimpleNamespace(returncode=0))

        code = codex_queue.main(
            ["--recipient", THREAD_ID, "--ledger", str(self.ledger_path), "--deliver"],
            broker=self.broker,
            runner=runner,
            stdout=self.output,
        )

        self.assertEqual(code, 0)
        runner.assert_called_once()
        self.assertEqual(self.ledger.get(MESSAGE_ID)["state"], "accepted")
        self.assertEqual(self.broker.acks, [])


if __name__ == "__main__":
    unittest.main()
