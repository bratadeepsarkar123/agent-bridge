import io
import json
import tempfile
import threading
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from urllib.parse import parse_qs, urlsplit
from unittest.mock import Mock, call, patch

from codex_client import BrokerClient, ClientError
from codex_native import ProxyError
from codex_queue import DeliveryLedger
from multi_dispatcher import (
    MAX_RECIPIENTS,
    MultiRecipientDispatcher,
    main,
    parse_credential_assignments,
    validate_holder_participants,
)


RECIPIENT_A = "123e4567-e89b-12d3-a456-426614174000"
RECIPIENT_B = "123e4567-e89b-12d3-a456-426614174001"
MESSAGE_ID = "987e6543-e21b-12d3-a456-426614174000"


class FakeResponse:
    def __init__(self, body):
        self.body = json.dumps(body).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def getcode(self):
        return 200

    def read(self):
        return self.body


class FakeBroker:
    def __init__(self, messages=None, wait=None):
        self.messages = list(messages or [])
        self.wait = wait
        self.registered = []
        self.wait_calls = []
        self.poll_calls = []

    def register(self, recipient, kind):
        self.registered.append((recipient, kind))
        return {"id": recipient, "kind": kind}

    def poll(self, recipient, status=None):
        self.poll_calls.append(recipient)
        return {"messages": [dict(message) for message in self.messages]}

    def wait_for_messages(self, recipient, after, timeout=25):
        self.wait_calls.append((recipient, after, timeout))
        if self.wait is None:
            return {"messages": [], "cursor": after}
        return self.wait(recipient, after, timeout)


class MultiRecipientDispatcherTests(unittest.TestCase):
    def test_unavailable_target_is_rechecked_on_short_retry_interval(self):
        broker = FakeBroker()
        dispatcher = self.make_dispatcher([RECIPIENT_A], broker,
                                          drain_function=Mock(return_value=[{'status': 'waiting_for_target'}]))
        def wait(recipient, cursor, timeout):
            dispatcher.stop()
            return {'messages': [], 'cursor': cursor}
        broker.wait = wait
        self.start_and_join_without_filesystem_locks(dispatcher)
        self.assertTrue(broker.wait_calls)
        self.assertEqual(dispatcher.poll_interval, broker.wait_calls[0][2])

    def test_holder_accepts_only_explicitly_registered_codex_threads(self):
        broker = Mock()
        broker.check_session.side_effect = lambda ident: {'id': ident, 'kind': 'codex' if ident == RECIPIENT_A else 'claude'}
        self.assertEqual([RECIPIENT_A], validate_holder_participants(broker, [RECIPIENT_A]))
        with self.assertRaisesRegex(ValueError, 'already be opted in'):
            validate_holder_participants(broker, [RECIPIENT_B])

    def test_holder_checks_each_recipient_with_its_own_credential(self):
        legacy, scoped = Mock(), Mock()
        legacy.check_session.return_value = {'id': RECIPIENT_A, 'kind': 'codex'}
        scoped.check_session.return_value = {'id': RECIPIENT_B, 'kind': 'codex'}
        self.assertEqual([RECIPIENT_A, RECIPIENT_B],
                         validate_holder_participants(legacy, [RECIPIENT_A, RECIPIENT_B], {RECIPIENT_B: scoped}))
        legacy.check_session.assert_called_once_with(RECIPIENT_A)
        scoped.check_session.assert_called_once_with(RECIPIENT_B)

    def test_recipient_brokers_route_each_drain_and_wait_to_the_matching_client(self):
        legacy = FakeBroker()
        scoped = FakeBroker()
        dispatcher = None
        entered = []
        entered_lock = threading.Lock()
        both_waiting = threading.Event()

        def wait(recipient, cursor, timeout):
            with entered_lock:
                entered.append((recipient, threading.current_thread().name))
                if len(entered) == 2:
                    both_waiting.set()
            both_waiting.wait(timeout=1)
            if both_waiting.is_set():
                dispatcher.stop()
            return {"messages": [], "cursor": cursor}

        legacy.wait = wait
        scoped.wait = wait
        drain = Mock(return_value=[])
        dispatcher = self.make_dispatcher(
            [RECIPIENT_A, RECIPIENT_B], legacy,
            recipient_brokers={RECIPIENT_A: scoped}, drain_function=drain,
        )
        self.start_and_join_without_filesystem_locks(dispatcher)

        self.assertEqual(2, len(entered))
        routed_drains = {call.args[0]: call.args[1] for call in drain.call_args_list}
        self.assertIs(scoped, routed_drains[RECIPIENT_A])
        self.assertIs(legacy, routed_drains[RECIPIENT_B])
        self.assertEqual(1, len(scoped.wait_calls))
        self.assertEqual(1, len(legacy.wait_calls))
        self.assertEqual(RECIPIENT_A, scoped.wait_calls[0][0])
        self.assertEqual(RECIPIENT_B, legacy.wait_calls[0][0])

    def test_recipient_broker_mapping_rejects_duplicate_and_unselected_ids(self):
        with self.assertRaisesRegex(ValueError, "unique"):
            self.make_dispatcher(
                [RECIPIENT_A], FakeBroker(),
                recipient_brokers={RECIPIENT_A: FakeBroker(), RECIPIENT_A.upper(): FakeBroker()},
            )
        with self.assertRaisesRegex(ValueError, "must be selected"):
            self.make_dispatcher(
                [RECIPIENT_A], FakeBroker(), recipient_brokers={RECIPIENT_B: FakeBroker()}
            )

    def test_credential_assignment_parser_checks_format_uniqueness_and_selection(self):
        parsed = parse_credential_assignments(
            [f"{RECIPIENT_A}=C:\\keys\\a.token", f"{RECIPIENT_B}=D:\\keys\\b.token"],
            [RECIPIENT_A, RECIPIENT_B],
        )
        self.assertEqual({
            RECIPIENT_A: "C:\\keys\\a.token",
            RECIPIENT_B: "D:\\keys\\b.token",
        }, parsed)

        invalid = [
            (["not-an-assignment"], "SESSION_UUID=PATH"),
            ([f"{RECIPIENT_A}=one", f"{RECIPIENT_A.upper()}=two"], "unique"),
            ([f"{RECIPIENT_B}=token"], "selected"),
            ([f"{RECIPIENT_A}=   "], "path"),
        ]
        for entries, message in invalid:
            with self.subTest(message=message), self.assertRaisesRegex(ValueError, message):
                parse_credential_assignments(entries, [RECIPIENT_A])

    def test_cli_builds_explicit_legacy_and_scoped_clients_before_dispatch(self):
        legacy, scoped = Mock(), Mock()
        worker = Mock()
        worker.start.return_value = worker
        worker.join.return_value = True
        args = [
            "--recipient", RECIPIENT_A,
            "--recipient", RECIPIENT_B,
            "--credential", RECIPIENT_A + r"=C:\scoped\a.token",
        ]
        with patch("multi_dispatcher.BrokerClient", side_effect=[legacy, scoped]) as client_factory, \
             patch("multi_dispatcher.MultiRecipientDispatcher", return_value=worker) as dispatcher_factory:
            self.assertEqual(0, main(args))

        self.assertEqual([
            call(credential_file=None),
            call(credential_file=r"C:\scoped\a.token"),
        ], client_factory.call_args_list)
        self.assertEqual((
            [RECIPIENT_A, RECIPIENT_B],
        ), dispatcher_factory.call_args.args)
        self.assertIs(legacy, dispatcher_factory.call_args.kwargs["broker"])
        self.assertEqual({RECIPIENT_A: scoped},
                         dispatcher_factory.call_args.kwargs["recipient_brokers"])
        worker.start.assert_called_once_with()
        worker.stop.assert_called_once_with()

    def test_unselected_credential_id_fails_before_client_or_dispatcher_start(self):
        worker = Mock()
        with patch("multi_dispatcher.BrokerClient") as client_factory, \
             patch("multi_dispatcher.MultiRecipientDispatcher", return_value=worker) as dispatcher_factory, \
             redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            main(["--recipient", RECIPIENT_A, "--credential", RECIPIENT_B + "=path"])

        client_factory.assert_not_called()
        dispatcher_factory.assert_not_called()
        worker.start.assert_not_called()

    def test_keep_loaded_still_checks_selected_participants_before_start(self):
        legacy = Mock()
        legacy.check_session.return_value = {"id": RECIPIENT_B, "kind": "codex"}
        worker = Mock()
        with patch("multi_dispatcher.BrokerClient", return_value=legacy) as client_factory, \
             patch("multi_dispatcher.MultiRecipientDispatcher", return_value=worker) as dispatcher_factory, \
             patch("multi_dispatcher.SharedSubscription") as holder_factory, \
             redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            main(["--recipient", RECIPIENT_A, "--keep-loaded"])

        client_factory.assert_called_once_with(credential_file=None)
        legacy.check_session.assert_called_once_with(RECIPIENT_A)
        dispatcher_factory.assert_not_called()
        holder_factory.assert_not_called()
        worker.start.assert_not_called()

    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.ledger = DeliveryLedger(Path(self.tempdir.name) / "ledger.sqlite3")

    @staticmethod
    def queued_message(recipient=RECIPIENT_A):
        return {
            "seq": 7,
            "id": MESSAGE_ID,
            "recipient": recipient,
            "sender": RECIPIENT_B,
            "text": "mock bridge payload",
            "status": "queued",
        }

    def make_dispatcher(self, recipients, broker, **kwargs):
        return MultiRecipientDispatcher(
            recipients,
            broker=broker,
            ledger=self.ledger,
            poll_interval=kwargs.pop("poll_interval", 0.01),
            reprobe_interval=kwargs.pop("reprobe_interval", 0.04),
            wait_timeout=kwargs.pop("wait_timeout", 0.05),
            **kwargs,
        )

    def start_and_join_without_filesystem_locks(self, dispatcher, timeout=1):
        with patch("multi_dispatcher.lockfile.acquire"), patch("multi_dispatcher.lockfile.release"):
            dispatcher.start()
            self.assertTrue(dispatcher.join(timeout=timeout))

    def test_broker_client_uses_the_wait_endpoint_with_recipient_cursor_and_timeout(self):
        discovery = Path(self.tempdir.name) / "broker-session.json"
        discovery.write_text(json.dumps({"port": 43123, "token": "mock-token"}), encoding="utf-8")
        client = BrokerClient(discovery)
        with patch(
            "codex_client._open_broker",
            return_value=FakeResponse({"messages": [], "cursor": 7}),
        ) as open_url:
            response = client.wait_for_messages(RECIPIENT_A, 7, timeout=25)

        request = open_url.call_args.args[0]
        parsed = urlsplit(request.full_url)
        self.assertEqual("/v1/messages/wait", parsed.path)
        self.assertEqual(
            {"recipient": [RECIPIENT_A], "after": ["7"], "timeout": ["25"]},
            parse_qs(parsed.query),
        )
        self.assertEqual("GET", request.get_method())
        self.assertEqual(35, open_url.call_args.kwargs["timeout"])
        self.assertEqual({"messages": [], "cursor": 7}, response)

    def test_construction_does_not_acquire_native_worker_locks(self):
        with patch("multi_dispatcher.lockfile.acquire") as acquire, patch(
            "multi_dispatcher.lockfile.release"
        ) as release:
            self.make_dispatcher([RECIPIENT_A], FakeBroker())

        acquire.assert_not_called()
        release.assert_not_called()

    def test_start_acquires_each_native_worker_lock_and_worker_exit_releases_it(self):
        broker = FakeBroker()
        dispatcher = None

        def wait(recipient, after, timeout):
            dispatcher.stop()
            return {"messages": [], "cursor": after}

        broker.wait = wait
        dispatcher = self.make_dispatcher(
            [RECIPIENT_A, RECIPIENT_B], broker, drain_function=Mock(return_value=[])
        )
        expected_paths = [
            Path(__file__).resolve().parents[1] / f"native-worker-{recipient}.json"
            for recipient in (RECIPIENT_A, RECIPIENT_B)
        ]
        with patch("multi_dispatcher.lockfile.acquire") as acquire, patch(
            "multi_dispatcher.lockfile.release"
        ) as release:
            dispatcher.start()
            self.assertTrue(dispatcher.join(timeout=1))

        self.assertEqual(
            [(path, {"recipient": recipient}) for path, recipient in zip(expected_paths, (RECIPIENT_A, RECIPIENT_B))],
            [call.args for call in acquire.call_args_list],
        )
        self.assertCountEqual([(path,) for path in expected_paths], [call.args for call in release.call_args_list])

    def test_partial_lock_acquisition_failure_releases_prior_locks_without_starting_threads(self):
        broker = FakeBroker()
        dispatcher = self.make_dispatcher(
            [RECIPIENT_A, RECIPIENT_B], broker, drain_function=Mock(return_value=[])
        )
        expected_first = Path(__file__).resolve().parents[1] / f"native-worker-{RECIPIENT_A}.json"
        with patch(
            "multi_dispatcher.lockfile.acquire",
            side_effect=[None, RuntimeError("mock lock is already owned")],
        ) as acquire, patch("multi_dispatcher.lockfile.release") as release:
            with self.assertRaisesRegex(RuntimeError, "already owned"):
                dispatcher.start()

        self.assertEqual(2, acquire.call_count)
        release.assert_called_once_with(expected_first)
        self.assertEqual({}, dispatcher.threads)
        self.assertEqual([], broker.registered)

    def test_partial_thread_start_failure_stops_started_worker_and_releases_all_locks(self):
        real_thread = threading.Thread

        class FailingThread:
            ident = None

            def __init__(self, **_kwargs):
                pass

            def start(self):
                raise RuntimeError("mock thread start failure")

        thread_count = 0

        def thread_factory(**kwargs):
            nonlocal thread_count
            thread_count += 1
            return real_thread(**kwargs) if thread_count == 1 else FailingThread()

        broker = FakeBroker()
        dispatcher = self.make_dispatcher(
            [RECIPIENT_A, RECIPIENT_B],
            broker,
            drain_function=Mock(return_value=[]),
            thread_factory=thread_factory,
        )
        expected_paths = [
            Path(__file__).resolve().parents[1] / f"native-worker-{recipient}.json"
            for recipient in (RECIPIENT_A, RECIPIENT_B)
        ]
        with patch("multi_dispatcher.lockfile.acquire") as acquire, patch(
            "multi_dispatcher.lockfile.release"
        ) as release:
            with self.assertRaisesRegex(RuntimeError, "thread start failure"):
                dispatcher.start()

        self.assertEqual(2, acquire.call_count)
        self.assertCountEqual([(path,) for path in expected_paths], [call.args for call in release.call_args_list])
        self.assertEqual({}, dispatcher.threads)
        self.assertEqual([], broker.registered)

    def test_drains_startup_and_wait_results_and_advances_per_recipient_cursor(self):
        broker = FakeBroker()
        drain = Mock(return_value=[])
        dispatcher = None
        responses = [
            {"messages": [self.queued_message()], "cursor": 7},
            {"messages": [], "cursor": 7},
        ]

        def wait(recipient, after, timeout):
            if not responses:
                dispatcher.stop()
                return {"messages": [], "cursor": after}
            response = responses.pop(0)
            return response

        broker.wait = wait
        dispatcher = self.make_dispatcher([RECIPIENT_A], broker, drain_function=drain)
        self.start_and_join_without_filesystem_locks(dispatcher)

        self.assertEqual([], broker.registered)
        self.assertEqual([0, 7, 7], [call[1] for call in broker.wait_calls])
        self.assertEqual(3, drain.call_count)  # startup, new message, then timeout retry
        self.assertEqual(7, dispatcher.cursors[RECIPIENT_A])
        self.assertTrue(all(call[2] == 0.05 for call in broker.wait_calls))

    def test_stop_during_wait_skips_the_post_wait_inbox_drain(self):
        broker = FakeBroker()
        wait_entered = threading.Event()
        release_wait = threading.Event()
        drain = Mock(return_value=[])

        def wait(recipient, cursor, timeout):
            wait_entered.set()
            release_wait.wait(timeout=1)
            return {"messages": [self.queued_message()], "cursor": cursor + 1}

        broker.wait = wait
        dispatcher = self.make_dispatcher([RECIPIENT_A], broker, drain_function=drain)
        with patch("multi_dispatcher.lockfile.acquire"), patch("multi_dispatcher.lockfile.release"):
            dispatcher.start()
            self.assertTrue(wait_entered.wait(timeout=1))
            dispatcher.stop()
            release_wait.set()
            self.assertTrue(dispatcher.join(timeout=1))

        self.assertEqual(1, drain.call_count)  # startup drain only
        self.assertEqual(0, dispatcher.cursors[RECIPIENT_A])

    def test_one_bounded_worker_is_created_per_recipient(self):
        broker = FakeBroker()
        dispatcher = None
        lock = threading.Lock()
        entered = []
        both_entered = threading.Event()

        def wait(recipient, after, timeout):
            with lock:
                entered.append(recipient)
                if len(set(entered)) == 2:
                    both_entered.set()
            both_entered.wait(timeout=2)
            dispatcher.stop()
            return {"messages": [], "cursor": after}

        broker.wait = wait
        dispatcher = self.make_dispatcher(
            [RECIPIENT_A, RECIPIENT_B], broker, max_recipients=2, drain_function=Mock(return_value=[])
        )
        self.start_and_join_without_filesystem_locks(dispatcher, timeout=3)

        self.assertEqual({RECIPIENT_A, RECIPIENT_B}, set(entered))
        self.assertEqual(2, len(dispatcher.threads))
        self.assertEqual([], broker.registered)

    def test_retries_waiting_target_after_long_poll_timeout(self):
        broker = FakeBroker([self.queued_message()])
        dispatcher = None
        waits = 0

        def wait(recipient, after, timeout):
            nonlocal waits
            waits += 1
            if waits <= 2:
                return {"messages": [self.queued_message()], "cursor": 7}
            dispatcher.stop()
            return {"messages": [], "cursor": after}

        broker.wait = wait
        proxy = Mock()
        proxy.request.side_effect = [
            {"thread": {"id": RECIPIENT_A, "status": {"type": "active", "activeFlags": ["waitingOnApproval"]}}},
            {"thread": {"id": RECIPIENT_A, "status": {"type": "active", "activeFlags": ["waitingOnApproval"]}}},
            {"thread": {"id": RECIPIENT_A, "status": {"type": "idle", "activeFlags": []}}},
            {"thread": {"id": RECIPIENT_A, "status": {"type": "idle", "activeFlags": []}}},
            {"turn": {"id": "mock-turn"}},
        ]
        proxy.close = Mock()
        results = []
        dispatcher = self.make_dispatcher(
            [RECIPIENT_A],
            broker,
            proxy_factory=lambda: proxy,
            on_results=lambda recipient, values: results.extend(values),
        )
        self.start_and_join_without_filesystem_locks(dispatcher)

        self.assertEqual(["waiting_for_target", "waiting_for_target", "submitted"],
                         [result["status"] for result in results])
        self.assertEqual("accepted", self.ledger.get(MESSAGE_ID)["state"])
        self.assertEqual(1, len([call for call in proxy.request.call_args_list if call.args[0] == "turn/start"]))
        self.assertEqual(7, dispatcher.cursors[RECIPIENT_A])

    def test_uncertain_ledger_entry_is_never_retried(self):
        broker = FakeBroker([self.queued_message()])
        dispatcher = None
        waits = 0

        def wait(recipient, after, timeout):
            nonlocal waits
            waits += 1
            if waits == 1:
                return {"messages": [self.queued_message()], "cursor": 7}
            dispatcher.stop()
            return {"messages": [], "cursor": after}

        broker.wait = wait
        proxy = Mock()
        proxy.request.return_value = {
            "thread": {"id": RECIPIENT_A, "status": {"type": "active", "activeFlags": []}}
        }
        proxy.close = Mock()
        proxy_factory = Mock(return_value=proxy)
        dispatcher = self.make_dispatcher(
            [RECIPIENT_A], broker, proxy_factory=proxy_factory
        )

        with patch("native_worker.deliver_message", side_effect=ProxyError("mock uncertain outcome")) as deliver:
            self.start_and_join_without_filesystem_locks(dispatcher)

        self.assertEqual("uncertain", self.ledger.get(MESSAGE_ID)["state"])
        self.assertEqual(1, deliver.call_count)
        self.assertEqual(1, proxy_factory.call_count)

    def test_missing_wait_endpoint_falls_back_to_polling_and_recovers(self):
        broker = FakeBroker()
        dispatcher = None
        wait_calls = 0

        def wait(recipient, after, timeout):
            nonlocal wait_calls
            wait_calls += 1
            if wait_calls == 1:
                raise ClientError("broker_error", "endpoint unavailable", 404)
            dispatcher.stop()
            return {"messages": [], "cursor": after}

        broker.wait = wait
        drain = Mock(return_value=[])
        dispatcher = self.make_dispatcher(
            [RECIPIENT_A],
            broker,
            drain_function=drain,
            poll_interval=0.005,
            reprobe_interval=0.02,
            on_error=Mock(),
        )
        self.start_and_join_without_filesystem_locks(dispatcher)

        # One startup drain and one immediate fallback drain are guaranteed.
        # Additional timed polls depend on thread scheduling before the reprobe.
        self.assertGreaterEqual(drain.call_count, 2)
        self.assertGreaterEqual(wait_calls, 2)
        self.assertEqual(0, dispatcher.cursors[RECIPIENT_A])

    def test_recipient_count_and_duplicates_are_bounded(self):
        recipients = [f"123e4567-e89b-12d3-a456-{index:012d}" for index in range(MAX_RECIPIENTS + 1)]
        with self.assertRaisesRegex(ValueError, "recipient count"):
            self.make_dispatcher(recipients, FakeBroker())
        with self.assertRaisesRegex(ValueError, "unique"):
            self.make_dispatcher([RECIPIENT_A, RECIPIENT_A], FakeBroker())
        with self.assertRaisesRegex(ValueError, "max_recipients"):
            self.make_dispatcher([RECIPIENT_A], FakeBroker(), max_recipients=MAX_RECIPIENTS + 1)

    def test_zero_wait_timeout_is_rejected_to_prevent_busy_polling(self):
        with self.assertRaisesRegex(ValueError, "wait_timeout must be greater than zero"):
            self.make_dispatcher([RECIPIENT_A], FakeBroker(), wait_timeout=0)


if __name__ == "__main__":
    unittest.main()
