"""Optional bounded-thread dispatcher for multiple loaded Codex recipients.

Run explicitly with repeated ``--recipient`` arguments. This module never
starts or resumes agent sessions. Delivery attempts use ``native_worker``'s
shared ledger-protected ``drain_once`` implementation.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import threading
import time
from collections.abc import Callable, Iterable, Mapping
from pathlib import Path
from typing import Any

import child_stop
from codex_client import BrokerClient, ClientError
from codex_native import AppServerProxy
from codex_queue import DeliveryLedger, QueueError, canonical_uuid
import lockfile
from native_worker import drain_once
from shared_subscription import SharedSubscription


ROOT = Path(__file__).resolve().parent
MAX_RECIPIENTS = 32
MAX_WAIT_SECONDS = 25
DEFAULT_POLL_INTERVAL_SECONDS = 5.0
DEFAULT_REPROBE_INTERVAL_SECONDS = 60.0
UNSUPPORTED_WAIT_STATUSES = {404, 405, 410, 501, 502, 503, 504}
LOGGER = logging.getLogger(__name__)


class _WaitEndpointUnavailable(RuntimeError):
    pass


def _normalize_recipients(recipients: Iterable[str], maximum: int = MAX_RECIPIENTS) -> list[str]:
    normalized = [canonical_uuid(recipient, "recipient") for recipient in recipients]
    if not normalized:
        raise ValueError("at least one recipient is required")
    if len(normalized) > maximum:
        raise ValueError(f"recipient count exceeds the configured limit of {maximum}")
    if len(set(normalized)) != len(normalized):
        raise ValueError("recipients must be unique")
    return normalized


def parse_credential_assignments(entries: Iterable[str] | None, recipients: Iterable[str]) -> dict[str, str]:
    """Parse repeated ``SESSION_UUID=PATH`` options without exposing their values."""
    selected = set(_normalize_recipients(recipients))
    if entries is None:
        return {}
    if isinstance(entries, (str, bytes)):
        raise ValueError("credentials must be repeated SESSION_UUID=PATH values")

    assignments: dict[str, str] = {}
    for entry in entries:
        if not isinstance(entry, str) or "=" not in entry:
            raise ValueError("each credential must use SESSION_UUID=PATH format")
        recipient_text, path = entry.split("=", 1)
        recipient = canonical_uuid(recipient_text, "credential recipient")
        if recipient not in selected:
            raise ValueError("credential recipient must also be selected with --recipient")
        if recipient in assignments:
            raise ValueError("credential recipient IDs must be unique")
        if not path or not path.strip():
            raise ValueError("credential file path must not be empty")
        assignments[recipient] = path
    return assignments


class MultiRecipientDispatcher:
    """Poll one long-poll stream per explicitly configured recipient.

    The hard recipient cap bounds both threads and concurrent deliveries.
    When the broker lacks ``/v1/messages/wait``, workers fall back to ordinary
    inbox polling and periodically probe the wait endpoint again.
    """

    def __init__(
        self,
        recipients: Iterable[str],
        *,
        broker: Any | None = None,
        recipient_brokers: Mapping[str, Any] | None = None,
        ledger: DeliveryLedger | None = None,
        idle_only: bool = False,
        wait_timeout: float = MAX_WAIT_SECONDS,
        poll_interval: float = DEFAULT_POLL_INTERVAL_SECONDS,
        reprobe_interval: float = DEFAULT_REPROBE_INTERVAL_SECONDS,
        max_recipients: int = MAX_RECIPIENTS,
        proxy_factory: Callable[[], Any] = AppServerProxy,
        drain_function: Callable[..., list[dict[str, Any]]] = drain_once,
        thread_factory: Callable[..., threading.Thread] = threading.Thread,
        on_results: Callable[[str, list[dict[str, Any]]], None] | None = None,
        on_error: Callable[[str, Exception], None] | None = None,
    ) -> None:
        if isinstance(max_recipients, bool) or not isinstance(max_recipients, int):
            raise ValueError("max_recipients must be an integer")
        if not 1 <= max_recipients <= MAX_RECIPIENTS:
            raise ValueError(f"max_recipients must be between 1 and {MAX_RECIPIENTS}")

        normalized = _normalize_recipients(recipients, max_recipients)

        self.wait_timeout = self._number(wait_timeout, "wait_timeout", minimum=0, maximum=MAX_WAIT_SECONDS)
        if self.wait_timeout == 0:
            raise ValueError("wait_timeout must be greater than zero")
        self.poll_interval = self._number(poll_interval, "poll_interval", minimum=0, maximum=None)
        self.reprobe_interval = self._number(
            reprobe_interval, "reprobe_interval", minimum=0, maximum=None
        )
        if self.poll_interval == 0 or self.reprobe_interval < self.poll_interval:
            raise ValueError("reprobe_interval must be at least the positive poll_interval")

        self.recipients = tuple(normalized)
        selected = set(self.recipients)
        normalized_brokers: dict[str, Any] = {}
        if recipient_brokers is not None:
            if not isinstance(recipient_brokers, Mapping):
                raise ValueError("recipient_brokers must map selected recipient UUIDs to clients")
            for recipient_value, client in recipient_brokers.items():
                recipient = canonical_uuid(recipient_value, "broker recipient")
                if recipient not in selected:
                    raise ValueError("broker client recipient must be selected")
                if recipient in normalized_brokers:
                    raise ValueError("broker client recipient IDs must be unique")
                if client is None:
                    raise ValueError("recipient broker client must not be empty")
                normalized_brokers[recipient] = client
        self.recipient_brokers = normalized_brokers
        self.broker = broker if broker is not None else BrokerClient(credential_file=None)
        self.ledger = ledger if ledger is not None else DeliveryLedger(
            ROOT / "codex-native-ledger.sqlite3"
        )
        self.idle_only = bool(idle_only)
        self.proxy_factory = proxy_factory
        self.drain_function = drain_function
        self.thread_factory = thread_factory
        self.on_results = on_results
        self.on_error = on_error

        self._stop_event = threading.Event()
        self._start_event = threading.Event()
        self._state_lock = threading.Lock()
        self._lock_guard = threading.Lock()
        self._cursors = {recipient: 0 for recipient in self.recipients}
        self._threads: dict[str, threading.Thread] = {}
        self._locks_by_recipient: dict[str, Path] = {}
        self._started = False

    @staticmethod
    def _number(value: float, name: str, *, minimum: float, maximum: float | None) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"{name} must be a finite number")
        number = float(value)
        if not math.isfinite(number) or number < minimum or (maximum is not None and number > maximum):
            suffix = f" and at most {maximum}" if maximum is not None else ""
            raise ValueError(f"{name} must be at least {minimum}{suffix}")
        return number

    @property
    def cursors(self) -> dict[str, int]:
        """Return a thread-safe snapshot of each recipient's wait cursor."""
        with self._state_lock:
            return dict(self._cursors)

    @property
    def threads(self) -> dict[str, threading.Thread]:
        with self._state_lock:
            return dict(self._threads)

    def start(self) -> "MultiRecipientDispatcher":
        with self._state_lock:
            if self._started:
                raise RuntimeError("dispatcher has already been started")
            if self._stop_event.is_set():
                raise RuntimeError("a stopped dispatcher cannot be started")
            self._started = True

        started_threads: list[threading.Thread] = []
        try:
            # Claim every existing native-worker lock before any recipient
            # thread can poll or deliver. The start gate keeps partial thread
            # startup from doing work before all workers have been created.
            for recipient in self.recipients:
                path = ROOT / f"native-worker-{recipient}.json"
                lockfile.acquire(path, {"recipient": recipient})
                with self._lock_guard:
                    self._locks_by_recipient[recipient] = path

            with self._state_lock:
                for recipient in self.recipients:
                    thread = self.thread_factory(
                        target=self._run_recipient,
                        args=(recipient,),
                        name=f"bridge-dispatch-{len(self._threads) + 1}",
                        daemon=True,
                    )
                    self._threads[recipient] = thread
                    try:
                        thread.start()
                    except BaseException:
                        if getattr(thread, "ident", None) is not None:
                            started_threads.append(thread)
                        raise
                    started_threads.append(thread)

            self._start_event.set()
            return self
        except BaseException:
            self._stop_event.set()
            self._start_event.set()
            for thread in started_threads:
                thread.join()
            self._release_all_locks()
            with self._state_lock:
                self._threads.clear()
            raise

    def stop(self) -> None:
        """Request shutdown; in-flight long polls finish within their timeout."""
        self._stop_event.set()

    def join(self, timeout: float | None = None) -> bool:
        """Join workers, returning whether all have stopped by the deadline."""
        deadline = None if timeout is None else time.monotonic() + max(0.0, timeout)
        for thread in self.threads.values():
            remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
            thread.join(remaining)
        return all(not thread.is_alive() for thread in self.threads.values())

    def _report_error(self, recipient: str, error: Exception) -> None:
        if self.on_error is not None:
            try:
                self.on_error(recipient, error)
                return
            except Exception:
                pass
        LOGGER.warning("dispatcher retry for recipient worker failed (%s)", type(error).__name__)

    def _drain(self, recipient: str) -> tuple[bool, bool]:
        try:
            results = self.drain_function(
                recipient,
                self._broker_for(recipient),
                self.ledger,
                proxy_factory=self.proxy_factory,
                idle_only=self.idle_only,
            )
            if not isinstance(results, list):
                raise ValueError("drain_once returned an invalid result")
            if self.on_results is not None:
                try:
                    self.on_results(recipient, results)
                except Exception as exc:
                    self._report_error(recipient, exc)
            pending = any(isinstance(item, dict) and item.get('status') == 'waiting_for_target' for item in results)
            return True, pending
        except Exception as exc:
            self._report_error(recipient, exc)
            return False, False

    @staticmethod
    def _wait_endpoint_unsupported(error: Exception) -> bool:
        if isinstance(error, (_WaitEndpointUnavailable, AttributeError, NotImplementedError)):
            return True
        return isinstance(error, ClientError) and error.status in UNSUPPORTED_WAIT_STATUSES

    def _broker_for(self, recipient: str) -> Any:
        return self.recipient_brokers.get(recipient, self.broker)

    @staticmethod
    def _validate_wait_response(response: Any, current_cursor: int) -> int:
        if not isinstance(response, dict) or not isinstance(response.get("messages"), list):
            raise ValueError("broker returned an invalid wait response")
        cursor = response.get("cursor")
        if isinstance(cursor, bool) or not isinstance(cursor, int) or cursor < current_cursor:
            raise ValueError("broker returned an invalid wait cursor")
        return cursor

    def _save_cursor(self, recipient: str, cursor: int) -> None:
        with self._state_lock:
            self._cursors[recipient] = cursor

    def _run_recipient(self, recipient: str) -> None:
        try:
            self._start_event.wait()
            if not self._stop_event.is_set():
                self._run_recipient_loop(recipient)
        finally:
            self._release_lock(recipient)

    def _release_lock(self, recipient: str) -> None:
        error = None
        with self._lock_guard:
            path = self._locks_by_recipient.get(recipient)
            if path is not None:
                try:
                    lockfile.release(path)
                except Exception as exc:
                    error = exc
                else:
                    del self._locks_by_recipient[recipient]
        if error is not None:
            self._report_error(recipient, error)

    def _release_all_locks(self) -> None:
        for recipient in self.recipients:
            self._release_lock(recipient)

    def _run_recipient_loop(self, recipient: str) -> None:
        cursor = 0
        fallback_until = 0.0
        startup_drain_pending = True
        pending_target = False

        while not self._stop_event.is_set():
            now = time.monotonic()
            if startup_drain_pending:
                _, pending_target = self._drain(recipient)
                startup_drain_pending = False

            now = time.monotonic()
            if fallback_until and now < fallback_until:
                _, pending_target = self._drain(recipient)
                self._stop_event.wait(min(self.poll_interval, fallback_until - now))
                continue
            fallback_until = 0.0

            try:
                broker = self._broker_for(recipient)
                wait_for_messages = getattr(broker, "wait_for_messages", None)
                if not callable(wait_for_messages):
                    raise _WaitEndpointUnavailable("broker client has no wait endpoint")
                timeout = min(self.wait_timeout, self.poll_interval) if pending_target else self.wait_timeout
                response = wait_for_messages(recipient, cursor, timeout=timeout)
                new_cursor = self._validate_wait_response(response, cursor)
            except Exception as exc:
                if self._stop_event.is_set():
                    break
                if self._wait_endpoint_unsupported(exc):
                    fallback_until = time.monotonic() + self.reprobe_interval
                    self._report_error(recipient, exc)
                    _, pending_target = self._drain(recipient)
                    continue
                self._report_error(recipient, exc)
                self._stop_event.wait(self.poll_interval)
                continue

            if self._stop_event.is_set():
                break

            # This full inbox drain also retries old queued messages whose
            # recipients were unavailable during an earlier delivery attempt.
            # The wait endpoint cursor only reduces network polls; the ledger
            # remains authoritative for accepted and uncertain attempts.
            success, pending_target = self._drain(recipient)
            if success:
                cursor = new_cursor
                self._save_cursor(recipient, cursor)
            else:
                self._stop_event.wait(self.poll_interval)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--recipient",
        action="append",
        required=True,
        help=f"explicit Codex thread ID; repeat up to {MAX_RECIPIENTS} times",
    )
    parser.add_argument(
        "--credential",
        action="append",
        default=[],
        metavar="SESSION_UUID=PATH",
        help="scoped credential file for one selected recipient; repeat as needed",
    )
    parser.add_argument("--idle-only", action="store_true")
    parser.add_argument("--keep-loaded", action="store_true",
                        help="explicitly permit resuming the selected opted-in threads using one shared connection")
    parser.add_argument("--wait-timeout", type=float, default=MAX_WAIT_SECONDS)
    parser.add_argument("--poll-interval", type=float, default=DEFAULT_POLL_INTERVAL_SECONDS)
    parser.add_argument("--reprobe-interval", type=float, default=DEFAULT_REPROBE_INTERVAL_SECONDS)
    return parser


def validate_holder_participants(broker, recipients, recipient_brokers=None):
    """Verify the correct credential for every opted-in holder recipient."""
    recipient_brokers = recipient_brokers or {}
    normalized = [canonical_uuid(value, 'recipient') for value in recipients]
    for recipient in normalized:
        result = recipient_brokers.get(recipient, broker).check_session(recipient)
        if not isinstance(result, dict) or result.get('id') != recipient or result.get('kind') != 'codex':
            raise ValueError('each holder recipient must already be opted in as Codex')
    return normalized


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    dispatcher = None
    holder = None
    try:
        selected = _normalize_recipients(args.recipient)
        credential_files = parse_credential_assignments(args.credential, selected)
        broker = BrokerClient(credential_file=None)
        recipient_brokers = {
            recipient: BrokerClient(credential_file=path)
            for recipient, path in credential_files.items()
        }
        if args.keep_loaded:
            selected = validate_holder_participants(broker, selected, recipient_brokers)
        dispatcher = MultiRecipientDispatcher(
            selected,
            broker=broker,
            recipient_brokers=recipient_brokers,
            idle_only=args.idle_only,
            wait_timeout=args.wait_timeout,
            poll_interval=args.poll_interval,
            reprobe_interval=args.reprobe_interval,
        ).start()
        if args.keep_loaded:
            holder = SharedSubscription(selected).connect()
    except (ClientError, QueueError, RuntimeError, ValueError) as exc:
        if dispatcher is not None:
            dispatcher.stop()
            dispatcher.join(timeout=MAX_WAIT_SECONDS + 15)
        _parser().error(str(exc))

    try:
        previous = None
        stop_request = child_stop.request_path(__file__)
        while not dispatcher.join(timeout=1):
            if stop_request.exists():
                # Break runs the finally block: the holder closes and the dispatcher stops.
                print(json.dumps({'status': 'stop_requested'}), flush=True)
                child_stop.consume(stop_request)
                break
            if holder is not None:
                status = (holder.status, holder.member_statuses)
                if status != previous:
                    print(json.dumps({'status': 'subscription_' + status[0], 'members': status[1]}), flush=True)
                    previous = status
    except KeyboardInterrupt:
        pass
    finally:
        if holder is not None:
            holder.close()
        dispatcher.stop()
        dispatcher.join(timeout=MAX_WAIT_SECONDS + 15)
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s: %(message)s")
    raise SystemExit(main())
