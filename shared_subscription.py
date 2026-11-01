"""Opt-in shared app-server subscription for a fixed set of Codex threads.

One proxy is shared by all selected threads. This module only resumes existing
threads and drains notifications; it never creates threads, starts turns, or
answers server requests such as approval prompts.
"""

from __future__ import annotations

import math
import threading
from typing import Callable, Iterable

from codex_native import AppServerProxy, ProxyError, RPCRejected, TargetUnavailable
from codex_queue import canonical_uuid

MAX_THREADS = 32


class SharedSubscription:
    def __init__(self, thread_ids: Iterable[str], *, proxy_factory: Callable = AppServerProxy,
                 reconnect_delay: float = 0.25, max_reconnect_delay: float = 5.0):
        if isinstance(thread_ids, (str, bytes)):
            raise ValueError('thread_ids must be a non-empty iterable of UUIDs')
        selected = tuple(canonical_uuid(value, 'thread_id') for value in thread_ids)
        if not selected:
            raise ValueError('at least one opted-in thread UUID is required')
        if len(selected) > MAX_THREADS:
            raise ValueError(f'at most {MAX_THREADS} thread UUIDs are supported')
        if len(set(selected)) != len(selected):
            raise ValueError('thread_ids must not contain duplicates')
        try:
            reconnect_delay = float(reconnect_delay)
            max_reconnect_delay = float(max_reconnect_delay)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError('reconnect delays must be finite positive numbers') from exc
        if (not math.isfinite(reconnect_delay) or not math.isfinite(max_reconnect_delay)
                or reconnect_delay <= 0 or max_reconnect_delay <= 0):
            raise ValueError('reconnect delays must be finite positive numbers')
        if max_reconnect_delay < reconnect_delay:
            raise ValueError('max_reconnect_delay must be at least reconnect_delay')

        self.thread_ids = selected
        self._selected_ids = frozenset(selected)
        self.proxy_factory = proxy_factory
        self.reconnect_delay = reconnect_delay
        self.max_reconnect_delay = max_reconnect_delay
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._proxy = None
        self._thread = None
        self._callback = None
        self._status = 'disconnected'
        self._member_status = {thread_id: 'disconnected' for thread_id in selected}
        self._member_error = {thread_id: None for thread_id in selected}

    @property
    def status(self) -> str:
        with self._lock:
            return self._status

    @property
    def member_statuses(self) -> dict[str, str]:
        with self._lock:
            return dict(self._member_status)

    @property
    def member_errors(self) -> dict[str, str | None]:
        with self._lock:
            return dict(self._member_error)

    def connect(self, callback=None) -> 'SharedSubscription':
        """Start the shared connection and explicitly resume selected threads."""
        with self._lock:
            if self._thread is not None or self._stop.is_set():
                raise RuntimeError('subscription cannot be started twice')
            self._callback = callback
            self._status = 'connecting'
            self._thread = threading.Thread(target=self._run, name='codex-shared-subscription', daemon=True)
            self._thread.start()
        return self

    @staticmethod
    def _error_text(error: BaseException) -> str:
        try:
            message = str(error)
        except Exception:
            message = ''
        return (type(error).__name__ + (': ' + message if message else ''))[:512]

    def _set_member(self, thread_id, status, error=None, *, clear_error=False):
        with self._lock:
            if self._stop.is_set():
                return
            self._member_status[thread_id] = status
            if error is not None:
                self._member_error[thread_id] = self._error_text(error)
            elif clear_error:
                self._member_error[thread_id] = None

    def _set_all(self, status, error=None):
        with self._lock:
            if self._stop.is_set():
                return
            self._status = status
            error_text = self._error_text(error) if error is not None else None
            for thread_id in self.thread_ids:
                self._member_status[thread_id] = status
                if error is not None:
                    self._member_error[thread_id] = error_text

    def _publish_connection(self, proxy):
        with self._lock:
            if self._stop.is_set():
                return False
            self._proxy = proxy
            self._status = 'resuming'
            return True

    def _close_current(self, proxy):
        with self._lock:
            if self._proxy is not proxy:
                return
            self._proxy = None
        self._safe_close(proxy)

    @staticmethod
    def _safe_close(proxy):
        try:
            proxy.close()
        except Exception:
            pass

    def _resume_members(self, proxy):
        active = set()
        for index, thread_id in enumerate(self.thread_ids, start=2):
            if self._stop.is_set():
                return active, None
            try:
                result = self._request_resume(proxy, thread_id, index)
                thread = result.get('thread') if isinstance(result, dict) else None
                if not isinstance(thread, dict) or thread.get('id') != thread_id:
                    raise TargetUnavailable('resumed thread identity could not be verified')
            except (RPCRejected, TargetUnavailable) as exc:
                # A missing rollout affects this member only; keep the shared
                # connection and continue resuming the remaining opt-ins.
                self._set_member(thread_id, 'unavailable', exc)
                continue
            except Exception as exc:
                # A transport or partial-protocol failure invalidates the whole
                # shared connection. The caller will close it and retry all IDs.
                return active, exc.with_traceback(None)
            active.add(thread_id)
            self._set_member(thread_id, 'subscribed', clear_error=True)
        return active, None

    def _request_resume(self, proxy, thread_id, ident):
        """Resume while draining notifications that arrive before the reply."""
        proxy.send_json({
            'jsonrpc': '2.0',
            'id': ident,
            'method': 'thread/resume',
            'params': {'threadId': thread_id, 'excludeTurns': True},
        })
        while not self._stop.is_set():
            message = proxy.receive_json()
            if not isinstance(message, dict):
                raise ProxyError('invalid app-server response while resuming thread')
            if message.get('id') == ident and 'method' not in message:
                if 'error' in message:
                    error = message['error']
                    detail = error.get('message', 'unknown error') if isinstance(error, dict) else 'unknown error'
                    raise RPCRejected('app-server rejected thread/resume: ' + str(detail))
                return message.get('result', {})
            # Requests are sequential on this one reader. Route opted-in
            # notifications instead of losing them while waiting for a reply.
            self._dispatch(message, self._selected_ids)
        return {}

    def _dispatch(self, event, active):
        if self._stop.is_set() or not isinstance(event, dict):
            return
        params = event.get('params')
        if ('method' not in event or 'id' in event or not isinstance(params, dict)
                or params.get('threadId') not in active):
            return
        callback = self._callback
        if callback is not None:
            try:
                callback(event)
            except Exception:
                # A consumer callback must not terminate the shared reader.
                pass

    def _run(self):
        delay = self.reconnect_delay
        while not self._stop.is_set():
            self._set_all('connecting')
            try:
                proxy = self.proxy_factory()
            except Exception as exc:
                self._set_all('disconnected', exc)
                if self._stop.wait(delay):
                    break
                delay = min(self.max_reconnect_delay, delay * 2)
                continue

            if not self._publish_connection(proxy):
                self._safe_close(proxy)
                break

            active, resume_error = self._resume_members(proxy)
            if self._stop.is_set():
                self._close_current(proxy)
                break
            if resume_error is not None:
                self._set_all('disconnected', resume_error)
                self._close_current(proxy)
            elif not active:
                # No usable subscription: do not retain an idle proxy process.
                with self._lock:
                    if not self._stop.is_set():
                        self._status = 'unavailable'
                self._close_current(proxy)
                break
            else:
                with self._lock:
                    if not self._stop.is_set():
                        self._status = 'connected'
                delay = self.reconnect_delay
                try:
                    # An idle notification connection is healthy; do not
                    # reconnect and resume every ten seconds when it is quiet.
                    proxy.timeout = None
                    while not self._stop.is_set():
                        self._dispatch(proxy.receive_json(), active)
                except Exception as exc:
                    if not self._stop.is_set():
                        self._set_all('disconnected', exc)
                self._close_current(proxy)

            if self._stop.wait(delay):
                break
            delay = min(self.max_reconnect_delay, delay * 2)

    def close(self):
        """Stop draining, close the active proxy, and join the reader thread."""
        with self._lock:
            self._stop.set()
            self._status = 'closed'
            for thread_id in self.thread_ids:
                self._member_status[thread_id] = 'closed'
            proxy = self._proxy
            self._proxy = None
            self._callback = None
            thread = self._thread
        if proxy is not None:
            self._safe_close(proxy)
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=5)

