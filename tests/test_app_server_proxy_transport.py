"""Isolated AppServerProxy wire and persistent-subscription tests.

All subprocess and pipe traffic is local to this test process. No Codex
executable, app-server process, or real session is opened.
"""

from __future__ import annotations

import base64
import hashlib
import json
import queue
import struct
import subprocess
import threading
import time
import unittest
from unittest.mock import Mock, patch

from codex_native import AppServerProxy, ProxyError, TargetUnavailable, deliver_message
from persistent_subscription import PersistentSubscription
from shared_subscription import SharedSubscription


THREAD_ID = "123e4567-e89b-12d3-a456-426614174000"
_EOF = object()


class PipeStream:
    """Blocking byte pipe that preserves write boundaries for deterministic tests."""

    def __init__(self):
        self._chunks = queue.Queue()
        self._buffer = bytearray()
        self._finish_lock = threading.Lock()
        self._finished = False

    def write(self, data):
        with self._finish_lock:
            if self._finished:
                raise BrokenPipeError("mock pipe is closed")
            self._chunks.put(bytes(data))
        return len(data)

    def flush(self):
        return None

    def read(self, size=-1):
        if size == 0:
            return b""
        if not self._buffer:
            chunk = self._chunks.get()
            if chunk is _EOF:
                return b""
            self._buffer.extend(chunk)
        if size is None or size < 0:
            size = len(self._buffer)
        count = min(size, len(self._buffer))
        result = bytes(self._buffer[:count])
        del self._buffer[:count]
        return result

    def finish(self):
        with self._finish_lock:
            if not self._finished:
                self._finished = True
                self._chunks.put(_EOF)

    def close(self):
        self.finish()


def _read_exact(stream, count):
    result = bytearray()
    while len(result) < count:
        block = stream.read(count - len(result))
        if not block:
            raise EOFError("mock client pipe closed")
        result.extend(block)
    return bytes(result)


def _server_frame(payload, opcode=1):
    size = len(payload)
    if size < 126:
        header = bytes([0x80 | opcode, size])
    elif size <= 65535:
        header = bytes([0x80 | opcode, 126]) + struct.pack("!H", size)
    else:
        header = bytes([0x80 | opcode, 127]) + struct.pack("!Q", size)
    return header + payload


class FakePopen:
    """Minimal subprocess-shaped WebSocket server over two in-memory pipes."""

    def __init__(self, args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.stdin = PipeStream()   # client writes; fake server reads
        self.stdout = PipeStream()  # fake server writes; client reads
        self.stderr = None
        self._returncode = None
        self._state_lock = threading.Lock()
        self._condition = threading.Condition()
        self.request_header = None
        self.requests = []
        self.client_frame_masked = []
        self.handshake_complete = threading.Event()
        self.initialized = threading.Event()
        self.server_error = None
        self._server_thread = threading.Thread(target=self._serve, daemon=True)
        self._server_thread.start()

    @staticmethod
    def _read_header(stream):
        data = bytearray()
        while not data.endswith(b"\r\n\r\n"):
            data.extend(_read_exact(stream, 1))
            if len(data) > 8192:
                raise ValueError("mock request header too large")
        return bytes(data)

    @staticmethod
    def _read_client_frame(stream):
        first, second = _read_exact(stream, 2)
        length = second & 0x7F
        if length == 126:
            length = struct.unpack("!H", _read_exact(stream, 2))[0]
        elif length == 127:
            length = struct.unpack("!Q", _read_exact(stream, 8))[0]
        masked = bool(second & 0x80)
        mask = _read_exact(stream, 4) if masked else None
        payload = _read_exact(stream, length)
        if mask:
            payload = bytes(value ^ mask[index % 4] for index, value in enumerate(payload))
        return first & 0x0F, masked, payload

    def _write_json(self, value):
        payload = json.dumps(value, separators=(",", ":")).encode("utf-8")
        self.stdout.write(_server_frame(payload))

    def _serve(self):
        try:
            header = self._read_header(self.stdin)
            self.request_header = header.decode("ascii")
            fields = {}
            for line in self.request_header.split("\r\n")[1:]:
                if ":" in line:
                    key, value = line.split(":", 1)
                    fields[key.strip().lower()] = value.strip()
            key = fields["sec-websocket-key"]
            accept = base64.b64encode(
                hashlib.sha1(
                    (key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode("ascii")
                ).digest()
            ).decode("ascii")
            response = (
                "HTTP/1.1 101 Switching Protocols\r\n"
                "Upgrade: websocket\r\n"
                "Connection: Upgrade\r\n"
                f"Sec-WebSocket-Accept: {accept}\r\n\r\n"
            )
            self.stdout.write(response.encode("ascii"))
            self.handshake_complete.set()

            while True:
                opcode, masked, payload = self._read_client_frame(self.stdin)
                self.client_frame_masked.append(masked)
                if opcode == 8:
                    return
                if opcode != 1:
                    continue
                message = json.loads(payload.decode("utf-8"))
                with self._condition:
                    self.requests.append(message)
                    self._condition.notify_all()
                if message.get("method") == "initialized":
                    self.initialized.set()
                if "id" in message:
                    method = message.get("method")
                    params = message.get("params", {})
                    if method == "thread/resume":
                        result = {"thread": {"id": params.get("threadId")}}
                    elif method == "test/echo":
                        result = {"echo": params}
                    else:
                        result = {}
                    self._write_json({"jsonrpc": "2.0", "id": message["id"], "result": result})
        except (EOFError, BrokenPipeError):
            pass
        except BaseException as exc:
            self.server_error = exc
            self.handshake_complete.set()

    def wait_for_method(self, method, timeout=1):
        deadline = time.monotonic() + timeout
        with self._condition:
            while not any(request.get("method") == method for request in self.requests):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._condition.wait(remaining)
        return True

    def matching_requests(self, method):
        with self._condition:
            return [request for request in self.requests if request.get("method") == method]

    def emit_notification(self, notification):
        payload = json.dumps(notification, separators=(",", ":")).encode("utf-8")
        self.stdout.write(_server_frame(payload))

    def drop_connection(self):
        with self._state_lock:
            if self._returncode is None:
                self._returncode = 1
        self.stdin.finish()
        self.stdout.finish()

    def poll(self):
        with self._state_lock:
            return self._returncode

    def terminate(self):
        with self._state_lock:
            if self._returncode is None:
                self._returncode = -15
        self.stdin.finish()
        self.stdout.finish()

    def kill(self):
        with self._state_lock:
            self._returncode = -9
        self.stdin.finish()
        self.stdout.finish()

    def wait(self, timeout=None):
        self._server_thread.join(timeout)
        if self._server_thread.is_alive():
            raise subprocess.TimeoutExpired(self.args, timeout)
        return self.poll()


class ShortWriteStream:
    """Pipe wrapper that makes each write accept only a small prefix."""

    def __init__(self, stream, chunk_size=5):
        self.stream = stream
        self.chunk_size = chunk_size

    def write(self, data):
        written = self.stream.write(bytes(data[:self.chunk_size]))
        # Give concurrent senders a chance to interleave between partial writes.
        time.sleep(0.0001)
        return written

    def flush(self):
        return self.stream.flush()

    def read(self, size=-1):
        return self.stream.read(size)

    def finish(self):
        return self.stream.finish()

    def close(self):
        return self.stream.close()


class AppServerProxyTransportTests(unittest.TestCase):
    def setUp(self):
        self.processes = []
        self.popen_patch = patch("codex_native.subprocess.Popen", side_effect=self._popen)
        self.popen_patch.start()
        self.addCleanup(self.popen_patch.stop)

    def _popen(self, args, **kwargs):
        process = FakePopen(args, **kwargs)
        self.processes.append(process)
        return process

    def make_proxy(self, timeout=1):
        return AppServerProxy(executable="mock-codex", timeout=timeout)

    def test_shared_holder_subscribes_two_threads_over_one_proxy(self):
        second = '223e4567-e89b-12d3-a456-426614174000'
        subscription = SharedSubscription([THREAD_ID, second], proxy_factory=lambda: self.make_proxy(timeout=1))
        try:
            subscription.connect()
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline and subscription.status != 'connected':
                time.sleep(0.01)
            self.assertEqual('connected', subscription.status)
            self.assertEqual({THREAD_ID: 'subscribed', second: 'subscribed'}, subscription.member_statuses)
            self.assertEqual(1, len(self.processes))
            requests = self.processes[0].matching_requests('thread/resume')
            self.assertEqual([THREAD_ID, second], [item['params']['threadId'] for item in requests])
        finally:
            subscription.close()

    def test_malformed_approval_flags_fail_closed_before_turn_submission(self):
        proxy = Mock()
        proxy.request.return_value = {
            "thread": {
                "id": THREAD_ID,
                "status": {"type": "active", "activeFlags": "waitingOnApproval"},
            }
        }
        message = {
            "id": "987e6543-e21b-12d3-a456-426614174000",
            "recipient": THREAD_ID,
            "sender": "claude-test",
            "text": "hello",
            "status": "queued",
        }

        with self.assertRaisesRegex(TargetUnavailable, "approval state could not be verified"):
            deliver_message(proxy, THREAD_ID, message)

        self.assertEqual(["thread/read"], [call.args[0] for call in proxy.request.call_args_list])

    def test_init_performs_websocket_handshake_and_json_rpc_initialization(self):
        proxy = self.make_proxy()
        process = self.processes[-1]
        try:
            self.assertTrue(process.handshake_complete.wait(1))
            self.assertTrue(process.initialized.wait(1))
            self.assertIsNone(process.server_error)
            self.assertIn("GET / HTTP/1.1", process.request_header)
            self.assertIn("Upgrade: websocket", process.request_header)
            self.assertIn("Sec-WebSocket-Version: 13", process.request_header)

            initialize = process.matching_requests("initialize")[0]
            self.assertEqual(1, initialize["id"])
            self.assertEqual("local_agent_bridge", initialize["params"]["clientInfo"]["name"])
            self.assertTrue(initialize["params"]["capabilities"]["experimentalApi"])
            self.assertTrue(all(process.client_frame_masked))
            self.assertTrue(
                any(request.get("method") == "initialized" for request in process.requests)
            )

            # Exercise masked outbound and extended-length inbound JSON frames.
            params = {"payload": "x" * 240}
            self.assertEqual({"echo": params}, proxy.request("test/echo", params, 9))
            proxy.timeout = None
            self.assertEqual({"echo": {"idle_timeout": True}},
                             proxy.request("test/echo", {"idle_timeout": True}, 10))
        finally:
            proxy.close()

    def test_concurrent_short_outbound_writes_preserve_complete_frames(self):
        proxy = self.make_proxy(timeout=1)
        process = self.processes[-1]
        proxy.proc.stdin = ShortWriteStream(proxy.proc.stdin)
        methods = ["test/concurrent/" + str(index) for index in range(8)]
        senders = [threading.Thread(target=proxy.send_json, args=(
            {"jsonrpc": "2.0", "method": method, "params": {"index": index}},
        )) for index, method in enumerate(methods)]
        try:
            for sender in senders:
                sender.start()
            for sender in senders:
                sender.join(2)
                self.assertFalse(sender.is_alive(), "outbound sender did not finish")

            deadline = time.monotonic() + 2
            expected = set(methods)
            while time.monotonic() < deadline:
                observed = {request.get("method") for request in process.requests}
                if expected <= observed or process.server_error is not None:
                    break
                time.sleep(0.005)

            observed = {request.get("method") for request in process.requests}
            self.assertIsNone(process.server_error)
            self.assertTrue(expected <= observed, "partial or interleaved frame writes corrupted a request")
        finally:
            proxy.close()

    def test_concurrent_rpc_requests_share_one_serial_response_reader(self):
        proxy = self.make_proxy(timeout=1)
        request_context = threading.local()
        active_lock = threading.Lock()
        active_receivers = 0
        max_active_receivers = 0

        def send(request):
            request_context.ident = request["id"]

        def receive():
            nonlocal active_receivers, max_active_receivers
            with active_lock:
                active_receivers += 1
                max_active_receivers = max(max_active_receivers, active_receivers)
            try:
                time.sleep(0.03)
                return {"id": request_context.ident, "result": {"ok": True}}
            finally:
                with active_lock:
                    active_receivers -= 1

        proxy.send_json = send
        proxy.receive_json = receive
        results = {}

        def request(index):
            results[index] = proxy.request("test/concurrent", {"index": index}, 100 + index)

        callers = [threading.Thread(target=request, args=(index,)) for index in range(2)]
        try:
            for caller in callers:
                caller.start()
            for caller in callers:
                caller.join(1)
                self.assertFalse(caller.is_alive(), "RPC request did not finish")
            self.assertEqual({0: {"ok": True}, 1: {"ok": True}}, results)
            self.assertEqual(1, max_active_receivers, "RPC callers competed to consume responses")
        finally:
            proxy.close()

    def test_idle_receive_waits_for_a_server_frame(self):
        proxy = self.make_proxy(timeout=1)
        process = self.processes[-1]
        result = queue.Queue()
        entered = threading.Event()

        def receive():
            entered.set()
            try:
                result.put(proxy.receive_json())
            except BaseException as exc:
                result.put(exc)

        reader = threading.Thread(target=receive, daemon=True)
        reader.start()
        try:
            self.assertTrue(entered.wait(1))
            time.sleep(0.05)
            self.assertTrue(reader.is_alive(), "idle receive returned before a frame arrived")
            notification = {
                "jsonrpc": "2.0",
                "method": "thread/status/changed",
                "params": {"threadId": THREAD_ID, "status": {"type": "idle"}},
            }
            process.emit_notification(notification)
            reader.join(1)
            self.assertFalse(reader.is_alive(), "receive did not wake after the frame arrived")
            self.assertEqual(notification, result.get_nowait())
        finally:
            proxy.close()

    def test_close_unblocks_a_receive_waiting_on_an_idle_pipe(self):
        proxy = self.make_proxy(timeout=1)
        proxy.timeout = None
        result = queue.Queue()
        entered = threading.Event()

        def receive():
            entered.set()
            try:
                proxy.receive_json()
            except BaseException as exc:
                result.put(exc)

        reader = threading.Thread(target=receive, daemon=True)
        reader.start()
        self.assertTrue(entered.wait(1))
        time.sleep(0.05)
        self.assertTrue(reader.is_alive(), "receive should be blocked before close")

        proxy.close()
        reader.join(1)

        self.assertFalse(reader.is_alive(), "close left receive blocked")
        error = result.get_nowait()
        self.assertIsInstance(error, ProxyError)
        self.assertIn("closed", str(error))

    def test_event_burst_stays_within_the_bounded_reader_queue_and_is_not_dropped(self):
        proxy = self.make_proxy(timeout=1)
        process = self.processes[-1]
        try:
            capacity = proxy.queue.maxsize
            self.assertGreater(capacity, 0, "reader queue must have a finite capacity")
            self.assertLessEqual(capacity, 4096, "reader queue capacity must remain operationally bounded")
            burst_size = capacity + 12
            for index in range(burst_size):
                process.emit_notification(
                    {"jsonrpc": "2.0", "method": "test/event", "params": {"index": index}}
                )

            deadline = time.monotonic() + 2
            while proxy.queue.qsize() < capacity and time.monotonic() < deadline:
                time.sleep(0.005)
            self.assertEqual(capacity, proxy.queue.qsize(), "reader did not apply backpressure at capacity")
            self.assertLessEqual(proxy.queue.qsize(), capacity)

            received = [proxy.receive_json()["params"]["index"] for _ in range(burst_size)]
            self.assertEqual(list(range(burst_size)), received)
        finally:
            proxy.close()

    def test_persistent_subscription_resumes_and_reconnects_over_fake_processes(self):
        callback_received = threading.Event()
        callback_events = []
        subscription = PersistentSubscription(
            lambda: self.make_proxy(timeout=1), THREAD_ID, reconnect_delay=0.02
        )

        def callback(event):
            callback_events.append(event)
            callback_received.set()

        try:
            subscription.connect(resume=True, callback=callback)
            self.assertGreaterEqual(len(self.processes), 1)
            first = self.processes[0]
            self.assertTrue(first.wait_for_method("thread/resume"))
            first.drop_connection()

            deadline = time.monotonic() + 2
            while len(self.processes) < 2 and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertGreaterEqual(len(self.processes), 2, "subscription did not create a replacement proxy")
            second = self.processes[1]
            self.assertTrue(second.wait_for_method("thread/resume"))
            second.emit_notification(
                {
                    "jsonrpc": "2.0",
                    "method": "thread/turn/completed",
                    "params": {"threadId": THREAD_ID, "turn": {"id": "turn-after-reconnect"}},
                }
            )

            self.assertTrue(callback_received.wait(2), "reconnected subscription did not deliver its event")
            resume_calls = first.matching_requests("thread/resume") + second.matching_requests("thread/resume")
            self.assertEqual(2, len(resume_calls))
            self.assertEqual([THREAD_ID, THREAD_ID], [call["params"]["threadId"] for call in resume_calls])
            self.assertEqual("thread/turn/completed", callback_events[0]["method"])
            self.assertFalse(
                any(request.get("method") == "thread/start" for process in self.processes
                    for request in process.requests)
            )
        finally:
            subscription.close()


if __name__ == "__main__":
    unittest.main()
