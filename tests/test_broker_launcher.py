import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

import broker_launcher
import broker_stop


WINDOWS_ONLY = unittest.skipUnless(os.name == 'nt', 'requires Windows process behavior')


class BrokerLauncherTests(unittest.TestCase):
    @WINDOWS_ONLY
    def test_stop_request_is_written_once_deleted_and_child_exit_code_returned(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            started = root / 'child-started'
            received = root / 'child-received'
            code = (
                'import pathlib,sys; '
                f"started=pathlib.Path({str(started)!r}); "
                f"received=pathlib.Path({str(received)!r}); "
                "started.write_text('ready'); "
                "line=sys.stdin.buffer.readline(); "
                "received.write_bytes(line); "
                "sys.exit(27 if line == b'stop\\n' else 28)"
            )
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                result = []
                runner = __import__('threading').Thread(target=lambda: result.append(
                    broker_launcher.run(
                        root=root,
                        broker_path=root / 'unused-broker.py',
                        broker_command=[sys.executable, '-c', code],
                        poll_interval=0.01,
                    )))
                runner.start()
                self._wait_for(started.exists)
                (root / 'broker-stop-request').write_bytes(b'')
                runner.join(timeout=10)

            self.assertFalse(runner.is_alive(), 'launcher did not return after the stop request')
            self.assertEqual([27], result)
            self.assertEqual(b'stop\n', received.read_bytes())
            self.assertFalse((root / 'broker-stop-request').exists())
            self.assertEqual(1, output.getvalue().count('Broker stop requested.'))
            self.assertIn('Broker exited with code 27.', output.getvalue())

    @WINDOWS_ONLY
    def test_killing_launcher_closes_child_stdin_and_child_observes_eof(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            started = root / 'child-started'
            eof_seen = root / 'child-eof'
            code = (
                'import os,pathlib,sys; '
                f"pathlib.Path({str(started)!r}).write_text(str(os.getpid())); "
                'sys.stdin.buffer.read(); '
                f'pathlib.Path({str(eof_seen)!r}).write_text("eof")'
            )
            driver = (
                'import broker_launcher,sys; from pathlib import Path; '
                'broker_launcher.run(root=Path(sys.argv[1]), '
                'broker_path=Path(sys.argv[1]) / "unused.py", '
                'broker_command=[sys.executable, "-c", sys.argv[2]], poll_interval=0.01)'
            )
            launcher = subprocess.Popen(
                [sys.executable, '-B', '-c', driver, str(root), code],
                cwd=str(Path(__file__).resolve().parents[1]),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            try:
                self._wait_for(started.exists)
                launcher.terminate()
                launcher.wait(timeout=10)
                self._wait_for(eof_seen.exists)
                self.assertEqual('eof', eof_seen.read_text())
            finally:
                if launcher.poll() is None:
                    launcher.kill()
                    launcher.wait(timeout=5)

    @staticmethod
    def _wait_for(predicate, timeout=10):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(0.02)
        raise AssertionError('timed out waiting for temporary process marker')


class BrokerStopTests(unittest.TestCase):
    def test_missing_state_reports_not_running(self):
        with tempfile.TemporaryDirectory() as temp:
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                result = broker_stop.stop_broker(root=Path(temp))
        self.assertEqual(0, result)
        self.assertEqual('Broker is not running.\n', output.getvalue())

    def test_dead_pid_is_refused_without_signalling_a_process(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / 'broker-session.json').write_text(json.dumps({'pid': 999999999}))
            output = io.StringIO()
            kill_patch = (mock.patch('broker_stop.os.kill', side_effect=AssertionError('must not signal'))
                          if os.name == 'nt' else
                          mock.patch('broker_stop.os.kill', wraps=broker_stop.os.kill))
            with mock.patch.object(broker_stop, 'process_is_alive',
                                   wraps=broker_stop.process_is_alive) as alive, kill_patch as kill:
                with contextlib.redirect_stdout(output):
                    result = broker_stop.stop_broker(root=root, timeout=0)
        self.assertEqual(2, result)
        self.assertEqual('Broker state exists, but its process is not running; refusing to stop.\n',
                         output.getvalue())
        alive.assert_called_once_with(999999999)
        if os.name == 'nt':
            kill.assert_not_called()
        else:
            kill.assert_called_once_with(999999999, 0)
        self.assertFalse((root / 'broker-stop-request').exists())


if __name__ == '__main__':
    unittest.main()
