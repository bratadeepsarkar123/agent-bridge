"""Startup failure regression using only a test-owned child and temporary data."""
from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap
import unittest


class BrokerStartupShutdownTests(unittest.TestCase):
    def test_early_eof_and_startup_print_failure_do_not_deadlock(self):
        source = textwrap.dedent('''\
            import builtins
            import sys
            import threading
            import broker

            drained = threading.Event()
            original_drain = broker.DrainingHTTPServer.wait_for_drain
            original_print = builtins.print

            def observed_drain(self, timeout):
                result = original_drain(self, timeout)
                drained.set()
                return result

            def fail_startup(*args, **kwargs):
                if args and str(args[0]).startswith('Agent bridge broker listening'):
                    # Force EOF handling to finish its drain before startup fails.
                    # The old control path would now be blocked in shutdown().
                    if not drained.wait(3):
                        raise RuntimeError('control thread did not observe EOF')
                    raise BrokenPipeError('synthetic startup output failure')
                return original_print(*args, **kwargs)

            broker.DrainingHTTPServer.wait_for_drain = observed_drain
            builtins.print = fail_startup
            sys.argv = ['broker.py', '--control-stdin', '--database', sys.argv[1],
                        '--state', sys.argv[2], '--drain-timeout', '1']
            broker.main()
        ''')
        with tempfile.TemporaryDirectory(prefix='bridge-startup-shutdown-test-') as folder:
            root = Path(folder)
            database = root / 'synthetic.sqlite3'
            state = root / 'state.json'
            # Empty input closes the child's private stdin immediately. The
            # timeout bounds a regression; subprocess.run reaps its own child.
            result = subprocess.run(
                [sys.executable, '-B', '-c', source, str(database), str(state)],
                cwd=Path(__file__).resolve().parents[1],
                input='', capture_output=True, text=True, timeout=10,
                creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
            self.assertNotEqual(0, result.returncode, result.stdout)
            self.assertIn('BrokenPipeError: synthetic startup output failure', result.stderr)
            self.assertNotIn('control thread did not observe EOF', result.stderr)
            self.assertTrue(database.is_file())
            self.assertFalse(state.exists())


if __name__ == '__main__':
    unittest.main()
