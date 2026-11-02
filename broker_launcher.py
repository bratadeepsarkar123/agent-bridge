"""Keep a broker's control stdin open and relay a coordinated stop request."""

import os
from pathlib import Path
import subprocess
import sys
import time


ROOT = Path(__file__).resolve().parent
BROKER = ROOT / 'broker.py'
STOP_REQUEST = ROOT / 'broker-stop-request'


def run(extra_args=None, *, root=ROOT, broker_path=BROKER, broker_command=None,
        popen_factory=subprocess.Popen, poll_interval=0.5):
    """Run the broker until it exits or the stop-request file appears.

    ``broker_command`` and the path arguments are injectable for isolated tests.
    Production calls use the broker.py command specified by the rollout plan.
    """
    root = Path(root)
    request_path = root / 'broker-stop-request'
    args = [os.fspath(arg) for arg in (extra_args or [])]
    if broker_command is None:
        command = [sys.executable, '-B', os.fspath(broker_path), '--control-stdin', *args]
    else:
        command = [os.fspath(arg) for arg in broker_command] + args

    child = popen_factory(
        command,
        cwd=os.fspath(root),
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0),
    )
    print(f'Broker launcher started child {child.pid}.', flush=True)

    try:
        while True:
            returncode = child.poll()
            if returncode is not None:
                break

            if request_path.exists():
                print('Broker stop requested.', flush=True)
                try:
                    child.stdin.write(b'stop\n')
                    child.stdin.flush()
                except OSError:
                    # The broker may have exited between poll() and the write.
                    pass
                finally:
                    try:
                        child.stdin.close()
                    except OSError:
                        pass
                    try:
                        request_path.unlink()
                    except FileNotFoundError:
                        pass
                returncode = child.wait()
                break

            time.sleep(poll_interval)
    finally:
        if child.stdin is not None and not child.stdin.closed:
            try:
                child.stdin.close()
            except OSError:
                pass

    print(f'Broker exited with code {returncode}.', flush=True)
    return returncode


def main():
    return run(sys.argv[1:])


if __name__ == '__main__':
    raise SystemExit(main())
