"""Coordinated stop and restart for the local bridge services.

Stop order: (1) service guard, so it cannot respawn children; (2) supervisor,
so it cannot auto-recover the broker; (3) broker drain, requested through
broker_stop.py; (4) remaining children from an optional guard-format config,
in config order; (5) cleanup check: no live guard, supervisor, broker, or
child process, and no discovery state, stop-pending marker or stop request
left behind.

Nothing is force-killed. Each process gets one graceful exit request and is
then waited on. A process that does not exit is a failed step, and the run
stops there. A failed drain is not retried. Restart runs the same stop, and
starts broker, supervisor, then guard only after the stop is confirmed.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional

from broker_stop import process_is_alive, stop_broker
from service_guard import validate_service


ROOT = Path(__file__).resolve().parent
GUARD_LOCK = 'service-guard.json'
SUPERVISOR_LOCK = 'supervisor-session.json'
BROKER_STATE = 'broker-session.json'
STOP_PENDING = 'broker-session.json.stop-pending'
STOP_REQUEST = 'broker-stop-request'
# Persistent by design: these are never unlinked, so their presence is expected.
PERSISTENT_LOCKS = (
    'broker-session.json.recovery-lock',
    'service-guard.json.guard',
    'supervisor-session.json.guard',
)

OK = 'ok'
SKIPPED = 'skipped'
FAILED = 'failed'
NOT_RUN = 'not run'

CREATE_NO_WINDOW = getattr(subprocess, 'CREATE_NO_WINDOW', 0)


@dataclass(frozen=True)
class Step:
    name: str
    status: str
    detail: str


@dataclass
class Controls:
    """Process-control callables. Tests inject fakes; production uses the defaults."""

    read_pid: Callable[[Path], Optional[int]]
    is_alive: Callable[[int], bool]
    command_line: Callable[[int], Optional[str]]
    request_exit: Callable[[int], None]
    drain_broker: Callable[[Path, float], int]
    start_process: Callable[[list, Path], Any]
    sleep: Callable[[float], None]


def _default_read_pid(path: Path) -> Optional[int]:
    """Return the 'pid' field of a lock or discovery file. Other fields are never kept."""
    if not path.exists():
        return None
    with path.open('r', encoding='utf-8') as stream:
        data = json.load(stream)
    pid = data.get('pid') if isinstance(data, dict) else None
    if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
        raise ValueError(f'{path.name} has no valid pid')
    return pid


def _default_is_alive(pid: int) -> bool:
    return process_is_alive(pid)


def _default_command_line(pid: int) -> Optional[str]:
    if os.name != 'nt':
        return None
    query = f"(Get-CimInstance Win32_Process -Filter 'ProcessId={int(pid)}').CommandLine"
    try:
        result = subprocess.run(
            ['powershell.exe', '-NoProfile', '-NonInteractive', '-Command', query],
            capture_output=True, text=True, timeout=30, creationflags=CREATE_NO_WINDOW,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if result.returncode != 0:
        return None
    return result.stdout.strip() or None


def _default_request_exit(pid: int) -> None:
    """Ask a process to exit gracefully. Never forces termination."""
    if os.name == 'nt':
        # No /F: taskkill sends a window-close request only. Windowless
        # processes stay running, and the caller reports that as a failed step.
        subprocess.run(['taskkill.exe', '/PID', str(pid)], capture_output=True, text=True,
                       timeout=30, creationflags=CREATE_NO_WINDOW)
        return
    os.kill(pid, signal.SIGINT)


def _default_drain_broker(root: Path, timeout: float) -> int:
    return stop_broker(root=root, timeout=timeout)


def _default_start_process(args: list, root: Path) -> Any:
    return subprocess.Popen(
        [os.fspath(arg) for arg in args], cwd=os.fspath(root),
        stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        creationflags=CREATE_NO_WINDOW,
    )


def default_controls() -> Controls:
    return Controls(
        read_pid=_default_read_pid,
        is_alive=_default_is_alive,
        command_line=_default_command_line,
        request_exit=_default_request_exit,
        drain_broker=_default_drain_broker,
        start_process=_default_start_process,
        sleep=time.sleep,
    )


def _alive(controls: Controls, pid: int) -> bool:
    try:
        return bool(controls.is_alive(pid))
    except OSError:
        # Unknown means alive: never treat an unverifiable process as gone.
        return True


def _pid_from(controls: Controls, path: Path) -> tuple[str, Optional[int]]:
    """Return ('absent', None), ('invalid', None) or ('pid', n) for a lock or state file."""
    try:
        pid = controls.read_pid(path)
    except FileNotFoundError:
        return 'absent', None
    except (OSError, ValueError):
        return 'invalid', None
    if pid is None:
        return 'absent', None
    return 'pid', pid


def _is_script(command_line: str, script: str) -> bool:
    pattern = r'(^|[\\/\s"\'])' + re.escape(script) + r'(["\'\s]|$)'
    return re.search(pattern, command_line) is not None


def _wait_for_exit(controls: Controls, pid: int, timeout: float, poll: float = 0.5) -> bool:
    attempts = int(timeout // poll) + 1
    for _ in range(attempts):
        if not _alive(controls, pid):
            return True
        controls.sleep(poll)
    return not _alive(controls, pid)


def _stop_process(controls: Controls, pid: int, script: str, exit_timeout: float) -> Optional[str]:
    """Request a graceful exit of one verified process. Return None or a failure reason."""
    command = controls.command_line(pid)
    if command is None:
        return f'cannot verify that pid {pid} is {script}; refusing to signal it'
    if not _is_script(command, script):
        return f'pid {pid} is not {script}; refusing to signal it'
    try:
        controls.request_exit(pid)
    except OSError as exc:
        return f'could not request exit of {script} (pid {pid}): {type(exc).__name__}'
    if _wait_for_exit(controls, pid, exit_timeout):
        return None
    return (f'{script} (pid {pid}) did not exit within {exit_timeout:g} s; '
            'not force-killed, so later steps were not run')


class Stopper:
    """Runs the stop steps in order and stops at the first failed step."""

    def __init__(self, root, controls: Controls, *, services_config=None, exit_timeout=30.0,
                 drain_timeout=120.0, say: Callable[[str], None] = print):
        self.root = Path(root)
        self.controls = controls
        self.services_config = Path(services_config) if services_config else None
        self.exit_timeout = exit_timeout
        self.drain_timeout = drain_timeout
        self.say = say
        self.steps: list[Step] = []
        self.broker_pid: Optional[int] = None
        self.child_pids: list[tuple[str, int]] = []

    def run(self) -> bool:
        plan = [
            ('guard', self._guard),
            ('supervisor', self._supervisor),
            ('broker drain', self._drain),
            ('children', self._children),
            ('cleanup', self._cleanup),
        ]
        for number, (name, method) in enumerate(plan, start=1):
            step = method()
            self.steps.append(step)
            self.say(f'[{step.status}] {number}. {step.name}: {step.detail}')
            if step.status == FAILED:
                for later_number, (later_name, _) in enumerate(plan[number:], start=number + 1):
                    self.say(f'[{NOT_RUN}] {later_number}. {later_name}')
                self.say(f'Stop FAILED at step {number} ({step.name}). Later steps were not run. '
                         'No force-kill was attempted and no drain was retried.')
                return False
        self.say('Stop confirmed: all steps passed.')
        return True

    def _stop_lock_owner(self, name: str, lock: str, script: str) -> Step:
        kind, pid = _pid_from(self.controls, self.root / lock)
        if kind == 'absent':
            return Step(name, OK, 'not running (no lock file)')
        if kind == 'invalid':
            return Step(name, FAILED, f'{lock} is unreadable or has no valid pid; refusing to act on it')
        if not _alive(self.controls, pid):
            return Step(name, OK, f'pid {pid} is not running; stale lock left for the cleanup check')
        problem = _stop_process(self.controls, pid, script, self.exit_timeout)
        if problem:
            return Step(name, FAILED, problem)
        return Step(name, OK, f'{script} (pid {pid}) exited after a graceful request')

    def _guard(self) -> Step:
        return self._stop_lock_owner('guard', GUARD_LOCK, 'service_guard.py')

    def _supervisor(self) -> Step:
        return self._stop_lock_owner('supervisor', SUPERVISOR_LOCK, 'supervisor.py')

    def _drain(self) -> Step:
        kind, pid = _pid_from(self.controls, self.root / BROKER_STATE)
        if kind == 'absent':
            return Step('broker drain', OK, 'no discovery state; broker is not running')
        if kind == 'invalid':
            return Step('broker drain', FAILED,
                        f'{BROKER_STATE} is unreadable or has no valid pid; refusing to request a drain')
        self.broker_pid = pid
        if not _alive(self.controls, pid):
            return Step('broker drain', FAILED,
                        f'discovery state names pid {pid}, which is not running; '
                        'stale state needs operator review (no drain requested)')
        command = self.controls.command_line(pid)
        if command is None or not _is_script(command, 'broker.py'):
            return Step('broker drain', FAILED,
                        f'cannot verify that pid {pid} is broker.py; refusing to request a drain')
        code = self.controls.drain_broker(self.root, self.drain_timeout)
        if code == 0:
            return Step('broker drain', OK, 'drain confirmed by broker_stop (exit 0)')
        reasons = {1: 'timed out waiting for the drain', 2: 'refused by broker_stop (exit 2)'}
        reason = reasons.get(code, f'broker_stop returned {code}')
        return Step('broker drain', FAILED, f'{reason}; not retried, so later steps were not run')

    def _children(self) -> Step:
        if self.services_config is None:
            return Step('children', SKIPPED,
                        'no --services-config given; remaining children were not checked')
        try:
            config = json.loads(self.services_config.read_text(encoding='utf-8'))
        except (OSError, ValueError):
            return Step('children', FAILED, 'services config is unreadable')
        services = config.get('services') if isinstance(config, dict) else None
        if not isinstance(services, list):
            return Step('children', FAILED, 'services config has no services list')
        try:
            for service in services:
                validate_service(service)
        except (ValueError, AttributeError, TypeError, KeyError):
            return Step('children', FAILED,
                        'services config has an entry with a disallowed script or bad arguments')
        stopped = 0
        for service in services:
            script = service['script']
            pid = service.get('pid')
            if pid is None:
                continue
            if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
                return Step('children', FAILED, f'{script} has an invalid pid in the config')
            self.child_pids.append((script, pid))
            if not _alive(self.controls, pid):
                continue
            problem = _stop_process(self.controls, pid, script, self.exit_timeout)
            if problem:
                return Step('children', FAILED, problem)
            stopped += 1
        return Step('children', OK, f'stopped {stopped} child process(es) in config order')

    def _cleanup(self) -> Step:
        problems: list[str] = []
        notes: list[str] = []
        for label, lock in (('service guard', GUARD_LOCK), ('supervisor', SUPERVISOR_LOCK)):
            path = self.root / lock
            if not path.exists():
                continue
            kind, pid = _pid_from(self.controls, path)
            if kind == 'invalid':
                problems.append(f'{lock} is present with an unreadable pid; not removed')
            elif kind == 'pid' and _alive(self.controls, pid):
                problems.append(f'{label} pid {pid} is still running')
            elif kind == 'pid':
                notes.append(f'{lock} is stale (pid {pid} is gone); the next start clears it')
        if self.broker_pid is not None and _alive(self.controls, self.broker_pid):
            problems.append(f'broker pid {self.broker_pid} is still running after the drain')
        if (self.root / BROKER_STATE).exists():
            problems.append(f'{BROKER_STATE} is still present; not removed by this helper')
        if (self.root / STOP_PENDING).exists():
            problems.append(f'{STOP_PENDING} is present; operator recovery is needed before any start')
        if (self.root / STOP_REQUEST).exists():
            problems.append(f'{STOP_REQUEST} is still present')
        for script, pid in self.child_pids:
            if _alive(self.controls, pid):
                problems.append(f'child {script} (pid {pid}) is still running')
        if self.services_config is None:
            notes.append('children were not checked (no --services-config)')
        for lock in PERSISTENT_LOCKS:
            if (self.root / lock).exists():
                notes.append(f'{lock} is kept by design')
        if problems:
            return Step('cleanup', FAILED, '; '.join(problems))
        detail = 'no live guard, supervisor, broker or child; no discovery or stop state remains'
        if notes:
            detail += '; ' + '; '.join(notes)
        return Step('cleanup', OK, detail)


def start_services(root: Path, controls: Controls, services_config: Optional[Path],
                   say: Callable[[str], None]) -> bool:
    """Start broker, then supervisor, then guard. Stop at the first failure."""
    python = sys.executable
    child = controls.start_process([python, '-B', root / 'broker_launcher.py'], root)
    for _ in range(50):
        if (root / BROKER_STATE).exists():
            say('[ok] start broker: discovery state created')
            break
        if child.poll() is not None:
            say(f'[failed] start broker: launcher exited during startup (code {child.poll()})')
            return False
        controls.sleep(0.1)
    else:
        say('[failed] start broker: discovery state not created within 5 s')
        return False

    child = controls.start_process([python, '-B', root / 'supervisor.py'], root)
    controls.sleep(1)
    if child.poll() is not None:
        say(f'[failed] start supervisor: exited during startup (code {child.poll()})')
        return False
    say(f'[ok] start supervisor: started pid {child.pid}')

    if services_config is None:
        say('[skipped] start guard: no --services-config given, so the guard was NOT started '
            'and its children are not supervised')
        return True
    child = controls.start_process(
        [python, '-u', root / 'service_guard.py', '--config', services_config.resolve()], root)
    controls.sleep(1)
    if child.poll() is not None:
        say(f'[failed] start guard: exited during startup (code {child.poll()})')
        return False
    say(f'[ok] start guard: started pid {child.pid}')
    return True


def describe_plan(root: Path, services_config: Optional[Path], restart: bool) -> list[str]:
    lines = [
        'Dry run: nothing will be signalled, drained or started.',
        f'Root: {root}',
        '1. guard: read service-guard.json; verify it is service_guard.py; request a graceful exit; wait',
        '2. supervisor: read supervisor-session.json; verify it is supervisor.py; request a graceful exit; wait',
        '3. broker drain: read broker-session.json pid only; verify broker.py; one drain request via broker_stop.py',
    ]
    if services_config is None:
        lines.append('4. children: skipped (no --services-config)')
    else:
        lines.append(f'4. children: from {services_config}, in config order, verified and stopped gracefully')
    lines.append('5. cleanup: no live guard, supervisor, broker or child; no discovery or stop state left')
    if restart:
        lines.append('6. start (only after a confirmed stop): broker_launcher.py, then supervisor.py, '
                     'then service_guard.py if --services-config is given')
    lines.append('No process is force-killed. A failed step stops the run.')
    return lines


def build_parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument('--root', type=Path, default=ROOT,
                        help='bridge directory (default: the directory of this script)')
    common.add_argument('--dry-run', action='store_true',
                        help='print the plan and act on nothing')
    common.add_argument('--services-config', type=Path, default=None,
                        help='guard-format JSON listing children to stop (and the guard to start on restart)')
    common.add_argument('--exit-timeout', type=float, default=30,
                        help='seconds to wait for each guard, supervisor or child to exit (default: 30)')
    common.add_argument('--drain-timeout', type=float, default=120,
                        help='seconds broker_stop waits for the drain (default: 120)')

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    subcommands = parser.add_subparsers(dest='command', required=True)
    subcommands.add_parser('stop', parents=[common], help='coordinated stop of the bridge services')
    subcommands.add_parser('restart', parents=[common],
                           help='stop, confirm, then start broker, supervisor and guard')
    return parser


def main(argv=None, controls: Optional[Controls] = None, say: Callable[[str], None] = print) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not args.root.is_dir():
        parser.error('--root must be an existing directory')
    for name in ('exit_timeout', 'drain_timeout'):
        value = getattr(args, name)
        if not math.isfinite(value) or value < 0:
            parser.error(name.replace('_', '-') + ' must be a finite, non-negative number')
    restart = args.command == 'restart'
    root = args.root.resolve()

    if args.dry_run:
        for line in describe_plan(root, args.services_config, restart):
            say(line)
        return 0

    stopper = Stopper(root, controls or default_controls(),
                      services_config=args.services_config,
                      exit_timeout=args.exit_timeout,
                      drain_timeout=args.drain_timeout, say=say)
    if not stopper.run():
        return 1
    if not restart:
        return 0
    if not start_services(root, stopper.controls, args.services_config, say):
        say('Restart FAILED while starting services; see the step above.')
        return 1
    say('Restart finished.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
