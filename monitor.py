"""Opt-in, report-only process status monitoring for explicit PIDs.

Process presence is not evidence that an agent is responsive or healthy. This
module checks only whether an operating-system process appears to exist.
Nothing runs unless ``main`` is called explicitly.
"""

from __future__ import annotations

import argparse
import ctypes
from ctypes import wintypes
from datetime import datetime, timezone
import json
import math
import os
import sys
import time
from collections.abc import Callable, Iterable
from typing import Any


PROCESS_STATUSES = {"alive", "not_running", "unknown"}
HEALTH_UNKNOWN_REASON = "Process presence does not establish agent health."


def _validate_pid(pid: int) -> int:
    if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
        raise ValueError("PID must be a positive integer")
    return pid


def _windows_process_status(pid: int) -> str:
    process_query_limited_information = 0x1000
    error_access_denied = 5
    error_invalid_parameter = 87
    error_not_found = 1168
    still_active = 259

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    open_process = kernel32.OpenProcess
    open_process.argtypes = (wintypes.DWORD, wintypes.BOOL, wintypes.DWORD)
    open_process.restype = wintypes.HANDLE
    handle = open_process(process_query_limited_information, False, pid)
    if not handle:
        error = ctypes.get_last_error()
        if error == error_access_denied:
            return "alive"
        if error in (error_invalid_parameter, error_not_found):
            return "not_running"
        return "unknown"

    try:
        exit_code = wintypes.DWORD()
        get_exit_code = kernel32.GetExitCodeProcess
        get_exit_code.argtypes = (wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD))
        get_exit_code.restype = wintypes.BOOL
        if not get_exit_code(handle, ctypes.byref(exit_code)):
            return "unknown"
        return "alive" if exit_code.value == still_active else "not_running"
    finally:
        close_handle = kernel32.CloseHandle
        close_handle.argtypes = (wintypes.HANDLE,)
        close_handle.restype = wintypes.BOOL
        close_handle(handle)


def process_status(pid: int) -> str:
    """Return ``alive``, ``not_running`` or ``unknown`` for one explicit PID."""
    _validate_pid(pid)
    if os.name == "nt":
        # Windows APIs take a DWORD; avoid silently truncating a larger PID.
        if pid > 0xFFFFFFFF:
            return "unknown"
        return _windows_process_status(pid)
    try:
        os.kill(pid, 0)
    except PermissionError:
        return "alive"
    except ProcessLookupError:
        return "not_running"
    except (OSError, OverflowError, ValueError):
        return "unknown"
    return "alive"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _status_event(pid: int, status: str, previous: str | None, observed_at: str) -> dict[str, Any]:
    if status not in PROCESS_STATUSES:
        raise ValueError("status check returned an invalid process status")
    return {
        "event": "process_status" if previous is None else "process_status_changed",
        "pid": pid,
        "process_status": status,
        "previous_process_status": previous,
        "agent_health": "unknown",
        "health_reason": HEALTH_UNKNOWN_REASON,
        "observed_at": observed_at,
    }


def monitor_pids(
    pids: Iterable[int],
    *,
    interval: float | None = None,
    checks: int | None = None,
    status_check: Callable[[int], str] | None = None,
    emit: Callable[[dict[str, Any]], None] | None = None,
    sleep: Callable[[float], None] = time.sleep,
    timestamp: Callable[[], str] = _utc_now,
) -> None:
    """Check the supplied PIDs and emit initial and changed status events.

    Without an interval this performs one check. With an interval it polls until
    interrupted, unless ``checks`` limits the total number of polling rounds.
    Repeated observations with unchanged status are suppressed.
    """
    explicit_pids = list(dict.fromkeys(_validate_pid(pid) for pid in pids))
    if not explicit_pids:
        raise ValueError("at least one explicit PID is required")
    if interval is None:
        if checks is not None:
            raise ValueError("checks requires an interval")
        rounds = 1
    else:
        if isinstance(interval, bool) or not isinstance(interval, (int, float)):
            raise ValueError("interval must be a positive finite number")
        try:
            interval_seconds = float(interval)
        except OverflowError as exc:
            raise ValueError("interval must be a positive finite number") from exc
        if not math.isfinite(interval_seconds) or interval_seconds <= 0:
            raise ValueError("interval must be a positive finite number")
        if checks is not None and (
            isinstance(checks, bool) or not isinstance(checks, int) or checks <= 0
        ):
            raise ValueError("checks must be a positive integer")
        rounds = checks

    check = status_check or process_status
    output = emit or _emit_event
    previous: dict[int, str] = {}
    completed = 0
    while rounds is None or completed < rounds:
        observed_at = timestamp()
        for pid in explicit_pids:
            current = check(pid)
            if current not in PROCESS_STATUSES:
                raise ValueError("status check returned an invalid process status")
            old = previous.get(pid)
            if old is None or old != current:
                output(_status_event(pid, current, old, observed_at))
            previous[pid] = current
        completed += 1
        if rounds is not None and completed >= rounds:
            break
        sleep(interval_seconds)


def _emit_event(event: dict[str, Any]) -> None:
    print(json.dumps(event, ensure_ascii=False, sort_keys=True, separators=(",", ":")))


class _ArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise ValueError(message)


def _parser() -> argparse.ArgumentParser:
    parser = _ArgumentParser(
        description="Report local process status events for explicitly supplied PIDs."
    )
    parser.add_argument(
        "--pid", action="append", required=True, type=int, metavar="PID",
        help="process ID to check (repeat for additional explicit PIDs)",
    )
    parser.add_argument(
        "--interval", type=float, metavar="SECONDS",
        help="poll repeatedly at this interval; without --checks, continue until interrupted",
    )
    parser.add_argument(
        "--checks", type=int, metavar="COUNT",
        help="limit polling to this many rounds (requires --interval)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    try:
        args = _parser().parse_args(argv)
        monitor_pids(args.pid, interval=args.interval, checks=args.checks)
        return 0
    except KeyboardInterrupt:
        return 130
    except ValueError:
        return 2


if __name__ == "__main__":
    sys.exit(main())
