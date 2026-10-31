"""Read-only status inspection for the local agent bridge broker."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

import codex_client


class JsonArgumentParser(argparse.ArgumentParser):
    def error(self, _message: str) -> None:
        raise codex_client.ClientError("invalid_arguments", "Invalid command-line arguments")


def _parser() -> argparse.ArgumentParser:
    parser = JsonArgumentParser(description="Inspect broker liveness and explicit message queues.")
    parser.add_argument("ids", nargs="*", metavar="ID", help="explicit recipient session ID")
    parser.add_argument(
        "--id", dest="option_ids", action="append", default=[], metavar="ID",
        help="explicit recipient session ID (repeatable)",
    )
    return parser


def _read_pid(discovery_file: Path) -> int:
    try:
        config = json.loads(Path(discovery_file).read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise codex_client.ClientError("discovery_file_missing", "Broker discovery file is missing") from exc
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise codex_client.ClientError("discovery_file_invalid", "Broker discovery data is invalid") from exc

    if not isinstance(config, dict):
        raise codex_client.ClientError("discovery_file_invalid", "Broker discovery data is invalid")
    pid = config.get("pid")
    if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
        raise codex_client.ClientError("discovery_file_invalid", "Broker discovery data is invalid")
    return pid


def _process_is_alive(pid: int) -> bool:
    if os.name == "nt":
        return _windows_process_is_alive(pid)
    try:
        os.kill(pid, 0)
    except PermissionError:
        return True
    except (ProcessLookupError, OSError, OverflowError, ValueError):
        return False
    return True


def _windows_process_is_alive(pid: int) -> bool:
    import ctypes
    from ctypes import wintypes

    process_query_limited_information = 0x1000
    error_access_denied = 5
    still_active = 259
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    open_process = kernel32.OpenProcess
    open_process.argtypes = (wintypes.DWORD, wintypes.BOOL, wintypes.DWORD)
    open_process.restype = wintypes.HANDLE
    handle = open_process(process_query_limited_information, False, pid)
    if not handle:
        # Access denied still confirms a process with that PID exists.
        return ctypes.get_last_error() == error_access_denied

    try:
        exit_code = wintypes.DWORD()
        get_exit_code = kernel32.GetExitCodeProcess
        get_exit_code.argtypes = (wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD))
        get_exit_code.restype = wintypes.BOOL
        if not get_exit_code(handle, ctypes.byref(exit_code)):
            return False
        return exit_code.value == still_active
    finally:
        close_handle = kernel32.CloseHandle
        close_handle.argtypes = (wintypes.HANDLE,)
        close_handle.restype = wintypes.BOOL
        close_handle(handle)


def _queue_summary(response: Any) -> dict[str, Any]:
    if not isinstance(response, dict) or not isinstance(response.get("messages"), list):
        raise codex_client.ClientError("invalid_response", "Broker returned an invalid queue response")

    counts = {"queued": 0, "acknowledged": 0}
    for message in response["messages"]:
        if not isinstance(message, dict):
            raise codex_client.ClientError("invalid_response", "Broker returned an invalid queue response")
        message_status = message.get("status")
        if not isinstance(message_status, str) or message_status not in counts:
            raise codex_client.ClientError("invalid_response", "Broker returned an invalid queue response")
        counts[message_status] += 1

    return {"count": sum(counts.values()), "statuses": counts}


def inspect_status(ids: list[str], client: Any | None = None) -> dict[str, Any]:
    client = client or codex_client.BrokerClient()
    pid = _read_pid(client.discovery_file)
    process_alive = _process_is_alive(pid)

    queues: dict[str, Any] = {}
    if ids:
        for session_id in ids:
            queues[session_id] = _queue_summary(client.poll(session_id))
    else:
        # A read-only request confirms the broker endpoint when no queue was requested.
        client.discover()

    return {
        "ok": True,
        "broker": {"process_alive": process_alive, "reachable": True},
        "queues": queues,
    }


def _error_result(exc: codex_client.ClientError) -> dict[str, Any]:
    errors = {
        "discovery_file_missing": ("discovery_file_missing", "Broker discovery file is missing"),
        "discovery_file_invalid": ("discovery_file_invalid", "Broker discovery data is invalid"),
        "transport_error": ("broker_unreachable", "Could not reach the local broker"),
        "invalid_response": ("invalid_response", "Broker returned an invalid status response"),
        "broker_error": ("broker_error", "Broker rejected the status request"),
        "invalid_argument": ("invalid_arguments", "An explicit ID is invalid"),
        "invalid_arguments": ("invalid_arguments", "Invalid command-line arguments"),
    }
    code, message = errors.get(exc.code, ("status_error", "Could not inspect broker status"))
    error: dict[str, Any] = {"code": code, "message": message}
    if exc.code == "broker_error" and exc.status is not None:
        error["status"] = exc.status
    return {"ok": False, "error": error}


def _emit(value: dict[str, Any]) -> None:
    print(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")))


def main(argv: list[str] | None = None) -> int:
    try:
        args = _parser().parse_args(argv)
        ids = list(dict.fromkeys([*args.ids, *args.option_ids]))
        _emit(inspect_status(ids))
        return 0
    except codex_client.ClientError as exc:
        _emit(_error_result(exc))
        return 2
    except Exception:
        # Keep unexpected local or transport details (which may contain secrets) out of output.
        _emit({"ok": False, "error": {"code": "status_error", "message": "Could not inspect broker status"}})
        return 2


if __name__ == "__main__":
    sys.exit(main())
