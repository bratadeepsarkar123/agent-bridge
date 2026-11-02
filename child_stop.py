"""Graceful stop request for child service scripts.

Each child script watches for a file named `<script-stem>.stop-request` in the
bridge root (for example `watch_errors.stop-request`). When it sees the file it
consumes it and exits its loop itself. Nothing is signalled or killed, so a
windowless child can be stopped the same way as a windowed one.

`service_stop.py` writes the request for each child it verifies, then waits.
"""
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def request_path(script: str, root: Path = ROOT) -> Path:
    return Path(root) / f'{Path(script).stem}.stop-request'


def consume(path: Path) -> None:
    try:
        Path(path).unlink()
    except FileNotFoundError:
        pass


def pause(seconds: float, path: Path, sleep=time.sleep) -> None:
    """Sleep in slices of at most one second, returning early when a stop request appears."""
    remaining = seconds
    while remaining > 0 and not Path(path).exists():
        step = min(1.0, remaining)
        sleep(step)
        remaining -= step
