"""Checked stop-then-copy backup of the broker database.

The copy is made only while the broker is verifiably stopped: discovery state
and the stop note are both absent, checked while holding the same recovery gate
that every broker startup takes, so a broker cannot start during the copy.

Limits (see LIMITS_REGISTER.md item 3): other same-user programs that write to
the database are not fenced, and a copy is not proven to be the latest one.
The copy is made with SQLite's backup API, verified, and published under a new
name only. An existing destination is never overwritten.
"""
import hashlib
import os
from pathlib import Path
import sqlite3
import sys
import uuid

from broker import recovery_gate, require_current_schema_or_empty, stop_marker_path


class BackupRefused(RuntimeError):
    """The broker is not verifiably stopped, or the copy could not be proven."""


def _sha256(path):
    digest = hashlib.sha256()
    with open(path, 'rb') as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b''):
            digest.update(chunk)
    return digest.hexdigest()


def checked_backup(database, state, destination):
    """Copy a stopped broker database to a new file and return its digest.

    Raises BackupRefused if discovery state or a stop note exists, if the
    destination already exists, or if the copy fails verification.
    """
    database = Path(database)
    state = Path(state)
    destination = Path(destination)
    if destination.exists():
        raise BackupRefused('destination already exists; choose a new name')
    if not database.is_file():
        raise BackupRefused('database not found')
    temporary = destination.with_name(destination.name + '.' + uuid.uuid4().hex + '.partial')
    try:
        with recovery_gate(state):
            if os.path.lexists(state):
                raise BackupRefused('broker discovery state exists; stop the broker first')
            if os.path.lexists(stop_marker_path(state)):
                raise BackupRefused('stop note exists; a stop is pending or unresolved')
            source = sqlite3.connect(str(database))
            try:
                target = sqlite3.connect(str(temporary))
                try:
                    source.backup(target)
                    target.commit()
                finally:
                    target.close()
            finally:
                source.close()
            # Verify the copy itself: integrity, then the schema the broker requires.
            verify = sqlite3.connect(str(temporary))
            try:
                result = verify.execute('PRAGMA integrity_check').fetchone()
            finally:
                verify.close()
            if result is None or result[0] != 'ok':
                raise BackupRefused('copy failed integrity check')
            try:
                require_current_schema_or_empty(temporary, require_existing=True)
            except (ValueError, OSError, sqlite3.Error) as exc:
                raise BackupRefused('copy does not carry the current schema') from exc
            digest = _sha256(temporary)
            with open(temporary, 'rb+') as stream:
                os.fsync(stream.fileno())
            # Publish under the new name only; never replace an existing file.
            os.link(str(temporary), str(destination))
            os.unlink(str(temporary))
            return digest
    except BackupRefused:
        raise
    except (OSError, sqlite3.Error) as exc:
        raise BackupRefused('backup could not be completed: ' + type(exc).__name__) from None
    finally:
        if temporary.exists():
            temporary.unlink()


def main(argv):
    if len(argv) != 4:
        print('usage: backup_tool.py <database> <state> <new-destination>', file=sys.stderr)
        return 2
    try:
        digest = checked_backup(argv[1], argv[2], argv[3])
    except BackupRefused as exc:
        print('Backup refused: ' + str(exc), file=sys.stderr)
        return 1
    print('Backup written. sha256=' + digest)
    return 0


if __name__ == '__main__':
    raise SystemExit(main(sys.argv))
