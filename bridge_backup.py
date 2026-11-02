"""Paired backup set: broker database plus its DPAPI credential files, as one unit.

make_set() copies a stopped broker database (via backup_tool.checked_backup)
and every credential-*.dpapi file in a credential directory into a NEW set
directory, then writes a manifest of SHA-256 digests. It refuses unless every
credential hash bound in the copied database has a matching credential file in
the same set (pairing). No secret is written to the manifest.

restore_set() restores a set to NEW paths only. It requires:
  - every file digest to match the manifest (tamper/corruption check);
  - the operator to type the database digest they reviewed (human-confirmed
    freshness check: the newest set must be chosen by a person; see item 6);
  - the pairing check to pass again on the restored copies.
Restore never replaces an existing database or credential file.

Limits: the manifest cannot prove a set is the latest one; that is the human
confirmation. Same-user software can decrypt credential files (ACCEPT, item 5).
"""
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import time

import backup_tool
import credential_store


MANIFEST = 'manifest.json'
DATABASE = 'broker.sqlite3'
CREDENTIALS = 'credentials'
CREDENTIAL_GLOB = 'credential-*.dpapi'
VERSION = 1


class BundleError(RuntimeError):
    """Sanitized failure: the set was not made or restored."""


def _private_dir(path):
    """Give a directory this process created a private ACL, verified before use."""
    try:
        credential_store.apply_private_acl(path, directory=True)
    except credential_store.CredentialStoreError:
        raise BundleError('could not make a private ACL on ' + Path(path).name) from None


def _sha256_file(path):
    digest = hashlib.sha256()
    with open(path, 'rb') as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _copy_new(source, destination):
    """Copy bytes to a path that must not exist, then fsync."""
    with open(source, 'rb') as reader:
        data = reader.read()
    try:
        with open(destination, 'xb') as writer:
            writer.write(data)
            writer.flush()
            os.fsync(writer.fileno())
    except FileExistsError:
        raise BundleError('refusing to overwrite an existing file') from None


def _bound_hashes(database):
    uri = Path(database).resolve().as_uri() + '?mode=ro'
    with sqlite3.connect(uri, uri=True) as db:
        return {row[0] for row in db.execute('SELECT token_sha256 FROM session_credentials')}


def _credential_hashes(directory):
    """Map each credential file's token hash to its file name (decrypted in memory)."""
    found = {}
    for path in sorted(Path(directory).glob(CREDENTIAL_GLOB)):
        try:
            token = credential_store.read_generation(path)
        except credential_store.CredentialStoreError:
            raise BundleError('credential file failed to decrypt: ' + path.name) from None
        found.setdefault(hashlib.sha256(token).hexdigest(), []).append(path.name)
    return found


def verify_pairing(database, credentials_dir):
    """Every credential hash bound in the database must have a file here."""
    bound = _bound_hashes(database)
    have = _credential_hashes(credentials_dir)
    missing = bound - set(have)
    if missing:
        raise BundleError('pairing failed: %d bound credential(s) have no matching file' % len(missing))
    return len(bound)


def make_set(database, state, credentials_dir, set_dir):
    """Create a new paired set in a new directory. Returns the manifest dict."""
    set_dir = Path(set_dir)
    try:
        set_dir.mkdir(parents=True, exist_ok=False)
    except FileExistsError:
        raise BundleError('set directory already exists; choose a new name') from None
    credentials_out = set_dir / CREDENTIALS
    credentials_out.mkdir()
    try:
        _private_dir(set_dir)
        _private_dir(credentials_out)
        db_out = set_dir / DATABASE
        try:
            db_digest = backup_tool.checked_backup(database, state, db_out)
        except backup_tool.BackupRefused as exc:
            raise BundleError('database backup refused: ' + str(exc)) from None
        files = {}
        for source in sorted(Path(credentials_dir).glob(CREDENTIAL_GLOB)):
            _copy_new(source, credentials_out / source.name)
            files[source.name] = _sha256_file(credentials_out / source.name)
        bound = verify_pairing(db_out, credentials_out)
        manifest = {
            'version': VERSION,
            'created_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
            'database': {'name': DATABASE, 'sha256': db_digest},
            'credential_files': files,
            'bound_credentials': bound,
        }
        temporary = set_dir / (MANIFEST + '.partial')
        with open(temporary, 'x', encoding='utf-8') as writer:
            writer.write(json.dumps(manifest, indent=2, sort_keys=True))
            writer.flush()
            os.fsync(writer.fileno())
        os.replace(temporary, set_dir / MANIFEST)
        return manifest
    except Exception:
        # A failed set is not a usable backup: leave nothing that looks like one.
        for path in set_dir.rglob('*'):
            if path.is_file():
                path.unlink()
        raise


def _read_manifest(set_dir):
    try:
        with open(Path(set_dir) / MANIFEST, encoding='utf-8') as stream:
            manifest = json.load(stream)
    except (OSError, ValueError):
        raise BundleError('manifest missing or unreadable') from None
    if not isinstance(manifest, dict) or manifest.get('version') != VERSION:
        raise BundleError('unsupported manifest version')
    return manifest


def restore_set(set_dir, database_target, credentials_target, *, expected_database_sha256):
    """Restore a verified set to NEW paths. Requires the reviewed digest (item 6)."""
    set_dir = Path(set_dir)
    manifest = _read_manifest(set_dir)
    if expected_database_sha256 != manifest['database']['sha256']:
        raise BundleError('confirmation does not match this set; a person must confirm the set to restore')
    db_source = set_dir / DATABASE
    if _sha256_file(db_source) != manifest['database']['sha256']:
        raise BundleError('database digest does not match the manifest')
    credentials_source = set_dir / CREDENTIALS
    if sorted(p.name for p in credentials_source.glob(CREDENTIAL_GLOB)) != sorted(manifest['credential_files']):
        raise BundleError('credential files do not match the manifest')
    for name, digest in manifest['credential_files'].items():
        if _sha256_file(credentials_source / name) != digest:
            raise BundleError('credential digest does not match the manifest: ' + name)
    verify_pairing(db_source, credentials_source)
    database_target = Path(database_target)
    credentials_target = Path(credentials_target)
    if database_target.exists():
        raise BundleError('restore target database already exists; choose a new path')
    if credentials_target.exists():
        raise BundleError('restore target credentials directory already exists; choose a new path')
    credentials_target.mkdir(parents=True)
    _private_dir(credentials_target)
    for name in manifest['credential_files']:
        if (credentials_target / name).exists():
            raise BundleError('restore target credential already exists: ' + name)
    _copy_new(db_source, database_target)
    for name in manifest['credential_files']:
        _copy_new(credentials_source / name, credentials_target / name)
    # Verify what was actually written, not what was read.
    verify_pairing(database_target, credentials_target)
    return {'database': str(database_target), 'credentials': str(credentials_target),
            'bound_credentials': manifest['bound_credentials']}
