"""Staged Windows user-bound DPAPI storage and explicit local initial binding.

Tokens are URL-safe ASCII bytes, 32..512 characters; callers must generate them
with a cryptographic RNG (for example secrets.token_urlsafe(32).encode('ascii')).
Syntax validation cannot establish entropy. Only CRYPTPROTECT_UI_FORBIDDEN is
used: no machine-wide flag, prompt, optional entropy, or plaintext fallback.

Processes running as the same Windows user can decrypt these credentials.
This binds no authenticated Codex thread identity and is not a same-user malware
boundary. The caller must supply an application-owned directory with restrictive
Windows ACLs; os.open's mode is not a Windows ACL policy. Never pass secrets via
command lines or logs. Python immutable bytes cannot be reliably erased.

Generation files are create-only by this API, not OS-enforced immutable files.
Incomplete files after errors/crashes remain unusable and must never be activated.
File flushing is not an atomic transaction with SQLite. The explicit local-only
provision_initial_profile helper creates/verifies a file before binding only an
unbound profile. It never chooses a profile, rotates, opts in, or creates parents.
Failures/crashes can leave an orphan encrypted file, never an intentional binding
to an unverified file. A crash before binding leaves any prior legacy authority
unchanged: this is not a fail-closed migration/reservation protocol. A crash after
commit but before returning may leave a successful binding with an unknown result;
retry must not overwrite it. No provisioning function is a model MCP tool.
"""
import ctypes
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import hmac
import os
from pathlib import Path
import re
import secrets
import sqlite3
import stat
import subprocess
import sys
import time
import uuid


MIN_TOKEN_BYTES = 32
MAX_TOKEN_BYTES = 512
MAX_CIPHERTEXT_BYTES = 16384
CRYPTPROTECT_UI_FORBIDDEN = 0x1
_FILE_MAGIC = b'AB-DPAPI-v1\x00'
_PAYLOAD_MAGIC = b'agent-bridge-token-v1\x00'
MAX_FILE_BYTES = len(_FILE_MAGIC) + MAX_CIPHERTEXT_BYTES
_TOKEN_RE = re.compile(rb'[A-Za-z0-9_-]{32,512}\Z')


class CredentialStoreError(Exception):
    """Sanitized failure: no input, filesystem path, or Windows error text."""


class GenerationExistsError(CredentialStoreError):
    """A generation already exists and has not been modified."""


def parse_rotation_window(value):
    """Parse an explicit ISO-8601 start/end interval with timezone offsets."""
    if not isinstance(value, str) or value.count('/') != 1:
        raise CredentialStoreError('Restart window must be START/END in ISO-8601 with time zones')
    parts = value.split('/')
    try:
        parsed = [datetime.fromisoformat(part[:-1] + '+00:00' if part.endswith('Z') else part)
                  for part in parts]
    except ValueError:
        raise CredentialStoreError('Restart window must be START/END in ISO-8601 with time zones') from None
    if any(item.tzinfo is None or item.utcoffset() is None for item in parsed):
        raise CredentialStoreError('Restart window must include time zone offsets')
    start, end = parsed
    if end <= start:
        raise CredentialStoreError('Restart window end must be later than its start')
    if end.astimezone(timezone.utc).timestamp() <= time.time():
        raise CredentialStoreError('Restart window has already ended')
    return start, end


def require_open_rotation_window(value):
    start, end = parse_rotation_window(value)
    now = time.time()
    if not start.timestamp() <= now < end.timestamp():
        raise CredentialStoreError('Rotation must start within its stated restart window')
    return start, end


class _DataBlob(ctypes.Structure):
    _fields_ = [('cbData', ctypes.c_uint32),
                ('pbData', ctypes.POINTER(ctypes.c_ubyte))]


def _require_windows():
    if sys.platform != 'win32':
        raise CredentialStoreError('Windows DPAPI is required')


def _validate_token(token):
    if not isinstance(token, bytes) or _TOKEN_RE.fullmatch(token) is None:
        raise CredentialStoreError('Invalid credential token')


def _load_api():
    _require_windows()
    try:
        # Restrict DLL resolution to System32, never the working directory.
        crypt = ctypes.WinDLL('crypt32.dll', use_last_error=True, winmode=0x800)
        kernel = ctypes.WinDLL('kernel32.dll', use_last_error=True, winmode=0x800)
        blob_ptr = ctypes.POINTER(_DataBlob)
        args = [blob_ptr, ctypes.c_void_p, blob_ptr, ctypes.c_void_p,
                ctypes.c_void_p, ctypes.c_uint32, blob_ptr]
        crypt.CryptProtectData.argtypes = args
        crypt.CryptProtectData.restype = ctypes.c_int
        crypt.CryptUnprotectData.argtypes = args
        crypt.CryptUnprotectData.restype = ctypes.c_int
        kernel.LocalFree.argtypes = [ctypes.c_void_p]
        kernel.LocalFree.restype = ctypes.c_void_p
        return crypt, kernel
    except (OSError, AttributeError):
        raise CredentialStoreError('Windows DPAPI is unavailable') from None


def _transform(data, *, decrypt):
    crypt, kernel = _load_api()
    source = (ctypes.c_ubyte * len(data)).from_buffer_copy(data)
    incoming = _DataBlob(len(data), source)
    outgoing = _DataBlob()
    operation = crypt.CryptUnprotectData if decrypt else crypt.CryptProtectData
    try:
        # Description output is NULL, so no additional LocalFree allocation.
        ok = operation(ctypes.byref(incoming), None, None, None, None,
                       CRYPTPROTECT_UI_FORBIDDEN, ctypes.byref(outgoing))
        limit = (len(_PAYLOAD_MAGIC) + MAX_TOKEN_BYTES + 32) if decrypt else MAX_CIPHERTEXT_BYTES
        if not ok or not outgoing.pbData or not 0 < outgoing.cbData <= limit:
            raise CredentialStoreError('DPAPI operation failed')
        return ctypes.string_at(outgoing.pbData, outgoing.cbData)
    except (OSError, ValueError):
        raise CredentialStoreError('DPAPI operation failed') from None
    finally:
        ctypes.memset(source, 0, len(data))
        if outgoing.pbData:
            try:
                if 0 < outgoing.cbData <= MAX_CIPHERTEXT_BYTES:
                    ctypes.memset(outgoing.pbData, 0, outgoing.cbData)
            finally:
                if kernel.LocalFree(ctypes.cast(outgoing.pbData, ctypes.c_void_p)):
                    raise CredentialStoreError('DPAPI buffer release failed')


def protect_token(token):
    """Return a versioned encrypted blob for high-entropy URL-safe token bytes."""
    _require_windows()
    _validate_token(token)
    payload = _PAYLOAD_MAGIC + token + hashlib.sha256(token).digest()
    return _FILE_MAGIC + _transform(payload, decrypt=False)


def unprotect_token(encrypted):
    """Decrypt and strictly validate; all failures leave no usable credential."""
    _require_windows()
    if (not isinstance(encrypted, bytes) or not len(_FILE_MAGIC) < len(encrypted) <= MAX_FILE_BYTES
            or not encrypted.startswith(_FILE_MAGIC)):
        raise CredentialStoreError('Invalid encrypted credential')
    payload = _transform(encrypted[len(_FILE_MAGIC):], decrypt=True)
    if not payload.startswith(_PAYLOAD_MAGIC) or len(payload) < len(_PAYLOAD_MAGIC) + 32:
        raise CredentialStoreError('Invalid decrypted credential')
    token = payload[len(_PAYLOAD_MAGIC):-32]
    _validate_token(token)
    if not hmac.compare_digest(payload[-32:], hashlib.sha256(token).digest()):
        raise CredentialStoreError('Invalid decrypted credential')
    return token


def create_generation(path, token):
    """Create and flush one encrypted file exclusively; never replace a file.

    On a write/flush failure a partial encrypted file may remain. It is never
    automatically deleted or reused. Success returns None, not a secret.
    """
    encrypted = protect_token(token)
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_BINARY', 0), 0o600)
    except FileExistsError:
        raise GenerationExistsError('Credential generation already exists') from None
    except (OSError, ValueError, TypeError):
        raise CredentialStoreError('Could not create credential generation') from None
    try:
        with os.fdopen(fd, 'wb') as stream:
            fd = None  # fdopen now owns the descriptor, including on I/O errors.
            if stream.write(encrypted) != len(encrypted):
                raise CredentialStoreError('Could not write credential generation')
            stream.flush()
            os.fsync(stream.fileno())
    except (OSError, ValueError):
        raise CredentialStoreError('Could not persist credential generation') from None
    finally:
        if fd is not None:
            os.close(fd)


def read_generation(path):
    """Read at most MAX_FILE_BYTES + 1 bytes and decrypt a regular file."""
    _require_windows()
    try:
        with open(path, 'rb') as stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode) or not 0 < info.st_size <= MAX_FILE_BYTES:
                raise CredentialStoreError('Invalid credential generation')
            encrypted = stream.read(MAX_FILE_BYTES + 1)
    except (OSError, ValueError, TypeError):
        raise CredentialStoreError('Could not read credential generation') from None
    return unprotect_token(encrypted)


_SYSTEM_SID = '*S-1-5-18'
_SYSTEM_NAME = 'nt authority\\system'
_ACE_RE = re.compile(r'(?P<who>.+?):(?P<rights>(?:\([A-Za-z,]+\))+)')


def _icacls_path():
    root = os.environ.get('SystemRoot') or r'C:\Windows'
    return os.path.join(root, 'System32', 'icacls.exe')


def _current_account():
    """Return (name to grant, bare user name) for the account running this process."""
    user = os.environ.get('USERNAME') or ''
    domain = os.environ.get('USERDOMAIN') or ''
    if not user:
        raise CredentialStoreError('Windows account name unavailable')
    return ((domain + '\\' + user) if domain else user), user.lower()


def _run_icacls(args):
    _require_windows()
    try:
        return subprocess.run([_icacls_path()] + [str(a) for a in args], capture_output=True,
                              text=True, timeout=60, check=False)
    except (OSError, subprocess.SubprocessError):
        raise CredentialStoreError('icacls is unavailable') from None


def apply_private_acl(path, *, directory):
    """Remove inherited ACEs and grant only the current account and SYSTEM.

    The caller must pass a path it owns: this changes its ACL. The result is
    verified before returning. Owner-SID checks are not performed here.
    """
    account, _ = _current_account()
    rights = '(OI)(CI)F' if directory else 'F'
    result = _run_icacls([path, '/inheritance:r', '/grant:r',
                          account + ':' + rights, _SYSTEM_SID + ':' + rights])
    if result.returncode != 0:
        raise CredentialStoreError('Could not set private ACL')
    verify_private_acl(path)


def verify_private_acl(path):
    """Fail unless every ACE on path names only the current account or SYSTEM.

    Inherited ACEs are allowed when their principals pass the check; a parent
    with broad access makes the child fail because its inherited ACEs name other
    principals. DENY ACEs and any other principal fail closed.
    """
    _, user = _current_account()
    result = _run_icacls([path])
    if result.returncode != 0:
        raise CredentialStoreError('Could not read ACL')
    prefix = str(path)
    seen_user = False
    for raw in result.stdout.splitlines():
        line = raw.strip()
        if line.startswith(prefix):
            line = line[len(prefix):].strip()
        for match in _ACE_RE.finditer(line):
            who = match.group('who').strip().lower()
            rights = match.group('rights').lower()
            if 'deny' in rights:
                raise CredentialStoreError('Credential ACL has a DENY entry')
            if who == _SYSTEM_NAME:
                continue
            if who.rsplit('\\', 1)[-1] == user:
                seen_user = True
                continue
            raise CredentialStoreError('Credential ACL grants access to another principal')
    if not seen_user:
        raise CredentialStoreError('Credential ACL does not grant the current account')


UNSURE_MESSAGE = ('Provisioning outcome is unsure: the profile may already be bound. '
                  'Do not retry blindly. Check the profile and review the credential file.')


class ProvisionError(CredentialStoreError):
    """Provisioning did not complete. unsure=True means the binding may have committed.

    credential_file names an encrypted file that was kept (never deleted) when one
    was created; it is an orphan unless its profile is bound.
    """

    def __init__(self, message, *, credential_file=None, unsure=False):
        super().__init__(message)
        self.credential_file = credential_file
        self.unsure = unsure


def prepare_credentials_directory(path):
    """Create a NEW credentials directory with a private ACL, or verify an existing one.

    Fails closed: an existing directory must already be private (it is never
    re-permissioned here), and a directory whose private ACL cannot be applied is
    removed only if it is still empty, which this function just created.
    """
    _require_windows()
    path = Path(path)
    if path.exists():
        if not path.is_dir():
            raise CredentialStoreError('Credentials path is not a directory')
        verify_private_acl(path)
        return
    try:
        os.mkdir(path)
    except OSError:
        raise CredentialStoreError('Could not create credentials directory') from None
    try:
        apply_private_acl(path, directory=True)
    except CredentialStoreError:
        try:
            os.rmdir(path)
        except OSError:
            pass
        raise


def provision_initial_profile(broker, profile_id, directory):
    """Explicit trusted local admin call; return profile/file metadata, no secret.

    The caller selects the profile and private directory. Refuses before writing
    anything when the profile is already bound or the generated token collides
    with a loaded shared token or an existing binding. Otherwise it creates and
    verifies one immutable file, then binds with replace=False. A failure before
    the binding leaves the encrypted file as an orphan and raises ProvisionError.
    A failure after a possible commit is confirmed by reading back the token hash;
    if it is bound or cannot be checked, ProvisionError(unsure=True) is raised and
    the caller must not retry blindly. No file is deleted or overwritten.
    Same-user processes can decrypt; this authenticates no actual thread identity.
    """
    _require_windows()
    if not broker.valid_id(profile_id):
        raise CredentialStoreError('Invalid profile id')
    # Fail closed: refuse to write credentials into a directory whose ACL is not private.
    verify_private_acl(directory)
    path = None
    try:
        if broker.profile_status(profile_id)['bound']:
            raise ProvisionError('Profile already has a credential; nothing was written')
        path = Path(directory) / ('credential-' + uuid.uuid4().hex + '.dpapi')
        secret = secrets.token_urlsafe(32).encode('ascii')
        if not broker.credential_is_available(secret.decode('ascii')):
            raise ProvisionError('Generated credential collides with a loaded or bound '
                                 'credential; nothing was written')
        create_generation(path, secret)
        verified = read_generation(path)
        _validate_token(verified)
        if not hmac.compare_digest(secret, verified):
            raise CredentialStoreError('Credential generation verification failed')
    except ProvisionError:
        raise
    except CredentialStoreError as exc:
        # Keep the sanitized type callers already match on; record any kept file.
        exc.credential_file = None if path is None else str(path)
        raise
    except (OSError, ValueError, TypeError, sqlite3.Error):
        raise ProvisionError('Could not provision initial profile',
                             credential_file=None if path is None else str(path)) from None
    token_text = secret.decode('ascii')
    try:
        broker.provision_session_credential(profile_id, token_text, replace=False)
    except Exception:
        # Read back the candidate's unique hash: present means the commit happened.
        try:
            committed = broker.credential_is_valid(token_text)
        except Exception:
            committed = None
        if committed is False:
            raise ProvisionError('Could not provision initial profile',
                                 credential_file=str(path)) from None
        raise ProvisionError(UNSURE_MESSAGE, credential_file=str(path), unsure=True) from None
    return {'id': profile_id, 'protected': True, 'credential_file': str(path)}


def rotate_profile_credential(broker, profile_id, directory, *, restart_window,
                              retention_days, _failure_hook=None):
    """Write one new generation and CAS the bound hash; never retry a rotation.

    `_failure_hook` is test-only crash injection at durable step boundaries. It
    receives `prepared`, `file_written`, and `committed`; production callers do
    not pass it.
    """
    _require_windows()
    if not broker.valid_id(profile_id):
        raise CredentialStoreError('Invalid profile id')
    if not isinstance(restart_window, str) or not restart_window.strip():
        raise CredentialStoreError('A stated client restart window is required')
    require_open_rotation_window(restart_window)
    if isinstance(retention_days, bool) or not isinstance(retention_days, int) or retention_days < 1:
        raise CredentialStoreError('Retention days must be a positive integer')
    directory = Path(directory)
    verify_private_acl(directory)
    old_hash = broker.binding_for_profile(profile_id)
    if old_hash is None:
        raise ProvisionError('Profile is not bound; use initial provisioning instead')

    old_matches = []
    for path in sorted(directory.glob('credential-*.dpapi')):
        try:
            candidate = read_generation(path)
        except CredentialStoreError:
            continue
        if hashlib.sha256(candidate).hexdigest() == old_hash:
            old_matches.append(path)
    if len(old_matches) != 1:
        raise ProvisionError('The current credential file is missing or ambiguous; nothing was changed')

    rotation_id = uuid.uuid4().hex
    new_path = directory / ('credential-' + uuid.uuid4().hex + '.dpapi')
    broker.begin_credential_rotation(rotation_id, profile_id, old_hash,
                                     old_matches[0].name, new_path.name,
                                     restart_window, retention_days)
    if _failure_hook:
        _failure_hook('prepared')

    secret = secrets.token_urlsafe(32).encode('ascii')
    token_text = secret.decode('ascii')
    if not broker.credential_is_available(token_text):
        raise ProvisionError('Generated credential collides with an existing or retired credential; '
                             'rotation remains prepared')
    try:
        create_generation(new_path, secret)
        apply_private_acl(new_path, directory=False)
        verified = read_generation(new_path)
        _validate_token(verified)
        if not hmac.compare_digest(secret, verified):
            raise CredentialStoreError('Credential generation verification failed')
        broker.mark_rotation_file_written(rotation_id, hashlib.sha256(verified).hexdigest())
    except CredentialStoreError as exc:
        exc.credential_file = str(new_path) if new_path.exists() else None
        raise
    except (OSError, ValueError, TypeError, sqlite3.Error):
        raise ProvisionError('Could not write and verify the new credential generation',
                             credential_file=str(new_path) if new_path.exists() else None) from None
    if _failure_hook:
        _failure_hook('file_written')

    try:
        broker.commit_credential_rotation(rotation_id, token_text)
    except Exception:
        # A thrown error can follow a successful COMMIT. Read the profile and
        # journal back; never retry the CAS or create another token here.
        try:
            current = broker.binding_for_profile(profile_id)
            row = broker.rotation_for_profile(profile_id)
            retired = broker.retired_credential(old_hash)
        except Exception:
            current = row = retired = None
        new_hash = hashlib.sha256(secret).hexdigest()
        if (current == new_hash and row and row['id'] == rotation_id and
                row['state'] == 'committed' and retired):
            pass
        elif current == old_hash:
            try:
                broker.abandon_credential_rotation(rotation_id)
            except Exception:
                pass
            raise ProvisionError('Rotation was not committed; the new encrypted file is an orphan',
                                 credential_file=str(new_path)) from None
        elif row and row['id'] == rotation_id and row['state'] == 'abandoned':
            raise ProvisionError('Profile binding changed; rotation was abandoned and its new file is an orphan',
                                 credential_file=str(new_path)) from None
        else:
            raise ProvisionError('Rotation outcome is unsure. Do not retry blindly.',
                                 credential_file=str(new_path), unsure=True) from None
    if _failure_hook:
        _failure_hook('committed')
    return {'rotation_id': rotation_id, 'id': profile_id, 'state': 'committed',
            'new_file': new_path.name, 'restart_window': restart_window.strip(),
            'retention_days': retention_days}


def verify_and_retire_old_generation(broker, profile_id, directory, *, client_check_passed):
    """Mark a confirmed cutover verified, then move the old file into `retired/`."""
    _require_windows()
    if client_check_passed is not True:
        raise ProvisionError('The operator must confirm the client authorization check')
    directory = Path(directory)
    verify_private_acl(directory)
    rotation = broker.rotation_for_profile(profile_id)
    if not rotation or rotation['state'] not in ('committed', 'verified'):
        raise ProvisionError('No committed rotation is awaiting client verification')
    filename_re = re.compile(r'credential-[0-9a-f]{32}\.dpapi\Z')
    if (not filename_re.fullmatch(rotation['new_file']) or
            not filename_re.fullmatch(rotation['old_file'])):
        raise ProvisionError('Rotation journal has an invalid credential file name')
    new_path = directory / rotation['new_file']
    try:
        verify_private_acl(new_path)
        token = read_generation(new_path).decode('ascii')
    except (CredentialStoreError, UnicodeDecodeError):
        raise ProvisionError('New credential generation is unreadable; rotation remains committed') from None
    if hashlib.sha256(token.encode('ascii')).hexdigest() != rotation['new_sha256']:
        raise ProvisionError('New credential generation does not match the journal')
    try:
        check = broker.check_session(profile_id, credential=token)
    except (PermissionError, ValueError):
        raise ProvisionError('New credential failed the authorization check; rotation remains committed') from None
    if check.get('id') != profile_id or check.get('kind') not in ('claude', 'codex', 'system'):
        raise ProvisionError('Authorization check returned an unexpected profile identity')
    if rotation['state'] == 'committed':
        broker.mark_rotation_verified(rotation['id'])
        rotation = broker.rotation_for_profile(profile_id)

    retired_dir = directory / 'retired'
    try:
        if not retired_dir.exists():
            os.mkdir(retired_dir)
            apply_private_acl(retired_dir, directory=True)
        else:
            verify_private_acl(retired_dir)
    except (OSError, CredentialStoreError):
        raise ProvisionError('Could not prepare the private retired-credential folder') from None

    destination = retired_dir / rotation['old_file']
    source = directory / rotation['old_file']
    if rotation['retired_at'] is None:
        try:
            if destination.exists():
                apply_private_acl(destination, directory=False)
                old = read_generation(destination)
                if hashlib.sha256(old).hexdigest() != rotation['old_sha256']:
                    raise ProvisionError('Retired destination already exists with different contents')
            else:
                apply_private_acl(source, directory=False)
                old = read_generation(source)
                if hashlib.sha256(old).hexdigest() != rotation['old_sha256']:
                    raise ProvisionError('Old credential file does not match the journal')
                os.rename(source, destination)
                verify_private_acl(destination)
            # Keep pairing validation on the active set after the move. Backups
            # consult their own DB plus the authoritative DB at restore time.
            import bridge_backup
            bridge_backup.verify_pairing(broker.database, directory,
                                         retired_database=broker.database)
            broker.mark_rotation_retired(rotation['id'], 'retired/' + destination.name)
        except ProvisionError:
            raise
        except Exception:
            raise ProvisionError('Old credential remains recorded as retired; file move needs review') from None
    return {'id': profile_id, 'state': 'verified', 'retired_file': 'retired/' + destination.name,
            'retention_days': rotation['retention_days']}


def inventory_credential_files(broker, directory):
    """Read-only report of active and retired credential generations.

    Nothing is written, bound, renamed, or deleted. Orphans and unreadable files
    are reported for an owner decision; this function never decides for them.
    """
    _require_windows()
    verify_private_acl(directory)
    directory = Path(directory)
    retired_directory = directory / 'retired'
    if retired_directory.exists():
        verify_private_acl(retired_directory)
    report = []
    paths = sorted(directory.glob('credential-*.dpapi'))
    if retired_directory.exists():
        paths.extend(sorted(retired_directory.glob('credential-*.dpapi')))
    with closing(broker.connect()) as db:
        rotating = {row['new_sha256']: row['session_id'] for row in db.execute(
            "SELECT session_id,new_sha256 FROM credential_rotations "
            "WHERE state IN ('prepared','file_written','committed') AND new_sha256 IS NOT NULL")}
    for path in paths:
        relative = path.relative_to(directory).as_posix()
        try:
            token = read_generation(path)
        except CredentialStoreError:
            report.append({'file': relative, 'status': 'unreadable', 'profile': None})
            continue
        digest = hashlib.sha256(token).hexdigest()
        retired = broker.retired_credential(digest)
        profile = broker.bound_profile_for_credential(token.decode('ascii'))
        rotating_profile = rotating.get(digest)
        status = ('retired' if retired else
                  'rotating' if rotating_profile else
                  'bound' if profile else 'orphan')
        report.append({'file': relative, 'status': status,
                       'profile': profile or rotating_profile or
                       (retired['session_id'] if retired else None)})
    return report
