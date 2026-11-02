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


def provision_initial_profile(broker, profile_id, directory):
    """Explicit trusted local admin call; return profile/file metadata, no secret.

    The caller selects the profile and private directory. Generate one unique
    immutable file, verify its token, then atomically bind with replace=False.
    Concurrent losers retain at most one orphan encrypted file each. No file is
    deleted or overwritten, and existing credential bindings are never replaced.
    Same-user processes can decrypt; this authenticates no actual thread identity.
    """
    _require_windows()
    if not broker.valid_id(profile_id):
        raise CredentialStoreError('Invalid profile id')
    # Fail closed: refuse to write credentials into a directory whose ACL is not private.
    verify_private_acl(directory)
    try:
        path = Path(directory) / ('credential-' + uuid.uuid4().hex + '.dpapi')
        secret = secrets.token_urlsafe(32).encode('ascii')
        create_generation(path, secret)
        verified = read_generation(path)
        _validate_token(verified)
        if not hmac.compare_digest(secret, verified):
            raise CredentialStoreError('Credential generation verification failed')
        broker.provision_session_credential(profile_id, secret.decode('ascii'), replace=False)
    except (OSError, ValueError, TypeError, sqlite3.Error):
        raise CredentialStoreError('Could not provision initial profile') from None
    return {'id': profile_id, 'protected': True, 'credential_file': str(path)}
