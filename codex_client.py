"""CLI client for the local agent bridge HTTP broker."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

import credential_store


DISCOVERY_FILE = Path(__file__).with_name("broker-session.json")
SESSION_CREDENTIAL_FILE_ENV = "BRIDGE_SESSION_CREDENTIAL_FILE"
SESSION_CREDENTIAL_FORMAT_ENV = "BRIDGE_SESSION_CREDENTIAL_FORMAT"
SESSION_CREDENTIAL_RE = re.compile(r"[A-Za-z0-9_-]{32,512}\Z")
DEFAULT_TIMEOUT = 10.0
MAX_GROUP_RECIPIENTS = 100
MAX_ROOM_MEMBERS = 100
MAX_ROOM_NAME = 120
MAX_ROOM_MESSAGE_LENGTH = 4000
ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


_BROKER_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect())


def _open_broker(request, timeout):
    return _BROKER_OPENER.open(request, timeout=timeout)


class _UseEnvironmentCredentialFile:
    """Sentinel for resolving the credential file from the environment per request."""


_USE_ENVIRONMENT_CREDENTIAL_FILE = _UseEnvironmentCredentialFile()


def require_single_environment_profile(profile_ids: Iterable[str], environ: Mapping[str, str] | None = None) -> None:
    """Refuse a one-credential-per-process client that would serve several profiles.

    The environment credential (BRIDGE_SESSION_CREDENTIAL_FILE) is one file for the
    whole process, so it can authenticate only one profile. When it is set and the
    process would act for more than one distinct profile, refuse before any
    connection is made. Without an environment credential nothing is shared, so
    only the caller's own one-profile rule applies. For several profiles use
    multi_dispatcher.py with one --credential SESSION_UUID=PATH per profile.
    """
    env = os.environ if environ is None else environ
    distinct = sorted(set(profile_ids))
    if len(distinct) <= 1:
        return
    if env.get(SESSION_CREDENTIAL_FILE_ENV):
        raise ClientError(
            "environment_credential_shared",
            f"Refused: {len(distinct)} profiles would share one environment credential "
            f"({SESSION_CREDENTIAL_FILE_ENV}), which can authenticate only one profile. "
            "Use multi_dispatcher.py with --recipient and --credential SESSION_UUID=PATH "
            "for each profile.",
        )
    raise ClientError(
        "multiple_profiles_unsupported",
        f"Refused: this client serves one profile per process, not {len(distinct)}. "
        "Use multi_dispatcher.py with --recipient and --credential SESSION_UUID=PATH "
        "for each profile.",
    )


class ClientError(Exception):
    """An error that can be returned to an agent as structured JSON."""

    def __init__(self, code: str, message: str, status: int | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status

    def as_json(self) -> dict[str, Any]:
        error: dict[str, Any] = {"code": self.code, "message": self.message}
        if self.status is not None:
            error["status"] = self.status
        return {"ok": False, "error": error}


def _redact(value: Any, secret: str) -> Any:
    """Prevent a broker response or transport error from echoing its token."""
    if isinstance(value, str):
        return value.replace(secret, "[redacted]") if secret else value
    if isinstance(value, list):
        return [_redact(item, secret) for item in value]
    if isinstance(value, dict):
        return {
            key.replace(secret, "[redacted]") if isinstance(key, str) and secret else key:
                _redact(item, secret)
            for key, item in value.items()
        }
    return value


class BrokerClient:
    def __init__(
        self,
        discovery_file: Path = DISCOVERY_FILE,
        timeout: float = DEFAULT_TIMEOUT,
        credential_file: str | Path | None | _UseEnvironmentCredentialFile = (
            _USE_ENVIRONMENT_CREDENTIAL_FILE
        ),
    ) -> None:
        self.discovery_file = Path(discovery_file)
        self.timeout = timeout
        self.credential_file = credential_file

    @staticmethod
    def _read_session_credential(path: str) -> str:
        if not path.strip():
            raise ClientError(
                "session_credential_file_invalid",
                "Scoped session credential file path is empty",
            )
        try:
            with Path(path).open(encoding="utf-8") as stream:
                contents = stream.read(514)
        except FileNotFoundError:
            raise ClientError(
                "session_credential_file_missing",
                "Scoped session credential file is missing",
            ) from None
        except (OSError, UnicodeError, TypeError, ValueError):
            raise ClientError(
                "session_credential_file_invalid",
                "Could not read scoped session credential file",
            ) from None

        # Permit a single conventional final line ending, but otherwise require
        # the file to contain exactly one raw URL-safe token.
        if contents.endswith("\n"):
            contents = contents[:-1]
        elif contents.endswith("\r"):
            contents = contents[:-1]
        if not contents or not SESSION_CREDENTIAL_RE.fullmatch(contents):
            raise ClientError(
                "session_credential_file_invalid",
                "Scoped session credential file must contain one raw URL-safe token",
            )
        return contents

    @staticmethod
    def _read_dpapi_session_credential(path: str | Path) -> str:
        try:
            token = credential_store.read_generation(path)
            if not isinstance(token, bytes):
                raise TypeError
            decoded = token.decode("ascii")
        except Exception:
            # Credential store failures may contain implementation details; keep
            # all scoped-file errors sanitized and never include credential data.
            raise ClientError(
                "session_credential_file_invalid",
                "Could not read scoped session credential file",
            ) from None
        if not SESSION_CREDENTIAL_RE.fullmatch(decoded):
            raise ClientError(
                "session_credential_file_invalid",
                "Scoped session credential file must contain one raw URL-safe token",
            )
        return decoded

    def _broker_config(self) -> tuple[str, str]:
        try:
            config = json.loads(self.discovery_file.read_text(encoding="utf-8"))
        except FileNotFoundError as exc:
            raise ClientError(
                "discovery_file_missing",
                f"Broker discovery file not found: {self.discovery_file}",
            ) from exc
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise ClientError(
                "discovery_file_invalid",
                f"Could not read broker discovery file: {self.discovery_file}",
            ) from exc

        if not isinstance(config, dict):
            raise ClientError("discovery_file_invalid", "Broker discovery data must be a JSON object")

        port = config.get("port")
        if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
            raise ClientError("discovery_file_invalid", "Broker discovery data has an invalid port")

        use_environment = self.credential_file is _USE_ENVIRONMENT_CREDENTIAL_FILE
        credential_file = self.credential_file
        if use_environment:
            credential_file = os.environ.get(SESSION_CREDENTIAL_FILE_ENV)

        # Explicit None opts out of both scoped-file environment settings.
        if credential_file is None and not use_environment:
            token = config.get("token")
            if not isinstance(token, str) or not token:
                raise ClientError("discovery_file_invalid", "Broker discovery data has an invalid token")
        else:
            credential_format = os.environ.get(SESSION_CREDENTIAL_FORMAT_ENV, "plaintext")
            if credential_format not in {"plaintext", "dpapi"}:
                raise ClientError(
                    "session_credential_format_invalid",
                    "Scoped session credential format is unsupported",
                )
            if credential_file is None:
                if credential_format == "dpapi":
                    raise ClientError(
                        "session_credential_file_missing",
                        "DPAPI credential format requires a scoped credential file",
                    )
                token = config.get("token")
                if not isinstance(token, str) or not token:
                    raise ClientError(
                        "discovery_file_invalid",
                        "Broker discovery data has an invalid token",
                    )
            elif credential_format == "dpapi":
                token = self._read_dpapi_session_credential(credential_file)
            else:
                token = self._read_session_credential(os.fspath(credential_file))

        return f"http://127.0.0.1:{port}", token

    def _request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
        timeout: float | None = None,
    ) -> Any:
        base_url, token = self._broker_config()
        data = None if payload is None else json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
        }
        if data is not None:
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(f"{base_url}{path}", data=data, headers=headers, method=method)

        try:
            with _open_broker(request, timeout=self.timeout if timeout is None else timeout) as response:
                status = response.getcode()
                raw = response.read()
        except urllib.error.HTTPError as exc:
            status = exc.code
            raw = exc.read()
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            message = _redact(str(exc), token)
            raise ClientError("transport_error", message or "Could not reach the local broker") from exc

        try:
            body = json.loads(raw.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise ClientError(
                "invalid_response",
                f"Broker returned invalid JSON (HTTP {status})",
                status,
            ) from exc

        if not 200 <= status < 300:
            message = body.get("error") if isinstance(body, dict) else None
            if not isinstance(message, str) or not message:
                message = f"Broker request failed (HTTP {status})"
            raise ClientError("broker_error", _redact(message, token), status)

        return _redact(body, token)

    @staticmethod
    def _require_id(value: str, name: str) -> None:
        if not isinstance(value, str) or not value.strip():
            raise ClientError("invalid_argument", f"{name} must be a nonempty string")

    @staticmethod
    def _require_room_id(value: str, name: str) -> None:
        if not isinstance(value, str) or not ID_RE.fullmatch(value):
            raise ClientError("invalid_argument", f"{name} must be a valid ID")

    @staticmethod
    def _require_batch_id(batch_id: str) -> None:
        if not isinstance(batch_id, str):
            raise ClientError("invalid_argument", "batch_id must be a canonical UUID")
        try:
            parsed_batch_id = uuid.UUID(batch_id)
        except (ValueError, TypeError, AttributeError) as exc:
            raise ClientError("invalid_argument", "batch_id must be a canonical UUID") from exc
        if str(parsed_batch_id) != batch_id:
            raise ClientError("invalid_argument", "batch_id must be a canonical UUID")

    def register(self, session_id: str, kind: str) -> Any:
        self._require_id(session_id, "id")
        if kind not in {"codex", "claude", "system"}:
            raise ClientError("invalid_argument", "kind must be codex, claude, or system")
        return self._request(
            "POST",
            "/v1/register",
            {"id": session_id, "kind": kind, "opt_in": True},
        )

    def discover(self) -> Any:
        return self._request("GET", "/v1/sessions")

    def check_session(self, session_id: str) -> Any:
        """Verify this client's credential can act as one opted-in session."""
        self._require_id(session_id, "session_id")
        return self._request("GET", "/v1/sessions/check?" + urllib.parse.urlencode({"id": session_id}))

    def wait_for_messages(self, recipient: str, after: int, timeout: float = 25) -> Any:
        """Wait for newly queued messages; the cursor prevents repeated delivery."""
        self._require_id(recipient, "recipient")
        if isinstance(after, bool) or not isinstance(after, int) or after < 0:
            raise ClientError("invalid_argument", "after must be a nonnegative integer")
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not 0 <= timeout <= 25:
            raise ClientError("invalid_argument", "timeout must be between 0 and 25 seconds")
        query = urllib.parse.urlencode({"recipient": recipient, "after": after, "timeout": timeout})
        return self._request("GET", "/v1/messages/wait?" + query, timeout=self.timeout + timeout)

    def send(
        self,
        sender: str,
        recipient: str,
        text: str,
        message_id: str | None = None,
        reply_to: str | None = None,
    ) -> Any:
        self._require_id(sender, "sender")
        self._require_id(recipient, "recipient")
        if not isinstance(text, str) or not text.strip():
            raise ClientError("invalid_argument", "text must be a nonempty string")
        payload = {"sender": sender, "recipient": recipient, "text": text}
        if message_id is not None:
            if not isinstance(message_id, str):
                raise ClientError("invalid_argument", "message_id must be a canonical UUID")
            try:
                parsed_message_id = uuid.UUID(message_id)
            except (ValueError, TypeError, AttributeError) as exc:
                raise ClientError("invalid_argument", "message_id must be a canonical UUID") from exc
            if str(parsed_message_id) != message_id:
                raise ClientError("invalid_argument", "message_id must be a canonical UUID")
            payload["id"] = message_id
        if reply_to is not None:
            if not isinstance(reply_to, str) or not ID_RE.fullmatch(reply_to):
                raise ClientError("invalid_argument", "reply_to must be a valid message ID")
            payload["reply_to"] = reply_to
        return self._request(
            "POST",
            "/v1/messages",
            payload,
        )

    def send_group(
        self,
        sender: str,
        recipients: list[str],
        text: str,
        batch_id: str,
    ) -> Any:
        """Fan out to explicit recipients; reuse batch_id when retrying a request."""
        self._require_id(sender, "sender")
        self._require_id(batch_id, "batch_id")
        try:
            parsed_batch_id = uuid.UUID(batch_id)
        except (ValueError, TypeError, AttributeError) as exc:
            raise ClientError("invalid_argument", "batch_id must be a canonical UUID") from exc
        if str(parsed_batch_id) != batch_id:
            raise ClientError("invalid_argument", "batch_id must be a canonical UUID")
        if not isinstance(recipients, list) or not 1 <= len(recipients) <= MAX_GROUP_RECIPIENTS:
            raise ClientError(
                "invalid_argument",
                f"recipients must contain 1 to {MAX_GROUP_RECIPIENTS} session IDs",
            )
        for recipient in recipients:
            self._require_id(recipient, "recipient")
        if len(set(recipients)) != len(recipients):
            raise ClientError("invalid_argument", "recipients must be unique")
        if sender in recipients:
            raise ClientError("invalid_argument", "sender must not be a recipient")
        if not isinstance(text, str) or not text.strip():
            raise ClientError("invalid_argument", "text must be a nonempty string")
        return self._request(
            "POST",
            "/v1/group-messages",
            {
                "sender": sender,
                "recipients": recipients,
                "text": text,
                "batch_id": batch_id,
            },
        )

    def poll(self, recipient: str, status: str | None = None) -> Any:
        self._require_id(recipient, "recipient")
        if status not in (None, "queued"):
            raise ClientError("invalid_argument", "status must be queued when specified")
        messages = []
        after = None
        while True:
            query = urllib.parse.urlencode({"recipient": recipient, **({"after": after} if after is not None else {}),
                                            **({"status": status} if status else {})})
            page = self._request("GET", f"/v1/messages?{query}")
            if not isinstance(page, dict) or not isinstance(page.get("messages"), list):
                raise ClientError("invalid_response", "Broker returned an invalid inbox")
            messages.extend(page["messages"])
            next_after = page.get("next_after")
            if next_after is None:
                return {"messages": messages}
            if isinstance(next_after, bool) or not isinstance(next_after, int) or next_after <= (after or 0):
                raise ClientError("invalid_response", "Broker returned an invalid page cursor")
            after = next_after

    def ack(self, message_id: str, recipient: str) -> Any:
        self._require_id(message_id, "id")
        self._require_id(recipient, "recipient")
        return self._request(
            "POST",
            "/v1/ack",
            {"id": message_id, "recipient": recipient},
        )

    def message_status(self, recipient: str, message_id: str, actor: str | None = None) -> Any:
        self._require_id(recipient, "recipient")
        self._require_id(message_id, "message_id")
        query = {"recipient": recipient, "id": message_id}
        if actor is not None:
            self._require_id(actor, "actor")
            query["actor"] = actor
        return self._request("GET", "/v1/messages/status?" + urllib.parse.urlencode(query))

    def create_room(self, room_id: str, name: str, owner: str, members: list[str]) -> Any:
        """Create an explicitly named room with an explicit member list."""
        self._require_room_id(room_id, "room_id")
        self._require_room_id(owner, "owner")
        if not isinstance(name, str) or not name.strip() or len(name.strip()) > MAX_ROOM_NAME:
            raise ClientError(
                "invalid_argument",
                f"name must contain 1 to {MAX_ROOM_NAME} characters",
            )
        if not isinstance(members, list) or not 1 <= len(members) <= MAX_ROOM_MEMBERS:
            raise ClientError(
                "invalid_argument",
                f"members must contain 1 to {MAX_ROOM_MEMBERS} session IDs",
            )
        for member in members:
            self._require_room_id(member, "member")
        if len(set(members)) != len(members):
            raise ClientError("invalid_argument", "members must be unique")
        if owner not in members:
            raise ClientError("invalid_argument", "owner must be an explicit member")
        return self._request(
            "POST",
            "/v1/rooms",
            {"room_id": room_id, "name": name, "owner": owner, "members": members},
        )

    def room_info(self, room_id: str, member: str) -> Any:
        """Read room details as an explicitly named member."""
        self._require_room_id(room_id, "room_id")
        self._require_room_id(member, "member")
        query = urllib.parse.urlencode({"member": member})
        return self._request("GET", f"/v1/rooms/{room_id}?{query}")

    def update_room_member(self, room_id: str, actor: str, member: str, action: str) -> Any:
        """Ask the broker to add or remove a member using explicit session IDs."""
        self._require_room_id(room_id, "room_id")
        self._require_room_id(actor, "actor")
        self._require_room_id(member, "member")
        if not isinstance(action, str) or action not in ("add", "remove"):
            raise ClientError("invalid_argument", "action must be 'add' or 'remove'")
        return self._request(
            "POST",
            f"/v1/rooms/{room_id}/members",
            {"actor": actor, "member": member, "action": action},
        )

    def send_room_message(self, room_id: str, sender: str, text: str, batch_id: str) -> Any:
        """Fan out one idempotent message to the room's current members."""
        self._require_room_id(room_id, "room_id")
        self._require_room_id(sender, "sender")
        if not isinstance(text, str) or not text.strip() or len(text) > MAX_ROOM_MESSAGE_LENGTH:
            raise ClientError(
                "invalid_argument",
                f"text must contain 1 to {MAX_ROOM_MESSAGE_LENGTH} characters",
            )
        self._require_batch_id(batch_id)
        return self._request(
            "POST",
            f"/v1/rooms/{room_id}/messages",
            {"sender": sender, "text": text, "batch_id": batch_id},
        )

    def reply_room(self, room_id: str, sender: str, text: str, batch_id: str, reply_to: str) -> Any:
        """Reply to a message in a room, fanning out to its members."""
        self._require_room_id(room_id, "room_id")
        self._require_room_id(sender, "sender")
        self._require_room_id(reply_to, "reply_to")
        if not isinstance(text, str) or not text.strip() or len(text) > MAX_ROOM_MESSAGE_LENGTH:
            raise ClientError(
                "invalid_argument",
                f"text must contain 1 to {MAX_ROOM_MESSAGE_LENGTH} characters",
            )
        self._require_batch_id(batch_id)
        return self._request(
            "POST",
            f"/v1/rooms/{room_id}/reply-all",
            {"sender": sender, "text": text, "batch_id": batch_id, "reply_to": reply_to},
        )


class JsonArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise ClientError("invalid_arguments", message)


def _parser() -> argparse.ArgumentParser:
    parser = JsonArgumentParser(description="Use the local agent bridge broker.")
    commands = parser.add_subparsers(dest="command", required=True)

    register = commands.add_parser("register", help="register an explicitly named session")
    register.add_argument("--id", required=True)
    register.add_argument("--kind", choices=("codex", "claude"), required=True)

    commands.add_parser("discover", help="list registered sessions")

    send = commands.add_parser("send", help="send a message between explicit session IDs")
    send.add_argument("--sender", required=True)
    send.add_argument("--recipient", required=True)
    send.add_argument("--text", required=True)
    send.add_argument(
        "--message-id",
        help="stable canonical UUID; reuse it with identical request fields when retrying (otherwise retries may duplicate)",
    )
    send.add_argument("--reply-to", help="message ID being replied to")

    send_group = commands.add_parser(
        "send-group", help="fan out one idempotent message to explicit session IDs"
    )
    send_group.add_argument("--sender", required=True)
    send_group.add_argument("--recipient", dest="recipients", action="append", required=True)
    send_group.add_argument("--batch-id", required=True, help="canonical UUID reused for retries")
    send_group.add_argument("--text", required=True)

    poll = commands.add_parser("poll", help="list messages for an explicit recipient ID")
    poll.add_argument("--recipient", required=True)

    ack = commands.add_parser("ack", help="acknowledge a message for an explicit recipient ID")
    ack.add_argument("--id", required=True)
    ack.add_argument("--recipient", required=True)
    return parser


def _emit(value: dict[str, Any]) -> None:
    print(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")))


def main(argv: list[str] | None = None) -> int:
    try:
        args = _parser().parse_args(argv)
        client = BrokerClient()
        if args.command == "register":
            result = client.register(args.id, args.kind)
        elif args.command == "discover":
            result = client.discover()
        elif args.command == "send":
            send_options = {"reply_to": args.reply_to}
            if args.message_id is not None:
                send_options["message_id"] = args.message_id
            result = client.send(args.sender, args.recipient, args.text, **send_options)
        elif args.command == "send-group":
            result = client.send_group(args.sender, args.recipients, args.text, args.batch_id)
        elif args.command == "poll":
            result = client.poll(args.recipient)
        elif args.command == "ack":
            result = client.ack(args.id, args.recipient)
        else:  # argparse requires a subcommand, so this is defensive only.
            raise ClientError("invalid_arguments", "A command is required")
        _emit({"ok": True, "result": result})
        return 0
    except ClientError as exc:
        _emit(exc.as_json())
        return 2


if __name__ == "__main__":
    sys.exit(main())
