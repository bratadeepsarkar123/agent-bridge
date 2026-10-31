"""CLI client for the local agent bridge HTTP broker."""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path
from typing import Any


DISCOVERY_FILE = Path(__file__).with_name("broker-session.json")
DEFAULT_TIMEOUT = 10.0
MAX_GROUP_RECIPIENTS = 100


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
    ) -> None:
        self.discovery_file = Path(discovery_file)
        self.timeout = timeout

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
        token = config.get("token")
        if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
            raise ClientError("discovery_file_invalid", "Broker discovery data has an invalid port")
        if not isinstance(token, str) or not token:
            raise ClientError("discovery_file_invalid", "Broker discovery data has an invalid token")

        return f"http://127.0.0.1:{port}", token

    def _request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
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
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
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

    def register(self, session_id: str, kind: str) -> Any:
        self._require_id(session_id, "id")
        if kind not in {"codex", "claude", "system"}:
            raise ClientError("invalid_argument", "kind must be codex, claude, or system")
        return self._request("POST", "/v1/register", {"id": session_id, "kind": kind})

    def discover(self) -> Any:
        return self._request("GET", "/v1/sessions")

    def send(self, sender: str, recipient: str, text: str, message_id: str | None = None) -> Any:
        self._require_id(sender, "sender")
        self._require_id(recipient, "recipient")
        if not isinstance(text, str) or not text.strip():
            raise ClientError("invalid_argument", "text must be a nonempty string")
        payload = {"sender": sender, "recipient": recipient, "text": text}
        if message_id is not None:
            payload["id"] = message_id
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

    def poll(self, recipient: str) -> Any:
        self._require_id(recipient, "recipient")
        messages = []
        after = None
        while True:
            query = urllib.parse.urlencode({"recipient": recipient, **({"after": after} if after is not None else {})})
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
            result = client.send(args.sender, args.recipient, args.text)
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
