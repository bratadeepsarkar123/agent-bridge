# Local agent bridge experiment

The local broker stores explicitly addressed messages for participating Claude Code
and Codex sessions. The Claude adapter pushes channel events into a session; the
Codex MCP adapter exposes an inbox, send, and acknowledgement tools. Messages are
agent content, not instructions from the user. This is an experiment, not yet a
universal live-chat connection.

## Current test

- Claude session: `24f2b207-1068-4fff-88b3-97227d2a962f`
- Codex thread: `01a18ed9-5766-7ef1-9328-67ed7465809a`
- A live exchange on these exact sessions confirmed Claude acknowledgement and
  automatic Claude-to-Codex delivery as a native tool event.
- The current Codex thread has a native delivery worker. A Claude message was
  held until this thread became idle, then started a new turn as a native agent
  event and was acknowledged. Delivery to an active turn has also been tested.
- A second loaded Codex thread replied directly using Codex's native thread
  messaging. This does not establish delivery to unloaded threads.

## Runtime

`broker.py` runs on loopback and stores registered session IDs and messages in
`broker.sqlite3`. `broker-session.json` contains a bearer token and port: keep the
workspace private, do not share that file, and do not expose the listener. All
local clients with that token share authority, so sender names are labels, not
cryptographically verified identities. Registration is not an online signal.

In this routing branch, registration is an explicit participation opt-in. Legacy
registrations are not automatically trusted on database migration: each session
must opt in again, and old queued messages remain hidden until their sender and
recipient have opted in. Each send names its destination; launching Claude does
not register another session or start its native delivery worker. The shared
bearer token does not authenticate the claimed session identity, so opt-in relies
on trusted local clients. Queued messages still do not establish delivery or
whether a session is online.

The broker currently runs as a background process. A supervisor watches for a
broker crash and attempts to recover the same port and token. It does not restart
Claude, recover an intentionally stopped broker, or start after a machine reboot.
New adapter versions read the current discovery file again on each request.
The local service guard watches the broker supervisor, native Codex worker, and
the two monitoring workers; it restarts those services if they stop. It never
starts or restarts an agent chat. The guard itself does not start after reboot.

`launch-claude.ps1` is an opt-in launcher that creates a new Claude session when
no Claude ID is given, or resumes an explicit stopped session ID. It merges its
bridge MCP configuration with ordinary session configuration and does not change
the default model unless `-Model` or `-Effort` is supplied. Claude's development channel and project trust
screens can require interaction when launching or resuming. It is not safe to
assume that an unattended restart will clear those prompts. The currently running
Claude session should not be restarted while the user is away.

Codex has a global `local_agent_messaging` MCP configuration for future sessions.
Current sessions do not automatically acquire new tools. A Codex participant must
register its actual thread ID, opt in via `bridge_enable_native`, and acknowledge
each message after handling it. The worker targets loaded threads and does not
bypass approval or user-input waits. `codex_client.py` provides a CLI inbox.
`status.py` inspects broker reachability and counts for explicit session IDs,
without printing message contents. Inbox retrieval uses pagination; a registered
ID is not proof that its chat is running.
`project_status.py` summarizes the current test setup and guarded services. Its
Claude status covers the original monitored PID only, not later resumptions.

`codex_queue.py` is an optional fallback for forwarding queued broker messages
as follow-up input in an existing Codex thread. It requires an explicit thread
UUID. Without `--deliver`, it only reports what it would forward. With
`--deliver`, it runs `codex queue` using that UUID; the user-facing status
is **“queued as follow-up input (not native agent message)”**. This is not a
native message to an already-running agent. The adapter never acknowledges the
broker message. A local SQLite ledger records each message UUID before launch
and prevents another attempt after accepted, uncertain, or interrupted output.

Example (delivery stays disabled unless `--deliver` is included):

```powershell
python codex_queue.py --recipient 123e4567-e89b-12d3-a456-426614174000
```

`monitor.py` is a separate opt-in, report-only process check. Run it explicitly
with one or more `--pid` arguments; add `--interval SECONDS` to poll repeatedly,
and optionally `--checks COUNT` to limit polling. It emits JSON events only when
a PID is first observed or its process status changes. Process presence does not
establish agent health, so every event reports `agent_health: "unknown"`. It
does not discover sessions, send messages, or control processes. Its earlier log,
`monitor-events.ndjson`, covers the previous test session.
A separate watcher queues labeled `system:process-monitor` alerts to the designated
Codex inbox on process-state changes. It retries broker failures with a stable
message ID. The native worker can deliver alerts to a loaded Codex thread. It does
not restart Claude or judge agent health.

`watch_errors.py` watches only new structured tool-error records in the explicitly
configured Claude transcript. Alerts omit tool output and are rate-limited to one
per five minutes. They are queued to that Claude session; a closed or disconnected
Claude session will not receive them until it runs with the channel enabled.

## Delivery guarantees and limits

`queued` means stored; it does not mean delivered or read. Claude explicitly
acknowledges receipt. Unacknowledged messages may be replayed after the adapter
restarts; consumers must avoid duplicate effects. The queue survives broker
restarts, but there is no expiry policy, end-to-end encryption, per-session
identity authentication, automatic startup after reboot, automatic resumption of
closed chats, or general-purpose monitoring yet. Only the one Claude and one Codex session above
have been tested live for the Claude-to-Codex bridge; multi-recipient routing
has been tested locally with simulated sessions.

The source includes an explicit-recipient group fan-out endpoint. It stores one
message and acknowledgement per recipient atomically, and a caller-supplied batch
UUID makes retries idempotent. This is fan-out, not yet a shared group conversation:
recipients do not automatically receive a membership list or reply-all route. The
currently running broker predates this endpoint; it becomes available on a later
broker restart. Existing sessions may also need their adapters reloaded to expose
new tools.

## Checks

Run `python -B -m unittest -v test_broker test_claude_adapter test_integration tests.test_codex_client tests.test_status tests.test_monitor tests.test_codex_queue tests.test_supervisor tests.test_watch_session tests.test_watch_errors tests.test_codex_native tests.test_native_worker tests.test_codex_mcp tests.test_lockfile tests.test_service_guard`
from this directory. These tests use isolated local data and no model inference.
