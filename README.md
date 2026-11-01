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

This worktree integrates explicit session opt-in, event-driven notifications,
and named rooms with reply-all. The live experiment does not automatically use
this worktree. A legacy registration without `opt_in` is accepted only as a
heartbeat for a session already opted in under the same ID and kind. Registration
and room membership are routing controls, not authenticated sender identities.
The bounded long-poll endpoint lets consumers wait without repeated short polls;
`multi_dispatcher.py` can serve multiple explicitly opted-in Codex threads in
one process. Existing workers for those threads must stop before it starts.
By default, closed chats are not reopened automatically.

For a specifically authorized thread, `native_worker.py --recipient <thread-id>
--keep-loaded` explicitly permits resuming that thread and keeps a dedicated
app-server subscription open. Reconnects may resume the same saved thread; a
missing rollout is reported unavailable and never replaced with a new thread.
This opt-in mode uses an additional proxy process per recipient and has not been
live-tested. The default worker does not resume unloaded threads. Pending human
approval remains a separate concern; the holder never responds to approval
requests.

For several explicitly opted-in Codex threads, `multi_dispatcher.py` accepts
repeated `--recipient` arguments. Its optional `--keep-loaded` flag shares one
app-server subscription connection across the selected threads; the flag may
resume those exact saved threads. This mode is tested with simulated app-server
traffic and has not been activated for live chats.

`message_status.py --recipient <thread-id> --id <message-id>` reports broker
acknowledgement separately from Codex submission. Submission is not proof of
handling or reply. `measure_resources.ps1` samples helper CPU and memory on
Windows; pass `-ProjectPath` to measure a different bridge checkout.

The broker also supports optional scoped profile credentials. A credential can
be provisioned only by trusted local code; the broker stores its hash and rejects
claims on that protected profile from legacy or differently scoped clients.
Clients may read the credential from `BRIDGE_SESSION_CREDENTIAL_FILE`; a pooled
dispatcher can instead use repeated `--credential <session-id>=<file>` arguments.
On Windows, trusted local code can use `credential_store.provision_initial_profile`
to create a user-bound DPAPI encrypted credential and bind its hash to a profile.
The caller must supply a private directory with restrictive Windows ACLs. Set
`BRIDGE_SESSION_CREDENTIAL_FORMAT=dpapi` when reading that encrypted file.
Automatic assignment to chats and credential rotation are not implemented.
A credential identifies a configured profile, not an independently verified
Codex thread, and it does not protect against software running as the same OS
user. Existing unprotected participants retain legacy shared-token behavior.

The broker CLI rejects an existing older schema by default; a planned migration
requires `--allow-legacy-migration`. This schema check cannot detect an old
snapshot that already has the current schema or prove a backup is complete.

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

`broker.py --control-stdin` has an isolated cooperative HTTP drain for a launcher-
owned private pipe. It first publishes a flushed recovery-inhibit marker, then
`stop` or pipe EOF closes application admission, rejects new requests with 503,
and waits up to `--drain-timeout` for tracked handlers. Once marker publication
succeeds, the supervisor's recovery path is fenced while that marker remains,
including after a failed drain. If marker publication fails, drain is refused;
if the broker then exits with discovery state retained and no marker, the
supervisor may still treat it as a crash and auto-recover. That publication
failure remains a rollout blocker. A failed drain reports failure and retains
state and marker; it does not perform a second drain wait. Non-daemon handlers
can keep the broker process alive until they finish. A successful drain removes
discovery state and the marker, though final bounded server/process cleanup may
still be needed. The listener remains open during the wait, so new connections
can still arrive and be rejected and may delay completion. Focused isolated tests
cover an admitted long poll, a short POST body, slow-trickling headers with a
1-second hard deadline, startup EOF, pipe EOF, blocked work, rejection of new
requests, failed-drain behavior, and the recovery fence. These tests do not
establish a coordinated live stop/restart mechanism. A sustained stream of new
arrivals is not tested and may delay shutdown. Marker publication does not
guarantee that Windows directory metadata survives power loss or rollback.

`lifecycle_intent_prototype.py` tests supervisor-owned stop intent using only
synthetic child processes. It is not connected to the live broker or launcher.

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
is **â€œqueued as follow-up input (not native agent message)â€**. This is not a
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

The broker, Codex Python client, CLI (`--message-id`), and Codex and Claude
`bridge_send` MCP tools support an optional caller-supplied stable canonical UUID.
Reuse the same UUID with the same sender, recipient, text, and `reply_to` on a
retry; the broker returns the existing message for identical content and rejects
reuse with different content. If a send omits the UUID, the broker generates a
new one, so retrying after an
uncertain response can enqueue a duplicate; no-ID sends are not retry-safe.

Named rooms provide explicit membership and reply-all, using an atomic message
and acknowledgement per recipient. Batch UUIDs make retries idempotent. Five
simulated participants have been tested; live multi-session delivery has not.
The running broker and adapters predate these features and need an appropriate
upgrade or reload before they become available in existing chats.

## Checks

Run `python -B -m unittest -v test_broker test_claude_adapter test_integration tests.test_codex_client tests.test_status tests.test_monitor tests.test_codex_queue tests.test_supervisor tests.test_watch_session tests.test_watch_errors tests.test_codex_native tests.test_native_worker tests.test_codex_mcp tests.test_lockfile tests.test_service_guard tests.test_message_status tests.test_multi_dispatcher tests.test_persistent_subscription tests.test_shared_subscription tests.test_app_server_proxy_transport tests.test_mcp_stdio_subprocess tests.test_rollback_rehearsal tests.test_session_credentials tests.test_credential_store tests.test_scoped_e2e tests.test_redirect_security tests.test_sqlite_backup_restore_rehearsal tests.test_startup_schema_guard tests.test_stop_helper tests.test_synthetic_stop tests.test_backup_writer_fence tests.test_dpapi_backup_restore_rehearsal tests.test_broker_cooperative_stop tests.test_broker_http_drain tests.test_broker_slow_shutdown tests.test_broker_startup_shutdown tests.test_broker_long_poll_shutdown tests.test_direct_retry_conflicts tests.test_direct_retry_persistence tests.test_snapshot_freshness tests.test_stop_recovery_fence`
from this directory. These tests use isolated local data and no model inference.

Also run `python -B -m unittest -v tests.test_lifecycle_intent_prototype tests.test_lifecycle_policy tests.test_client_lost_send_response`
for the isolated lifecycle policy, prototype and lost-response client checks.

For the optional Windows encoding regression against a real isolated app-server:

```powershell
$env:BRIDGE_RUN_APP_SERVER_E2E='1'
python -B -m unittest -v tests.test_mcp_appserver_e2e
```

It starts no chats and uses no model inference.

## License

The code in this repository is released under the Creative Commons CC0 1.0 Universal dedication (see [LICENSE](LICENSE)). To the extent possible under law, the author has waived copyright and related rights.

The code was written with AI tools. Under current US Copyright Office guidance, purely AI-generated material may not be copyrightable, so this dedication states that no copyright is claimed. CC0 gives no warranty and no express patent license.

Releases before this change were published under Apache License 2.0; those tags keep the Apache terms.
