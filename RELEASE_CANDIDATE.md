# Bridge candidate review

This is a review copy, not a release. The branch is `bridge/integrated-plan`,
based on `f57d010`; changes and new files are uncommitted. There is no push.

The existing live two-chat experiment previously showed an exchange between the
authorized Claude and Codex sessions. This review copy is a separate worktree;
its new features are not active in that live bridge. A queued message alone does
not prove a chat received it. This review did not repeat the live exchange.

For the existing two-chat experiment, loaded-thread eligibility, setup and
retry behavior remain practical limits. Switching that experiment to this build
additionally requires the coordinated shutdown and recovery work below. Real
five-chat routing, group use and their performance measurements are later goals.

## Checks

The latest full run passed **403 tests**: 65 root tests and 338 tests in
`tests/`, including the optional isolated app-server encoding check. A prior run
found a timing-dependent dispatcher test assertion, which was corrected before
this run. The app-server check does not exercise a real chat or prove idle wake.

## Known limits

- The isolated broker has drain and recovery experiments, but broker, supervisor,
  guard and launchers do not yet share a complete safe stop/restart protocol.
  A failed stop-marker write followed by broker exit can allow auto-recovery.
- Scoped credential setup and recovery, paired database/credential backup,
  writer fencing, rollback freshness, Windows power-loss durability and launcher
  handoff are not complete. See `ROLLOUT.md` and `SNAPSHOT_FRESHNESS.md`.
- Credentials authorize configured profiles; they do not prove a real chat
  identity or protect against other code running as the same Windows user.
- Multi-recipient routing and group features use synthetic sessions and fake
  proxies in tests. Five real chats, group delivery, real approval flows and
  live multi-session resource usage have not been proven.
- The dispatcher uses one worker and broker wait per recipient. Shared
  subscriptions do not eliminate all per-message proxy processes; five-chat
  latency and memory have not been benchmarked.
- Caller-supplied message IDs support safe retries. Legacy no-ID retries can
  duplicate messages. Native Codex delivery requires a loaded, eligible thread;
  `bridge_wait` cannot wake an idle thread on its own.
- The compact policy and lifecycle-intent prototype are isolated and unwired.
  They rely on a trusted caller and do not prove external writers have stopped.
- Public-release uniqueness, licensing, secret review and maintenance have not
  been established by this candidate; no public repository was created.

## Commit and rollback

A future commit would need an explicit review of the source, tests and docs
listed below. The modified tracked `tests/__pycache__/test_codex_client.cpython-310.pyc`
is a generated artifact and should not be included without a separate reason.
No files have been staged by this review.

The live experiment was not switched to this worktree, so no live rollback was
performed. A clean rollback of a future live upgrade is **not proven**: the old
broker cannot safely serve the upgraded schema, and an old backup may lose
new messages or credential state. Keep the service stopped if no compatible
recovery path is available; do not replace live data from this note.

## Git-visible file inventory

The inventory below includes the tracked base and untracked candidate files,
including this note; it excludes ignored runtime files. Status is a snapshot.

```text
.gitignore
RELEASE_CANDIDATE.md
ROLLOUT.md
SNAPSHOT_FRESHNESS.md
credential_store.py
lifecycle_intent_prototype.py
lifecycle_intent_test_service.py
lifecycle_policy.py
measure_resources.ps1
message_status.py
multi_dispatcher.py
persistent_subscription.py
shared_subscription.py
snapshot_freshness.py
stop_helper.py
synthetic_service.py
synthetic_stop.py
tests/test_app_server_proxy_transport.py
tests/test_backup_writer_fence.py
tests/test_broker_cooperative_stop.py
tests/test_broker_http_drain.py
tests/test_broker_long_poll_shutdown.py
tests/test_broker_slow_shutdown.py
tests/test_broker_startup_shutdown.py
tests/test_client_lost_send_response.py
tests/test_credential_store.py
tests/test_direct_retry_conflicts.py
tests/test_direct_retry_persistence.py
tests/test_dpapi_backup_restore_rehearsal.py
tests/test_lifecycle_intent_prototype.py
tests/test_lifecycle_policy.py
tests/test_mcp_appserver_e2e.py
tests/test_mcp_stdio_subprocess.py
tests/test_message_status.py
tests/test_multi_dispatcher.py
tests/test_persistent_subscription.py
tests/test_redirect_security.py
tests/test_rollback_rehearsal.py
tests/test_scoped_e2e.py
tests/test_session_credentials.py
tests/test_shared_subscription.py
tests/test_snapshot_freshness.py
tests/test_sqlite_backup_restore_rehearsal.py
tests/test_startup_schema_guard.py
tests/test_stop_helper.py
tests/test_stop_recovery_fence.py
tests/test_synthetic_stop.py
README.md
bridge.py
broker.py
claude_adapter.py
codex_client.py
codex_mcp.py
codex_native.py
codex_queue.py
launch-claude.ps1
lockfile.py
monitor.py
native_worker.py
project_status.py
service_guard.py
start-broker.ps1
start-current-test.ps1
start-visible.ps1
status.py
supervisor.py
test_broker.py
test_claude_adapter.py
test_integration.py
tests/__pycache__/test_codex_client.cpython-310.pyc
tests/test_codex_client.py
tests/test_codex_mcp.py
tests/test_codex_native.py
tests/test_codex_queue.py
tests/test_lockfile.py
tests/test_monitor.py
tests/test_native_worker.py
tests/test_service_guard.py
tests/test_status.py
tests/test_supervisor.py
tests/test_watch_errors.py
tests/test_watch_session.py
watch_errors.py
watch_session.py
```

## Changed and untracked paths

```text
 M README.md
 M broker.py
 M claude_adapter.py
 M codex_client.py
 M codex_mcp.py
 M codex_native.py
 M native_worker.py
 M service_guard.py
 M supervisor.py
 M test_broker.py
 M test_claude_adapter.py
 M test_integration.py
 M tests/__pycache__/test_codex_client.cpython-310.pyc
 M tests/test_codex_client.py
 M tests/test_codex_mcp.py
 M tests/test_codex_native.py
 M tests/test_native_worker.py
 M tests/test_service_guard.py
?? .gitignore
?? RELEASE_CANDIDATE.md
?? ROLLOUT.md
?? SNAPSHOT_FRESHNESS.md
?? credential_store.py
?? lifecycle_intent_prototype.py
?? lifecycle_intent_test_service.py
?? lifecycle_policy.py
?? measure_resources.ps1
?? message_status.py
?? multi_dispatcher.py
?? persistent_subscription.py
?? shared_subscription.py
?? snapshot_freshness.py
?? stop_helper.py
?? synthetic_service.py
?? synthetic_stop.py
?? tests/test_app_server_proxy_transport.py
?? tests/test_backup_writer_fence.py
?? tests/test_broker_cooperative_stop.py
?? tests/test_broker_http_drain.py
?? tests/test_broker_long_poll_shutdown.py
?? tests/test_broker_slow_shutdown.py
?? tests/test_broker_startup_shutdown.py
?? tests/test_client_lost_send_response.py
?? tests/test_credential_store.py
?? tests/test_direct_retry_conflicts.py
?? tests/test_direct_retry_persistence.py
?? tests/test_dpapi_backup_restore_rehearsal.py
?? tests/test_lifecycle_intent_prototype.py
?? tests/test_lifecycle_policy.py
?? tests/test_mcp_appserver_e2e.py
?? tests/test_mcp_stdio_subprocess.py
?? tests/test_message_status.py
?? tests/test_multi_dispatcher.py
?? tests/test_persistent_subscription.py
?? tests/test_redirect_security.py
?? tests/test_rollback_rehearsal.py
?? tests/test_scoped_e2e.py
?? tests/test_session_credentials.py
?? tests/test_shared_subscription.py
?? tests/test_snapshot_freshness.py
?? tests/test_sqlite_backup_restore_rehearsal.py
?? tests/test_startup_schema_guard.py
?? tests/test_stop_helper.py
?? tests/test_stop_recovery_fence.py
?? tests/test_synthetic_stop.py
```
