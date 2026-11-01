# Agent bridge: limits register

Date: 2026-11-01. Status: working document, not committed, not published.

This register lists every known limit of the two-chat bridge and decides what
to do about each one. It does not change code, the live bridge, or the public
repository.

## Plain-language summary

- **Daily use:** the two-chat bridge works for personal use. No code change is
  needed for that.
- **Before any upgrade, planned stop, or restore:** four work items must be
  done first, as one piece of work: stop-and-restart safety (items 1 and 2),
  a checked stop-then-copy backup step (item 3), paired database and
  credential backup and restore (item 4), and a tested rollback (item 8).
- **Do now:** a secret review of the public repository (item 12) and
  correcting out-of-date documents (item 13). Both are cheap, and the first
  matters because the repository is already public.
- **Deferred:** five-chat and group routing (item 9), a stronger
  backup-freshness guarantee (part of item 6), and the repeatability of idle
  wake-ups (item 10).

## Dispositions

Key: **ACCEPT** = acceptable for the personal two-chat tool, with the stated
boundary. **FIX-BEFORE-RELIANCE** = must be done before we depend on it for
upgrades, restores, or unattended use. **DEFER** = out of scope now, with a
reopen trigger. **DO NOW** = cheap and worth doing immediately.

| #   | Limit (plain words)                                                                                                                                                                      | Disposition                                | Why                                                                                                                                                         | Reopen or next step                                                                                          |
| --- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| 1   | If the stop note cannot be saved and the broker then exits, the auto-restarter can bring it back during a planned stop. A process that outlives a timed-out drain can also be restarted. | FIX-BEFORE-RELIANCE                        | Breaks any planned stop, backup, or upgrade. Normal messaging is unaffected.                                                                                | Before any planned stop, upgrade, or restore. Until then: stop guard, then supervisor, then broker, by hand. |
| 2   | The stop-intent, drain, and fence prototypes are not wired into the real broker, supervisor, or launchers. Their passing tests protect nothing live.                                     | FIX-BEFORE-RELIANCE (same work item as 1)  | These prototypes are the fix for item 1. Count items 1 and 2 once.                                                                                          | Done together with item 1.                                                                                   |
| 3   | Other programs can write to the database while it is copied.                                                                                                                             | FIX-BEFORE-RELIANCE, narrowed              | Only a checked "stop everything, then copy" step is needed. Blocking hostile writers is not needed; same-user software is covered by the item 5 boundary.   | Before any backup described as consistent.                                                                   |
| 4   | Database and matching credential files cannot yet be backed up and restored as one set. A restore of one without the other can lock profiles out.                                        | FIX-BEFORE-RELIANCE                        | Needed before the bridge holds anything you could not recreate.                                                                                             | Until then, treat bridge data as disposable. Include the human-confirmed restore rule from item 6 here.      |
| 5   | Credentials identify configured profiles, not verified chats. Same-user software can read the protected credential files.                                                                | ACCEPT                                     | One trusted Windows user on a private PC. Make no stronger identity claim.                                                                                  | Reopen if a second user, a second machine, or untrusted software is on the PC. Documentation fix only.       |
| 6   | A counter plus a file cannot prove that a backup is the latest. A stale backup could be restored unnoticed.                                                                              | FIX-BEFORE-RELIANCE, minimal               | Cheap fix: a human confirms the backup is the most recent before any restore, and the latest quiet copy is kept. A stronger anti-replay design is deferred. | Before the first real restore. Reopen the stronger design if backups leave the machine.                      |
| 7   | A power cut may lose the last few messages or the stop note.                                                                                                                             | ACCEPT                                     | Bridge history is not the only record of anything important.                                                                                                | Reopen if bridge history becomes the only copy of something important.                                       |
| 8   | The old broker cannot serve the new database layout, and an old backup loses newer messages. Rollback is not proven.                                                                     | FIX-BEFORE-RELIANCE (upgrade gate)         | Blocks upgrading the live bridge. Keep the bridge stopped if no compatible rollback exists.                                                                 | Any request to switch the live bridge to the new build.                                                      |
| 9   | Five-chat and group routing is tested only with fake proxies. Speed and memory are unmeasured.                                                                                           | DEFER                                      | Separate milestone; the current use is two chats.                                                                                                           | Reopen when a third real chat or group use is wanted.                                                        |
| 10  | Idle wake-up worked once and is not proven repeatable. A queued message is not a delivered message.                                                                                      | ACCEPT (rule); repeatability DEFER         | Rule: important exchanges require an acknowledgement.                                                                                                       | Reopen for unattended or time-critical use; then run a repeated wake-up test.                                |
| 11  | Retries sent without a message ID can duplicate messages. Stable IDs already exist.                                                                                                      | FIX-BEFORE-RELIANCE (small)                | Make the clients always supply an ID. Small change, and it removes the duplicate risk for new traffic.                                                      | Before anyone else uses the bridge, and before any automated retry loop.                                     |
| 12  | The repository is public. The secret review is incomplete. License is CC0 and AI authorship is recorded. Third-party code provenance has only a license-marker grep.                     | DO NOW (secret review); DEFER (provenance) | A leaked secret cannot be undone, so this is the one item with possible present harm. Provenance matters only for promotion beyond "experimental".          | Secret review: now, then on every push. Provenance: before a release tag or any wider promotion.             |
| 13  | Out-of-date documents: "no public repository" and "uncommitted" statements are false. The live build is not recorded beside the published build.                                         | DO NOW (docs only)                         | Costs nothing; misleads readers.                                                                                                                            | Next housekeeping step. Add dated corrections and a published-versus-running table.                          |

## Where the reviews disagreed, and what was chosen

- **Item 6 (stale backup):** the personal-use review said ACCEPT; the
  safety review said FIX; the minimal-scope review said DEFER. Chosen: FIX,
  minimal, folded into item 4 as a human-confirmed restore rule. It costs a
  checklist step, not infrastructure.
- **Item 11 (duplicate retries):** the personal-use review said ACCEPT; the
  safety and minimal-scope reviews said FIX, small. Chosen: FIX, small. Stable
  IDs already exist, so the change is to always supply one.
- **Item 3 (database writers):** reviews agreed the full fencing design is too
  large. Chosen: only the checked stop-then-copy step, as stated above.
- **Item 12 (secret review):** the personal-use review classed it as a
  later item; the safety review said do it now. Chosen: do it now, because the
  repository is already public and a leak cannot be undone.

## Gate before any upgrade or restore

Items 1, 2, 3, 4, 6 (restore rule), and 8 must be completed as one work
item, with pass/fail evidence for each, before the live bridge is switched to
a new build or before any restore is relied on. Everything else is accepted,
deferred, or documentation.

## Scope note

Phase 2 remains deferred as the owner decided. The gate above is the smallest
set of items needed before upgrade or restore. It does not include five-chat
support, identity proof, malware resistance, or power-loss durability.

## Verification status

- Dispositions come from three independent reviews (personal use, safety,
  minimal scope) read against RELEASE_CANDIDATE.md, ROLLOUT.md and the
  decision answer. The reviews did not grep the code.
- No test was run for this register. Nothing was changed in code, the live
  bridge, git history, or GitHub.
