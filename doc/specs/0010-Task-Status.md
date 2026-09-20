# Background Task Status

The application runs six kinds of work in the background: backups, snapshot
pruning, restore testing, database scrubbing, pack pruning, and workspace
cleanup. When any of them fails, the failure is recorded by the
`ErrorRepository` (see @doc/specs/0003-Error-Capture.md) and surfaced on the
errors page. When any of them *succeeds*, nothing is recorded anywhere but the
log file. The user therefore has no way to answer the most basic question about
the background tasks: did they run, and what did they do?

## Problem Statement

- An empty errors page is ambiguous. It means either "everything ran and was
  fine" or "nothing has run at all", and those are very different situations.
  The second one is precisely the failure mode that silent background work is
  prone to, and it currently looks identical to success.
- There is no record of when any task last ran. `ScheduleSupervisor::started()`
  (@server/src/tasks/schedule.rs) arms every periodic task with
  `ctx.run_interval`, which keeps its state in memory and is re-armed from zero
  at each process start.
- Because `run_interval` fires only *after* one full interval has elapsed, a
  server that restarts more often than every seven days will **never** run the
  database scrub, the pack prune, or the restore test. Nothing in the
  application reports this, and from the outside it is indistinguishable from
  those tasks running cleanly.
- The work performed is not reported even when it happens. `prune_packs()` may
  delete hundreds of packs and reclaim gigabytes; the only trace is an `info!`
  line. `prune_snapshots()` returns the number of snapshots pruned and the
  caller in `LeaderSupervisor::process_prune()` discards it.
- Restore testing can silently do nothing. `RestorerImpl::restore_test()`
  returns `Ok` when there is no dataset with a snapshot, or no file small
  enough to satisfy `RESTORE_TEST_MAX_FILE_MB`. A recorded "success" in that
  case would be actively misleading, since verifying the restore path is the
  entire purpose of the task.

The existing prune status is the one partial exception: `RingLeaderImpl::prunes()`
(@server/src/tasks/leader.rs) keeps the last 32 `prune::Request` objects with
start and finish timestamps. It is not on the `RingLeader` trait, is not exposed
via GraphQL, and is lost on restart.

## Requirements

- The most recent run of each kind of background task is remembered across
  server restarts.
- For each task the user can see when it last ran, how long it took, whether it
  succeeded, and a short summary of what it did.
- A task that has never run is shown as such, rather than being omitted.
- A task that ran but had nothing to do is distinguishable from one that ran and
  did work.
- The status is viewed on demand, not pushed at the user. The information is
  reassuring rather than urgent; errors remain the urgent channel.
- Recording a run must never interfere with the run itself. As with
  `capture_error`, a failure to record is logged and swallowed.
- The recording must be mockable so that the leader tests can assert on it.

## Proposal

### Storage

Add a `task_runs` table to the **existing** SQLite database used by the
`ErrorRepository` (@server/src/data/repositories/errors.rs). The reasoning from
0003 applies unchanged — this is structured, low-volume, operational data that
is orthogonal to the backup and restore workflow — and reusing that database
costs nothing:

- The schema is created by a single `CREATE TABLE IF NOT EXISTS` batch executed
  on every open, so adding a table is a no-op for existing installations. There
  is no migration machinery to extend.
- By contrast, putting this in `EntityDataSource` would require implementing it
  twice, once for the RocksDB CBOR encoding and once for the SQLite normalized
  schema, and `verify_schema_version()` (@server/src/data/sources.rs) hard-errors
  on any version mismatch, telling the user to wipe `DB_PATH`. That is an absurd
  price for a status display.
- The database already has an `ERROR_DB_PATH` and a connection held behind a
  mutex for the process lifetime. The status data has the same access pattern:
  rare writes, rare reads.

Store **one row per operation kind**, keyed by operation, replaced on each run.
That is at most six rows and it satisfies the requirement exactly, with no
retention logic needed. Snapshot pruning and backups are per-dataset, so their
key is the operation plus the dataset id. Should a run history become desirable
later, dropping the unique key and adding a limit to the query is the whole
change.

Proposed columns:

| Column | Notes |
|---|---|
| `operation` | reuses the existing `ErrorOperation` taxonomy |
| `dataset_id` | `NULL` for the four global tasks |
| `started_at` | RFC 3339, as in the `errors` table |
| `finished_at` | RFC 3339; duration is derived, not stored |
| `outcome` | see below |
| `issue_count` | number of `ScrubIssue`s recorded for this run |
| `summary` | human-readable description of the work performed |

### Outcome

A three-valued success is not enough; four are needed:

- `SUCCESS` — ran, did its work, no problems.
- `ISSUES` — ran to completion but recorded one or more `ScrubIssue`s. The
  issues themselves are already on the errors page; `issue_count` links the two.
- `FAILED` — returned `Err`, the run did not complete.
- `SKIPPED` — ran but had nothing to do, with the reason in `summary`. This
  exists primarily for restore testing, where `Ok` does not imply that anything
  was verified.

### Summary content

Three of the four scan operations currently return `Result<Vec<ScrubIssue>, Error>`
(@server/src/tasks/prune.rs), which carries no count of the work performed. With
no further change, the best available summary is a duration and an issue count —
which does answer most of the question. To answer "what was the result of the
last prune" properly, widen those returns to carry the counts alongside the
issues:

- `database_scrub` — records checked, by kind.
- `prune_packs` — packs deleted, database archives removed.
- `cleanup_workspaces` — files removed, bytes freed.
- `prune_snapshots` already returns the count pruned; it only needs the caller
  to stop discarding it.

This touches the `Pruner` trait, its implementation, the four leader handlers,
and the `MockPruner` expectations in the existing tests.

Bytes reclaimed by pack pruning would be the most interesting number here, but
it is not available: `Pack` records a digest, locations, and an upload time, not
a size, and there is no reverse index from a pack to the chunks it holds.
Totalling it would mean asking each store. Counts of objects and records
deleted are cheap and honest, so those are what the summary reports.

### Recording sites

The four global tasks are already uniform in shape in `LeaderSupervisor`
(@server/src/tasks/leader.rs) — `restore_test`, `database_scrub`, `prune_packs`,
and `cleanup_workspaces` each end with the same `match { Ok(issues) => capture
each, Err(err) => capture one }` block. Each gains a start timestamp and a
`record_run` call, a sibling to `LeaderContext::capture_error` with the same
never-propagate-failures discipline.

Snapshot pruning is recorded in `process_prune`, which handles one dataset per
request and so writes a row keyed by operation and dataset id.

Backups are the open question below.

### Scheduling

With last-run timestamps persisted, `ScheduleSupervisor::started()` can consult
them at startup and immediately run anything already overdue, instead of arming
a timer that a restart will discard. This closes the gap described in the
problem statement, and it is the reason this feature is worth more than its
display value. It is separable from the rest of the work and can land second.

### GraphQL

A `TaskRun` object and a `taskRuns` query returning the latest run for every
operation, including entries for those that have never run. The operation field
should reuse the enum that `CapturedError` already uses
(@server/src/preso/graphql.rs); since it will now describe two types, renaming
`CapturedErrorOperation` to `BackgroundOperation` is appropriate. That is a
breaking change to the published SDL, and the API is young enough that now is
the time to make it.

No use case layer, consistent with 0003: the resolver calls the repository
directly.

### Web interface

Promote the existing errors page to a status page that shows both halves of the
story, with recent activity above the error log. The pairing is natural — here
is when things ran, and here is what went wrong — and it avoids adding a nav
entry for something consulted occasionally.

- The recent-activity table lists every task with its last run time, duration,
  outcome, and summary. A task that has never run says so.
- The error log below it is unchanged.
- The Home page button (@client/pages/home.tsx) currently appears only when the
  error count is non-zero. It becomes permanent, labelled for status, and turns
  red when there are errors. The empty-errors ambiguity disappears: the button
  is always there, and the page behind it always has something to say.
- The page stays out of the navbar, as the errors page is today.

## Out of scope

- Run history. One row per operation; the schema does not preclude adding
  history later.
- A "Run now" control for each task. `RingLeader` already exposes
  `database_scrub()`, `prune_packs()`, `cleanup_workspaces()`, and
  `restore_test()`, so these would be four thin mutations, but they are a
  separate feature and should not gate this one. The design must not preclude
  them.
- Progress reporting for a task that is currently running. Backups and restores
  have that already through their request objects; the periodic tasks are short
  enough that last-run status is sufficient.
- Auto-repair of anything the scrub or prune reports.

## Open questions

- **Backups.** They are the one task with a complete in-memory status already
  (`BackupState` on the dataset, shown on the Home page cards), and unlike the
  others they are per-dataset and user-initiated as well as scheduled. Including
  them in `task_runs` would duplicate what the dataset cards show; excluding
  them makes the taxonomy inconsistent with `ErrorOperation`. Leaning toward
  recording them for completeness and letting the status page show the periodic
  tasks only.
- **Restore requests.** These are user-initiated rather than scheduled and
  already have a page of their own, so they are probably not "background tasks"
  for the purpose of this feature. Noting that their errors are also not
  currently captured to the `ErrorRepository`, which is a separate gap in 0003.
- Whether the sidecar database's name and `ERROR_DB_PATH` should change now that
  it holds more than errors. Renaming the variable breaks existing deployments
  for no functional gain; leaving it is mildly confusing. Leaning toward leaving
  it and noting it in the deployment docs.

---

## Implementation

### Design decisions

- **Storage**: a `task_runs` table in the existing status database, one row per
  operation and dataset, replaced on each run via `INSERT ... ON CONFLICT`. At
  most six rows, so no retention sweep. The `dataset_id` column stores the empty
  string rather than `NULL` for the global tasks, because SQLite treats `NULL`s
  as distinct in a primary key and the upsert needs them to collide.
- **Reporting shape**: one `TaskReport { issues, summary, skipped }` in the
  `tasks` module root rather than a typed counts struct per operation. The
  display is a sentence, so the task composes its own summary; only the task
  knows what is worth counting. `ScrubIssue` moved there too, since the restorer
  now returns a report as well.
- **Outcome**: four values. `Skipped` was added after finding that
  `restore_test` returns `Ok` when no dataset has a snapshot or no file is under
  `RESTORE_TEST_MAX_FILE_MB`; recording that as success would be a false
  assurance from the one task whose purpose is to prove restores work.
- **Backups excluded**: they already carry a richer per-dataset `BackupState`
  shown on the dataset cards. A second record of the same thing could disagree
  with the first. `BackgroundOperation::PERIODIC` names the five that are
  recorded.
- **Renames**: `ErrorRepository` became `StatusRepository`, `ErrorOperation`
  became `BackgroundOperation`, and `ERROR_DB_PATH` became `STATUS_DB_PATH`
  (default `./tmp/status.db`). Pointing the new variable at an existing
  `errors.db` keeps the captured errors, since the schema is applied with
  `CREATE TABLE IF NOT EXISTS` on open. `ERROR_DB_PATH` is still honored, with
  a deprecation warning: the default path is relative, so a deployment that
  kept the old variable would otherwise have silently opened a new database
  whose location depends on the service manager's working directory.
- **Dataset deletion**: `delete_runs_for_dataset` is called from the
  `deleteDataset` resolver, after the use case succeeds. Per-dataset rows would
  otherwise outlive the dataset, leaving a row on the status page for something
  that no longer exists and letting a dead dataset's timestamp answer for the
  live ones in the overdue calculation. The failure is logged rather than
  returned, since the dataset is already gone. An empty dataset id is rejected,
  because that is how the global operations are keyed.
- **Startup catch-up**: `ScheduleSupervisor` consults the recorded run times
  after starting and triggers anything overdue or never run, one task per slot,
  the first at 60 seconds and the rest spaced a backup-check interval apart.
  This fixes a real gap rather than merely displaying one — see below.
- **Why the slots are spread**: the database scrub, pack prune, and workspace
  cleanup are delivered to the leader as their own messages, whose handlers do
  the work directly instead of going through the request queues. The
  backup-before-prune priority in `process_queues` therefore cannot reorder a
  backup ahead of them, and enqueuing all three at once would put them in front
  of any backup coming due afterwards. Snapshot pruning is unaffected, since it
  goes through the queue where that priority does apply. Spreading the slots
  does not shorten the catch-up or make anything concurrent — the arbiter is
  single-threaded either way — it only keeps the mailbox short enough for a
  backup to be picked up in between.
- **Re-checking per slot**: each slot re-reads the run records rather than
  acting on one decision made at startup, so a task that has since run under
  its own interval is left alone. This also rules out the tempting alternative
  of a single re-arming timer that picks the most overdue task each tick: the
  run record is written on completion, so a task still sitting in the mailbox
  would keep reading as overdue and be enqueued repeatedly.

### The scheduling bug this uncovered

`ctx.run_interval` fires only after a full period has elapsed and keeps its
state in memory. A server restarted more often than the interval never ran the
affected task at all: at the 7-day defaults, a machine rebooted weekly would
never scrub the database, prune packs, or test a restore. Nothing reported this,
and from the outside it was indistinguishable from those tasks running cleanly.
Persisting the run times is what makes the fix possible, so the catch-up landed
with this feature rather than separately.

### Backend

- `server/src/domain/entities.rs`: `TaskOutcome`, `TaskRun` with
  `duration_millis()`, and `BackgroundOperation::PERIODIC`.
- `server/src/domain/repositories.rs`: `record_run` and `list_runs` on
  `StatusRepository`.
- `server/src/data/repositories/status.rs`: the `task_runs` table and its two
  methods, plus five unit tests covering replacement, per-dataset keying, and
  survival across a reopen.
- `server/src/tasks.rs`: `ScrubIssue` and `TaskReport`.
- `server/src/tasks/prune.rs`: the three scan operations return `TaskReport`;
  `prune_pack_locations` returns a count of deleted objects instead of a
  changed flag.
- `server/src/tasks/restore.rs`: `restore_test` returns `TaskReport`, skipped
  when it verified nothing.
- `server/src/tasks/leader.rs`: `LeaderContext::record_run` derives the outcome
  and captures each issue as an error. A unit test caught this recording a
  failed run as `Success` on the first pass, since the error path produced a
  report with no issues.
- `server/src/tasks/schedule.rs`: `TaskIntervals::from_env` shared by the armed
  timers and the catch-up pass; `run_overdue_tasks` with four unit tests.
  `PRUNE_INTERVAL_HOURS` is now clamped to at least 1 like the others.
- `server/src/preso/graphql.rs`: `TaskOutcome`, `TaskRun` and `TaskStatus`
  types, the `taskStatus` query, and `GraphContext.errors` renamed to `status`.

### Frontend

- `client/pages/errors.tsx` became `client/pages/status.tsx`, route `/status`:
  a Recent Activity table above the existing error log, with a row per task and
  an explicit "Never run" for those with no recorded run.
- `client/pages/home.tsx`: the button is now always present. Its absence
  previously carried the same ambiguity as the empty errors page.
- `Skipped` is tagged blue rather than green, so a task that verified nothing
  does not read as a pass.

### Still open

- Interleaving backups with the periodic tasks rests on spacing the catch-up
  slots rather than on priority. The structural fix would be for the scan
  handlers to drain the pending queues before returning, which would also
  apply to the ordinary weekly runs rather than only to startup; it trades
  this queue-ordering question for another, since a long backup would then
  delay the scan.
- Captured errors for a deleted dataset are not removed the way its run records
  now are; they age out under `ERROR_RETENTION_DAYS` instead.
- Backups are not recorded; the open question above was resolved by excluding
  them.
- No "Run now" control yet. `RingLeader` already exposes the four operations, so
  these would be thin mutations.
- Run history is still one row per operation.
