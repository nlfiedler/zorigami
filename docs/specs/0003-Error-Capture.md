# Capturing Errors

When errors occur during a backup, the backup process stops and the error message is captured in the `Request` object in @server/src/tasks/backup.rs which is exposed via GraphQL to the web interface. While that may work for the backups which are spawned by requests, some actions occur in the background that are not represented with requests in a queue. Surfacing errors that occur in the background is a shortcoming of the current design.

## Problem Statement

- Errors during snapshot pruning in `LeaderSupervisor::process_prune()` are only logged to a file
- Errors during a test restore in `RestorerImpl::restore_test()` are only logged to a file

Most normal users are never going to look at the log file, including the author of this application. The errors needs to be surfaced somewhere obvious, like the home page of the web interface.

## Requirements

- Errors encountered during restore testing or snapshot pruning need to be made known to the user.
- In the future, a weekly database scrub operation will need a means of surfacing issues as they are discovered.
- Regardless of the source, errors should be captured in a semi-permanent fashion, not merely in memory
- Need to consider how the error capturing can be mocked and tested
- Errors can be reviewed in the web interface, provided by the GraphQL API
- If any errors are capture, @client/pages/home.tsx should show an _Errors_ button in the top `nav` element
- Clicking the _Errors_ button will navigate to a new errors page that lists all errors using a Bulma CSS table (?)
- The new errors page does not need to be in the navigation bar, normally it should be hidden

## Proposal

Current backing store of this application is RocksDB which is not suited to structured data. What's more, the database is set up for the express purpose of supporting the backup and restore functionality. Error logging is a separate concern and therefore should be stored separately from the application data.

SQLite offers several advantages for this type of information capture and retrieval:

- Structured, well-defined schema, standard query language
- Database is stored entirely in a single, compact file
- Small, fast, and reliable as stated on the web site
- The `sqlite` Rust crate works very well

With regards to the error capturing and querying, it seems reasonable to implement a _repository_ like the _record_ and _pack_ repositories. With a trait and the `mockall` crate, writing unit tests becomes very easy. A default implementation of the repository would utilize the `sqlite` crate to:

1. Create the database file and define the tables (maybe just one table?)
2. Provide a function for recording an error, including the date-time, operation (restore, test restore, prune, etc), and error message
3. Provide functions for querying errors, if any, perhaps with a limit to the number of returned results
4. Limit the growth of the database in some manner, either by limiting the total number of errors captured, or by pruning records whose date-time is older than some number of days

---

## Implementation

### Design decisions

- **SQLite crate**: `rusqlite` with the `bundled` feature, so there is no runtime dependency on the system `libsqlite3`.
- **Layering**: a single `ErrorRepository` trait with one SQLite-backed implementation. The `RecordRepository`/`EntityDataSource` split used by the RocksDB layer is overkill for a feature this narrow.
- **Retention**: time-based only. Records older than `ERROR_RETENTION_DAYS` (default 90) are deleted at server startup and opportunistically from inside `record_error` (throttled to at most once per hour).
- **Dismiss UX**: the errors page exposes per-row delete and a "Clear All" action, so a single transient failure does not leave the Errors button visible for weeks.
- **Backup errors too**: backup failures, which previously only lived in the in-memory `backup::Request`, are also persisted here, so the SQLite store is a single source of truth.
- **DB file location**: `ERROR_DB_PATH` env var; default `./tmp/errors.db`.
- **Operation field**: typed Rust enum (`Backup`, `Prune`, `RestoreTest`, `DatabaseScrub`) exposed as a GraphQL enum. `DatabaseScrub` is reserved for the future weekly scrub named in the requirements.

### Backend

- `server/Cargo.toml`: added `rusqlite = { version = "0.33", features = ["bundled"] }`.
- `server/src/domain/entities.rs`: `ErrorOperation` enum and `CapturedError` struct (`id`, `timestamp`, `operation`, `dataset_id`, `message`).
- `server/src/domain/repositories.rs`: `ErrorRepository` trait with `record_error`, `list_errors(limit)`, `count_errors`, `delete_error(id)`, `clear_all`, `prune_older_than(days)`, guarded by `#[cfg_attr(test, automock)]` for mockall-based unit tests.
- `server/src/data/repositories/errors.rs` (new): `ErrorRepositoryImpl` wrapping `Mutex<rusqlite::Connection>`. Creates the `errors` table (id, timestamp, operation, dataset_id, message) and a `timestamp` index on startup; lists rows ordered by `timestamp DESC, id DESC`. Four unit tests cover insert/list, limits, delete/clear, and retention — all using in-memory databases for hermeticity.
- `server/src/tasks/leader.rs`: `RingLeader::start` takes `Arc<dyn ErrorRepository>`. The supervisor installs the repo on `LeaderContext`, and a `LeaderContext::capture_error` helper is called from:
  - `process_prune` (`ErrorOperation::Prune`, with the dataset id),
  - the `RestoreTest` handler (`ErrorOperation::RestoreTest`),
  - the backup `Subscriber::error` impl (`ErrorOperation::Backup`, with the dataset id).
  If the record call itself fails, it is logged with `warn!` so that error-capture failures never mask the original error.
- `server/src/preso/graphql.rs`:
  - new enum `CapturedErrorOperation` and object `CapturedError`,
  - queries `capturedErrors(limit: Int): [CapturedError!]!` and `capturedErrorCount: BigInt!`,
  - mutations `deleteCapturedError(id: BigInt!): Boolean!` and `clearCapturedErrors: BigInt!`,
  - `GraphContext` gains an `Arc<dyn ErrorRepository>` field.
- `server/src/main.rs`: new `ERROR_DB_PATH` and `ERROR_RETENTION_DAYS` env vars; a shared `ERROR_REPO` is built once at startup (one long-lived SQLite connection shared behind a mutex), pruned once at boot, then handed to both the GraphQL handler and the ring leader.

### Frontend

- Regenerated `public/schema.graphql` and `generated/graphql.ts`.
- `client/pages/errors.tsx` (new): Bulma table with columns _Time_, _Operation_, _Dataset_, _Message_, and a per-row delete button. A "Clear All" button at the top of the page is gated on `window.confirm`. The page is reached via the router but is **not** present in the site navbar.
- `client/index.tsx`: added `/errors` route.
- `client/pages/home.tsx`: polls `capturedErrorCount` alongside the existing datasets query. When the count is non-zero a red "Errors (N)" button appears in the `level-right` of the top nav, linking to `/errors`. The `AutoRefreshCheckbox` now refreshes both queries.

### Environment variables

| Variable                 | Default                 | Purpose                                                        |
|--------------------------|-------------------------|----------------------------------------------------------------|
| `ERROR_DB_PATH`          | `./tmp/errors.db`       | Location of the SQLite error database.                         |
| `ERROR_RETENTION_DAYS`   | `90`                    | Rows older than this are pruned at startup and opportunistically. |
