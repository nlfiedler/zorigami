# Testing Restore Weekly

## Overview

The application should schedule a weekly test in which it selects a random file from the database and attempts to retrieve it from one of the pack stores. The purpose of this is to ensure the restore functionality is working at the most basic level -- can the application connect to a pack store, retrieve the appropriate pack, and extract the file from that pack. This is only meant to be a test, no changes should be made to the user's dataset, the set of files that are being backed up by the application. The test should be performed in a temporary directory and it should clean up after the test is complete.

## Files Involved

- @server/src/tasks/schedule.rs
- @server/src/tasks/schedule.rs
- @server/src/tasks/restore.rs

## Basic Plan

- The `ScheduleSupervisor` will perform the restore test on an interval, like the backup and prune that it already does.
- The default interval for the restore test should be 7 days but can be changed by setting an environment variable.
- When the interval fires, the scheduler will call a new function in `RingLeader` named `restore_test()` that takes a passphrase as an argument.
- The ring leader will queue up a message to the `LeaderSupervisor` to perform the restore test when all other requests are done. This message will be like the `RestoreDatabase` message, it is sent from the leader to the actor separately from the other request queues.
- When `LeaderSupervisor` receives this new message, it should call a new function on itself named `restore_test()`.
- The `restore_test()` in `LeaderSupervisor` will invoke a new `restore_test()` function on the `Restorer`.
- The `restore_test()` function in `RestorerImpl` will find a random `file/` record in the database, look up the chunk or pack records containing that file, and begin the process of retrieving those packs and testing the file recovery.

## Implementation Details

- The default restore test interval will be 7 days, but this should be configurable with an environment variable named `RESTORE_TEST_INTERVAL_DAYS` that is parsed as an integer and limited to the range of 1 to 30.
- Add a new `get_random_file()` to `RecordRepository` that returns a `Result<Option<File>, Error>`
- Add an implementation of `get_random_file()` to `RecordRepositoryImpl` that simply delegates to the data source.
- Add a new `get_random_file()` to `EntityDataSource` that returns a `Result<Option<File>, Error>`
- Add an implementation for `get_random_file()` to `EntityDataSourceImpl` that is like `get_random_bucket()` except that it returns an `Option<File>` versus an `Option<String>` and instead of counting and fetching with the `bucket/` prefix, it will use the `file/` prefix.
  - See `get_file()` for the method of deserializing the `File` from the raw bytes in the entity data source.
- Write `restore_test()` in `RestorerImpl` to call `get_random_file()` on its record repository (called `dbase` in the code).
- If `get_random_file()` returns `None` then the test is considered done and `restore_test()` will simply return without doing anything.
- If `get_random_file()` returns `Some(File)` then `restore_test()` will use that to perform the test.
- Like `restore_files()` in `RestorerImpl` the `restore_test()` will construct a `FileRestorer` via the `fetch_factory`.
- `restore_test()` should then call `fetch_file()` on the file restorer, passing it a file path to a temporary file in the `dataset.basepath` -- the `FileRestorer` always restores files under the dataset basepath and using a temporary file is easier than refactoring the file restorer.
- Once `fetch_file()` returns success, `restore_test()` must compute the blake3 digest of the temporary file and validate that it matches the `digest` from the `File` entity.
- Regardless of the success or failure of the test, delete the temporary file.

## Answers to Claude's questions

- A random `File` record has no backward link to a dataset, but
  `FileRestorer::load_dataset()` must be called before `fetch_file()` because it
  sets the pack stores (`self.stores = load_dataset_stores(...)`). The chosen
  dataset's stores must actually contain the file's packs (each pack has specific
  `locations`). How should the test pick the dataset? → Random dataset, walk its
  snapshot for a file
- The spec says the restore-test message is queued "like `RestoreDatabase`" and
  runs "when all other requests are done." But `RestoreDatabase` actually
  _clears_ all queues before running (leader.rs:522-537) — that's the wrong
  semantics for a periodic health check. What should happen if the leader is busy
  with a backup/restore/prune when the weekly interval fires? → Fire out-of-band
  like RestoreDatabase but without clearing queues
- A random file could be 10 GB. The weekly test would then download 10 GB every
  time it hits that file. Should the test constrain which files are eligible? →
  Reject files above a configurable size, retry (Recommended)
- When the restore test fails (pack fetch error, digest mismatch, etc.), where
  should that surface? The spec only describes computing and validating the
  digest — not what to do with the result. → Log at error/warn level only
  (Recommended)
