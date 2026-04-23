# Database Scrub

The `Scheduler` background task should perform a validation of all database records on a weekly basis. Any errors discovered should be recorded via the `ErrorRepository` as is being done with backup, restore, and prune operations.

## Problem Statement

In the past, there was a bug in which a file was not recorded correctly in the database, such that restoring that file became nearly impossible. In addition to bugs, it is conceivable that the RocksDB database itself may become corrupted, although it has not happened in the last 6 years. To detect issues as early as possible, the application should perform a verification of the database records on a regular basis and report any issues.

## Proposal

- The `PrunerImpl` has similar functionality already (see `prune_unreachable_records()` for an example) and seems a sensible place for this new function to live.
- The `SchedulerImpl` will start an interval (like the weekly prune) to invoke a new `database_scrub()` function on the `RingLeader`.
- The scheduled interval should default to 7 days but be configurable via an environment variable, such as `DATABASE_SCRUB_INTERVAL_DAYS`.
- The `RingLeader` will enqueue a new actix/actor message to the `LeaderSupervisor` to perform the scrub separately from other tasks, similar to the `RestoreTest` message.
- The `LeaderSupervisor` will instantiate a `PrunerImpl` and invoke a new `database_scrub()` function on the pruner.
- The pruner will use logic similar to the snapshot pruning, but instead of finding unreachable records, it is to make sure all referenced records are reachable and readable.
- Scanning would go in this order: datasets, snapshots, trees, files, chunks, packs, xattrs, and stores.
- Avoid visiting the same tree more than once since it is very likely that datasets and snapshots refer to the same tree many times.

---

## Implementation Summary

### Design decisions

- **Error granularity**: one `ErrorRepository` entry per problem (missing tree, missing file, etc.).
- **Datasets/stores scope**: shallow — record loads, referenced ids exist, no remote connectivity check.
- **Packs/chunks depth**: DB-only — `chunk.packfile` resolves to a pack record; each `pack.locations[].store` resolves to a store record. No remote HEAD/download.
- **Cancellability**: scrub reuses the existing `prune_stopper`; `PrunerImpl::stop_requested` is checked between phases and inside the tree walk.
- **Placement**: scrub lives on `PrunerImpl` per the spec — no new `scrub.rs` module.

### Out of scope

- Ensuring scrub runs even if application restarts weekly.
- Remote pack integrity (no HEAD / list on stores).
- Auto-repair — scrub only reports.
- Progress UI for scrub — issues surface on the existing errors page.
