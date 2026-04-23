# Pack Pruning

Over time, pack files and database archives will accumulate in the pack stores. Gradually, as archives are pruned and old files become unreachable and are removed from the database, there will be pack files that no longer hold relevant content. Similarly, the database archives that are uploaded to the pack store eventually become outdated; in fact, only one snapshot is really necessary. The only reason to keep any of the older database archives is to avoid paying an extra fee for deleting remotely stored objects before they have reached a certain age. All 3 major cloud providers use a similar policy regarding object retention: 30 days for the _cool_ storage class, 90 days for _cold_, and 180 days for _archive_.

## Proposal

The `Scheduler` should start an interval timer to send a request to the `RingLeader` to begin the process of looking for unreferenced pack files and removing them, along with database archives that are older than a configured retention policy. The pack file and archive pruning implementation should live in the `Pruner` implementation since it already contains similar functionality for finding unreachable database records.

The default scheduler interval should be 30 days since it is unlikely that pack files will become useless on a frequent basis. Additionally, the database archives must be _at least_ 30 days old before they can be deleted without paying an extra fee.

The first step in the process is to find unreachable pack records -- if the records in the database are unreachable, then the corresponding pack files in the pack store are no longer needed. The second step is to check when the pack file was uploaded by examining its `upload_time` field in the `Pack` entity. This value is then compared with any pack stores that contain that pack. The pack stores holding the pack are identified by the `locations` field of the `Pack` entity. The `Store` entity will have a `retention` field that indicates the policy for retaining old pack files. If the `retention` is `PackRetention::Days` then its contained value must be compared to the pack `upload_time` -- if a sufficient number of days have passed, then that pack file is ready to be deleted.

If an unreachable pack record is found, it should be removed from the database.

Regarding the database archives, they are stored in the database using the `dbase/` prefix and are represented by a `Pack` entity and thus have an `upload_time`. In the same manner as pack files, the archives can be considered old if their `upload_time` is sufficiently old compared to the associated pack store's `retention`. For any database archive that can be removed from **all** of the pack stores, then its corresponding `dbase/` record can be removed from the database.

Pack stores that have a retention of `PackRetention::All` will not have any pack files or database archives removed.

---

## Implementation Summary

The feature landed behind a new scheduler interval that dispatches a `PackPrune` message through the existing `RingLeader` → supervisor → `Pruner` path, mirroring the `DatabaseScrub` wiring.

### Decisions refined from the proposal

- **Partial-deletion safety.** The proposal's statement that "if an unreachable pack record is found, it should be removed from the database" was tightened to match the database-archive rule: locations are removed one-by-one as each store's retention elapses and the `Pack` record is deleted only when `locations` becomes empty. Any store with `PackRetention::ALL` therefore pins the pack (and its record) indefinitely — no pack objects are ever orphaned.
- **Newest database archive is always preserved**, even if its age would otherwise make it eligible for deletion, so disaster recovery remains possible.
- **Per-object failures** (network error, auth error, missing store, etc.) are captured via `ErrorRepository` under a new `ErrorOperation::PackPrune` and surfaced in the UI alongside other background-operation errors. The run continues after each failure; the failed location is retained so the next run retries it.
- **Scheduler cadence** is controlled by `PACK_PRUNE_INTERVAL_DAYS` (default 30, clamped 1–180) to accommodate the cloud "archive" retention tier's 180-day minimum.
