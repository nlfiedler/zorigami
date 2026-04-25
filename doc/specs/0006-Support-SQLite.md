# Support SQLite

This application has long relied on RocksDB as its primary data store for storing the entity records. This works very well and there is no need to migrate away from RocksDB. However, providing support for at least one other data store would prove that the application employs a good design in which the domain layer is not tightly coupled to the implementations in the data layer.

## Proposal

In short, compartmentalize the RocksDB-specific code into a `rocksdb` module under @server/src/data/sources, then add another module named `sqlite` in that same directory. The `EntityDataSourceImpl` type would be renamed to `RocksDBEntityDataSource`, and the SQLite equivalent would be named `SQLiteEntityDataSource`.

## Details

The SQLite component can probably live entirely within the `SQLiteEntityDataSource` implementation, without the need to create a new workspace member crate in the @database directory -- @database/database_rocks was a special case that added additional functions on top of the RocksDB API that may not be necessary for SQLite given its rich functionality.

### Database Migrations

Database migrations are not a concern, this is a backup and restore application, its database is _not_ the source of truth. If there are significant changes to the schema, then the user is expected to throw away the old database and start over. It may be worth while, from a user experience perspective, to detect that the on-disk database is using an older schema than the application's current schema, and fail to start up fully until the user takes action (throw away the database and start from scratch). This check would need to be performed in @server/src/main.rs early in the startup process.

### Configuration

The user can select between RocksDB and SQLite by setting an environment variable -- maybe call it `DATABASE_TYPE` with accepted values being either `rocksdb` or `sqlite`, defaulting to `rocksdb`. May make sense to define a function to construct the appropriate entity data source in @server/src/main.rs since it is currently constructing `EntityDataSourceImpl` in multiple places.

## Testing

The tests in @server/tests/data_sources_test.rs need to be modified such that every test runs two times, once for the RocksDB entity data source and again for the SQLite entity data source. A simple way to achieve this would be to change the existing tests to take a data source as a parameter and run through the test, and that function would be invoked from two new tests, one for RocksDB and another for SQLite. In this way the exact same tests are performed on both data source implementations, and the tests stay DRY.

The other tests (@server/src/tasks/backup.rs, @server/tests/backuper_test.rs, @server/tests/backup_restore_test.rs) should use the RocksDB implementation of the entity data source, no need to run these tests with both RocksDB and SQLite.

---

## Implementation Summary

### Schema version handling

- A new pair of trait methods on `EntityDataSource` (`get_schema_version`/`set_schema_version`) is implemented by both backends: RocksDB stores a 4-byte little-endian `u32` under the reserved key `schema_version`; SQLite uses `PRAGMA user_version`. A fresh database reports `0`.
- `CURRENT_SCHEMA_VERSION` is a module-level constant in `sources.rs`. `verify_schema_version()` initializes a fresh DB to the current version, accepts a match, and returns an `Err` on mismatch instructing the user to wipe `DB_PATH`.
- `main.rs` calls `verify_schema_version()` once at startup, before any supervisor or HTTP server is brought up. Mismatch is fatal.
