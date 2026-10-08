# Data Format History

This document describes all of the incompatible changes to the database and pack file formats.

## 2014

Originally [akashita](https://github.com/nlfiedler/akashita) would use ZFS to create clones and snapshots of the file system, then use `tar` and `split` to create primitive pack files, uploading them to Amazon Glacier.

## 2018

Inspired by the [Arq](https://www.arqbackup.com) data format, developed a new data model that utilized a key/value store to record all files, directories, and so on. Pack files were created using the [tar](https://crates.io/crates/tar) crate with the individual files and chunks stored in compressed form. Encrypted the tar files using [gpgme](https://www.gnupg.org/software/gpgme).

## 2019

Replaced gpgme encryption with [libsodium](https://github.com/jedisct1/libsodium) and at some point [sodiumoxide](https://crates.io/crates/sodiumoxide).

## 2020

### June

Moved `computer_id` and `latest_snapshot` fields out of the `dataset` records and instead stored them in separate `computer` and `snapshot` records.

Changed `key` field to `id` for `dataset` records.

## 2021

### August

Added `excludes` field to `dataset` records.

## 2022

### March

Chunk length is no longer written to the `chunk` records.

Files that can be stored as a single chunk no longer have `chunk` records and instead the `file` record points to the pack file directly.

Very small files (80 bytes or less) are stored directly in the `tree` records to save space.

Snapshot file counts were added to the `snapshot` records.

Symbolic links now stored as raw bytes in `tree` records.

Removed `fstype` field from `tree` records.

## 2024

### May

Replaced weird tar-based pack file format with [EXAF](https://github.com/nlfiedler/exaf-rs). The pack file salt was moved from the database to the header of the pack file.

All SHA256 digests were replaced with BLAKE3 for improved performance.

### October

Added `retention` field to `dataset` records, defaults to all snapshots.

## 2025

### June

Replace the generated serde code that was used to serialize the entities to the data store with hand-written code based on [ciborium](https://crates.io/crates/ciborium).

No longer shortening the computer UUID value.

Removed `snapshot/` and `computer/` records, folded the latest snapshot checksum into the dataset record (again). The computer records are not needed, the configuration record already has that information, and there is no need to have that value associated with the datasets.

Added `retention` field to pack stores for future pack file and database snapshot pruning (defaults to all retaining packs).

## 2026

### April

Added `chunk_size` to `dataset` records, defaults to 1mb.

Bucket names no longer include the computer identifier (a type 5 UUID) suffix. Instead, the value is akin to a ULID with 256 bits of randomness instead of only 80. This results in a name that is 61 characters long, which conforms to all supported cloud storage providers. This greatly improves the entropy value of the generated names without losing the lexicographical sorting that is a hallmark of ULID.

Bucket naming policy is now a part of the `configuration` record, along with optional IANA time zone string.

Added a `schema_version` entry in RocksDB that is a monotonically increasing number that indicates the current database schema.

Added support for [SQLite](https://sqlite.org) as an alternative to RocksDB.

Errors encountered in background tasks are written to an SQLite database named `errors.db`.

### May

The Azure pack store now authenticates with the Microsoft identity platform (Entra ID) via a service-principal client secret rather than a storage-account shared key. The `access_key` property has been removed from the Azure pack store configuration; `tenant_id`, `client_id`, and `client_secret` properties are required in its place. Existing Azure pack stores must be re-entered through the Stores page of the web UI with the new credentials. The change was forced by the migration to the official Microsoft Azure SDK for Rust, which does not support shared-key authentication and per Microsoft is not planned to (see [Azure/azure-sdk-for-rust#3614](https://github.com/Azure/azure-sdk-for-rust/issues/3614)).

### September

The SQLite database that records errors from background tasks now also holds the outcome of the most recent run of each periodic task, in a new `task_runs` table, and is named `status.db` by default. The `ERROR_DB_PATH` environment variable became `STATUS_DB_PATH`; the old name is still honored, with a deprecation warning. Pointing either at an existing `errors.db` preserves the captured errors, as the new table is created on open.

Added `data_shards` and `parity_shards` fields to `dataset` records, both defaulting to zero, which disables erasure coding. When both are non-zero, the pack files for that dataset are written with Reed-Solomon erasure coding and can repair themselves while being read, which matters for stores that provide no redundancy of their own (local disk, SFTP, and MinIO). In the SQLite backend these arrive as two columns added to the `datasets` table; an existing database gains them automatically on startup, so the schema version is unchanged and no database needs to be recreated.

The database archive uploaded to the pack stores is now always written with erasure coding, using a fixed 10 data and 2 parity shards. It is small next to the packs it describes and must be readable to recover anything at all, so the roughly 20% size increase is not configurable.

Pack files and database archives written with erasure coding use EXAF major version 2 and **cannot be read by earlier releases of this application**. Packs written without it are unaffected and remain readable by any release.

The GraphQL `DatasetInput` type gained required `dataShards` and `parityShards` fields, which is a breaking change for any API client other than the bundled web interface: an `updateDataset` mutation that omits them is now rejected during input validation. Send zero for both to keep erasure coding disabled.
