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

Bucket naming policy is now a part of the `configuration` record.
