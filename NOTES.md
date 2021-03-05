# NOTES

## Features

* Unlimited backup: all files of any size
* Maintains multiple versions, not just the most recent
* Efficiency (compression, de-duplication, block-level incremental backup)
* Encryption (all remotely stored data is encrypted with libsodium)
* Service agnostic (SFTP, Amazon, Google, etc)
* Full restore or file-level restore
* Restore to dissimilar hardware
* Local and Cloud storage
* Scheduled backups (to better manage resources)
* Cross Platform (macOS, Windows, Linux)
* Amazon Glacier support (this seems to be uncommon at best)
* Fault tolerant (automatically recovers from crashes)

### Ransomware protection

> CloudBerry Backup detects encryption changes in files and prevents existing
> backups from being overwritten until an administrator confirms if there is an
> issue.

Arq backup describes this as:

> Ransomware protection - point-in-time recovery of files

https://ruderich.org/simon/notes/append-only-backups-with-restic-and-rclone

> One issue with most backup solutions is that an attacker controlling the local
> system can also wipe its old backups. To prevent this the backup must permit
> append-only backups (also called add-only backups).

They change the SSH config to run the backup command with "append only" flag.

## Interface

* Web browser
* Desktop application
* System tray icon/menu
* Browse by snapshot, then folders and files
* Move through snapshots for a particular path

## Architecture

* HTTP backend as GraphQL server
* Web frontend as GraphQL client
* Desktop application as GraphQL client
* Key/Value store for metadata
* Local or Remote file store(s) for pack storage

## Design

* Backend written in Rust, uses Juniper GraphQL server
* Front-end written in Flutter, uses Angel GraphQL client
* Desktop application written in Flutter
* Key/Value store is RocksDB
* Storage implementations support local, SFTP, Google, Amazon, etc

## Use Cases

### Initial Setup

* Backend waits for initial setup via web or desktop.
* Upon startup of desktop app, walk user through setup.
* Offer choice of recovering from backup, or starting anew.

### Regular Backup

* Backend performs backups according to configured schedule.
* Manual backup for data sets without a configured schedule.

### Full Restore

* Retrieve the database from the pack store.
* Restore selected dataset(s)
* Walk the latest snapshot, restoring all files.
* May need to allow user to choose a different "computer UUID" in case it changed

### File Restore

* Given a file/tree digest, retrieve packs to get chunks, reassemble.

## Data Format

### Overview

* Snapshots record what was saved and when
* Files are indexed in what is essentially a hash tree
* Trees describe what files/folders are in the snapshot
    - Trees are nested, a la git
* File content is stored in pack files
    - Large files are split across multiple pack files
    - Small files are combined into pack files
* Pack files are stored remotely
* Database is used to store metadata
    - Database is saved just like any other file set

### Computer UUID

1. Use type 5, with URL namespace
1. The "name" is the computer host name and current user name, separated by slash
    * e.g. `localhost/charlie` yields `747267d5-6e70-5711-8a9a-a40c24c1730f`
    * stored in the database using a shortened form (e.g. `dHJn1W5wVxGKmqQMJMFzDw`)
    * makes finding the backup records for a computer reproducible

* May need to allow the user to customize the user/host names to avoid conflicts.
* Computer UUID may change after (re)installation.
* Computer UUID only needs to be reasonably unique to generate useful bucket names.

### Bucket Name

* bucket name will be ULID + computer UUID (without dash separators)
    - e.g. `01arz3ndektsv4rrffq69g5fav747267d56e7057118a9aa40c24c1730f`
    - conforms to Google bucket name restrictions
        + https://cloud.google.com/storage/docs/naming
        + should be sufficiently unique despite global bucket namespace
    - conforms to Amazon Glacier vault name restrictions
        + https://docs.aws.amazon.com/amazonglacier/latest/dev/creating-vaults.html
    - conforms to Amazon S3 bucket name restrictions
        + https://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html
* ULID contains the time, so no need for a timestamp
* UUID makes it easy to find buckets associated with this computer and user
* UUID may change after (re)installation and this is okay
    - Pack records contain the fully-qualified locations regardless of UUID

### Pack Files

* contains raw file content
* default pack file size of 64MB
    - allow configuring pack file size, since it is inconsequential
    - should restrict pack files to between 16 to 256 MB in size
* pack file format
    - plain tar file
    - entry names are the chunk hash digest plus prefix
    - entry dates are always UTC epoch to yield consistent results
    - compressed using zlib
    - encrypted with libsodium using passphrase

### Database Schema

The database is a key/value store (technically a log-structured merge tree)
provided by [RocksDB](https://rocksdb.org). The records are all stored using
[CBOR](https://cbor.io) unless noted otherwise, in which case they are probably
JSON. The records consist of key/value pairs with shortened names, to keep
storage consumption to a minimum. Each record key has a prefix that indicates
what type of record it is, such as `chunk/` for chunk records, and so on.

#### Primary Database

* configuration record
    - database key: `configuration`
    - host name
    - user name
    - computer UUID
    - peers: list of peer installations for chunk deduplication
* dataset records:
    - key: `dataset/` + ULID
    - base path
    - schedule/frequency
    - ignore patterns
    - pack size
    - store identifiers
* latest snapshot records:
    - key: `latest/` + dataset-id
    - checksum of latest snapshot
* computer id records:
    - key: `computer/` + dataset-id
    - computer UUID
* store records:
    - key: `store/` + ULID
    - store type
    - user-defined label
    - list of name/value pairs for configuration
* snapshot records
    - key: `snapshot/` + SHA1 of snapshot
    - SHA1 of previous snapshot
    - time when snapshot started
    - time when snapshot finished
    - number of files in this snapshot
    - base tree reference
* tree records
    - key: `tree/` + SHA1 of tree data
    - entries: (sorted by name)
        + mode (indicates if tree or file)
        + user, group (strings)
        + uid, gid (numbers)
        + ctime, mtime
        + xattrs[]
          - name
          - digest (SHA1 for xattr)
        + reference (SHA1 for tree, SHA256 for file, base64-encoded value for symlink)
        + entry name
* file records
    - key: `file/` + SHA256 at time of snapshot
    - length: size of file in bytes
    - chunks:
        + offset: file position for this chunk
        + digest: chunk SHA256
* database snapshots (identical to pack records)
    - key: `dbase/` + SHA256 of pack file (with "sha256-" prefix)
    - coordinates:
        + store ULID
        + remote bucket/vault name
        + remote object/archive name
    - upload_time: date/time of successful upload, for conflict resolution
* extended attribute records
    - key: `xattr/` + SHA1 of the attribute value
    - value: attribute data as a Buffer

#### Chunk Database

Sync with peers for multi-host chunk deduplication.

* chunk records
    - key: `chunk/` + SHA256 of chunk (with "sha256-" prefix)
    - size of chunk in bytes
    - SHA256 of pack
* pack records
    - key: `pack/` + SHA256 of pack file (with "sha256-" prefix)
    - coordinates:
        + store ULID
        + remote bucket/vault name
        + remote object/archive name
    - upload_time: date/time of successful upload, for conflict resolution

## Implementation

### Deduplication

Uses a content-defined chunking (a.k.a. content-dependent chunking) algorithm to
determine suitable chunk boundaries, and stores each unique chunk once based on
the SHA256 digest of the chunk. In particular, uses
[FastCDC](https://www.usenix.org/system/files/conference/atc16/atc16-paper-xia.pdf)
which is much faster than Rabin-based fingerprinting, and somewhat faster than
Gear. This avoids the shortcomings of fixed-size chunking due to boundary
shifting.

#### Database Sync

When the peer(s) are available, sync with their chunk/pack database to get
recent records. If there are conflicts (which seems unlikely given the pack
would have to have the exact same chunks) resolve by keeping the pack record
that has the most recent `upload_time`.

### Procedure

#### Backup

1. Sync with any configured peers to get recent chunk updates.
1. Check for an existing snapshot:
    1. If present but lacking `end_time` value, continue backup procedure
    1. Otherwise, generate the tree objects to represent the state of the dataset
1. Find the differences from the previous snapshot.
1. If there are no changes, delete snapshot record, exit the procedure.
1. For each new/changed file, check if record exists; if not:
    1. For small files, treat as a single chunk
    1. For large files, use CDC to find chunks
    1. For each chunk that does not exist in database, add to pack
    1. If pack file is large enough, upload to storage
    1. Add `chunk` and `pack` records upon successful pack storage
1. Set the `end_time` of snapshot record to indicate completion.
1. Store latest snapshot identifier in `dataset/` record.
1. Backup the database files.

#### Uploading Packs

1. Select an existing bucket, or create a new one.
    * For Amazon Glacier, limited to 1,000 vaults, so reuse will be necessary.
    * For Amazon S3, limited to 100 buckets, so reuse will be necessary.
    * For Google, no hard limit, but perhaps a practical limit.
1. Upload the pack file to the cloud.
1. Update pack record to track remote coordinates.

#### Crash Recovery

If the latest snapshot is missing an end time, there is pending work to finish.

#### File Restore

1. Use selected file SHA256 to find list of chunks.
1. For each chunk, look up the pack record to get bucket and object.
1. Download pack and verify checksum to detect corruption.
1. Extract the chunk of the file in the pack to a temporary file.
1. Repeat for each chunk of the file (finding pack, downloading, extracting).
1. Sort the chunks by the `offset` value from the file record.
1. Reproduce the original file from the downloaded chunks.
1. Apply ownership and mode values according to the tree object.

#### Full Recovery

1. Find all buckets with type 5 UUID for a name
1. Fetch metadata object/archive for those buckets
1. Present the list to the user to choose which to recover
1. Retrieve the most recent database
    * For Glacier, this means listing archives of master vault to find database pack
1. Iterate entries in database, fetching packs and extracting files

#### Garbage Collection

* Automatic garbage collection:
    - Remove tree and file records that are no longer referenced
    - Find pack files that are no longer referenced
    - Remove the pack files from the remote side, delete the record
* Aggressive garbage collection
    - Retrieve pack files, remove stale entries, repack, upload
    - Remove the old pack files from the remote side
    - Update file records affected by repacking
* Start fresh and purge the old backups after successful completion

#### Deleting Old Backups

Remove the snapshot record to be deleted, then garbage collect.

### Encryption

#### Master Password

* Need to prompt the user for their password when starting up
* If available, use a "secret vault" provided by the OS
    - macOS Keychain
    - Windows Data Protection API
    - Linux gnome-keyring
* If an environment variable is set, can use that
    - c.f. https://forum.duplicacy.com/t/passwords-credentials-and-environment-variables/1094
