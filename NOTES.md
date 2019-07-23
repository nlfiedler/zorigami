# NOTES

## Features

* Unlimited backup: all files of any size
* Maintains multiple versions, not just the most recent
* Efficiency (compression, de-duplication, block-level incremental backup)
* Encryption (all remotely stored data is encrypted with OpenPGP)
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
* Document-oriented database for metadata
* Local or Remote file store(s) for pack storage

## Design

* Backend written in Rust, uses Juniper GraphQL server
* Front-end written in ReasonML, uses Apollo GraphQL client
* Desktop application written in ReasonML, uses Electron
* Key/Value store is RocksDB
* Storage implementations support local, SFTP, Google, Amazon, etc

## Use Cases

### Initial Setup

* Backend waits for initial setup via web or desktop
* Upon startup of desktop app, walk user through setup
* Offer choice of recovering from backup, or starting anew

### Regular Backup

* Backend performs backups according to configured schedule

### Full Restore

* Retrieve the database from the pack store.
* Walk the latest snapshot, restoring all files from packs.

### File Restore

* Given a file digest, retrieve pack to get chunks, reassemble.

## Data Format

### Overview

* Snapshots record what was saved and when
* Files are indexed in what is essentially a hash tree
* Trees describe what files/folders are in the snapshot
    - Trees are nested, a la git
* File content is stored in pack files
    - Large files are split across multiple pack files
    - Small files are stored in whole
    - Pack files are used to minimize the number of files stored remotely
* Pack files are stored remotely
* Database is used to store metadata
    - Database is saved just like any other file

### Computer UUID

1. Use type 5, with URL namespace
1. The "name" is the computer host name and current user name, separated by slash
    * e.g. `chihiro/nfiedler` yields `f6ce9ef2-3059-5f8d-9f3b-7d532fe15bf8`
    * makes finding the backup records for a computer reproducible

### Bucket Name

* bucket name will be ULID + computer UUID (without dash separators)
    - e.g. `01arz3ndektsv4rrffq69g5favf6ce9ef230595f8d9f3b7d532fe15bf8`
    - conforms to Google bucket name restrictions
        + https://cloud.google.com/storage/docs/naming
        + should be sufficiently unique despite global bucket namespace
    - conforms to Amazon Glacier vault name restrictions
        + https://docs.aws.amazon.com/amazonglacier/latest/dev/creating-vaults.html
    - conforms to Amazon S3 bucket name restrictions
        + https://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html
* ULID contains the time, so no need for a timestamp
* UUID makes it easy to find buckets associated with this computer and user

### Pack Files

* contains raw file content
* default pack file size of 64MB
    - allow configuring pack file size, since it is inconsequential
    - restrict pack files to between 16 to 256 MB in size
* pack file format
    - tar file format
    - entry names are the chunk hash digest plus prefix
    - entry dates are always UTC epoch to yield consistent results
    - encrypted with OpenPGP (RFC 4880) using passphrase

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
    - default frequency with which to perform backups (hourly, daily, weekly, monthly)
    - default time ranges in which to upload snapshots
    - default preferred size of pack files (in MB)
    - default ignore file patterns (applies to all datasets)
* dataset records:
    - key: `dataset/` + ULID
    - root local path
    - latest snapshot
    - schedule/frequency
    - ignore patterns
    - pack size
    - store identifier
* store records:
    - key: `store/` + type + ULID (e.g. `store/local/01arz3ndektsv4rrffq69g5fav`)
    - (the identifier is universally unique even without key prefix)
    - value: opaque JSON blob of the store configuration
* snapshot records
    - key: `snapshot/` + SHA1 of snapshot (with "sha1-" prefix) or `index` for pending
    - parent: SHA1 of previous snapshot (`null` if first snapshot)
    - start_time: when snapshot started
    - end_time: when snapshot finished
    - file_count: number of files in this snapshot
    - tree: root tree reference
* tree records
    - key: `tree/` + SHA1 of tree data (with "sha1-" prefix)
    - entries: (sorted by name)
        + mode (also indicates if tree or file, e.g. `drwxr-xr-x`)
        + user, group (strings)
        + uid, gid (numbers)
        + ctime, mtime
        + xattrs[]
          - name
          - digest (SHA1 for xattr, with "sha1-" prefix)
        + reference (SHA1 for tree, SHA256 for file, base64-encoded value for symlink)
        + entry name
* file records
    - key: `file/` + SHA256 at time of snapshot (with "sha256-" prefix)
    - length: size of file in bytes (absent if `changed`)
    - chunks: (absent if `changed`)
        + offset: file position for this chunk
        + digest: chunk SHA256
    - changed: SHA256 at time of backup, if different from key
* extended attribute records
    - key: `xattr/` + SHA1 of the attribute value (with "sha1-" prefix)
    - value: attribute data as a Buffer

#### Chunk Database

Sync with peers for multi-host chunk deduplication.

* chunk records
    - key: `chunk/` + SHA256 of chunk (with "sha256-" prefix)
    - length: size of chunk in bytes
    - pack: SHA256 of pack
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
1. Check for a existing snapshot:
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
    1. If file checksum changed after snapshot, add two records:
        * set `changed` on the record with the checksum at time of snapshot
        * set `chunks` on the record with the checksum at time of packing
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

#### Deleting Old Backups

Remove the snapshot record to be deleted, then garbage collect.

#### Database Migrations

* Store the version information in a `meta` record.
* Move the old struct implementations into a separate, versioned module.
* Use the old structs to deserialize and convert to the new struct.
* Two choices for migration procedure:
    1. Each version iterates entire database, processing records as needed
    1. Main code iterates database and calls each version with a record to update
* Some versions will only need to change certain records.
* Could use `#[serde(default)]` and `Default` to fill in blanks of new fields.
    - i.e. avoid writing a migration for simple things like adding a new optional field

### Encryption

#### Master Password

* Need to prompt the user for their password when starting up
* If available, use a "secret vault" provided by the OS
    - macOS Keychain
    - Windows Data Protection API
    - Linux gnome-keyring
* If an environment variable is set, can use that
    - c.f. https://forum.duplicacy.com/t/passwords-credentials-and-environment-variables/1094

## Alternatives

### Commercial

#### Arq

* https://www.arqbackup.com
* Windows, Mac
* Uses a single master password
* Supports numerous backends

#### CloudBerry

* https://www.cloudberrylab.com/backup
* Consumer and business
* Windows, Mac, Linux
* Supports Glacier and other services
* Freeware version lacks compression, encryption, limited to 200GB

#### Duplicacy

* https://github.com/gilbertchen/duplicacy
* Lists other open source tools and compares them
* Deduplicates chunks across systems
* Does not use a database supposedly
* Does not and can not support Glacier

#### JungleDisk

* https://www.jungledisk.com/encrypted-backups/
* Primarily business oriented
* Seems to rely on their servers
* Probably stores data elsewhere

#### qBackup

* https://www.qualeed.com/en/qbackup/
* Windows, Mac, Linux
* Supports numerous backends
* Has copious documentation with screen shots

#### tarsnap

* https://www.tarsnap.com
* Free client
* Uses public key encryption rather than a password
* Stores data in Amazon S3
* Relies on tarsnap servers
* 10x the price of Google Cloud or Amazon Glacier
* Command-line interface

### Open Source

#### Attic

* https://attic-backup.org
* Development stopped in 2015
* Only supports SSH remote host
* Command-line interface

#### Borg

* https://borgbackup.readthedocs.io/en/stable/
* Fork of Attic
* Only supports SSH remote host
* Command-line interface

#### bup

* https://bup.github.io
* Git-like (uses Python and Git) pack file storage
* Requires a bup server for remote storage
* Command-line interface

#### Duplicati

* https://www.duplicati.com/
* Requires .NET or Mono
* Web-based interface

#### duplicity

* http://duplicity.nongnu.org
* Uses GnuPG, a tar-like format, and rsync
* Supports backends with a filesystem-like interface
* Command-line interface

#### restic

* https://restic.net
* Git-like data model
* Supports numerous backends
* Command-line interface
