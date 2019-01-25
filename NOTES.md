# NOTES

## Features

* Unlimited backup: all files of any size
* Maintains multiple versions, not just the most recent
* Efficiency (compression, de-duplication, block-level incremental backup)
* Encryption (all remotely stored data is encrypted with 256-bit AES)
    - Uses unique 256-bit key and IV for each remotely stored pack
* Service agnostic (SFTP, Amazon, Google, etc)
* Full restore or file-level restore
* Restore to dissimilar hardware
* Local and Cloud storage
* Scheduled backups (to better manage resources)
* Cross Platform (macOS, Windows, Linux)
* Amazon Glacier support (this seems to be uncommon at best)
* Fault tolerant (automatically recovers from crashes)
* Ransomware protection

> CloudBerry Backup detects encryption changes in files and prevents existing
> backups from being overwritten until an administrator confirms if there is an
> issue.

Arq backup describes this as:

> Ransomware protection - point-in-time recovery of files

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
* Local or Remote file store for pack storage

## Design

* Backend written in TypeScript, uses Apollo GraphQL server
* Front-end written in ReasonML, uses Apollo GraphQL client
* Desktop application written in TypeScript, uses Electron
* Document-oriented database is PouchDB, with PouchDB Server
* Storage implementations support local, SFTP, Google, Amazon, etc

## Use Cases

### Initial Setup

* Backend waits for initial setup via web or desktop
* Upon startup of desktop app, walk user through setup
* Offer choice of recovering from backup, or starting anew

### Regular Backup

* Backend performs backups according to configured schedule

## Data Format

### Overview

* Snapshots record what was saved and when
* Content is stored in what is essentially a hash tree
* Trees describe what files/folders are in the snapshot
    - Trees are nested, a la git
* File content is stored in pack files
    - Large files are split across multiple pack files
    - Small files are stored in whole
    - Pack files are used to minimize the number of files stored remotely
* Pack files are stored remotely
* PouchDB is used to store metadata
    - PouchDB database is saved just like any other file

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
* ULID contains the time, so no need for a timestamp
* UUID makes it easy to find buckets associated with this computer and user

### Pack Files

* contains raw file content
* default pack file size of 64MB
    - allow configuring pack file size, since it is inconsequential
    - restrict pack files to between 16 to 256 MB in size
* pack file format
    - pack starts with `P4CK`
    - version number (4 bytes)
    - number of entries (4 bytes)
    - entries:
        + byte length (4 bytes)
        + SHA256 checksum (32 bytes)
        + data
    - compressed using gzip, if the result is smaller
* encrypted pack file format
    - header: `C4PX` (rot13 of `P4CK`)
    - version number (4 bytes)
    - HMAC-SHA256 (32 bytes)
    - master init vector (16 bytes)
    - encrypted data init vector and session key (48 bytes)
    - encrypted pack data

### PouchDB

#### Primary Database

* configuration record
    - database key: `configuration`
    - host name
    - user name
    - computer UUID
    - peers: list of peer installations for chunk deduplication
    - default cloud service provider (e.g. `sftp`, `aws`, `gcp`)
    - default cloud storage type (e.g. "nearline")
    - default cloud service region (e.g. `us-west1`)
    - default sftp host and credentials, if any
    - default local path for local backup, if any
    - default local path to cloud service credentials file
    - default frequency with which to perform backups (hourly, daily, weekly, monthly)
    - default time ranges in which to upload snapshots
    - default preferred size of pack files (in MB)
    - default ignore file patterns (applies to all datasets)
    - list of datasets:
        + root local path
        + latest snapshot
        + schedule/frequency overrides
        + ignore overrides
        + pack size overrides
        + storage overrides (e.g. `local` vs `aws`)
* encryption record
    - database key: `encryption`
    - random salt (16 bytes)
    - random init vector (16 bytes)
    - HMAC-SHA256 of user password and salt
    - encrypted master keys
* snapshot records
    - key: `snapshot/` + SHA1 of snapshot (with "sha1-" prefix) or `index` for pending
    - parent: SHA1 of previous snapshot (`null` if first snapshot)
    - start_time: when snapshot started
    - end_time: when snapshot finished
    - num_files: number of files in this snapshot
    - root tree reference
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
    - remote bucket/vault name
    - remote archive identifier (e.g. AWS Glacier)
    - upload_date: date/time of successful upload, for conflict resolution

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
that has the most recent `upload_date`.

### Procedure

#### Backup

1. Check for a existing snapshot:
    1. If present but lacking `end_time` value, continue backup procedure
    1. Otherwise, generate the tree objects to represent the state of the dataset
1. Find the differences from the previous snapshot.
1. If there are no changes, delete snapshot record, exit the procedure.
1. Sync with any configured peers to get recent chunk updates.
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
1. Store latest snapshot identifier in `configuration` record.
1. Backup the PouchDB database files.

#### Duplicate chunk detection

The [datproject/rabin](https://github.com/datproject/rabin) project is ISC
licensed (according to the `package.json`) and is easy to use. It works for
small and large chunk sizes. However, it is slow compared to the FastCDC
implementation in
[ronomon/deduplication](https://github.com/ronomon/deduplication) which has the
advantage of being actively maintained. The drawback of this package is that a
working buffer of fairly large size must be allocated.

#### Uploading Packs

1. Select an existing bucket, or create a new one.
    * For Amazon, limited to 1,000 vaults, so reuse will be necessary.
    * For Google, no hard limit, but perhaps a practical limit.
    * Can add new records to the database (and index) to keep track.
1. Insert `pending` pack record in database to facilitate cleaning up botched backups.
    * e.g. if the backup crashes, and files changed, old pack will be left dangling forever
1. Upload the pack file to the cloud.
1. Update pack record to track bucket/vault and object/archive ID.
1. Remove `pending` from pack record in database.

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
1. Retrieve the most recent PouchDB database
    * For Glacier, this means listing archives of master vault to find database pack
1. Iterate entries in database, fetching packs and extracting files

#### Garbage Collection

* Automatic garbage collection:
    - Remove tree and file records that are no longer referenced
    - Find pack files that are no longer referenced
    - Remove the pack files from the remote side, delete the record
    - Find old `pending` pack records and remove remote file
* Aggressive garbage collection
    - Retrieve pack files, remove stale entries, repack, upload
    - Remove the old pack files from the remote side
    - Update file records affected by repacking

#### Deleting Old Backups

Remove the snapshot record to be deleted, then garbage collect.

### Encryption

#### Master Password

* Need to prompt the user for their password when starting up
    - Once decrypted, hold the master keys in process state
* If available, use a "secret vault" provided by the OS
    - macOS Keychain
    - Windows Data Protection API
    - Linux gnome-keyring
* If an environment variable is set, can use that
    - c.f. https://forum.duplicacy.com/t/passwords-credentials-and-environment-variables/1094

#### Generating Encryption Data

1. Generate a random salt.
1. Generate a random initialization vector (IV).
1. Generate two random "master keys".
1. Derive encryption key from user provided password and the salt.
1. Encrypt the master keys with AES/CTR using the the derived key and the IV.
1. Calculate the HMAC-SHA256 of (IV + encrypted master keys) using the derived key.
1. Store everything in the PouchDB encryption record.

#### Extracting Master Keys

1. Retrieve salt from the encryption record.
1. Derive encryption key from user-supplied password using scrypt and the salt.
1. Calculate HMAC-SHA256 of (IV + encrypted master keys) the key.
1. Verify computed HMAC against HMAC-SHA256 in the encryption record.
1. Decrypt the encrypted master keys using the derived key.

#### Encrypting Pack Files

1. Generate a random session key.
1. Generate a random "data IV".
1. Encrypt pack data with AES/CTR using session key and data IV.
1. Generate a random "master IV".
1. Encrypt (data IV + session key) with AES/CTR using the first master key and the "master IV".
1. Calculate HMAC-SHA256 of (master IV + "encrypted data IV + session key" + ciphertext) using the second "master key".
1. Write as described in the pack file data format.

#### Decrypting Pack Files

1. Calculate HMAC-SHA256 of (master IV + "encrypted data IV + session key" + ciphertext) using the second "master key".
1. Ensure the calculated HMAC-SHA256 matches the value in the object header.
1. Decrypt "encrypted data IV + session key" using the first "master key" and the "master IV".
1. Decrypt the ciphertext with AES/CTR using the session key and data IV.

## Alternatives

### Arq

* https://www.arqbackup.com
* Mac and Windows apps
* Uses a single master password
* Supports numerous backends

### Attic

* https://attic-backup.org
* Open source, development stopped in 2015
* Seems to use an old chunking algorithm
* Only supports SSH remote host

### CloudBerry

* Has enterprise versions
* https://www.cloudberrylab.com/backup/desktop/windows.aspx
* Windows-only

### Duplicacy

* https://github.com/gilbertchen/duplicacy
* Lists other open source tools and compares them
* Deduplicates chunks across systems
* Does not support Glacier
    - Their design depends on accessing chunks by their checksum

### JungleDisk

* https://www.jungledisk.com/encrypted-backups/
* Primarily business oriented
* Seems to rely on their servers
* Probably stores data elsewhere

### qBackup

* https://www.qualeed.com/en/qbackup/
* Mac, Linux, Windows
* Supports numerous backends
* Has copious documentation with screen shots

### restic

* https://restic.net
* Open source command line tool
* Supports numerous backends

### tarsnap

* https://www.tarsnap.com
* Free client
* Uses public key encryption rather than a password
* Stores data in Amazon S3
* Relies on tarsnap servers
* 10x the price of Google Cloud or Amazon Glacier
