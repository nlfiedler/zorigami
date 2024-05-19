# NOTES

## Architecture

* HTTP backend as GraphQL server
* Web frontend as GraphQL client
* Key/Value store for metadata
* Local or Remote store for pack storage

## Design

### Clean Architecture

Within the `server` crate the general design of the application conforms to the [Clean Architecture](https://blog.cleancoder.com/uncle-bob/2012/08/13/the-clean-architecture.html) in which the application is divided into three layers: domain, data, and presentation. The domain layer defines the "policy" or business logic of the application, consisting of entities, use cases, and repositories. The data layer is the interface to the underlying system, defining the data models that are ultimately stored in a database. The presentation layer is what the user generally sees, the web interface, and to some extent, the GraphQL interface.

### Workspace and Packages

The overall application is broken into several crates, or packages as they are sometimes called. The principle benefit of this is that the overall scale of the application is easier to manage as it grows. In particular, the dependencies and build time steadily grow as more store backends are added. These pack store implementations, and the database implementation as well, are in packages that are kept separate from the main `server` package. In theory this will help with build times for the crates, as each crate forms a single compilation unit in Rust, meaning that a change to any file within that crate will result in the entire crate being compiled again. Note also that the dependencies among crates form a directed acyclic graph, meaning there can be no mutual dependencies.

## Implementation

* Backend written in Rust, uses Juniper GraphQL server
* Front-end written in Flutter, uses Zino & Co GraphQL client
* Key/Value store is RocksDB
* Pack storage supports local, Amazon, Azure, Google, MinIO, SFTP

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
    - Database is saved in special bucket

### Definitions

| Name | Description                                              |
| ---- | -------------------------------------------------------- |
| UUID | Universally Unique IDentifier                            |
| ULID | Universally Unique Lexicographically Sortable Identifier |
| XID  | sortable 12-byte unique identifier                       |

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
    - conforms to Azure blob storage name restrictions
        + https://learn.microsoft.com/en-us/rest/api/storageservices/naming-and-referencing-containers--blobs--and-metadata
* ULID contains the time, so no need for a timestamp
* UUID makes it easy to find buckets associated with this computer and user
* UUID may change after (re)installation and this is okay
    - Pack records contain the fully-qualified locations regardless of UUID
* Database snapshots saved to bucket whose name is the computer UUID
    - Cloud-based pack stores handle bucket collision using remote database
        + Amazon pack store uses DynamoDB, Google uses Firestore

### Pack Files

* Compressed archive containing raw file content
* Default pack file size of 64MB
* Pack file format is [EXAF](https://github.com/nlfiedler/exaf-rs)
    - entry names are the chunk hash digest plus prefix
    - encrypted with key derived from passphrase and random salt

### Database Schema

The database is a key/value store provided by [RocksDB](https://rocksdb.org). The records are all stored using [CBOR](https://cbor.io) unless noted otherwise, most likely JSON. The records consist of key/value pairs with abbreviated names to minimize storage use. Each record key has a prefix that indicates what type of record it is, such as `chunk/` for chunk records.

* configuration record
    - database key: `configuration`
    - host name
    - user name
    - computer UUID
    - peers: list of peer installations for chunk deduplication
* dataset records:
    - key: `dataset/` + XID
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
    - key: `store/` + XID
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
        + reference (SHA1 for tree, BLAKE3 for file, base64-encoded value for symlink)
        + entry name
* file records
    - key: `file/` + BLAKE3 at time of snapshot
    - length: size of file in bytes
    - chunks:
        + offset: file position for this chunk
        + digest: chunk BLAKE3 (or pack if only one chunk)
* database snapshots (identical to pack records)
    - key: `dbase/` + BLAKE3 of pack file (with "blake3-" prefix)
    - coordinates:
        + store XID
        + remote bucket/vault name
        + remote object/archive name
* extended attribute records
    - key: `xattr/` + SHA1 of the attribute value
    - value: attribute data as a Buffer
* chunk records
    - key: `chunk/` + BLAKE3 of chunk (with "blake3-" prefix)
    - size of chunk in bytes
    - BLAKE3 of pack
* pack records
    - key: `pack/` + BLAKE3 of pack file (with "blake3-" prefix)
    - coordinates:
        + store XID
        + remote bucket/vault name
        + remote object/archive name

## More Implementation Details

### Deduplication

Uses a content-defined chunking (a.k.a. content-dependent chunking) algorithm to
determine suitable chunk boundaries, and stores each unique chunk once based on
the BLAKE3 digest of the chunk. In particular, uses
[FastCDC](https://crates.io/crates/fastcdc)
which is much faster than Rabin-based fingerprinting, and somewhat faster than
Gear. This avoids the shortcomings of fixed-size chunking due to boundary
shifting.

### Database Snapshots

Database files are copied to an off-line archive using RocksDB functionality, then that directory structure is written to a compressed archive and uploaded to the pack store in the special bucket.

### Procedure Details

#### Backup

1. Sync with any configured peers to get recent chunk updates.
1. Check for an existing snapshot:
    1. If present but lacking `end_time` value, continue backup procedure
    1. Otherwise, generate `tree` objects to record the dataset state
1. Compare the root `tree` with the previous snapshot.
    1. If there are no changes, exit the procedure.
1. For each new/changed file, check if record exists; if not:
    1. For small files, treat as a single chunk
    1. For large files, use CDC to find chunks
    1. For each chunk that does not exist in database, add to pack
    1. If pack file is large enough, upload to storage
    1. Add `chunk` and `pack` records upon successful pack storage
1. Set the `end_time` of snapshot record to indicate completion.
1. Store latest snapshot identifier as a `latest` record.
1. Backup the database files.

#### Uploading Packs

1. Select an existing bucket, or create a new one.
1. Upload the pack file to the cloud.
1. Update pack record to track remote coordinates.

#### Crash Recovery

If the latest snapshot is missing an end time, there is pending work to finish, in which case the backup will essentially resume the snapshot that was in progress.

#### File Restore

1. Use selected file BLAKE3 to find list of chunks.
1. For each chunk, look up the pack record to get bucket and object.
1. Download pack and verify checksum to detect corruption.
1. Extract the chunk of the file in the pack to a temporary file.
1. Repeat for each chunk of the file (finding pack, downloading, extracting).
1. Sort the chunks by the `offset` value from the file record.
1. Reproduce the original file from the downloaded chunks.
1. Apply ownership and mode values according to the tree object.

#### Full Recovery

_This is not yet implemented._

1. Find all buckets with type 5 UUID for a name
1. Fetch metadata object/archive for those buckets
1. Present the list to the user to choose which to recover
1. Retrieve the most recent database
    * For Glacier, this means listing archives of master vault to find database pack
1. Iterate entries in database, fetching packs and extracting files

#### Garbage Collection

_This is not yet implemented._

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

_This is not yet implemented._

Remove the snapshot record to be deleted, then garbage collect.

### Bucket Collision

Generated bucket names are random and long but collisions with existing buckets owned by other accounts can still happen. As a result, the pack repository will generate a new name and try again. The updated bucket name is returned as the _pack location_ that is stored in the database.

For the database backups, the bucket rename solution is different, and relies on the cloud service provider to offer a per-account database of some sort. When a bucket collision occurs when storing the database snapshot, the pack source (not repository) will use the cloud services to record a new name, and use that name each time thereafter. This is not possible for all pack sources, such as MinIO, which does not offer other services. It is assumed that MinIO, as well as the local and SFTP pack sources, can reliably use whatever bucket names they need.
