# Architecture, Design, Data Format

## Architecture

* GraphQL and REST backend
* Browser-based frontend
* Key/Value store for metadata
* Local/Remote storage for pack files

## Design

### Clean Architecture

Within the `server` crate the general design of the application conforms to the [Clean Architecture](https://blog.cleancoder.com/uncle-bob/2012/08/13/the-clean-architecture.html) in which the application is divided into three layers: domain, data, and presentation. The domain layer defines the "policy" or business logic of the application, consisting of entities, use cases, and repositories. The data layer is the interface to the underlying system, defining the data models that are ultimately stored in a database. The presentation layer is what the user generally sees, the web interface, and to some extent, the GraphQL interface.

### Workspace and Packages

The overall application is broken into several crates, or packages as they are sometimes called. The principle benefit of this is that the overall scale of the application is easier to manage as it grows. In particular, the dependencies and build time steadily grow as more store backends are added. These pack store implementations, and the database implementation as well, are in packages that are kept separate from the main `server` package. In theory this will help with build times for the crates, as each crate forms a single compilation unit in Rust, meaning that a change to any file within that crate will result in the entire crate being compiled again. Note also that the dependencies among crates form a directed acyclic graph, meaning there can be no mutual dependencies.

## Data Format

### Overview

* Snapshots record what was saved and when
* Files are indexed in what is essentially a hash tree
* Trees describe what files and folders are in the snapshot
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
| ULID | Universally unique Lexicographically sortable IDentifier |
| XID  | sortable 12-byte unique identifier                       |

### Computer UUID

Used to generate unique bucket names and facilitate discovering database backups without an existing database.

The computer UUID is a type 5 using the URL namespace comprised of the host name of the computer and the name of the current user. The result will be adequately unique for the purpose of generating a bucket name for storing the database backups. The individual pack store implementations will resolve any bucket name conflicts.

### Bucket Names

The packs are stored in buckets whose names are described below.

* bucket name will be ULID + computer UUID (without dash separators)
    - consists of lowercase letters and numbers only
    - conforms to Google bucket name restrictions
        + https://cloud.google.com/storage/docs/naming
        + should be sufficiently unique despite global bucket namespace
    - conforms to Amazon Glacier vault name restrictions
        + https://docs.aws.amazon.com/amazonglacier/latest/dev/creating-vaults.html
    - conforms to Amazon S3 bucket name restrictions
        + https://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html
    - conforms to Azure blob storage name restrictions
        + https://learn.microsoft.com/en-us/rest/api/storageservices/naming-and-referencing-containers--blobs--and-metadata
* ULID contains the date/time
* UUID makes it easy to find buckets associated with this computer
* UUID may change after (re)installation and this is okay
    - Pack records contain the fully-qualified locations regardless of UUID
* Database snapshots saved to bucket whose name is the computer UUID
    - Cloud-based pack stores handle bucket collision using remote database
        + Amazon pack store uses DynamoDB, Google uses Firestore

### Pack Files

* Compressed and encrypted archive containing raw file content
* Default pack file size of 64MB
* Pack file format is [EXAF](https://github.com/nlfiedler/exaf-rs)
    - entry names are the chunk hash digest plus algorithm prefix
    - encrypted with key derived from passphrase and random salt

### Database Schema

The database is a key/value store provided by [RocksDB](https://rocksdb.org). The records are all stored using [CBOR](https://cbor.io) unless noted otherwise. The records consist primarily of key/value pairs with abbreviated names to minimize storage use. Each record key has a prefix that indicates what type of record it is, such as `chunk/` for chunk records.

* configuration record
    - key: `configuration`
    - host name
    - user name
    - computer UUID
* dataset records:
    - key: `dataset/` + XID
    - base path
    - schedule
    - excludes
    - pack size
    - stores
    - latest snapshot
    - retention policy
* store records:
    - key: `store/` + XID
    - store type
    - label
    - properties[]
        + name
        + value
    - retention policy
* snapshot records
    - key: `snapshot/` + SHA1
    - digest of previous snapshot
    - date/time when started
    - date/time when finished
    - file counts
    - base tree reference
* tree records
    - key: `tree/` + SHA1
    - entries: (sorted by name)
        + entry name
        + unix mode
        + user, group (strings)
        + uid, gid (numbers)
        + ctime, mtime
        + reference
            - SHA1 for tree
            - BLAKE3 for file
            - raw bytes for symlink
            - raw bytes for very small file
        + xattrs[]
            - name
            - xattr digest
* file records
    - key: `file/` + BLAKE3
    - file size
    - chunks:
        + offset
        + digest
* extended attribute records
    - key: `xattr/` + SHA1
    - value: raw attribute data
* chunk records
    - key: `chunk/` + BLAKE3
    - chunk size
    - pack digest
* pack records
    - key: `pack/` + BLAKE3
    - coordinates:
        + store
        + bucket
        + object
    - upload date/time
* database snapshots
    - key: `dbase/` + BLAKE3
    - coordinates:
        + store
        + bucket
        + object
    - upload date/time

## Further Details

### Deduplication

Files larger than the desired chunk size are broken up using a content-defined chunking algorithm to determine suitable chunk boundaries. Each chunk is stored once based on the BLAKE3 digest of the chunk. The CDC algorithm is [FastCDC](https://crates.io/crates/fastcdc).

### Database Snapshots

Database files are copied to an off-line archive using RocksDB functionality, then that directory structure is written to a compressed archive and uploaded to the pack store in the special bucket.

### Bucket Collision

Generated bucket names are random and long but collisions with existing buckets owned by other accounts can still happen. As a result, the pack repository will generate a new name and try again. The updated bucket name is returned as the _pack location_ that is stored in the database.

For the database backups, the bucket rename solution is different, and relies on the cloud service provider to offer a per-account database of some sort. When a bucket collision occurs when storing the database snapshot, the pack source (not repository) will use the cloud services to record a new name, and use that name each time thereafter. This is not possible for all pack sources, such as MinIO, which does not offer other services. It is assumed that MinIO, as well as the local and SFTP pack sources, can reliably use whatever bucket names they need.
