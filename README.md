# zorigami

A backup and restore application written in [Rust](https://www.rust-lang.org) and [SolidJS](https://www.solidjs.com) with a [GraphQL](https://graphql.org) wire protocol. Metadata is stored in [RocksDB](https://rocksdb.org) and file content is stored in encrypted packs using the [EXAF](https://github.com/nlfiedler/exaf-rs) archive file format.

## Features

* Can backup any number of files of any size
* Maintains multiple versions of files, not only the most recent
* Deduplication of files as well as content-defined chunking of large files
* All remotely stored data is encrypted using AES256-GCM [AEAD](https://en.wikipedia.org/wiki/Authenticated_encryption)
* Supports several remote stores: Amazon, Azure, Google, MinIO, SFTP
* Restore entire directory tree as well as individual files
* Backups can be stored both locally and remotely
* Backups can be run manually, hourly, or daily at a set time of day
* Cross Platform: Linux, macOS, Windows

## Shortcomings

This project has been a work-in-progress since 2014, originally started as [akashita](https://github.com/nlfiedler/akashita). Despite all that time and the gradual progress, there are several notable shortcomings:

* This software is provided "as-is", WITHOUT WARRANTY OF ANY KIND, see the `LICENSE` for details.
* The database schema and pack file formats have changed radically over the years. It may happen again.
* No automatic pruning of the snapshots (yet) which would keep the database size to a reasonable limit.
* Object-to-bucket allocation is very primitive and needs improvement.
* Restoring to dissimilar hardware is not (yet) an easy task.
* No readily available binaries, so you will need to build and deploy it yourself.
* No QoS control on uploads over the network so a backup can slow everything else to a crawl.
* Backup procedure operates file-by-file (not _point-in-time_) and hence may record changed content incorrectly or inconsistently (such as with database files).

## Building and Testing

### Prerequisites

- [Rust](https://www.rust-lang.org) stable, 2024 edition
- [Bun](https://bun.com)

### Initial Setup

### Building and Testing the Backend

```shell
cargo update
cargo build
cargo test
```

To build or run tests for a single package, use the `-p` option, like so:

```shell
cargo build -p store_minio
cargo test -p store_minio
```

### Building the Frontend, Starting Everything

```shell
bunx vite build
RUST_LOG=info cargo run
```

### Docker

[Docker](https://www.docker.com) is used for testing some features of the application, such as the various remote pack stores. A Docker Compose file is located in the `containers` directory, which describes the services used for testing. With the services running, and an appropriately configured `.env` file in the base directory, the tests will leverage the services.

## Tools

### Finding Outdated Crates

Use https://github.com/kbknapp/cargo-outdated and run `cargo outdated`

## Origin of the name

A zorigami is a clock possessed by a spirit, as described on the [Wikipedia](https://en.wikipedia.org/wiki/Tsukumogami) page about Tsukumogami, which includes zorigami. This has nothing at all do with this application, accept maybe for the association with time.
