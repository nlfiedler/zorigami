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
bun run codegen
bunx vite build
RUST_LOG=info cargo run
```

By default the server will be running on port `3000` on localhost.

### Docker

[Docker](https://www.docker.com) is used for testing some features of the application, such as the various remote pack stores. A Docker Compose file is located in the `containers` directory, which describes the services used for testing. With the services running, and an appropriately configured `.env` file in the base directory, the tests will leverage the services.

### Changing the GraphQL schema

After making changes to the `graphql.rs` source, the generated schema definition language (SDL) file and generated TypeScript file need to be updated.

```shell
env GENERATE_SDL=public/schema.graphql cargo run
# use ctrl-c to terminate the server
bun run codegen
```

### Testing Restore

To test the restore functionality without actually processing the queue, set the `RESTORE_ALWAYS_PENDING` environment variable to a non-null value. This will allow for enqueuing restore requests and then developing and testing the interface and GraphQL queries related to restore requests, lest they be processed too quickly.

```shell
env RESTORE_ALWAYS_PENDING=1 RUST_LOG=info cargo run
```

Similarly, to have restore requests stuck in the _processing_ state indefinitely, set `RESTORE_ALWAYS_PROCESSING` -- a single restore request will move from _pending_ to _processing_ and stay there. This will block all other processing of requests, allowing for the testing of conditions in which there are both pending requests and a request that is being processed.

```shell
env RESTORE_ALWAYS_PROCESSING=1 RUST_LOG=info cargo run
```

### Code Coverage

Using [grcov](https://github.com/mozilla/grcov) seems to be the easiest at this time.

```shell
cargo install grcov
rustup component add llvm-tools
export RUSTFLAGS="-Cinstrument-coverage"
export LLVM_PROFILE_FILE="zorigami-%p-%m.profraw"
cargo clean
cargo build
cargo test
grcov . -s . --binary-path ./target/debug/ -t html --branch --ignore-not-existing -o ./target/debug/coverage/
open target/debug/coverage/index.html
```

## Tools

### Finding Outdated Crates

Use https://github.com/kbknapp/cargo-outdated and run `cargo outdated`

## Origin of the name

A zorigami is a clock possessed by a spirit, as described on the [Wikipedia](https://en.wikipedia.org/wiki/Tsukumogami) page about Tsukumogami, which includes zorigami. This has nothing at all do with this application, accept maybe for the association with time.
